//! gRPC client transport configuration and connection utilities.
//!
//! This gear provides production-grade gRPC client configuration with:
//! - Configurable connect and RPC timeouts
//! - HTTP/2 keepalive settings for connection health
//! - Tracing spans around connection establishment
//!
//! **Note:** This gear handles both transport-level configuration and connection retries
//! ([`connect_with_retry`]). For RPC-level retry logic, see the [`crate::rpc_retry`] gear.

use std::time::Duration;

use secrecy::ExposeSecret;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::Instrument;

use crate::tls::{TlsGeneration, TlsPaths};

fn duration_to_i64_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Client-side mTLS settings for an outbound gRPC channel.
///
/// The cert/key/CA are read from the well-known [`TlsPaths`] at connect time
/// (so a rotation is picked up on the next connect), and `domain_name` is the
/// name verified against the server certificate's DNS/IP SAN — this is *not*
/// the SPIFFE URI SAN: rustls verifies DNS/IP names, not URI SANs, so the
/// server cert must carry a DNS/IP SAN matching the endpoint host (the SPIFFE
/// URI SAN rides along for identity only). In Profile 3 `domain_name` is the
/// service DNS name, e.g. `{gear}.{namespace}.svc.cluster.local`.
#[derive(Debug, Clone)]
pub struct ClientTlsSettings {
    /// Well-known cert/key/CA PEM paths.
    pub paths: TlsPaths,
    /// The DNS/IP name to verify against the server certificate (and use for SNI).
    pub domain_name: String,
}

/// Configuration for gRPC client transport stack.
///
/// This configuration controls transport-level settings such as timeouts and keepalive.
/// Retry-related fields (`max_retries`, `backoff_base_ms`, `backoff_factor`,
/// `max_backoff`) are stored here for convenience but are used by the
/// [`crate::rpc_retry`] gear, not by the transport layer.
#[derive(Debug, Clone)]
#[must_use]
pub struct GrpcClientConfig {
    /// Timeout for establishing the initial connection.
    pub connect_timeout: Duration,

    /// Timeout for individual RPC calls (applied at transport level).
    pub rpc_timeout: Duration,

    /// Maximum number of retry attempts.
    ///
    /// Used by both [`connect_with_retry`] (connection retries) and
    /// [`crate::rpc_retry::call_with_retry`] (RPC-call retries).
    pub max_retries: u32,

    /// Base fed to `ExponentialBackoff::from_millis` — the growth ratio between
    /// attempts (delay sequence `backoff_base_ms^n * backoff_factor`).
    ///
    /// Used by both [`connect_with_retry`] and [`crate::rpc_retry::call_with_retry`].
    pub backoff_base_ms: u64,

    /// Multiplicative factor applied to every backoff delay. With the defaults
    /// (base `2`, factor `50`) the schedule is 100ms, 200ms, 400ms, ….
    ///
    /// Used by both [`connect_with_retry`] and [`crate::rpc_retry::call_with_retry`].
    pub backoff_factor: u64,

    /// Strict upper bound on backoff duration; each delay is capped here before
    /// full jitter (which only reduces it) is applied.
    ///
    /// Used by both [`connect_with_retry`] and [`crate::rpc_retry::call_with_retry`].
    pub max_backoff: Duration,

    /// Service name for metrics and tracing.
    pub service_name: &'static str,

    /// Enable Prometheus metrics collection.
    pub enable_metrics: bool,

    /// Enable OpenTelemetry tracing.
    pub enable_tracing: bool,

    /// Client-side mTLS. `None` (the default) builds a plaintext channel — the
    /// backward-compatible behavior; `Some` makes the channel present a client
    /// certificate and verify the server against the configured CA.
    pub tls: Option<ClientTlsSettings>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(30),
            max_retries: 3,
            backoff_base_ms: 2,
            backoff_factor: 50,
            max_backoff: Duration::from_secs(5),
            service_name: "grpc_client",
            enable_metrics: true,
            enable_tracing: true,
            tls: None,
        }
    }
}

impl GrpcClientConfig {
    /// Create a new configuration with the given service name.
    pub fn new(service_name: &'static str) -> Self {
        Self {
            service_name,
            ..Default::default()
        }
    }

    /// Set the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the RPC timeout.
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    /// Set the maximum number of retries.
    ///
    /// This value is used by [`crate::rpc_retry::call_with_retry`].
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the [`ExponentialBackoff`](tokio_retry::strategy::ExponentialBackoff)
    /// base (growth ratio between delays).
    pub fn with_backoff_base_ms(mut self, base_ms: u64) -> Self {
        self.backoff_base_ms = base_ms;
        self
    }

    /// Set the multiplicative factor applied to every backoff delay.
    pub fn with_backoff_factor(mut self, factor: u64) -> Self {
        self.backoff_factor = factor;
        self
    }

    /// Set the strict upper bound on any single backoff delay.
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = duration;
        self
    }

    /// Disable metrics collection.
    pub fn without_metrics(mut self) -> Self {
        self.enable_metrics = false;
        self
    }

    /// Disable tracing.
    pub fn without_tracing(mut self) -> Self {
        self.enable_tracing = false;
        self
    }

    /// Enable client-side mTLS with material at the given well-known paths,
    /// verifying the server against `domain_name` (its DNS/IP SAN).
    pub fn with_tls(mut self, paths: TlsPaths, domain_name: impl Into<String>) -> Self {
        self.tls = Some(ClientTlsSettings {
            paths,
            domain_name: domain_name.into(),
        });
        self
    }
}

/// Build a tonic `Endpoint` with timeouts, keepalive, and — when
/// [`GrpcClientConfig::tls`] is set — client mTLS.
///
/// Configures:
/// - Connect timeout
/// - Per-RPC timeout
/// - TCP keepalive (30 seconds)
/// - HTTP/2 keepalive interval (30 seconds)
/// - Keepalive timeout (10 seconds)
/// - Keep alive while idle
/// - Client mTLS (present a client cert, verify the server against the CA and
///   `domain_name`) when `cfg.tls` is `Some`
///
/// Async because the mTLS material is read from the well-known paths at build
/// time, so a rotation is picked up on the next connect. The crypto provider is
/// the process-installed rustls default (installed by
/// `toolkit::bootstrap::init_crypto_provider`); tonic builds its `ClientConfig`
/// through it, so a FIPS build inherits the FIPS provider with no extra wiring.
///
/// # Errors
/// Returns an error if the URI is invalid or the TLS material cannot be read,
/// validated, or applied.
async fn build_endpoint(uri: String, cfg: &GrpcClientConfig) -> anyhow::Result<Endpoint> {
    use anyhow::Context as _;

    let mut endpoint = Endpoint::from_shared(uri)
        .context("invalid gRPC endpoint URI")?
        .connect_timeout(cfg.connect_timeout)
        .timeout(cfg.rpc_timeout)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);

    if let Some(tls) = &cfg.tls {
        let material = TlsGeneration::load(&tls.paths)
            .await
            .context("loading client mTLS material")?;
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(material.ca_pem()))
            .identity(Identity::from_pem(
                material.cert_pem(),
                material.key_pem().expose_secret(),
            ))
            .domain_name(tls.domain_name.clone());
        endpoint = endpoint
            .tls_config(client_tls)
            .context("applying client mTLS config")?;
    }

    Ok(endpoint)
}

/// Build a lazily-connecting [`Channel`] with the configured transport stack
/// (timeouts, keepalive, and optional client mTLS).
///
/// This is the framework-owned seam a gear SDK uses to obtain a channel without
/// touching cert material or owning a `tls_config` of its own: it passes a
/// [`GrpcClientConfig`] (optionally carrying [`ClientTlsSettings`]) and gets
/// back a ready channel. `connect_lazy` defers the TCP/TLS handshake to first
/// use, but the mTLS material is still read here (async) so a bad path fails
/// fast rather than at first RPC.
///
/// # Errors
/// Returns an error if the URI is invalid or the TLS material cannot be read,
/// validated, or applied.
pub async fn build_channel_lazy(
    uri: impl Into<String>,
    cfg: &GrpcClientConfig,
) -> anyhow::Result<Channel> {
    let endpoint = build_endpoint(uri.into(), cfg).await?;
    Ok(endpoint.connect_lazy())
}

/// Connect to a gRPC service with the configured transport stack.
///
/// This function establishes a connection with:
/// - Configurable connect and RPC timeouts
/// - HTTP/2 keepalive for connection health
/// - A tracing span around the connection attempt
///
/// **Note:** This function does **not** perform retries or backoff at the transport level.
/// For RPC-level retry logic, use [`crate::rpc_retry::call_with_retry`] after obtaining
/// a client from this function.
///
/// # Example
///
/// ```ignore
/// use toolkit_transport_grpc::client::{connect_with_stack, GrpcClientConfig};
/// use toolkit_transport_grpc::rpc_retry::{call_with_retry, RpcRetryConfig};
/// use std::sync::Arc;
///
/// let config = GrpcClientConfig::new("my_service");
/// let client: MyServiceClient<Channel> = connect_with_stack(
///     "http://localhost:50051",
///     &config
/// ).await?;
///
/// // For retries, use the rpc_retry gear:
/// let retry_cfg = Arc::new(RpcRetryConfig::from(&config));
/// let response = call_with_retry(
///     &mut client,
///     retry_cfg,
///     request,
///     |c, r| async move { c.my_method(r).await.map(|r| r.into_inner()) },
///     "my_service.my_method",
/// ).await?;
/// ```
///
/// # Errors
/// Returns an error if the connection cannot be established.
pub async fn connect_with_stack<TClient>(
    uri: impl Into<String>,
    cfg: &GrpcClientConfig,
) -> anyhow::Result<TClient>
where
    TClient: From<Channel>,
{
    let uri_string = uri.into();
    let span = tracing::debug_span!(
        "grpc_connect",
        service = cfg.service_name,
        uri = %uri_string
    );

    async move {
        let endpoint = build_endpoint(uri_string, cfg).await?;
        let channel = endpoint.connect().await?;

        if cfg.enable_tracing {
            let connect_timeout_ms = duration_to_i64_ms(cfg.connect_timeout);
            let rpc_timeout_ms = duration_to_i64_ms(cfg.rpc_timeout);
            tracing::info!(
                service_name = cfg.service_name,
                connect_timeout_ms,
                rpc_timeout_ms,
                "gRPC client connected"
            );
        }

        Ok(TClient::from(channel))
    }
    .instrument(span)
    .await
}

/// Connect to a gRPC service with retry logic using exponential backoff and jitter.
///
/// This function attempts to establish a connection and retries on failure
/// using the retry parameters from [`GrpcClientConfig`]:
/// - `max_retries`: Maximum number of retry attempts
/// - `backoff_base_ms` / `backoff_factor`: `ExponentialBackoff` base and factor
///   (defaults `2`/`50` → 100ms, 200ms, 400ms, …)
/// - `max_backoff`: Strict upper bound on backoff duration (each delay is capped here)
///
/// Full jitter is then applied to each delay to spread out concurrent retries.
///
/// # Example
///
/// ```ignore
/// use toolkit_transport_grpc::client::{connect_with_retry, GrpcClientConfig};
///
/// let config = GrpcClientConfig::new("my_service")
///     .with_max_retries(5);
///
/// let client: MyServiceClient<Channel> = connect_with_retry(
///     "http://localhost:50051",
///     &config
/// ).await?;
/// ```
///
/// # Errors
/// Returns an error if the connection fails after all retry attempts.
pub async fn connect_with_retry<TClient>(
    uri: impl Into<String>,
    cfg: &GrpcClientConfig,
) -> anyhow::Result<TClient>
where
    TClient: From<Channel>,
{
    use anyhow::Context as _;

    let uri_string = uri.into();
    let uri_ref: &str = uri_string.as_str();
    let mut attempt: u32 = 0;
    let action = || {
        attempt += 1;
        let this_attempt = attempt;
        async move {
            let res = connect_with_stack::<TClient>(uri_ref, cfg).await;
            // A failure that still has retries left mirrors the previous
            // per-attempt WARN; the final failure is logged once after the loop.
            if let Err(ref e) = res
                && this_attempt <= cfg.max_retries
            {
                tracing::warn!(
                    service = cfg.service_name,
                    attempt = this_attempt,
                    max_retries = cfg.max_retries,
                    error = %e,
                    "gRPC connection failed, retrying..."
                );
            }
            res
        }
    };

    // Exponential backoff capped at `max_backoff`, with full jitter, over
    // `max_retries` retries — the config fields map straight onto the strategy.
    //
    // BEHAVIORAL CHANGE: `jitter` is tokio-retry's *full* jitter — delay is
    // `computed * U(0, 1)`, so a wait can drop close to 0. The pre-tokio-retry
    // code used *additive* jitter (`computed * (1 + U(0, 0.25))`), which only
    // ever increased the delay above the computed backoff. Full jitter is the
    // generally recommended policy (better de-correlation of concurrent
    // retries), but it is a real change in retry timing, not a pure refactor.
    let strategy = ExponentialBackoff::from_millis(cfg.backoff_base_ms)
        .factor(cfg.backoff_factor)
        .max_delay(cfg.max_backoff)
        .map(jitter)
        .take(cfg.max_retries as usize);
    let result = Retry::start(strategy, action).await;

    match &result {
        Ok(_) if attempt > 1 => {
            tracing::info!(
                service = cfg.service_name,
                attempt,
                "gRPC connection established after retries"
            );
        }
        Err(e) => {
            tracing::error!(
                service = cfg.service_name,
                attempt,
                error = %e,
                "gRPC connection failed after all retries"
            );
        }
        Ok(_) => {}
    }

    result.with_context(|| {
        format!(
            "Failed to connect to {} after {} attempts",
            cfg.service_name, attempt
        )
    })
}

/// Simple connection helper without custom configuration.
///
/// Uses default configuration with the provided service name.
/// This only sets up the transport connection; retries and backoff
/// for RPC calls should be handled using [`crate::rpc_retry::call_with_retry`].
///
/// # Errors
/// Returns an error if the connection cannot be established.
pub async fn connect<TClient>(
    uri: impl Into<String>,
    service_name: &'static str,
) -> anyhow::Result<TClient>
where
    TClient: From<Channel>,
{
    let cfg = GrpcClientConfig::new(service_name);
    connect_with_stack(uri, &cfg).await
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = GrpcClientConfig::default();
        assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
        assert_eq!(cfg.rpc_timeout, Duration::from_secs(30));
        assert_eq!(cfg.max_retries, 3);
        assert!(cfg.enable_metrics);
        assert!(cfg.enable_tracing);
    }

    #[test]
    fn test_config_builder() {
        let cfg = GrpcClientConfig::new("test_service")
            .with_connect_timeout(Duration::from_secs(5))
            .with_rpc_timeout(Duration::from_secs(15))
            .with_max_retries(5)
            .without_metrics()
            .without_tracing();

        assert_eq!(cfg.service_name, "test_service");
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
        assert_eq!(cfg.rpc_timeout, Duration::from_secs(15));
        assert_eq!(cfg.max_retries, 5);
        assert!(!cfg.enable_metrics);
        assert!(!cfg.enable_tracing);
    }

    #[tokio::test]
    async fn test_build_endpoint_succeeds() {
        let cfg = GrpcClientConfig::default();
        let result = build_endpoint("http://localhost:50051".to_owned(), &cfg).await;
        assert!(
            result.is_ok(),
            "build_endpoint should succeed with valid URI"
        );
    }

    #[tokio::test]
    async fn test_build_endpoint_empty_uri() {
        let cfg = GrpcClientConfig::default();
        let result = build_endpoint(String::new(), &cfg).await;
        assert!(result.is_err(), "build_endpoint should fail with empty URI");
    }

    #[test]
    fn with_tls_sets_settings() {
        let cfg = GrpcClientConfig::new("svc").with_tls(
            TlsPaths {
                cert: "/x/tls.crt".into(),
                key: "/x/tls.key".into(),
                ca: "/x/ca.crt".into(),
            },
            "gear.ns.svc.cluster.local",
        );
        let tls = cfg.tls.expect("tls set");
        assert_eq!(tls.domain_name, "gear.ns.svc.cluster.local");
        assert_eq!(tls.paths.cert, std::path::PathBuf::from("/x/tls.crt"));
    }

    #[tokio::test]
    async fn build_endpoint_with_missing_tls_material_errors() {
        let cfg = GrpcClientConfig::new("svc").with_tls(
            TlsPaths {
                cert: "/nonexistent/tls.crt".into(),
                key: "/nonexistent/tls.key".into(),
                ca: "/nonexistent/ca.crt".into(),
            },
            "localhost",
        );
        let result = build_endpoint("https://localhost:50051".to_owned(), &cfg).await;
        assert!(
            result.is_err(),
            "TLS enabled with unreadable material must fail the build"
        );
    }

    // ---- end-to-end mTLS: the client seam against a real tonic TLS server ----

    use rcgen::{CertificateParams, Issuer, KeyPair, SanType};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
    use tonic::transport::{Certificate as TCert, Identity as TIdentity, Server, ServerTlsConfig};

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("clienttls-{tag}-{}-{n}", std::process::id()))
    }

    struct Pki {
        ca_pem: String,
        server_cert: String,
        server_key: String,
        client_cert: String,
        client_key: String,
    }

    /// CA + a server leaf (SAN DNS:localhost) + a client leaf, all signed by CA.
    fn make_pki() -> Pki {
        let ca_key = KeyPair::generate().unwrap();
        let ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();
        let issuer = Issuer::new(ca_params, ca_key);

        let leaf = |sans: Vec<SanType>| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = sans;
            let cert = params.signed_by(&key, &issuer).unwrap();
            (cert.pem(), key.serialize_pem())
        };
        let (server_cert, server_key) =
            leaf(vec![SanType::DnsName("localhost".try_into().unwrap())]);
        let (client_cert, client_key) =
            leaf(vec![SanType::DnsName("client.local".try_into().unwrap())]);
        Pki {
            ca_pem,
            server_cert,
            server_key,
            client_cert,
            client_key,
        }
    }

    // A no-op `()` codec so we can issue a unary RPC with no .proto codegen; we
    // only care whether the request traverses the mTLS channel to the server.
    #[derive(Default)]
    struct EmptyCodec;
    struct Noop;
    impl Codec for EmptyCodec {
        type Encode = ();
        type Decode = ();
        type Encoder = Noop;
        type Decoder = Noop;
        fn encoder(&mut self) -> Noop {
            Noop
        }
        fn decoder(&mut self) -> Noop {
            Noop
        }
    }
    impl Encoder for Noop {
        type Item = ();
        type Error = tonic::Status;
        fn encode(&mut self, _item: (), _dst: &mut EncodeBuf<'_>) -> Result<(), tonic::Status> {
            Ok(())
        }
    }
    impl Decoder for Noop {
        type Item = ();
        type Error = tonic::Status;
        fn decode(&mut self, _: &mut DecodeBuf<'_>) -> Result<Option<()>, tonic::Status> {
            Ok(Some(()))
        }
    }

    /// Write the client's cert/key/ca to files and return a TLS-enabled config.
    async fn client_cfg(dir: &std::path::Path, pki: &Pki) -> GrpcClientConfig {
        tokio::fs::create_dir_all(dir).await.unwrap();
        let paths = TlsPaths {
            cert: dir.join("tls.crt"),
            key: dir.join("tls.key"),
            ca: dir.join("ca.crt"),
        };
        tokio::fs::write(&paths.cert, &pki.client_cert)
            .await
            .unwrap();
        tokio::fs::write(&paths.key, &pki.client_key).await.unwrap();
        tokio::fs::write(&paths.ca, &pki.ca_pem).await.unwrap();
        GrpcClientConfig::new("test").with_tls(paths, "localhost")
    }

    /// Spawn a tonic server terminating mTLS (server identity + client CA) on an
    /// ephemeral port; returns (addr, shutdown, join).
    async fn spawn_mtls_server(
        pki: &Pki,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server_tls = ServerTlsConfig::new()
            .identity(TIdentity::from_pem(&pki.server_cert, &pki.server_key))
            .client_ca_root(TCert::from_pem(&pki.ca_pem));
        let handle = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls)
                .expect("server tls")
                .add_routes(tonic::service::Routes::default())
                .serve_with_shutdown(addr, async {
                    rx.await.ok();
                })
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        (addr, tx, handle)
    }

    /// Issue one unary RPC over `channel`; `true` if it reached the server
    /// (empty router answers `Unimplemented` — proof the mTLS channel is up).
    async fn rpc_reaches_server(channel: Channel) -> bool {
        let mut grpc = tonic::client::Grpc::new(channel);
        if grpc.ready().await.is_err() {
            return false;
        }
        let path = http::uri::PathAndQuery::from_static("/probe.Svc/Probe");
        match grpc
            .unary::<(), (), _>(tonic::Request::new(()), path, EmptyCodec)
            .await
        {
            Ok(_) => true,
            Err(status) => status.code() == tonic::Code::Unimplemented,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_client_reaches_server_through_the_seam() {
        let pki = make_pki();
        let (addr, tx, handle) = spawn_mtls_server(&pki).await;
        let dir = unique_dir("ok");
        let cfg = client_cfg(&dir, &pki).await;

        let channel = build_channel_lazy(format!("https://localhost:{}", addr.port()), &cfg)
            .await
            .expect("channel built");
        assert!(
            rpc_reaches_server(channel).await,
            "an mTLS client built through the seam should reach the server"
        );

        tx.send(()).ok();
        handle.await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_rejects_server_signed_by_an_untrusted_ca() {
        let pki = make_pki();
        let (addr, tx, handle) = spawn_mtls_server(&pki).await;
        // Client trusts a DIFFERENT CA than the one that signed the server cert.
        let other = make_pki();
        let dir = unique_dir("badca");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let paths = TlsPaths {
            cert: dir.join("tls.crt"),
            key: dir.join("tls.key"),
            ca: dir.join("ca.crt"),
        };
        tokio::fs::write(&paths.cert, &pki.client_cert)
            .await
            .unwrap();
        tokio::fs::write(&paths.key, &pki.client_key).await.unwrap();
        tokio::fs::write(&paths.ca, &other.ca_pem).await.unwrap();
        let cfg = GrpcClientConfig::new("test").with_tls(paths, "localhost");

        // Server-cert verification is client-side, so it fails at connect().
        let channel = build_channel_lazy(format!("https://localhost:{}", addr.port()), &cfg)
            .await
            .expect("channel builds lazily");
        assert!(
            !rpc_reaches_server(channel).await,
            "a server cert signed by an untrusted CA must be rejected"
        );

        tx.send(()).ok();
        handle.await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
