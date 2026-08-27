//! mTLS material for the gRPC transport, loaded from well-known PEM paths and
//! refreshed in place so a long-lived process picks up cert-manager / SPIRE
//! rotation without a restart.
//!
//! Per ADR-0006 the out-of-process runtime consumes rotated certs from a well-known path
//! (Profile 2: a host file; Profile 3: a projected volume). This reader mirrors
//! [`ServiceAccountTokenReader`](crate::sa_token::ServiceAccountTokenReader): it
//! **re-reads on a bounded interval** rather than subscribing to filesystem
//! events, so it needs no `notify` dependency and covers both the host-file and
//! projected-volume cases with one code path.
//!
//! A [`TlsGeneration`] bundles the three PEM artifacts — the leaf cert chain,
//! the private key, and the platform CA — as a single unit. The reader only
//! ever swaps a **fully validated** generation into place ([`arc_swap`]), so a
//! consumer never observes a torn mix of an old key with a new cert, and a bad
//! read (unreadable, empty, or malformed PEM) is logged and **ignored**, keeping
//! the last-known-good material live rather than breaking the next handshake.
//!
//! The reader does not build [`tonic`](https://docs.rs/tonic) TLS configs
//! itself; that is the server's / client's job. It exposes the validated PEM
//! plus a [`watch`](tokio::sync::watch) generation counter
//! ([`subscribe`](TlsMaterialReader::subscribe)) so those layers can rebuild
//! their `ServerTlsConfig` / channel when — and only when — the material
//! actually changes.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Default cadence for re-reading the cert paths. Certificate rotation is
/// infrequent (cert-manager renews well before expiry) and a re-read is cheap,
/// so a coarse interval keeps overhead negligible while still picking up a
/// rotation within roughly one interval. Matches the order of magnitude of
/// [`crate::sa_token::DEFAULT_REFRESH_INTERVAL`].
pub const DEFAULT_TLS_REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// The well-known PEM paths for a workload's mTLS material. Mirrors the field
/// shape of `toolkit_security`'s `InternalCredential::MtlsIdentity`, so the
/// outbound credential maps onto it one-to-one.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    /// Leaf certificate chain, PEM, leaf-first.
    pub cert: PathBuf,
    /// Private key, PEM (PKCS#8 / PKCS#1 / SEC1).
    pub key: PathBuf,
    /// Platform CA used to verify the peer, PEM.
    pub ca: PathBuf,
}

/// A single, fully-validated generation of mTLS material.
///
/// The private key is held in a [`SecretString`] so it is redacted in `Debug`
/// output and zeroized on drop; the certificate and CA are public material.
#[derive(Clone)]
pub struct TlsGeneration {
    cert: Arc<str>,
    key: SecretString,
    ca: Arc<str>,
}

impl TlsGeneration {
    /// The leaf certificate chain, PEM, leaf-first.
    #[must_use]
    pub fn cert_pem(&self) -> &str {
        &self.cert
    }

    /// The platform CA certificate(s), PEM.
    #[must_use]
    pub fn ca_pem(&self) -> &str {
        &self.ca
    }

    /// The private key, PEM. The secret is exposed only at the call site that
    /// hands it to the TLS-config builder; it never escapes as a plain `String`
    /// through this type's `Debug`/`Clone`.
    #[must_use]
    pub fn key_pem(&self) -> &SecretString {
        &self.key
    }

    /// Load and validate the cert/key/CA PEM triple from `paths` once.
    ///
    /// This is the one-shot path a server or client uses to build its TLS
    /// config at startup. [`TlsMaterialReader`] wraps the same load to keep the
    /// material refreshed for live rotation.
    ///
    /// # Errors
    /// Returns [`TlsMaterialError`] if any path cannot be read, is empty, or
    /// does not parse as valid PEM.
    pub async fn load(paths: &TlsPaths) -> Result<Self, TlsMaterialError> {
        read_and_validate(paths).await
    }

    /// Content equality across a reload: two generations are equal when all
    /// three PEM artifacts match byte-for-byte. Used to suppress a no-op swap
    /// (and its rebuild signal) when a re-read returns unchanged material.
    fn content_eq(&self, other: &Self) -> bool {
        self.cert == other.cert
            && self.ca == other.ca
            && self.key.expose_secret() == other.key.expose_secret()
    }
}

impl fmt::Debug for TlsGeneration {
    /// Never prints PEM contents — the key is secret and the certs are noise;
    /// report byte lengths and a redaction marker instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsGeneration")
            .field("cert_len", &self.cert.len())
            .field("ca_len", &self.ca.len())
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Errors from loading mTLS material off the well-known paths.
#[derive(Debug, thiserror::Error)]
pub enum TlsMaterialError {
    /// A PEM file could not be read from disk.
    #[error("failed to read TLS material at {path}")]
    Read {
        /// The path that failed to read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A PEM file was present but empty (or whitespace only).
    #[error("TLS material at {path} is empty")]
    Empty {
        /// The path that was empty.
        path: PathBuf,
    },
    /// A **public** PEM artifact (certificate or CA) did not parse. The parse
    /// error is preserved as `source` for diagnostics — safe because cert/CA
    /// material is public. (The private key uses [`Self::InvalidKey`], which
    /// deliberately carries no source.)
    #[error("TLS material at {path} is not valid PEM ({kind})")]
    InvalidPem {
        /// The path that failed to parse.
        path: PathBuf,
        /// What the artifact was expected to be (`certificate` / `CA certificate`).
        kind: &'static str,
        /// The underlying PEM decode error.
        #[source]
        source: rustls_pki_types::pem::Error,
    },
    /// The private-key PEM did not parse. The underlying parse error is
    /// **deliberately not carried** as a `source`: its `Display` can render
    /// bytes of the offending line, and for the private key a fragment must
    /// never be able to reach a log via the error chain.
    #[error("TLS private key at {path} is not valid PEM")]
    InvalidKey {
        /// The path whose private key failed to parse.
        path: PathBuf,
    },
    /// A certificate PEM parsed but contained no certificate blocks.
    #[error("TLS material at {path} contains no {kind}")]
    NoCertificate {
        /// The path with no certificate blocks.
        path: PathBuf,
        /// Which certificate artifact was empty (`certificate`, `CA certificate`).
        kind: &'static str,
    },
}

/// Reads a workload's mTLS PEM triple and keeps a validated, atomically
/// swappable copy in memory so the server / client always build against the
/// current (post-rotation) material.
///
/// Dropping the reader stops the background refresh task at the next tick: the
/// task holds a [`Weak`] handle to the shared state and exits once the reader
/// (the sole strong holder) is gone.
#[derive(Debug)]
pub struct TlsMaterialReader {
    paths: TlsPaths,
    current: Arc<ArcSwap<TlsGeneration>>,
    /// Generation counter; ticks each time a *changed* generation is swapped in.
    generation: watch::Sender<u64>,
}

impl TlsMaterialReader {
    /// Load the material once and spawn a background task that re-reads it every
    /// [`DEFAULT_TLS_REFRESH_INTERVAL`].
    ///
    /// # Errors
    /// Returns [`TlsMaterialError`] if any of the three paths cannot be read, is
    /// empty, or does not parse as valid PEM on the initial load.
    pub async fn new(paths: TlsPaths) -> Result<Self, TlsMaterialError> {
        Self::with_refresh_interval(paths, DEFAULT_TLS_REFRESH_INTERVAL, None).await
    }

    /// Like [`new`](Self::new) but ties the refresh task to a
    /// [`CancellationToken`] so it stops promptly on shutdown instead of
    /// lingering for up to a full interval.
    ///
    /// # Errors
    /// See [`new`](Self::new).
    pub async fn with_cancellation(
        paths: TlsPaths,
        cancel: CancellationToken,
    ) -> Result<Self, TlsMaterialError> {
        Self::with_refresh_interval(paths, DEFAULT_TLS_REFRESH_INTERVAL, Some(cancel)).await
    }

    /// Like [`new`](Self::new) but with a custom re-read `interval` and optional
    /// cancellation. Primarily for tests and the bootstrap layer.
    ///
    /// # Errors
    /// See [`new`](Self::new).
    pub async fn with_refresh_interval(
        paths: TlsPaths,
        interval: Duration,
        cancel: Option<CancellationToken>,
    ) -> Result<Self, TlsMaterialError> {
        let generation = read_and_validate(&paths).await?;
        let current = Arc::new(ArcSwap::from_pointee(generation));
        let (tx, _rx) = watch::channel(0u64);

        spawn_refresh_task(
            paths.clone(),
            Arc::downgrade(&current),
            tx.clone(),
            interval,
            cancel,
        );

        Ok(Self {
            paths,
            current,
            generation: tx,
        })
    }

    /// The well-known paths this reader watches.
    #[must_use]
    pub fn paths(&self) -> &TlsPaths {
        &self.paths
    }

    /// Snapshot the current validated generation.
    #[must_use]
    pub fn current(&self) -> Arc<TlsGeneration> {
        self.current.load_full()
    }

    /// Subscribe to the generation counter. The value increments each time a
    /// *changed* generation is swapped in (an unchanged re-read does not tick),
    /// so the server / client rebuild their TLS config only on a real rotation.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }
}

/// Spawn the interval re-read loop. Terminates when `current` can no longer be
/// upgraded (the reader has been dropped) or, when a [`CancellationToken`] is
/// supplied, as soon as it is cancelled.
fn spawn_refresh_task(
    paths: TlsPaths,
    current: Weak<ArcSwap<TlsGeneration>>,
    generation: watch::Sender<u64>,
    interval: Duration,
    cancel: Option<CancellationToken>,
) {
    tokio::spawn(async move {
        loop {
            match &cancel {
                Some(token) => {
                    tokio::select! {
                        () = tokio::time::sleep(interval) => {}
                        () = token.cancelled() => break,
                    }
                }
                None => tokio::time::sleep(interval).await,
            }
            let Some(current) = current.upgrade() else {
                break;
            };
            reload_once(&paths, &current, &generation).await;
        }
    });
}

/// Perform one re-read: on success, swap in only if the material changed and
/// tick the generation counter; on failure, keep the previous generation.
async fn reload_once(
    paths: &TlsPaths,
    current: &ArcSwap<TlsGeneration>,
    generation: &watch::Sender<u64>,
) {
    match read_and_validate(paths).await {
        Ok(next) => {
            if current.load().content_eq(&next) {
                return;
            }
            current.store(Arc::new(next));
            // Read the current counter into an owned value FIRST so the
            // `borrow()` read guard is dropped before `send_replace` takes the
            // write lock — holding both in one statement self-deadlocks the
            // task. `send_replace` tolerates zero receivers (unlike `send`), so
            // no meaningful `Result` is discarded.
            let next_generation = generation.borrow().wrapping_add(1);
            generation.send_replace(next_generation);
            tracing::info!(cert = %paths.cert.display(), "reloaded rotated TLS material");
        }
        Err(e) => {
            tracing::warn!(
                cert = %paths.cert.display(),
                error = %e,
                "failed to reload TLS material; keeping previous generation",
            );
        }
    }
}

/// Read and validate the three PEM artifacts into a [`TlsGeneration`].
async fn read_and_validate(paths: &TlsPaths) -> Result<TlsGeneration, TlsMaterialError> {
    let cert = read_nonempty(&paths.cert).await?;
    let key = read_nonempty(&paths.key).await?;
    let ca = read_nonempty(&paths.ca).await?;

    validate_certs(&paths.cert, &cert, "certificate")?;
    validate_key(&paths.key, &key)?;
    validate_certs(&paths.ca, &ca, "CA certificate")?;

    Ok(TlsGeneration {
        cert: Arc::from(cert),
        key: SecretString::from(key),
        ca: Arc::from(ca),
    })
}

/// Read a file to a string, erroring if it is missing/unreadable or empty.
async fn read_nonempty(path: &Path) -> Result<String, TlsMaterialError> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| TlsMaterialError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    if raw.trim().is_empty() {
        return Err(TlsMaterialError::Empty {
            path: path.to_path_buf(),
        });
    }
    Ok(raw)
}

/// Validate that `pem` parses as one or more X.509 certificates.
fn validate_certs(path: &Path, pem: &str, kind: &'static str) -> Result<(), TlsMaterialError> {
    let mut count = 0usize;
    for item in CertificateDer::pem_slice_iter(pem.as_bytes()) {
        item.map_err(|source| TlsMaterialError::InvalidPem {
            path: path.to_path_buf(),
            kind,
            source,
        })?;
        count += 1;
    }
    if count == 0 {
        return Err(TlsMaterialError::NoCertificate {
            path: path.to_path_buf(),
            kind,
        });
    }
    Ok(())
}

/// Validate that `pem` parses as a private key.
fn validate_key(path: &Path, pem: &str) -> Result<(), TlsMaterialError> {
    PrivateKeyDer::from_pem_slice(pem.as_bytes())
        .map(|_| ())
        // Drop the parse error's `source`: it can render key-file bytes.
        .map_err(|_| TlsMaterialError::InvalidKey {
            path: path.to_path_buf(),
        })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Unique temp dir per test, without pulling in a UUID dependency.
    fn unique_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("tls-{tag}-{}-{n}", std::process::id()))
    }

    /// A CA plus one leaf (cert + key) signed by it, all PEM. Uses the
    /// workspace `rcgen` so the fixtures are real, parseable X.509.
    struct Fixture {
        ca: String,
        cert: String,
        key: String,
    }

    fn make_fixture(dns: &str) -> Fixture {
        use rcgen::{CertificateParams, KeyPair};
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        let ca = ca_params.self_signed(&ca_key).expect("self-sign ca");

        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf_params = CertificateParams::new(vec![dns.to_owned()]).expect("leaf params");
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let leaf = leaf_params
            .signed_by(&leaf_key, &issuer)
            .expect("sign leaf");

        Fixture {
            ca: ca.pem(),
            cert: leaf.pem(),
            key: leaf_key.serialize_pem(),
        }
    }

    /// Write a fixture's three artifacts into `dir` and return the paths.
    async fn write_fixture(dir: &Path, fx: &Fixture) -> TlsPaths {
        tokio::fs::create_dir_all(dir).await.expect("mkdir");
        let paths = TlsPaths {
            cert: dir.join("tls.crt"),
            key: dir.join("tls.key"),
            ca: dir.join("ca.crt"),
        };
        tokio::fs::write(&paths.cert, &fx.cert)
            .await
            .expect("write cert");
        tokio::fs::write(&paths.key, &fx.key)
            .await
            .expect("write key");
        tokio::fs::write(&paths.ca, &fx.ca).await.expect("write ca");
        paths
    }

    #[tokio::test]
    async fn loads_valid_material() {
        let dir = unique_dir("valid");
        let fx = make_fixture("localhost");
        let paths = write_fixture(&dir, &fx).await;

        let reader = TlsMaterialReader::new(paths).await.expect("load");
        let material = reader.current();
        assert!(material.cert_pem().contains("BEGIN CERTIFICATE"));
        assert!(material.ca_pem().contains("BEGIN CERTIFICATE"));
        assert!(material.key_pem().expose_secret().contains("PRIVATE KEY"));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn missing_file_is_an_error() {
        let dir = unique_dir("missing");
        let paths = TlsPaths {
            cert: dir.join("nope.crt"),
            key: dir.join("nope.key"),
            ca: dir.join("nope-ca.crt"),
        };
        match TlsMaterialReader::new(paths).await {
            Err(TlsMaterialError::Read { .. }) => {}
            _ => panic!("expected Read error"),
        }
    }

    #[tokio::test]
    async fn empty_file_is_an_error() {
        let dir = unique_dir("empty");
        let fx = make_fixture("localhost");
        let paths = write_fixture(&dir, &fx).await;
        tokio::fs::write(&paths.cert, "   \n").await.expect("blank");

        match TlsMaterialReader::new(paths).await {
            Err(TlsMaterialError::Empty { .. }) => {}
            _ => panic!("expected Empty error"),
        }
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn non_pem_cert_is_rejected() {
        // Load-time validation is PEM-structural (framing + base64); it is the
        // fail-safe against an unreadable / empty / truncated / non-PEM file, so
        // a rotation never swaps in obviously-bad material. Full X.509 structural
        // validity is enforced by rustls at handshake time. A file with no PEM
        // certificate block therefore yields `NoCertificate`.
        let dir = unique_dir("nonpem");
        let fx = make_fixture("localhost");
        let paths = write_fixture(&dir, &fx).await;
        tokio::fs::write(&paths.cert, "this is not a certificate\n")
            .await
            .expect("garbage");

        assert!(
            matches!(
                TlsMaterialReader::new(paths).await,
                Err(TlsMaterialError::NoCertificate {
                    kind: "certificate",
                    ..
                })
            ),
            "expected NoCertificate(certificate)"
        );
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn malformed_key_is_rejected_without_leaking_bytes() {
        // A malformed private key yields `InvalidKey`, and neither the error's
        // Display nor its full source chain carries the key-file bytes.
        let dir = unique_dir("badkey");
        let fx = make_fixture("localhost");
        let paths = write_fixture(&dir, &fx).await;
        // No valid PRIVATE KEY PEM block ⇒ the key parser rejects it. The marker
        // stands in for private-key bytes; it must not resurface via the error.
        let secret_marker = "SUPERSECRETKEYBYTES";
        tokio::fs::write(&paths.key, format!("not a private key {secret_marker}"))
            .await
            .expect("garbage key");

        let err = TlsMaterialReader::new(paths)
            .await
            .expect_err("malformed key must be rejected");
        assert!(
            matches!(err, TlsMaterialError::InvalidKey { .. }),
            "expected InvalidKey, got {err:?}"
        );
        // Walk Display, Debug, and the full source chain — none may leak bytes.
        let mut rendered = format!("{err} || {err:?}");
        let mut src = std::error::Error::source(&err);
        while let Some(inner) = src {
            rendered = format!("{rendered} || {inner}");
            src = inner.source();
        }
        assert!(
            !rendered.contains(secret_marker),
            "key-file bytes must never appear in the error chain: {rendered}"
        );
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn debug_redacts_private_key() {
        let dir = unique_dir("redact");
        let fx = make_fixture("localhost");
        let key_body = fx.key.clone();
        let paths = write_fixture(&dir, &fx).await;

        let reader = TlsMaterialReader::new(paths).await.expect("load");
        let dbg = format!("{:?}", reader.current());
        assert!(dbg.contains("<redacted>"), "debug must mark key redacted");
        assert!(
            !dbg.contains(key_body.trim()),
            "debug must not leak the private-key PEM body"
        );
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn rotation_bumps_generation_only_on_change() {
        let dir = unique_dir("rotate");
        let fx1 = make_fixture("localhost");
        let paths = write_fixture(&dir, &fx1).await;

        let reader = TlsMaterialReader::with_refresh_interval(
            paths.clone(),
            Duration::from_millis(20),
            None,
        )
        .await
        .expect("load");
        let mut rx = reader.subscribe();
        assert_eq!(*rx.borrow(), 0);
        let first = reader.current().cert_pem().to_owned();

        // Unchanged re-reads must not tick the generation counter.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            *rx.borrow(),
            0,
            "unchanged material must not bump generation"
        );

        // Rotate to a fresh cert/key/ca.
        let fx2 = make_fixture("localhost");
        write_fixture(&dir, &fx2).await;

        rx.changed().await.expect("generation should tick");
        // At least one tick — `write_fixture` writes the three files separately,
        // so an interval read can land on an intermediate state and bump the
        // counter more than once before it settles.
        assert!(
            *rx.borrow() >= 1,
            "generation should advance on a real rotation"
        );
        assert_ne!(
            reader.current().cert_pem(),
            first.as_str(),
            "current material should reflect the rotation"
        );

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
