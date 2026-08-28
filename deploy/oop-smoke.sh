#!/usr/bin/env bash
#
# Profile-3 (Kubernetes) OoP demo, scripted end-to-end.
#
# Codifies the manual walkthrough in deploy/README.md into a repeatable,
# asserting smoke test. By default it only runs the HTTP smoke against an
# already-deployed stack; pass stage flags to also build images, load them into
# minikube, and (re)deploy the umbrella chart.
#
# Usage:
#   deploy/oop-smoke.sh                 # smoke only (assumes stack is deployed)
#   deploy/oop-smoke.sh --deploy        # helm upgrade + rollout restart + smoke
#   deploy/oop-smoke.sh --load --deploy # reload images into minikube, redeploy, smoke
#   deploy/oop-smoke.sh --all           # build + load + deploy + smoke (full)
#
# Flags (stages run in this fixed order regardless of flag order):
#   --build    Rebuild all 5 images from the current working tree.
#   --load     Save + load the 5 images into minikube (tarball; reliable overwrite).
#   --deploy   helm dependency build/update + upgrade --install + rollout restart.
#   --all      Shorthand for --build --load --deploy.
#   --keep     Do not run cleanup on exit (default: nothing is torn down anyway).
#   -h|--help  Show this help.
#
# Environment overrides:
#   NS=cf-gears            Kubernetes namespace.
#   RELEASE=platform       Helm release name.
#   IMAGE_TAG=dev          Image tag.
#   REGISTRY=ghcr.io/constructorfabric
#   HOST=platform-host.local
#   TOKEN=test-token       Bearer token (accept_all maps any non-empty token).
#   BUILD_PROFILE=dev
set -euo pipefail

# ── Resolve repo root (this script lives in deploy/) ───────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"  # absolute; usage() reads it after cd
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# ── Config ─────────────────────────────────────────────────────────────────
NS="${NS:-cf-gears}"
RELEASE="${RELEASE:-platform}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
REGISTRY="${REGISTRY:-ghcr.io/constructorfabric}"
HOST="${HOST:-platform-host.local}"
# Host port used when the edge is reached via a port-forward fallback (docker
# driver). Matches the api-gateway bind (:8087).
EDGE_PORT="${EDGE_PORT:-8087}"
TOKEN="${TOKEN:-test-token}"
BUILD_PROFILE="${BUILD_PROFILE:-dev}"
TID="00000000-df51-5b42-9538-d2b56b7ee953"
# Max wait for a gear's route to appear at the edge (pod readiness + directory
# registration + edge sync interval).
ROUTE_TIMEOUT="${ROUTE_TIMEOUT:-150}"

DO_BUILD=0
DO_LOAD=0
DO_DEPLOY=0

# The images and their generic-per-gear build args (see deploy/README.md §1).
# Format: "<image>|<gear_package>|<gear_bin>|<config>|<features>".
# <features> is the cargo feature list for the OoP build. Most gears need
# "oop_module,k8s-auth"; cluster's OoP bin is unconditional (no oop_module
# feature), so it builds with just "k8s-auth".
GEARS=(
  "hello|hello|hello-oop|config/oop-hello.yaml|oop_module,k8s-auth"
  "users-info|users-info|users-info-oop|config/oop-users-info.yaml|oop_module,k8s-auth"
  "api-contracts|cf-api-contracts|api-contracts-oop|config/oop-api-contracts.yaml|oop_module,k8s-auth"
  "api-contracts-consumer|cf-api-contracts-consumer|api-contracts-consumer-oop|config/oop-api-contracts-consumer.yaml|oop_module,k8s-auth"
  "cluster|cf-gears-cluster|cluster-oop|config/oop-cluster.yaml|k8s-auth"
  "cluster-consumer|cluster-consumer|cluster-consumer-oop|config/oop-cluster-consumer.yaml|oop_module,k8s-auth"
)
ALL_IMAGES=(platform-host hello users-info api-contracts api-contracts-consumer cluster cluster-consumer)

# ── Pretty output + assertions ─────────────────────────────────────────────
PASS=0
FAIL=0
RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

log()  { printf '%s\n' "${BOLD}==>${RESET} $*"; }
warn() { printf '%s\n' "${YELLOW}warning:${RESET} $*" >&2; }
die()  { printf '%s\n' "${RED}error:${RESET} $*" >&2; exit 1; }

pass() { PASS=$((PASS+1)); printf '  %sPASS%s %s\n' "$GREEN" "$RESET" "$*"; }
fail() { FAIL=$((FAIL+1)); printf '  %sFAIL%s %s\n' "$RED" "$RESET" "$*"; }

assert_eq() { # <label> <expected> <actual>
  if [[ "$2" == "$3" ]]; then pass "$1 (= $3)"; else fail "$1 (expected $2, got $3)"; fi
}
assert_contains() { # <label> <needle> <haystack>
  if [[ "$3" == *"$2"* ]]; then pass "$1"; else fail "$1 (missing '$2' in: ${3:0:200})"; fi
}

# Print the leading comment block (skip the shebang, stop at the first code line).
usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$SELF"; exit 0; }

# Poll an edge route until it is registered. A synced route answers with a real
# HTTP status (e.g. 200/201); before the edge syncs it from the DirectoryService
# it returns 404 "no upstream route registered" (or 000 while the edge warms up).
# Uses globals G_BASE / G_RESOLVE set by smoke().
#
# The probe MUST carry the bearer token. The edge auth middleware default-denies
# any path it does not yet know with 401 (authenticated-by-default), so an
# unauthenticated probe of an authenticated route returns 401 *before* the route
# is registered — which would trip the "any non-404 == ready" gate prematurely
# and let the real assertions race an unregistered route (-> 404). With a valid
# token the auth layer passes, so an unregistered route falls through to the
# proxy fallback and still returns 404 (keep waiting), while a registered route
# returns its real status (gate releases). Anonymous routes are unaffected.
wait_route() { # <desc> <method> <path> [json-body]
  local desc="$1" method="$2" path="$3" data="${4:-}"
  local deadline=$(( SECONDS + ROUTE_TIMEOUT )) code
  while :; do
    if [[ -n "$data" ]]; then
      code="$(curl -s -o /dev/null -w '%{http_code}' -X "$method" \
        -H "Authorization: Bearer $TOKEN" \
        -H 'Content-Type: application/json' -d "$data" \
        "${G_RESOLVE[@]}" "$G_BASE$path" || true)"
    else
      code="$(curl -s -o /dev/null -w '%{http_code}' -X "$method" \
        -H "Authorization: Bearer $TOKEN" \
        "${G_RESOLVE[@]}" "$G_BASE$path" || true)"
    fi
    case "$code" in
      404|000|0|"") ;;          # not registered yet / edge still warming
      *) return 0 ;;            # route exists (any real HTTP status)
    esac
    (( SECONDS < deadline )) || { warn "$desc still 404 after ${ROUTE_TIMEOUT}s"; return 1; }
    sleep 3
  done
}

# ── Arg parsing ────────────────────────────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    --build)  DO_BUILD=1 ;;
    --load)   DO_LOAD=1 ;;
    --deploy) DO_DEPLOY=1 ;;
    --all)    DO_BUILD=1; DO_LOAD=1; DO_DEPLOY=1 ;;
    --keep)   : ;;
    -h|--help) usage ;;
    *) die "unknown flag: $arg (see --help)" ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"; }

# ── Stage: build ───────────────────────────────────────────────────────────
build_images() {
  need docker
  log "Building platform-host image"
  DOCKER_BUILDKIT=1 docker build \
    -f deploy/docker/platform-host.Dockerfile \
    --build-arg BUILD_PROFILE="$BUILD_PROFILE" \
    --build-arg CARGO_FEATURES="k8s" \
    -t "$REGISTRY/platform-host:$IMAGE_TAG" .
  for spec in "${GEARS[@]}"; do
    IFS='|' read -r img pkg bin cfg feats <<<"$spec"
    log "Building $img image (pkg=$pkg bin=$bin feats=$feats)"
    DOCKER_BUILDKIT=1 docker build \
      -f deploy/docker/oop-gear.Dockerfile \
      --build-arg GEAR_PACKAGE="$pkg" \
      --build-arg GEAR_BIN="$bin" \
      --build-arg GEAR_FEATURES="$feats" \
      --build-arg GEAR_CONFIG="$cfg" \
      --build-arg BUILD_PROFILE="$BUILD_PROFILE" \
      -t "$REGISTRY/$img:$IMAGE_TAG" .
  done
}

# ── Stage: load (tarball → reliable overwrite of an existing tag) ──────────
load_images() {
  need docker; need minikube
  for img in "${ALL_IMAGES[@]}"; do
    local tag="$REGISTRY/$img:$IMAGE_TAG"
    log "Loading $tag into minikube"
    local tar="/tmp/oop-img-$img.tar"
    docker save "$tag" -o "$tar"
    minikube image load "$tar"
    rm -f "$tar"
  done
}

# ── Stage: deploy ──────────────────────────────────────────────────────────
deploy_stack() {
  need kubectl; need helm
  log "Ensuring namespace $NS"
  kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

  log "Building chart dependencies"
  for c in platform-host hello users-info api-contracts api-contracts-consumer cluster cluster-consumer; do
    helm dependency build "deploy/helm/$c" >/dev/null
  done
  helm dependency update deploy/helm/toolkit-platform >/dev/null

  log "helm upgrade --install $RELEASE"
  helm upgrade --install "$RELEASE" deploy/helm/toolkit-platform \
    -n "$NS" \
    -f deploy/helm/toolkit-platform/values-dev.yaml \
    --timeout 240s

  # Same image tag => pod template unchanged => force new pods to pick up the
  # freshly-loaded images (pullPolicy: Never).
  log "Rolling out fresh pods"
  for d in platform-host hello users-info api-contracts api-contracts-consumer cluster cluster-consumer; do
    kubectl -n "$NS" rollout restart "deploy/$d" >/dev/null
  done
  for d in platform-host hello users-info api-contracts api-contracts-consumer cluster cluster-consumer; do
    kubectl -n "$NS" rollout status "deploy/$d" --timeout=180s
  done
}

# ── Stage: smoke ───────────────────────────────────────────────────────────
smoke() {
  need kubectl; need curl; need minikube
  local mip base resolve
  resolve=()

  # How the host reaches the edge depends on the minikube driver. On Linux / VM
  # drivers the edge Ingress is reachable at `minikube ip`; on the docker driver
  # (macOS / Windows) that IP is not routable from the host without a running
  # `minikube tunnel`. Probe the Ingress for a short window; if it never answers,
  # fall back to a port-forward of the platform-host edge Service, which works on
  # every driver. The fallback bypasses the nginx Ingress hop but still exercises
  # the edge (api-gateway) -> OoP pod seams the assertions below check.
  mip="$(minikube ip 2>/dev/null || true)"
  local ingress_ok="" probe_deadline=$(( SECONDS + 30 ))
  if [[ -n "$mip" ]]; then
    log "Probing edge Ingress at $HOST -> $mip (up to 30s)"
    while (( SECONDS < probe_deadline )); do
      if [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
              --resolve "$HOST:80:$mip" "http://$HOST/healthz" || true)" == "200" ]]; then
        ingress_ok=1; break
      fi
      sleep 2
    done
  fi

  if [[ -n "$ingress_ok" ]]; then
    base="http://$HOST"; resolve=(--resolve "$HOST:80:$mip")
    log "Edge reachable via Ingress $base (minikube ip $mip)"
  else
    log "Ingress not reachable from the host (docker driver without 'minikube tunnel'?)"
    log "Falling back to a port-forward of svc/platform-host -> 127.0.0.1:$EDGE_PORT"
    kubectl -n "$NS" port-forward "svc/platform-host" "${EDGE_PORT}:8087" \
      >/tmp/oop-smoke-pf.log 2>&1 &
    PF_PID=$!
    # Kill the port-forward on any exit (success, failure, or Ctrl-C).
    trap '[[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null || true' EXIT
    base="http://127.0.0.1:${EDGE_PORT}"
    # An inert --resolve (127.0.0.1 -> 127.0.0.1) rather than an empty array: on
    # macOS bash 3.2 with `set -u`, expanding an empty `"${resolve[@]}"` errors as
    # an unbound variable, and this keeps the ~10 call sites below unchanged.
    resolve=(--resolve "127.0.0.1:${EDGE_PORT}:127.0.0.1")
    log "Edge via port-forward $base (pid $PF_PID)"
  fi

  # Wait for the edge to answer before asserting (route sync happens shortly after,
  # and the port-forward needs a moment to establish).
  local deadline=$(( SECONDS + 120 ))
  until [[ "$(curl -s -o /dev/null -w '%{http_code}' "${resolve[@]}" "$base/healthz" || true)" == "200" ]]; do
    (( SECONDS < deadline )) || die "edge /healthz never returned 200 within 120s"
    sleep 2
  done

  # Gate on each gear's route being synced to the edge before asserting. Pods
  # become Ready before they register with the DirectoryService and the edge
  # syncs the route (~5s interval), so a fresh deploy briefly 404s per route.
  G_BASE="$base"; G_RESOLVE=("${resolve[@]}")
  log "Waiting for gear routes to sync at the edge"
  wait_route "hello route"      GET  "/hello/v1/ping"
  wait_route "users-info route" GET  "/users-info/v1/cities"
  wait_route "consumer route"   POST "/api-contracts-consumer/v1/charge" \
    '{"amount_cents":1,"currency":"USD","description":"warmup"}'
  wait_route "cluster-consumer route" GET "/cluster-consumer/v1/ping"

  local code body
  # 1) edge healthz
  code="$(curl -s -o /dev/null -w '%{http_code}' "${resolve[@]}" "$base/healthz")"
  assert_eq "edge /healthz" "200" "$code"

  # 2) hello ping — anonymous, cross-pod reverse-proxy
  body="$(curl -s "${resolve[@]}" "$base/hello/v1/ping")"
  assert_contains "hello ping proxied (pong)" '"message":"pong"' "$body"
  assert_contains "hello served_by is the OoP pod" 'hello-oop' "$body"

  # 3) users-info — missing bearer rejected at the edge
  code="$(curl -s -o /dev/null -w '%{http_code}' "${resolve[@]}" "$base/users-info/v1/cities")"
  assert_eq "users-info cities no-token -> 401" "401" "$code"

  # 4) users-info POST — authenticated + remote-authz (PEP over REST) + Postgres
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"name\":\"Tokyo\",\"country\":\"JP\",\"tenant_id\":\"$TID\"}" \
    "${resolve[@]}" "$base/users-info/v1/cities")"
  assert_eq "users-info POST city -> 201" "201" "$code"

  # 5) users-info GET — the row persisted
  body="$(curl -s -H "Authorization: Bearer $TOKEN" "${resolve[@]}" "$base/users-info/v1/cities")"
  assert_contains "users-info GET returns persisted city" '"name":"Tokyo"' "$body"

  # 6) consumer charge — missing bearer rejected
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    -d '{"amount_cents":1000,"currency":"USD","description":"t"}' \
    "${resolve[@]}" "$base/api-contracts-consumer/v1/charge")"
  assert_eq "consumer charge no-token -> 401" "401" "$code"

  # 7) consumer charge — OoP -> OoP over REST (consumer pod -> provider pod)
  body="$(curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"amount_cents":1000,"currency":"USD","description":"test charge"}' \
    "${resolve[@]}" "$base/api-contracts-consumer/v1/charge")"
  assert_contains "consumer charge -> payment_id" '"payment_id"' "$body"
  assert_contains "consumer charge -> status pending" '"status":"pending"' "$body"

  # 8) cluster coordination — the consumer resolves the cluster facades from the
  #    cluster POD over gRPC (endpoint derived by DNS convention, discovered at
  #    :50051) and exercises all three primitives: distributed lock (acquire +
  #    release), cache (put + get), and leader election (join + observe + resign).
  #    Unlike the loopback e2e (where the k8s DNS name does not resolve and this
  #    returns 503), in-cluster this round-trips.
  body="$(curl -s "${resolve[@]}" "$base/cluster-consumer/v1/ping")"
  assert_contains "cluster-consumer ping proxied (pong)" '"message":"pong"' "$body"
  body="$(curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"key":"seat/12","value":"held"}' \
    "${resolve[@]}" "$base/cluster-consumer/v1/roundtrip")"
  assert_contains "cluster cache: reads back the value" '"value":"held"' "$body"
  assert_contains "cluster lock: acquired + released" '"lock_released":true' "$body"
  assert_contains "cluster leader election: leader observed" '"is_leader":true' "$body"
  assert_contains "cluster round-trip served_by is the consumer pod" 'cluster-consumer-oop' "$body"

  # 8) Log-level evidence of the platform-plane internals (best-effort).
  # Capture full logs into variables first, then grep: piping `kubectl logs`
  # straight into `grep -q` trips `set -o pipefail` (grep exits on first match,
  # SIGPIPEs kubectl -> 141 -> the pipeline "fails" even on a match). The
  # startup-time enforcement line also scrolls past a bounded --tail.
  log "Checking platform-host / provider logs"
  local host_logs prov_logs
  host_logs="$(kubectl -n "$NS" logs "deploy/platform-host" 2>/dev/null || true)"
  prov_logs="$(kubectl -n "$NS" logs "deploy/api-contracts" 2>/dev/null || true)"

  if grep -q "platform-plane enforcement enabled" <<<"$host_logs"; then
    pass "platform-plane enforcement enabled (log)"
  else
    fail "platform-plane enforcement log line not found"
  fi
  if grep -q "authz-resolver/v1/evaluate" <<<"$host_logs"; then
    pass "PEP over REST: authz-resolver evaluate (log)"
  else
    warn "authz-resolver/v1/evaluate not seen in platform-host logs"
  fi
  if grep -q 'method":"charge"' <<<"$prov_logs"; then
    pass "provider executed charge — OoP->OoP (log)"
  else
    fail "provider charge log line not found"
  fi
}

# ── Main ───────────────────────────────────────────────────────────────────
[[ $DO_BUILD  -eq 1 ]] && build_images
[[ $DO_LOAD   -eq 1 ]] && load_images
[[ $DO_DEPLOY -eq 1 ]] && deploy_stack
smoke

echo
log "Result: ${GREEN}${PASS} passed${RESET}, $([[ $FAIL -gt 0 ]] && echo "${RED}${FAIL} failed${RESET}" || echo "${FAIL} failed")"
[[ $FAIL -eq 0 ]] || exit 1
