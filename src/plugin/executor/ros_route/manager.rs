//! Route manager state machine for `ros_route`.
//!
//! Responsibilities:
//! - maintain domain -> IP bindings with per-IP expiry
//! - maintain route-level reference states and router ids
//! - reconcile local state with RouterOS route table/comment metadata
//! - execute idempotent create/update/delete through [`MikrotikApi`]

use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::{AHashMap, AHashSet};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::RosRouteMetrics;
use super::api::{MikrotikApi, RouterRoute};
use crate::infra::clock::AppClock;
use crate::infra::error::{DnsError, Result};
use crate::infra::task as task_center;
use crate::plugin::executor::ros_common::ObservedAddr;

const ROUTE_DEFAULT_V4: &str = "0.0.0.0/0";
const ROUTE_DEFAULT_V6: &str = "::/0";
const ROUTE_PREFIX_V4: u8 = 32;
const ROUTE_PREFIX_V6: u8 = 128;
const PERSISTENT_COMMENT_DOMAIN: &str = "persistent";
const PERSISTENT_EXPIRES_AT_UNIX: u64 = u64::MAX;
const MANAGER_QUEUE_SIZE: usize = 1024;
const MAX_PENDING_OBSERVATIONS: usize = MANAGER_QUEUE_SIZE;
const SWEEP_INTERVAL_SECS: u64 = 30;
const RECONCILE_INTERVAL_SECS: u64 = 180;
const PERSISTENT_RELOAD_INTERVAL_SECS: u64 = 60;
const CONNECTION_GUARD_RETRY_INTERVAL_SECS: u64 = SWEEP_INTERVAL_SECS;
const CONNECTION_QUERY_BATCH_SIZE: usize = 128;
/// Maximum number of route upserts in flight on the shared RouterOS API
/// connection. This preserves CDN-answer throughput without allowing one DNS
/// response to monopolize the command response buffer.
const UPSERT_PIPELINE_SIZE: usize = 16;

const COMMENT_FIELD_PLUGIN: &str = "pg";
const COMMENT_FIELD_DOMAIN: &str = "dm";
const COMMENT_FIELD_EXP: &str = "exp";
const COMMENT_FIELD_SEEN: &str = "seen";
const COMMENT_FIELD_KIND: &str = "kind";
const COMMENT_KIND_DYNAMIC: &str = "dynamic";
const COMMENT_KIND_PERSISTENT: &str = "persistent";
const COMMENT_KIND_MIXED: &str = "mixed";
const COMMENT_KIND_GATEWAY_CHECK: &str = "gateway-check";
const MAX_COMMENT_REFRESH_INTERVAL_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub(super) struct RouteManagerConfig {
    /// Plugin tag used in comment codec for ownership check.
    pub(super) plugin_tag: String,
    /// Dedicated RouterOS routing table name.
    pub(super) routing_table: String,
    /// Optional IPv4 gateway for managed routes.
    pub(super) gateway4: Option<String>,
    /// Optional IPv6 gateway for managed routes.
    pub(super) gateway6: Option<String>,
    /// Always-present routes in CIDR form (`ip/prefix`).
    pub(super) persistent_ips: AHashSet<String>,
    /// Comment prefix that marks managed routes.
    pub(super) comment_prefix: String,
    /// Route distance written to RouterOS.
    pub(super) distance: u8,
    /// Minimum TTL clamp in seconds.
    pub(super) min_ttl: u32,
    /// Maximum TTL clamp in seconds.
    pub(super) max_ttl: u32,
    /// Optional fixed TTL override in seconds.
    pub(super) fixed_ttl: Option<u32>,
    /// Whether normal desired-state deletion waits for RouterOS conntrack.
    pub(super) conntrack_guard: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum RouteFamily {
    Ipv4,
    Ipv6,
}

impl RouteFamily {
    #[inline]
    pub(super) fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }

    #[inline]
    fn prefix(self) -> u8 {
        match self {
            Self::Ipv4 => ROUTE_PREFIX_V4,
            Self::Ipv6 => ROUTE_PREFIX_V6,
        }
    }

    #[inline]
    fn is_valid_prefix(self, prefix: u8) -> bool {
        match self {
            Self::Ipv4 => prefix <= 32,
            Self::Ipv6 => prefix <= 128,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) struct DomainBinding {
    /// Normalized domain name.
    pub(super) domain: String,
    /// Active IP set observed for this domain.
    pub(super) ips: AHashSet<IpAddr>,
    /// Per-IP expiry timestamp for this domain.
    pub(super) ip_expiries: AHashMap<IpAddr, u64>,
    /// Max expiry among `ip_expiries`.
    pub(super) expires_at_unix: u64,
    /// Last refresh timestamp.
    pub(super) last_refresh_unix: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct RouteKey {
    /// Route network/base IP address.
    pub(super) ip: IpAddr,
    /// Route CIDR prefix.
    pub(super) prefix: u8,
    /// RouterOS routing table name.
    pub(super) table: String,
}

impl RouteKey {
    pub(super) fn new(ip: IpAddr, table: String) -> Self {
        let prefix = RouteFamily::from_ip(ip).prefix();
        Self { ip, prefix, table }
    }

    pub(super) fn new_with_prefix(ip: IpAddr, prefix: u8, table: String) -> Option<Self> {
        let family = RouteFamily::from_ip(ip);
        if !family.is_valid_prefix(prefix) {
            return None;
        }
        Some(Self { ip, prefix, table })
    }

    #[inline]
    pub(super) fn family(&self) -> RouteFamily {
        RouteFamily::from_ip(self.ip)
    }

    #[inline]
    pub(super) fn dst_address(&self) -> String {
        format!("{}/{}", self.ip, self.prefix)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum SyncState {
    /// Route does not exist on RouterOS yet (or local state intentionally
    /// forgot the remote id) and must be created on next sync pass.
    ///
    /// Typical transitions:
    /// - new observation creates a fresh route entry
    /// - reconcile detects that a desired persistent route was removed remotely
    /// - recovered entry lost its `router_id`
    PendingCreate,
    /// Local route state is consistent with RouterOS.
    ///
    /// In this state no API call is needed unless route payload changes
    /// (gateway/comment/expiry metadata) or ref-count drops to zero.
    Synced,
    /// Route should be removed from RouterOS on next sync pass.
    ///
    /// This is set when the route has no active dynamic references, or when a
    /// stale recovered route is identified during reconciliation/expiration.
    PendingDelete,
    /// Route exists remotely but local payload changed and requires an update.
    ///
    /// The sync loop handles this as an idempotent upsert (`set` or `add`
    /// depending on remote presence), then returns to `Synced`.
    Dirty,
}

#[derive(Debug, Clone)]
pub(super) struct RouteEntry {
    /// Unique key of the managed route.
    pub(super) key: RouteKey,
    /// Gateway string written to RouterOS.
    pub(super) gateway: String,
    /// Route distance written to RouterOS.
    pub(super) distance: u8,
    /// Domain set currently referencing this route.
    pub(super) domains: AHashSet<String>,
    /// Comment `dm` field, using the first observed active domain when
    /// available.
    pub(super) comment_domain: String,
    /// Per-domain expiry timestamps for ref-count and max-exp calculations.
    pub(super) domain_expiries: AHashMap<String, u64>,
    /// Current dynamic DNS reference count from `domains`.
    pub(super) ref_count: u32,
    /// Whether this route is explicitly configured through `persistent_route`.
    /// Kept outside `domains` so no real qname can collide with internal state.
    pub(super) persistent: bool,
    /// Route-level expiry (max of active refs).
    pub(super) expires_at_unix: u64,
    /// Last refresh timestamp.
    pub(super) last_refresh_unix: u64,
    /// Expiry value last confirmed in the RouterOS comment. `None` means the
    /// route has not been synchronized yet.
    pub(super) synced_expires_at_unix: Option<u64>,
    /// RouterOS internal route id.
    pub(super) router_id: Option<String>,
    /// Whether RouterOS recovery could only restore one representative domain
    /// and therefore cannot prove that withdrawing it removes every owner.
    pub(super) recovered_ownership_incomplete: bool,
    /// Pending/synced transition state for API sync loop.
    pub(super) sync_state: SyncState,
}

impl RouteEntry {
    #[inline]
    fn has_owners(&self) -> bool {
        self.persistent || self.ref_count > 0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct RouteCommentMeta {
    pub(super) family: RouteFamily,
    pub(super) ip: IpAddr,
    pub(super) kind: RouteCommentKind,
    pub(super) comment_domain: String,
    pub(super) expires_at_unix: u64,
    pub(super) last_refresh_unix: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RouteCommentKind {
    Dynamic,
    Persistent,
    Mixed,
}

impl RouteCommentKind {
    fn for_route(route: &RouteEntry) -> Self {
        match (route.persistent, route.ref_count > 0) {
            (true, true) => Self::Mixed,
            (true, false) => Self::Persistent,
            (false, _) => Self::Dynamic,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Dynamic => COMMENT_KIND_DYNAMIC,
            Self::Persistent => COMMENT_KIND_PERSISTENT,
            Self::Mixed => COMMENT_KIND_MIXED,
        }
    }

    #[inline]
    fn has_dynamic_ownership(self) -> bool {
        matches!(self, Self::Dynamic | Self::Mixed)
    }
}

#[derive(Debug)]
pub(super) struct RouteCommentCodec;

impl RouteCommentCodec {
    fn expires_at_unix(route: &RouteEntry) -> u64 {
        match RouteCommentKind::for_route(route) {
            RouteCommentKind::Mixed => route
                .domain_expiries
                .values()
                .copied()
                .max()
                .unwrap_or(route.last_refresh_unix),
            RouteCommentKind::Dynamic | RouteCommentKind::Persistent => route.expires_at_unix,
        }
    }

    /// Encode route metadata into RouterOS comment payload.
    pub(super) fn encode(prefix: &str, plugin_tag: &str, route: &RouteEntry) -> String {
        let kind = RouteCommentKind::for_route(route);
        let expires_at_unix = Self::expires_at_unix(route);
        let mut out = String::new();
        if !prefix.is_empty() {
            out.push_str(prefix);
            out.push(';');
        }
        out.push_str(COMMENT_FIELD_PLUGIN);
        out.push('=');
        out.push_str(plugin_tag);
        out.push(';');
        out.push_str(COMMENT_FIELD_KIND);
        out.push('=');
        out.push_str(kind.as_str());
        out.push(';');
        out.push_str(COMMENT_FIELD_DOMAIN);
        out.push('=');
        out.push_str(&encode_comment_value(&route.comment_domain));
        out.push(';');
        out.push_str(COMMENT_FIELD_EXP);
        out.push('=');
        out.push_str(&expires_at_unix.to_string());
        out.push(';');
        out.push_str(COMMENT_FIELD_SEEN);
        out.push('=');
        out.push_str(&route.last_refresh_unix.to_string());
        out
    }

    pub(super) fn decode(
        prefix: &str,
        plugin_tag: &str,
        family: RouteFamily,
        dst_address: &str,
        comment: &str,
    ) -> Result<Option<RouteCommentMeta>> {
        // Prefix and plugin-tag checks provide cheap ownership filtering.
        if !prefix.is_empty() {
            if !comment.starts_with(prefix) {
                return Ok(None);
            }
            if comment.as_bytes().get(prefix.len()) != Some(&b';') {
                return Ok(None);
            }
        }

        let mut kv = AHashMap::new();
        for token in comment.split(';') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            if let Some((k, v)) = token.split_once('=') {
                kv.insert(k.trim().to_string(), v.trim().to_string());
            }
        }

        if kv.get(COMMENT_FIELD_PLUGIN).map(String::as_str) != Some(plugin_tag) {
            return Ok(None);
        }

        let kind = match kv.get(COMMENT_FIELD_KIND).map(String::as_str) {
            Some(COMMENT_KIND_DYNAMIC) => RouteCommentKind::Dynamic,
            Some(COMMENT_KIND_PERSISTENT) => RouteCommentKind::Persistent,
            Some(COMMENT_KIND_MIXED) => RouteCommentKind::Mixed,
            Some(value) => {
                return Err(DnsError::plugin(format!(
                    "ros_route comment decode failed: unsupported kind '{value}'"
                )));
            }
            None => {
                return Err(DnsError::plugin(
                    "ros_route comment decode failed: missing kind field",
                ));
            }
        };

        let (ip, _prefix) = parse_dst_address(dst_address).ok_or_else(|| {
            DnsError::plugin(format!(
                "ros_route comment decode failed: invalid dst-address '{dst_address}'"
            ))
        })?;

        if RouteFamily::from_ip(ip) != family {
            return Err(DnsError::plugin(format!(
                "ros_route comment decode failed: af/ip mismatch af={:?} ip={}",
                family, ip
            )));
        }

        let encoded_comment_domain = kv
            .get(COMMENT_FIELD_DOMAIN)
            .ok_or_else(|| DnsError::plugin("ros_route comment decode failed: missing dm field"))?;
        let comment_domain = decode_comment_value(encoded_comment_domain)?;
        let expires_at_unix = kv
            .get(COMMENT_FIELD_EXP)
            .ok_or_else(|| DnsError::plugin("ros_route comment decode failed: missing exp field"))?
            .parse::<u64>()
            .map_err(|e| {
                DnsError::plugin(format!("ros_route comment decode failed: invalid exp: {e}"))
            })?;
        let last_refresh_unix = kv
            .get(COMMENT_FIELD_SEEN)
            .ok_or_else(|| DnsError::plugin("ros_route comment decode failed: missing seen field"))?
            .parse::<u64>()
            .map_err(|e| {
                DnsError::plugin(format!(
                    "ros_route comment decode failed: invalid seen: {e}"
                ))
            })?;

        Ok(Some(RouteCommentMeta {
            family,
            ip,
            kind,
            comment_domain,
            expires_at_unix,
            last_refresh_unix,
        }))
    }
}

fn encode_comment_value(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0F)]));
        }
    }
    encoded
}

fn decode_comment_value(value: &str) -> Result<String> {
    let input = value.as_bytes();
    let mut decoded = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] != b'%' {
            decoded.push(input[index]);
            index += 1;
            continue;
        }

        let Some(encoded_byte) = input.get(index + 1..index + 3) else {
            return Err(DnsError::plugin(
                "ros_route comment decode failed: truncated percent escape in dm field",
            ));
        };
        let high = decode_hex_digit(encoded_byte[0]).ok_or_else(|| {
            DnsError::plugin("ros_route comment decode failed: invalid percent escape in dm field")
        })?;
        let low = decode_hex_digit(encoded_byte[1]).ok_or_else(|| {
            DnsError::plugin("ros_route comment decode failed: invalid percent escape in dm field")
        })?;
        decoded.push((high << 4) | low);
        index += 3;
    }

    String::from_utf8(decoded).map_err(|error| {
        DnsError::plugin(format!(
            "ros_route comment decode failed: dm field is not valid UTF-8: {error}"
        ))
    })
}

#[inline]
fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn owned_comment_has_kind(
    prefix: &str,
    plugin_tag: &str,
    comment: &str,
    expected_kind: &str,
) -> bool {
    if !prefix.is_empty()
        && (!comment.starts_with(prefix) || comment.as_bytes().get(prefix.len()) != Some(&b';'))
    {
        return false;
    }

    let mut owner_matches = false;
    let mut kind_matches = false;
    for token in comment.split(';') {
        let Some((key, value)) = token.trim().split_once('=') else {
            continue;
        };
        match key.trim() {
            COMMENT_FIELD_PLUGIN | "plugin" => owner_matches = value.trim() == plugin_tag,
            COMMENT_FIELD_KIND => kind_matches = value.trim() == expected_kind,
            _ => {}
        }
    }
    owner_matches && kind_matches
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(super) enum ObservationScope {
    Ipv4,
    Ipv6,
    Both,
}

impl ObservationScope {
    #[inline]
    pub(super) fn contains(self, ip: IpAddr) -> bool {
        matches!(
            (self, ip),
            (Self::Ipv4 | Self::Both, IpAddr::V4(_)) | (Self::Ipv6 | Self::Both, IpAddr::V6(_))
        )
    }
}

#[derive(Debug, Clone)]
struct PendingObservation {
    domain: String,
    replace_scope: ObservationScope,
    addrs: Vec<ObservedAddr>,
    observed_at_unix: u64,
    /// Upper bound for replaying this complete replacement observation.
    replay_until_unix: u64,
    /// Whether this family entry came from a name-level NXDOMAIN response.
    name_level_negative: bool,
}

#[derive(Debug)]
pub(super) enum ManagerCommand {
    ObserveDomain {
        domain: String,
        replace_scope: ObservationScope,
        addrs: Vec<ObservedAddr>,
        negative_ttl_secs: Option<u32>,
        wait: Option<oneshot::Sender<Result<()>>>,
    },
    UpdatePersistentIps {
        ips: AHashSet<String>,
    },
    Sweep,
    Reconcile,
}

#[derive(Debug)]
struct ShutdownRequest {
    cleanup: bool,
    done: oneshot::Sender<()>,
}

#[derive(Debug, Clone)]
pub(super) struct PersistentReloadConfig {
    /// Inline persistent routes in normalized `ip/prefix` format.
    pub(super) inline_ips: AHashSet<String>,
    /// Source files that contain persistent route entries.
    pub(super) files: Vec<String>,
    /// Initial desired set merged from inline + file content at startup.
    pub(super) initial_ips: AHashSet<String>,
    /// Whether IPv4 gateway is configured.
    pub(super) gateway4_enabled: bool,
    /// Whether IPv6 gateway is configured.
    pub(super) gateway6_enabled: bool,
}

#[derive(Debug)]
pub(super) struct RouteManagerRuntime {
    tx: mpsc::Sender<ManagerCommand>,
    shutdown_tx: Option<oneshot::Sender<ShutdownRequest>>,
    worker_handle: Option<JoinHandle<()>>,
    sweep_task_id: Option<u64>,
    reconcile_task_id: Option<u64>,
    persistent_reload_task_id: Option<u64>,
}

impl RouteManagerRuntime {
    pub(super) fn start(
        tag: String,
        manager: RouteManager,
        persistent_reload: Option<PersistentReloadConfig>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<ManagerCommand>(MANAGER_QUEUE_SIZE);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<ShutdownRequest>();

        let worker_tag = tag.clone();
        let worker_handle = Some(tokio::spawn(async move {
            run_manager_worker(worker_tag, manager, rx, shutdown_rx).await;
        }));

        // RouterOS validation and the initial table scan intentionally happen
        // on the background manager. They must never delay DNS startup.
        let _ = tx.try_send(ManagerCommand::Reconcile);

        let sweep_tx = tx.clone();
        let sweep_task_id = Some(task_center::spawn_fixed(
            format!("ros_route:{}:sweep", tag),
            Duration::from_secs(SWEEP_INTERVAL_SECS),
            move || {
                let sweep_tx = sweep_tx.clone();
                async move {
                    let _ = sweep_tx.send(ManagerCommand::Sweep).await;
                }
            },
        ));

        let reconcile_tx = tx.clone();
        let reconcile_task_id = Some(task_center::spawn_fixed(
            format!("ros_route:{}:reconcile", tag),
            Duration::from_secs(RECONCILE_INTERVAL_SECS),
            move || {
                let reconcile_tx = reconcile_tx.clone();
                async move {
                    let _ = reconcile_tx.send(ManagerCommand::Reconcile).await;
                }
            },
        ));

        let persistent_reload_task_id = persistent_reload.and_then(|reload_cfg| {
            if reload_cfg.initial_ips.is_empty() && reload_cfg.files.is_empty() {
                return None;
            }

            let maintain_tx = tx.clone();
            let maintain_tag = tag.clone();
            let last_loaded_ips = Arc::new(tokio::sync::Mutex::new(reload_cfg.initial_ips.clone()));
            Some(task_center::spawn_fixed(
                format!("ros_route:{}:persistent_reload", maintain_tag),
                Duration::from_secs(PERSISTENT_RELOAD_INTERVAL_SECS),
                move || {
                    let maintain_tx = maintain_tx.clone();
                    let maintain_tag = maintain_tag.clone();
                    let last_loaded_ips = last_loaded_ips.clone();
                    let reload_cfg = reload_cfg.clone();
                    async move {
                        match super::load_persistent_ips_from_files_async(
                            reload_cfg.files.as_slice(),
                            reload_cfg.gateway4_enabled,
                            reload_cfg.gateway6_enabled,
                        )
                        .await
                        {
                            Ok((file_ips, ignored_by_gateway, ignored_default_route)) => {
                                if ignored_by_gateway > 0 {
                                    debug!(
                                        plugin = %maintain_tag,
                                        ignored = ignored_by_gateway,
                                        "ros_route persistent file reload ignored entries without corresponding gateway family"
                                    );
                                }
                                if ignored_default_route > 0 {
                                    debug!(
                                        plugin = %maintain_tag,
                                        ignored = ignored_default_route,
                                        "ros_route persistent file reload ignored default-route entries (/0)"
                                    );
                                }

                                let mut desired_ips = reload_cfg.inline_ips.clone();
                                desired_ips.extend(file_ips);

                                let mut last_loaded_guard = last_loaded_ips.lock().await;
                                let manager_available = if desired_ips != *last_loaded_guard {
                                    *last_loaded_guard = desired_ips.clone();
                                    maintain_tx
                                        .send(ManagerCommand::UpdatePersistentIps {
                                            ips: desired_ips.clone(),
                                        })
                                        .await
                                        .is_ok()
                                } else {
                                    true
                                };

                                // Dedicated tick keeps persistent routes self-healed
                                // without requiring new DNS observations.
                                if manager_available {
                                    let _ = maintain_tx.send(ManagerCommand::Reconcile).await;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    plugin = %maintain_tag,
                                    err = %e,
                                    "ros_route persistent file reload failed"
                                );
                            }
                        }
                    }
                },
            ))
        });

        Self {
            tx,
            shutdown_tx: Some(shutdown_tx),
            worker_handle,
            sweep_task_id,
            reconcile_task_id,
            persistent_reload_task_id,
        }
    }

    #[inline]
    pub(super) fn sender(&self) -> mpsc::Sender<ManagerCommand> {
        self.tx.clone()
    }

    pub(super) async fn shutdown(mut self, cleanup: bool) {
        // Stop periodic producers before signaling the priority shutdown
        // channel. The worker then drops queued normal commands instead of
        // draining route writes ahead of cleanup.
        if let Some(task_id) = self.sweep_task_id.take() {
            task_center::stop_task(task_id).await;
        }
        if let Some(task_id) = self.reconcile_task_id.take() {
            task_center::stop_task(task_id).await;
        }
        if let Some(task_id) = self.persistent_reload_task_id.take() {
            task_center::stop_task(task_id).await;
        }

        let (done_tx, done_rx) = oneshot::channel::<()>();
        let shutdown_requested = self.shutdown_tx.take().is_some_and(|tx| {
            tx.send(ShutdownRequest {
                cleanup,
                done: done_tx,
            })
            .is_ok()
        });
        if shutdown_requested {
            // `cleanup_on_shutdown` is an explicit ownership contract. Route
            // deletion is serialized and can legitimately take longer than a
            // short fixed deadline for a large table or a slow RouterOS API.
            // Per-command API timeouts still bound individual operations; do
            // not abort the worker and silently leave managed routes behind.
            let _ = done_rx.await;
        }
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.await;
        }
    }
}

#[derive(Debug)]
pub(super) struct RouteManager {
    api: Arc<dyn MikrotikApi>,
    cfg: RouteManagerConfig,
    metrics: Option<Arc<RosRouteMetrics>>,
    persistent_ips: AHashSet<String>,
    pub(super) domain_bindings: AHashMap<String, DomainBinding>,
    pub(super) routes: AHashMap<RouteKey, RouteEntry>,
    pending_observations: VecDeque<PendingObservation>,
    /// Earliest next conntrack check for a deferred route deletion. This keeps
    /// repeated DNS withdrawals from turning into repeated RouterOS reads.
    connection_retry_after: AHashMap<RouteKey, u64>,
    initialized: bool,
}

impl RouteManager {
    pub(super) fn new(api: Arc<dyn MikrotikApi>, cfg: RouteManagerConfig) -> Self {
        Self {
            api,
            persistent_ips: cfg.persistent_ips.clone(),
            cfg,
            metrics: None,
            domain_bindings: AHashMap::new(),
            routes: AHashMap::new(),
            pending_observations: VecDeque::new(),
            connection_retry_after: AHashMap::new(),
            initialized: false,
        }
    }

    pub(super) fn with_metrics(
        api: Arc<dyn MikrotikApi>,
        cfg: RouteManagerConfig,
        metrics: Arc<RosRouteMetrics>,
    ) -> Self {
        let mut manager = Self::new(api, cfg);
        manager.metrics = Some(metrics);
        manager
    }

    async fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Local state may contain a partially replayed observation from a
        // previous failed initialization attempt. Prune it before retrying.
        let now = unix_now();
        self.prune_expired_local_state(now);

        // One-time bootstrap:
        // 1) transport healthcheck
        // 2) validate configured gateways against RouterOS
        // 3) seed persistent routes
        // 4) reconcile local state from RouterOS
        self.api.healthcheck().await?;
        self.validate_gateways().await?;
        self.ensure_persistent_routes(now);
        let mut first_error = self.reconcile_from_router_inner().await?;
        if !self.pending_observations.is_empty() {
            self.replay_pending_observations();
            let replay_now = unix_now();
            self.prune_expired_local_state(replay_now);
            if let Err(error) = self.sync_routes(replay_now).await {
                first_error.get_or_insert(error);
            }
            self.pending_observations.clear();
        }
        // Transport and table reads above are required for a valid baseline.
        // Per-route conflicts are isolated by the sync loops, so they must not
        // keep unrelated observations in the pre-initialization backlog.
        self.initialized = true;
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    #[inline]
    fn effective_expiry(&self, ttl_secs: u32, now: u64) -> u64 {
        match self.cfg.fixed_ttl {
            // Match ros_address_list: zero means the dynamic entry has no
            // time-based expiry. A later answer for the same domain can still
            // withdraw its reference.
            Some(0) => u64::MAX,
            Some(ttl) => now.saturating_add(u64::from(ttl)),
            None => now.saturating_add(u64::from(
                ttl_secs.clamp(self.cfg.min_ttl, self.cfg.max_ttl),
            )),
        }
    }

    #[inline]
    fn recovered_expiry(&self, meta: &RouteCommentMeta) -> u64 {
        if meta.kind == RouteCommentKind::Persistent {
            return meta.expires_at_unix;
        }

        let policy_cap = match self.cfg.fixed_ttl {
            Some(0) => return meta.expires_at_unix,
            Some(ttl) => meta.last_refresh_unix.saturating_add(u64::from(ttl)),
            None => meta
                .last_refresh_unix
                .saturating_add(u64::from(self.cfg.max_ttl)),
        };
        meta.expires_at_unix.min(policy_cap)
    }

    fn recovery_candidate_rank(
        &self,
        key: &RouteKey,
        meta: &RouteCommentMeta,
        now: u64,
    ) -> (bool, bool, bool, u64, u64) {
        let expects_persistent = self.routes.get(key).is_some_and(|route| route.persistent);
        let expires_at = self.recovered_expiry(meta);
        let recoverable = match meta.kind {
            RouteCommentKind::Dynamic => expires_at > now,
            RouteCommentKind::Persistent => expects_persistent,
            RouteCommentKind::Mixed => expects_persistent || expires_at > now,
        };
        let desired_kind = if expects_persistent {
            matches!(
                meta.kind,
                RouteCommentKind::Persistent | RouteCommentKind::Mixed
            )
        } else {
            matches!(
                meta.kind,
                RouteCommentKind::Dynamic | RouteCommentKind::Mixed
            )
        };
        (
            recoverable,
            desired_kind,
            meta.kind.has_dynamic_ownership() && expires_at > now,
            expires_at,
            meta.last_refresh_unix,
        )
    }

    #[inline]
    fn comment_refresh_due(route: &RouteEntry, now: u64) -> bool {
        let Some(synced_expiry) = route.synced_expires_at_unix else {
            return true;
        };
        let desired_expiry = RouteCommentCodec::expires_at_unix(route);
        if synced_expiry == desired_expiry {
            return false;
        }
        if desired_expiry == u64::MAX {
            return true;
        }
        if synced_expiry == u64::MAX || desired_expiry < synced_expiry {
            return true;
        }

        let desired_window = desired_expiry.saturating_sub(now);
        let refresh_lead = (desired_window / 2).clamp(1, MAX_COMMENT_REFRESH_INTERVAL_SECS);
        synced_expiry <= now.saturating_add(refresh_lead)
    }

    #[inline]
    fn gateway_for(&self, family: RouteFamily) -> Option<&str> {
        match family {
            RouteFamily::Ipv4 => self.cfg.gateway4.as_deref(),
            RouteFamily::Ipv6 => self.cfg.gateway6.as_deref(),
        }
    }

    async fn validate_gateways(&self) -> Result<()> {
        if let Some(gateway) = self.cfg.gateway4.as_deref() {
            let nonce = validation_nonce();
            let key =
                validation_route_key(RouteFamily::Ipv4, self.cfg.routing_table.as_str(), nonce);
            let comment = validation_comment(
                self.cfg.comment_prefix.as_str(),
                self.cfg.plugin_tag.as_str(),
                RouteFamily::Ipv4,
                nonce,
            );
            self.api
                .validate_route_config(&key, gateway, self.cfg.distance, &comment)
                .await
                .map_err(|e| {
                    DnsError::plugin(format!(
                        "ros_route gateway4 validation failed for '{gateway}': {e}"
                    ))
                })?;
        }

        if let Some(gateway) = self.cfg.gateway6.as_deref() {
            let nonce = validation_nonce();
            let key =
                validation_route_key(RouteFamily::Ipv6, self.cfg.routing_table.as_str(), nonce);
            let comment = validation_comment(
                self.cfg.comment_prefix.as_str(),
                self.cfg.plugin_tag.as_str(),
                RouteFamily::Ipv6,
                nonce,
            );
            self.api
                .validate_route_config(&key, gateway, self.cfg.distance, &comment)
                .await
                .map_err(|e| {
                    DnsError::plugin(format!(
                        "ros_route gateway6 validation failed for '{gateway}': {e}"
                    ))
                })?;
        }

        Ok(())
    }

    fn ensure_persistent_routes(&mut self, now: u64) {
        let mut desired_keys = AHashSet::new();
        let persistent_ips = self.persistent_ips.iter().cloned().collect::<Vec<_>>();
        for cidr in persistent_ips {
            let Some((ip, prefix)) = parse_dst_address(&cidr) else {
                warn!(
                    plugin = %self.cfg.plugin_tag,
                    route = %cidr,
                    "ros_route persistent route parse failed, skipping"
                );
                continue;
            };
            let family = RouteFamily::from_ip(ip);
            if !family.is_valid_prefix(prefix) {
                warn!(
                    plugin = %self.cfg.plugin_tag,
                    route = %cidr,
                    "ros_route persistent route prefix is invalid for family, skipping"
                );
                continue;
            }
            let Some(gateway) = self.gateway_for(family).map(str::to_string) else {
                continue;
            };
            let Some(key) = RouteKey::new_with_prefix(ip, prefix, self.cfg.routing_table.clone())
            else {
                continue;
            };
            desired_keys.insert(key.clone());

            if let Some(entry) = self.routes.get_mut(&key) {
                let was_pending_delete = matches!(entry.sync_state, SyncState::PendingDelete);
                let mut changed = false;

                if !entry.persistent {
                    entry.persistent = true;
                    changed = true;
                }
                if entry.expires_at_unix != PERSISTENT_EXPIRES_AT_UNIX {
                    entry.expires_at_unix = PERSISTENT_EXPIRES_AT_UNIX;
                    changed = true;
                }
                if entry.gateway != gateway {
                    entry.gateway = gateway.clone();
                    changed = true;
                }
                if entry.distance != self.cfg.distance {
                    entry.distance = self.cfg.distance;
                    changed = true;
                }
                if entry.domains.is_empty() && entry.comment_domain != PERSISTENT_COMMENT_DOMAIN {
                    entry.comment_domain = PERSISTENT_COMMENT_DOMAIN.to_string();
                    changed = true;
                }

                if entry.router_id.is_none() {
                    if !matches!(entry.sync_state, SyncState::PendingCreate) {
                        entry.sync_state = SyncState::PendingCreate;
                        changed = true;
                    }
                } else if matches!(entry.sync_state, SyncState::PendingDelete)
                    || (changed && matches!(entry.sync_state, SyncState::Synced))
                {
                    entry.sync_state = SyncState::Dirty;
                    changed = true;
                }

                if changed {
                    entry.last_refresh_unix = now;
                }
                if was_pending_delete {
                    self.connection_retry_after.remove(&key);
                }
                continue;
            }

            self.routes.insert(
                key.clone(),
                RouteEntry {
                    key,
                    gateway,
                    distance: self.cfg.distance,
                    domains: AHashSet::new(),
                    comment_domain: PERSISTENT_COMMENT_DOMAIN.to_string(),
                    domain_expiries: AHashMap::new(),
                    ref_count: 0,
                    persistent: true,
                    expires_at_unix: PERSISTENT_EXPIRES_AT_UNIX,
                    last_refresh_unix: now,
                    synced_expires_at_unix: None,
                    router_id: None,
                    recovered_ownership_incomplete: false,
                    sync_state: SyncState::PendingCreate,
                },
            );
        }

        // Remove typed persistent ownership from routes that are no longer
        // configured by persistent IP sources (e.g. file content changed).
        let persistent_keys = self
            .routes
            .iter()
            .filter_map(|(key, entry)| {
                if entry.persistent {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for key in persistent_keys {
            if desired_keys.contains(&key) {
                continue;
            }
            let Some(entry) = self.routes.get_mut(&key) else {
                continue;
            };
            entry.persistent = false;
            entry.last_refresh_unix = now;
            if entry.comment_domain == PERSISTENT_COMMENT_DOMAIN {
                entry.comment_domain = select_comment_domain(&entry.domains, false);
            }

            if entry.ref_count == 0 {
                entry.expires_at_unix = now;
                entry.sync_state = SyncState::PendingDelete;
            } else {
                entry.expires_at_unix =
                    entry.domain_expiries.values().copied().max().unwrap_or(now);
                if matches!(entry.sync_state, SyncState::Synced) {
                    entry.sync_state = SyncState::Dirty;
                }
            }
        }
    }

    fn apply_observation(
        &mut self,
        domain: String,
        replace_scope: ObservationScope,
        addrs: Vec<ObservedAddr>,
        now: u64,
    ) -> Vec<RouteKey> {
        let mut touched_keys = AHashSet::new();
        // Deduplicate answer IPs and keep max ttl per IP for this observation.
        let mut dedup_expiries = AHashMap::<IpAddr, u64>::new();
        for observed in addrs {
            let family = RouteFamily::from_ip(observed.addr);
            if self.gateway_for(family).is_none() {
                continue;
            }
            let expires_at_unix = self.effective_expiry(observed.ttl_secs.max(1), now);
            dedup_expiries
                .entry(observed.addr)
                .and_modify(|existing| *existing = (*existing).max(expires_at_unix))
                .or_insert(expires_at_unix);
        }
        let mut binding = self
            .domain_bindings
            .remove(&domain)
            .unwrap_or_else(|| DomainBinding {
                domain: domain.clone(),
                ips: AHashSet::new(),
                ip_expiries: AHashMap::new(),
                expires_at_unix: 0,
                last_refresh_unix: now,
            });
        let removed_ips = binding
            .ips
            .iter()
            .filter(|ip| replace_scope.contains(**ip) && !dedup_expiries.contains_key(ip))
            .copied()
            .collect::<Vec<_>>();
        for ip in &removed_ips {
            binding.ips.remove(ip);
            binding.ip_expiries.remove(ip);
        }
        for ip in removed_ips {
            if let Some(key) = self.detach_domain_from_route(&domain, ip, now) {
                touched_keys.insert(key);
            }
        }

        for (ip, expiry) in &dedup_expiries {
            binding.ips.insert(*ip);
            binding.ip_expiries.insert(*ip, *expiry);
            if let Some(key) = self.attach_or_refresh_route(&domain, *ip, *expiry, now) {
                touched_keys.insert(key);
            }
        }

        if !binding.ips.is_empty() {
            binding.expires_at_unix = binding.ip_expiries.values().copied().max().unwrap_or(now);
            binding.last_refresh_unix = now;
            self.domain_bindings.insert(domain, binding);
        }

        touched_keys.into_iter().collect()
    }

    fn queue_pending_observation(
        &mut self,
        domain: String,
        replace_scope: ObservationScope,
        mut addrs: Vec<ObservedAddr>,
        negative_ttl_secs: Option<u32>,
        observed_at_unix: u64,
    ) {
        let name_level_negative = replace_scope == ObservationScope::Both && addrs.is_empty();
        if name_level_negative {
            // A name-level denial supersedes every earlier replacement for the
            // same name, regardless of family.
            self.pending_observations
                .retain(|observation| observation.domain != domain);
        } else {
            // Any later response other than NXDOMAIN proves that a previously
            // queued name-level denial is stale for the whole name, including
            // its opposite-family entry. NODATA only scopes its own family,
            // but it still proves the name exists and cannot coexist with an
            // older NXDOMAIN.
            self.pending_observations.retain(|observation| {
                observation.domain != domain || !observation.name_level_negative
            });
        }

        // Keep only the newest replacement for a (domain, scope) pair. Merely
        // dropping older entries is not sufficient because an A response may
        // also carry additive AAAA answers (and vice versa). Carry forward
        // those opposite-family additions unless a later queued observation
        // has already replaced that family.
        let mut carried_expiries = AHashMap::<IpAddr, u64>::new();
        self.pending_observations.retain(|observation| {
            if observation.domain != domain {
                return true;
            }
            if observation.replace_scope == replace_scope {
                for observed in &observation.addrs {
                    if replace_scope.contains(observed.addr) {
                        continue;
                    }
                    let expires_at = observation
                        .observed_at_unix
                        .saturating_add(u64::from(observed.ttl_secs));
                    carried_expiries
                        .entry(observed.addr)
                        .and_modify(|current| *current = (*current).max(expires_at))
                        .or_insert(expires_at);
                }
                return false;
            }

            // A retained replacement after an older same-scope observation
            // is authoritative for its family, including an empty NODATA set.
            carried_expiries.retain(|ip, _| !observation.replace_scope.contains(*ip));
            true
        });
        let current_ips = addrs
            .iter()
            .map(|observed| observed.addr)
            .collect::<AHashSet<_>>();
        for (addr, expires_at) in carried_expiries {
            if current_ips.contains(&addr) {
                continue;
            }
            let remaining = expires_at.saturating_sub(observed_at_unix);
            if remaining == 0 {
                continue;
            }
            addrs.push(ObservedAddr {
                addr,
                ttl_secs: u32::try_from(remaining).unwrap_or(u32::MAX),
            });
        }

        let replay_until_unix = if addrs.is_empty() {
            // A negative response without an SOA is not safely cacheable, and
            // an explicit zero TTL expires immediately.
            observed_at_unix.saturating_add(u64::from(negative_ttl_secs.unwrap_or(0)))
        } else {
            addrs
                .iter()
                .map(|observed| observed_at_unix.saturating_add(u64::from(observed.ttl_secs)))
                .max()
                .unwrap_or(observed_at_unix)
        };
        if self.pending_observations.len() >= MAX_PENDING_OBSERVATIONS {
            self.prune_pending_observations(unix_now());
        }
        if self.pending_observations.len() >= MAX_PENDING_OBSERVATIONS {
            warn!(
                plugin = %self.cfg.plugin_tag,
                domain = %domain,
                ?replace_scope,
                capacity = MAX_PENDING_OBSERVATIONS,
                "ros_route pending observation backlog is full, observation dropped"
            );
            return;
        }
        self.pending_observations.push_back(PendingObservation {
            domain,
            replace_scope,
            addrs,
            observed_at_unix,
            replay_until_unix,
            name_level_negative,
        });
    }

    fn prune_pending_observations(&mut self, now: u64) {
        self.pending_observations
            .retain(|observation| observation.replay_until_unix > now);
    }

    fn replay_pending_observations(&mut self) {
        let now = unix_now();
        let pending = self
            .pending_observations
            .iter()
            .filter(|observation| observation.replay_until_unix > now)
            .cloned()
            .collect::<Vec<_>>();
        for observation in pending {
            self.apply_observation(
                observation.domain,
                observation.replace_scope,
                observation.addrs,
                observation.observed_at_unix,
            );
        }
    }

    fn attach_or_refresh_route(
        &mut self,
        domain: &str,
        ip: IpAddr,
        expires_at: u64,
        now: u64,
    ) -> Option<RouteKey> {
        let key = RouteKey::new(ip, self.cfg.routing_table.clone());
        if let Some(entry) = self.routes.get_mut(&key) {
            let was_pending_delete = matches!(entry.sync_state, SyncState::PendingDelete);
            let inserted = entry.domains.insert(domain.to_string());
            let comment_domain_changed = inserted
                && (entry.ref_count == 0
                    || entry.comment_domain.is_empty()
                    || entry.comment_domain == PERSISTENT_COMMENT_DOMAIN);
            if comment_domain_changed {
                entry.comment_domain = domain.to_string();
            }
            if inserted {
                entry.ref_count = entry.ref_count.saturating_add(1);
            }
            entry.domain_expiries.insert(domain.to_string(), expires_at);
            let known_expiry = entry
                .domain_expiries
                .values()
                .copied()
                .max()
                .unwrap_or(expires_at);
            entry.expires_at_unix = if entry.persistent {
                PERSISTENT_EXPIRES_AT_UNIX
            } else if entry.recovered_ownership_incomplete {
                entry.expires_at_unix.max(known_expiry)
            } else {
                known_expiry
            };
            if entry.router_id.is_none() {
                entry.sync_state = SyncState::PendingCreate;
            } else if matches!(entry.sync_state, SyncState::PendingDelete)
                || inserted
                || comment_domain_changed
                || (matches!(entry.sync_state, SyncState::Synced)
                    && Self::comment_refresh_due(entry, now))
            {
                entry.sync_state = SyncState::Dirty;
            }
            if was_pending_delete {
                self.connection_retry_after.remove(&key);
            }
            return Some(key);
        }

        let family = RouteFamily::from_ip(ip);
        let gateway = self.gateway_for(family).map(str::to_string)?;
        let mut domains = AHashSet::new();
        domains.insert(domain.to_string());
        let mut domain_expiries = AHashMap::new();
        domain_expiries.insert(domain.to_string(), expires_at);

        self.routes.insert(
            key.clone(),
            RouteEntry {
                key: key.clone(),
                gateway,
                distance: self.cfg.distance,
                domains,
                comment_domain: domain.to_string(),
                domain_expiries,
                ref_count: 1,
                persistent: false,
                expires_at_unix: expires_at,
                last_refresh_unix: now,
                synced_expires_at_unix: None,
                router_id: None,
                recovered_ownership_incomplete: false,
                sync_state: SyncState::PendingCreate,
            },
        );
        Some(key)
    }

    fn detach_domain_from_route(&mut self, domain: &str, ip: IpAddr, now: u64) -> Option<RouteKey> {
        let key = RouteKey::new(ip, self.cfg.routing_table.clone());
        let entry = self.routes.get_mut(&key)?;

        if !entry.domains.remove(domain) {
            return None;
        }

        let preserve_recovered_route = entry.recovered_ownership_incomplete
            && entry.ref_count == 1
            && entry.expires_at_unix != u64::MAX
            && entry.expires_at_unix > now;
        entry.domain_expiries.remove(domain);
        entry.ref_count = entry.ref_count.saturating_sub(1);
        if preserve_recovered_route {
            // A RouterOS comment stores only one representative qname. After a
            // restart, withdrawing that qname is not proof that no other
            // domains still reference the same IP. Keep the remote metadata
            // unchanged and let its persisted max expiry retire the route.
            return Some(key);
        }

        entry.last_refresh_unix = now;
        if entry.comment_domain == domain || entry.comment_domain.is_empty() {
            entry.comment_domain = select_comment_domain(&entry.domains, entry.persistent);
        }

        if entry.ref_count == 0 {
            if entry.persistent {
                entry.comment_domain = PERSISTENT_COMMENT_DOMAIN.to_string();
                entry.expires_at_unix = PERSISTENT_EXPIRES_AT_UNIX;
                if matches!(entry.sync_state, SyncState::Synced) {
                    entry.sync_state = SyncState::Dirty;
                }
            } else {
                entry.expires_at_unix = now;
                entry.sync_state = SyncState::PendingDelete;
            }
        } else {
            let known_expiry = entry.domain_expiries.values().copied().max().unwrap_or(now);
            entry.expires_at_unix = if entry.persistent {
                PERSISTENT_EXPIRES_AT_UNIX
            } else if entry.recovered_ownership_incomplete {
                entry.expires_at_unix.max(known_expiry)
            } else {
                known_expiry
            };
            if matches!(entry.sync_state, SyncState::Synced) {
                entry.sync_state = SyncState::Dirty;
            }
        }
        Some(key)
    }

    fn expire_domain_bindings(&mut self, now: u64) {
        let domains = self.domain_bindings.keys().cloned().collect::<Vec<_>>();
        for domain in domains {
            let mut to_remove = Vec::new();
            let mut remove_binding = false;

            if let Some(binding) = self.domain_bindings.get_mut(&domain) {
                if binding.expires_at_unix <= now {
                    to_remove.extend(binding.ips.iter().copied());
                } else {
                    for (ip, exp) in &binding.ip_expiries {
                        if *exp <= now {
                            to_remove.push(*ip);
                        }
                    }
                }

                for ip in &to_remove {
                    binding.ips.remove(ip);
                    binding.ip_expiries.remove(ip);
                }
                binding.expires_at_unix = binding.ip_expiries.values().copied().max().unwrap_or(0);
                remove_binding = binding.ips.is_empty();
            }

            for ip in &to_remove {
                self.detach_domain_from_route(&domain, *ip, now);
            }
            if remove_binding {
                self.domain_bindings.remove(&domain);
            }
        }
    }

    fn update_route_expiration(&mut self, now: u64) {
        for route in self.routes.values_mut() {
            if route.persistent {
                if route.expires_at_unix != PERSISTENT_EXPIRES_AT_UNIX {
                    route.expires_at_unix = PERSISTENT_EXPIRES_AT_UNIX;
                    if matches!(route.sync_state, SyncState::Synced) {
                        route.sync_state = SyncState::Dirty;
                    }
                }
                continue;
            }
            if route.ref_count == 0 {
                if route.expires_at_unix <= now {
                    route.sync_state = SyncState::PendingDelete;
                }
                continue;
            }

            let known_expiry = route.domain_expiries.values().copied().max().unwrap_or(now);
            let max_exp = if route.recovered_ownership_incomplete {
                route.expires_at_unix.max(known_expiry)
            } else {
                known_expiry
            };
            if max_exp != route.expires_at_unix {
                route.expires_at_unix = max_exp;
                if matches!(route.sync_state, SyncState::Synced) {
                    route.sync_state = SyncState::Dirty;
                }
            }
        }
    }

    fn prune_expired_local_state(&mut self, now: u64) {
        self.expire_domain_bindings(now);
        self.update_route_expiration(now);
    }

    fn recover_domain_binding(
        &mut self,
        domain: String,
        ip: IpAddr,
        expires_at_unix: u64,
        last_refresh_unix: u64,
    ) {
        let binding = self
            .domain_bindings
            .entry(domain.clone())
            .or_insert_with(|| DomainBinding {
                domain,
                ips: AHashSet::new(),
                ip_expiries: AHashMap::new(),
                expires_at_unix,
                last_refresh_unix,
            });
        binding.ips.insert(ip);
        binding.ip_expiries.insert(ip, expires_at_unix);
        binding.expires_at_unix = binding
            .ip_expiries
            .values()
            .copied()
            .max()
            .unwrap_or(expires_at_unix);
        binding.last_refresh_unix = binding.last_refresh_unix.max(last_refresh_unix);
    }

    async fn sync_routes(&mut self, now: u64) -> Result<()> {
        let keys = self.routes.keys().cloned().collect::<Vec<_>>();
        self.sync_route_keys(keys, now).await
    }

    async fn sync_route_keys(&mut self, keys: Vec<RouteKey>, now: u64) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // Snapshot first, then pipeline upserts in bounded batches. A DNS
        // answer can contain many CDN addresses; serial writes amplify RouterOS
        // round-trip latency, while an unbounded batch can exhaust the client
        // response buffer.
        let mut first_error = None;
        let mut pending_upserts = Vec::new();
        let mut pending_deletes = Vec::new();
        for key in keys {
            let Some(entry_snapshot) = self.routes.get(&key).cloned() else {
                first_error.get_or_insert_with(|| {
                    DnsError::plugin("ros_route route state disappeared during sync")
                });
                continue;
            };

            match entry_snapshot.sync_state {
                SyncState::PendingCreate | SyncState::Dirty if entry_snapshot.has_owners() => {
                    let mut comment_snapshot = entry_snapshot.clone();
                    comment_snapshot.last_refresh_unix = now;
                    let comment = RouteCommentCodec::encode(
                        &self.cfg.comment_prefix,
                        &self.cfg.plugin_tag,
                        &comment_snapshot,
                    );
                    pending_upserts.push((key, entry_snapshot, comment));
                }
                SyncState::PendingDelete => {
                    pending_deletes.push(entry_snapshot.key);
                }
                _ => {}
            }
        }

        for batch in pending_upserts.chunks(UPSERT_PIPELINE_SIZE) {
            let api = self.api.as_ref();
            let prefix = self.cfg.comment_prefix.as_str();
            let plugin_tag = self.cfg.plugin_tag.as_str();
            let results =
                futures::future::join_all(batch.iter().map(|(_, entry_snapshot, comment)| {
                    api.upsert_host_route(
                        &entry_snapshot.key,
                        &entry_snapshot.gateway,
                        entry_snapshot.distance,
                        comment,
                        prefix,
                        plugin_tag,
                    )
                }))
                .await;

            for ((key, entry_snapshot, _), result) in batch.iter().zip(results) {
                match result {
                    Ok(route_id) => {
                        if let Some(route) = self.routes.get_mut(key) {
                            route.router_id = Some(route_id);
                            route.sync_state = SyncState::Synced;
                            route.last_refresh_unix = now;
                            route.synced_expires_at_unix =
                                Some(RouteCommentCodec::expires_at_unix(entry_snapshot));
                        }
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        self.sync_pending_deletes(pending_deletes, now, &mut first_error)
            .await;
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn sync_pending_deletes(
        &mut self,
        keys: Vec<RouteKey>,
        now: u64,
        first_error: &mut Option<DnsError>,
    ) {
        let mut ipv4_candidates = Vec::<(RouteKey, RouterRoute)>::new();
        let mut ipv6_candidates = Vec::<(RouteKey, RouterRoute)>::new();
        for key in keys {
            if self.cfg.conntrack_guard && !self.conntrack_check_due(&key, now) {
                continue;
            }

            // Always re-read current ownership. The cached RouterOS id may now
            // belong to a route whose comment was changed by an operator and
            // must not be deleted. A remotely missing row is an operator
            // deletion, so no conntrack check is needed in that case.
            match self
                .api
                .find_route(&key, &self.cfg.comment_prefix, &self.cfg.plugin_tag)
                .await
            {
                Ok(Some(found)) => match found.family {
                    RouteFamily::Ipv4 => ipv4_candidates.push((key, found)),
                    RouteFamily::Ipv6 => ipv6_candidates.push((key, found)),
                },
                Ok(None) => {
                    self.routes.remove(&key);
                    self.connection_retry_after.remove(&key);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        for (family, family_candidates) in [
            (RouteFamily::Ipv4, ipv4_candidates),
            (RouteFamily::Ipv6, ipv6_candidates),
        ] {
            if family_candidates.is_empty() {
                continue;
            }

            let active_mask = if self.cfg.conntrack_guard {
                match self
                    .active_connection_mask(family, &family_candidates)
                    .await
                {
                    Ok(active) => active,
                    Err(error) => {
                        for (key, _) in &family_candidates {
                            self.defer_connection_check(key, now);
                        }
                        self.record_connection_check_error();
                        warn!(
                            plugin = %self.cfg.plugin_tag,
                            ?family,
                            routes = family_candidates.len(),
                            retry_secs = CONNECTION_GUARD_RETRY_INTERVAL_SECS,
                            err = %error,
                            "ros_route connection-tracking check failed; keeping pending route deletions"
                        );
                        first_error.get_or_insert(error);
                        continue;
                    }
                }
            } else {
                vec![false; family_candidates.len()]
            };

            for ((key, route), has_active_connection) in
                family_candidates.into_iter().zip(active_mask)
            {
                if has_active_connection {
                    self.defer_connection_check(&key, now);
                    self.record_delete_deferred();
                    debug!(
                        plugin = %self.cfg.plugin_tag,
                        dst = %key.dst_address(),
                        retry_secs = CONNECTION_GUARD_RETRY_INTERVAL_SECS,
                        "ros_route deferred route deletion because a matching connection exists"
                    );
                    continue;
                }

                match self.api.delete_route_by_id(&route.id, route.family).await {
                    Ok(()) => {
                        self.routes.remove(&key);
                        self.connection_retry_after.remove(&key);
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
    }

    #[inline]
    fn conntrack_check_due(&self, key: &RouteKey, now: u64) -> bool {
        self.connection_retry_after
            .get(key)
            .is_none_or(|retry_at| *retry_at <= now)
    }

    #[inline]
    fn defer_connection_check(&mut self, key: &RouteKey, now: u64) {
        self.connection_retry_after.insert(
            key.clone(),
            now.saturating_add(CONNECTION_GUARD_RETRY_INTERVAL_SECS),
        );
    }

    async fn active_connection_mask(
        &self,
        family: RouteFamily,
        candidates: &[(RouteKey, RouterRoute)],
    ) -> Result<Vec<bool>> {
        let has_cidr = candidates
            .iter()
            .any(|(key, _)| key.prefix != family.prefix());
        if has_cidr {
            // RouterOS API query words only support exact property matching.
            // A CIDR therefore needs one destination-only snapshot and local
            // containment checks. Sort destinations once so every route uses
            // a logarithmic range lookup rather than scanning the whole table.
            let mut active_values = self
                .api
                .connection_destinations(family, &[])
                .await?
                .into_iter()
                .map(ip_numeric_value)
                .collect::<Vec<_>>();
            active_values.sort_unstable();
            return Ok(candidates
                .iter()
                .map(|(key, _)| sorted_destinations_cover_route(&active_values, key))
                .collect());
        }

        let destinations = candidates.iter().map(|(key, _)| key.ip).collect::<Vec<_>>();
        let mut active = AHashSet::new();
        for chunk in destinations.chunks(CONNECTION_QUERY_BATCH_SIZE) {
            active.extend(self.api.connection_destinations(family, chunk).await?);
        }
        Ok(candidates
            .iter()
            .map(|(key, _)| active.contains(&key.ip))
            .collect())
    }

    #[inline]
    fn record_delete_deferred(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .delete_deferred_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[inline]
    fn record_connection_check_error(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .connection_check_error_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn reconcile_from_router_inner(&mut self) -> Result<Option<DnsError>> {
        // Reconcile algorithm:
        // 1) classify every exact route key as owned or foreign
        // 2) choose the best owned survivor and prune duplicates
        // 3) recover managed rows under the current TTL policy
        // 4) accept missing synced dynamic rows as operator deletions, while preserving
        //    pending writes and configured persistent routes
        // 5) execute one sync pass
        let now = unix_now();
        let rows = self
            .api
            .list_managed_routes(
                &self.cfg.routing_table,
                self.cfg.gateway4.is_some(),
                self.cfg.gateway6.is_some(),
            )
            .await?;
        let mut owned_candidates =
            AHashMap::<RouteKey, Vec<(RouterRoute, RouteCommentMeta)>>::new();
        let mut foreign_keys = AHashSet::new();
        let mut first_error = None;

        for route in rows {
            if is_default_route_dst(&route.dst_address) {
                continue;
            }

            let Some((ip, prefix)) = parse_dst_address(&route.dst_address) else {
                continue;
            };
            let family = RouteFamily::from_ip(ip);
            if !family.is_valid_prefix(prefix) {
                continue;
            }
            let Some(key) = RouteKey::new_with_prefix(ip, prefix, self.cfg.routing_table.clone())
            else {
                continue;
            };

            let meta = if let Some(comment) = route.comment.as_deref() {
                if owned_comment_has_kind(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                    comment,
                    COMMENT_KIND_GATEWAY_CHECK,
                ) {
                    if let Err(error) = self.api.delete_route_by_id(&route.id, route.family).await {
                        warn!(
                            plugin = %self.cfg.plugin_tag,
                            route_id = %route.id,
                            err = %error,
                            "ros_route failed to remove stale gateway validation route"
                        );
                        first_error.get_or_insert(error);
                    }
                    continue;
                }

                match RouteCommentCodec::decode(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                    route.family,
                    &route.dst_address,
                    comment,
                ) {
                    Ok(Some(meta)) => Some(meta),
                    Ok(None) => None,
                    Err(error) => {
                        warn!(
                            plugin = %self.cfg.plugin_tag,
                            route_id = %route.id,
                            err = %error,
                            "ros_route route comment parse failed, treating route as foreign"
                        );
                        None
                    }
                }
            } else {
                None
            };
            let Some(meta) = meta else {
                foreign_keys.insert(key);
                continue;
            };
            if meta.family != family || meta.ip != ip {
                warn!(
                    plugin = %self.cfg.plugin_tag,
                    route_id = %route.id,
                    dst = %route.dst_address,
                    "ros_route route comment metadata mismatches route dst, treating route as foreign"
                );
                foreign_keys.insert(key);
                continue;
            }

            // A route owned by this plugin must not survive after its address
            // family is disabled in the new configuration. It cannot be
            // refreshed safely because there is no configured gateway.
            if self.gateway_for(family).is_none() {
                if let Err(error) = self.api.delete_route_by_id(&route.id, route.family).await {
                    warn!(
                        plugin = %self.cfg.plugin_tag,
                        route_id = %route.id,
                        err = %error,
                        "ros_route failed to remove owned route for disabled address family"
                    );
                    first_error.get_or_insert(error);
                }
                self.routes.remove(&key);
                self.connection_retry_after.remove(&key);
                continue;
            }

            owned_candidates.entry(key).or_default().push((route, meta));
        }

        let mut seen_keys = AHashSet::new();
        for (key, mut candidates) in owned_candidates {
            let mut survivor_index = 0;
            for index in 1..candidates.len() {
                let candidate_rank = self.recovery_candidate_rank(&key, &candidates[index].1, now);
                let survivor_rank =
                    self.recovery_candidate_rank(&key, &candidates[survivor_index].1, now);
                if candidate_rank > survivor_rank {
                    survivor_index = index;
                }
            }
            let (route, meta) = candidates.swap_remove(survivor_index);
            for (duplicate, _) in candidates {
                if let Err(error) = self
                    .api
                    .delete_route_by_id(&duplicate.id, duplicate.family)
                    .await
                {
                    warn!(
                        plugin = %self.cfg.plugin_tag,
                        route_id = %duplicate.id,
                        dst = %duplicate.dst_address,
                        err = %error,
                        "ros_route failed to remove duplicate owned route"
                    );
                    first_error.get_or_insert(error);
                }
            }
            seen_keys.insert(key.clone());

            let family = key.family();
            let ip = key.ip;
            let prefix = key.prefix;
            let recovered_expiry = self.recovered_expiry(&meta);
            let persistent_residue = meta.kind == RouteCommentKind::Persistent;
            let expired = recovered_expiry <= now;
            let recover_dynamic_binding = !expired
                && meta.kind.has_dynamic_ownership()
                && !meta.comment_domain.is_empty()
                && prefix == family.prefix();
            let foreign_conflict = foreign_keys.contains(&key);

            if let Some(existing) = self.routes.get_mut(&key) {
                let mut restored_dynamic_binding = false;
                existing.router_id = Some(route.id.clone());
                existing.synced_expires_at_unix = Some(meta.expires_at_unix);
                if !existing.has_owners() {
                    // A local withdrawal already decided this route must be
                    // deleted. Seeing the still-existing remote row must not
                    // resurrect it from its old comment metadata.
                    if matches!(existing.sync_state, SyncState::PendingDelete) {
                        continue;
                    }
                    existing.comment_domain = meta.comment_domain.clone();
                    existing.expires_at_unix = recovered_expiry;
                    existing.last_refresh_unix = meta.last_refresh_unix;
                    existing.sync_state = if recovered_expiry <= now || persistent_residue {
                        SyncState::PendingDelete
                    } else {
                        SyncState::Synced
                    };
                    if matches!(existing.sync_state, SyncState::PendingDelete) {
                        continue;
                    }
                    let gateway_drift = route.gateway.as_deref() != Some(existing.gateway.as_str());
                    let distance_drift = route.distance != Some(existing.distance);
                    let disabled_drift = route.disabled;
                    let expected_comment = RouteCommentCodec::encode(
                        &self.cfg.comment_prefix,
                        &self.cfg.plugin_tag,
                        existing,
                    );
                    let comment_drift = route.comment.as_deref() != Some(expected_comment.as_str());
                    if gateway_drift
                        || distance_drift
                        || comment_drift
                        || disabled_drift
                        || foreign_conflict
                    {
                        existing.sync_state = SyncState::Dirty;
                    }
                } else {
                    if recover_dynamic_binding {
                        if existing.domains.insert(meta.comment_domain.clone()) {
                            existing.ref_count = existing.ref_count.saturating_add(1);
                        }
                        existing
                            .domain_expiries
                            .insert(meta.comment_domain.clone(), recovered_expiry);
                        existing.comment_domain = meta.comment_domain.clone();
                        existing.last_refresh_unix =
                            existing.last_refresh_unix.max(meta.last_refresh_unix);
                        existing.recovered_ownership_incomplete = true;
                        restored_dynamic_binding = true;
                        if !existing.persistent {
                            existing.expires_at_unix =
                                existing.expires_at_unix.max(recovered_expiry);
                        }
                    }
                    let gateway_drift = route.gateway.as_deref() != Some(existing.gateway.as_str());
                    let distance_drift = route.distance != Some(existing.distance);
                    let disabled_drift = route.disabled;
                    let expected_comment = RouteCommentCodec::encode(
                        &self.cfg.comment_prefix,
                        &self.cfg.plugin_tag,
                        existing,
                    );
                    let comment_drift = route.comment.as_deref() != Some(expected_comment.as_str());
                    if gateway_drift
                        || distance_drift
                        || comment_drift
                        || disabled_drift
                        || foreign_conflict
                        || matches!(existing.sync_state, SyncState::PendingCreate)
                    {
                        existing.sync_state = SyncState::Dirty;
                    }
                }
                if restored_dynamic_binding {
                    self.recover_domain_binding(
                        meta.comment_domain.clone(),
                        ip,
                        recovered_expiry,
                        meta.last_refresh_unix,
                    );
                }
                continue;
            }

            let Some(gateway) = self.gateway_for(family).map(str::to_string) else {
                continue;
            };
            let mut domains = AHashSet::new();
            let mut domain_expiries = AHashMap::new();
            if recover_dynamic_binding {
                domains.insert(meta.comment_domain.clone());
                domain_expiries.insert(meta.comment_domain.clone(), recovered_expiry);
            }
            let mut entry = RouteEntry {
                key: key.clone(),
                gateway,
                distance: self.cfg.distance,
                domains,
                comment_domain: meta.comment_domain.clone(),
                domain_expiries,
                ref_count: u32::from(recover_dynamic_binding),
                persistent: false,
                expires_at_unix: recovered_expiry,
                last_refresh_unix: meta.last_refresh_unix,
                synced_expires_at_unix: Some(meta.expires_at_unix),
                router_id: Some(route.id.clone()),
                recovered_ownership_incomplete: true,
                sync_state: if expired || persistent_residue {
                    SyncState::PendingDelete
                } else {
                    SyncState::Synced
                },
            };
            if !matches!(entry.sync_state, SyncState::PendingDelete) {
                let gateway_drift = route.gateway.as_deref() != Some(entry.gateway.as_str());
                let distance_drift = route.distance != Some(entry.distance);
                let disabled_drift = route.disabled;
                let expected_comment = RouteCommentCodec::encode(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                    &entry,
                );
                let comment_drift = route.comment.as_deref() != Some(expected_comment.as_str());
                if gateway_drift
                    || distance_drift
                    || comment_drift
                    || disabled_drift
                    || foreign_conflict
                {
                    entry.sync_state = SyncState::Dirty;
                }
            }
            self.routes.insert(key.clone(), entry);
            if recover_dynamic_binding {
                self.recover_domain_binding(
                    meta.comment_domain,
                    ip,
                    recovered_expiry,
                    meta.last_refresh_unix,
                );
            }
        }

        let keys = self.routes.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if self.gateway_for(key.family()).is_none() {
                self.routes.remove(&key);
                self.connection_retry_after.remove(&key);
                continue;
            }
            if seen_keys.contains(&key) {
                continue;
            }
            let Some(route) = self.routes.get_mut(&key) else {
                continue;
            };

            if !route.has_owners() || matches!(route.sync_state, SyncState::PendingDelete) {
                // The remote side already matches the local withdrawal. Drop
                // the local route without issuing a redundant delete request.
                self.routes.remove(&key);
                self.connection_retry_after.remove(&key);
            } else if route.router_id.is_some()
                && matches!(route.sync_state, SyncState::Synced)
                && !route.persistent
            {
                // A previously confirmed dynamic route disappeared without a
                // new local write. Treat that as an operator deletion instead
                // of immediately recreating it. Domain bindings intentionally
                // remain: the next matching DNS observation will attach the IP
                // again and create a fresh route.
                self.routes.remove(&key);
                self.connection_retry_after.remove(&key);
            } else {
                // Pending local writes and explicitly configured persistent
                // routes remain desired state even when no remote row exists.
                route.router_id = None;
                route.synced_expires_at_unix = None;
                route.sync_state = SyncState::PendingCreate;
            }
        }

        if let Err(error) = self.sync_routes(now).await {
            first_error.get_or_insert(error);
        }
        Ok(first_error)
    }

    async fn reconcile_from_router(&mut self) -> Result<()> {
        match self.reconcile_from_router_inner().await? {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) async fn observe_domain(
        &mut self,
        domain: String,
        replace_scope: ObservationScope,
        addrs: Vec<ObservedAddr>,
        negative_ttl_secs: Option<u32>,
        wait_for_sync: bool,
    ) -> Result<()> {
        let now = unix_now();
        if !self.initialized {
            // Preserve the latest replacement per domain/family. Every queued
            // entry retains all answer addresses, and replay is ordered across
            // families so an older response cannot resurrect stale routes.
            self.queue_pending_observation(
                domain.clone(),
                replace_scope,
                addrs.clone(),
                negative_ttl_secs,
                now,
            );
            if !wait_for_sync {
                return Ok(());
            }

            // A synchronous caller needs the current observation applied once
            // even if it could not enter the bounded replay backlog or its DNS
            // TTL is zero/has elapsed while initialization was in progress.
            // Keep the original timestamp so this does not extend the route's
            // effective lifetime beyond the DNS response that triggered it.
            let initialization_result = self.ensure_initialized().await;
            if !self.initialized {
                return initialization_result;
            }

            let touched = self.apply_observation(domain, replace_scope, addrs, now);
            let sync_result = self.sync_route_keys(touched, now).await;
            return match (initialization_result, sync_result) {
                (_, Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Ok(()), Ok(())) => Ok(()),
            };
        }
        let touched = self.apply_observation(domain, replace_scope, addrs, now);
        self.sync_route_keys(touched, now).await
    }

    pub(super) async fn sweep(&mut self) -> Result<()> {
        let now = unix_now();
        self.prune_expired_local_state(now);
        self.ensure_initialized().await?;
        self.ensure_persistent_routes(now);
        self.sync_routes(now).await
    }

    pub(super) async fn update_persistent_ips(&mut self, ips: AHashSet<String>) -> Result<()> {
        // Store desired state before touching RouterOS. If initialization fails,
        // a later reconcile must still apply the latest file contents even when
        // the files themselves have not changed again.
        self.persistent_ips = ips;
        self.ensure_initialized().await?;
        let now = unix_now();
        self.ensure_persistent_routes(now);
        self.update_route_expiration(now);
        self.sync_routes(now).await
    }

    pub(super) async fn reconcile(&mut self) -> Result<()> {
        self.prune_expired_local_state(unix_now());
        self.ensure_initialized().await?;
        self.ensure_persistent_routes(unix_now());
        self.reconcile_from_router().await?;
        Ok(())
    }

    pub(super) async fn shutdown(&mut self, cleanup: bool) -> Result<()> {
        if !cleanup {
            return Ok(());
        }

        // Shutdown cleanup must never initialize or reconcile desired state:
        // doing so could create/refresh routes immediately before deleting
        // them. Read RouterOS directly and remove only rows owned by this
        // plugin namespace.
        let routes = self
            .api
            .list_managed_routes(
                &self.cfg.routing_table,
                self.cfg.gateway4.is_some(),
                self.cfg.gateway6.is_some(),
            )
            .await?;
        for route in routes {
            let comment = route.comment.as_deref().unwrap_or_default();
            let owned_route = RouteCommentCodec::decode(
                &self.cfg.comment_prefix,
                &self.cfg.plugin_tag,
                route.family,
                &route.dst_address,
                comment,
            )
            .ok()
            .flatten()
            .is_some();
            let owned_gateway_check = owned_comment_has_kind(
                &self.cfg.comment_prefix,
                &self.cfg.plugin_tag,
                comment,
                COMMENT_KIND_GATEWAY_CHECK,
            );
            if owned_route || owned_gateway_check {
                self.api.delete_route_by_id(&route.id, route.family).await?;
            }
        }
        self.routes.clear();
        self.domain_bindings.clear();
        self.connection_retry_after.clear();
        Ok(())
    }
}

fn validation_nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn validation_route_key(family: RouteFamily, table: &str, nonce: u128) -> RouteKey {
    let ip = match family {
        RouteFamily::Ipv4 => {
            let third = ((nonce >> 8) & 0xFF) as u8;
            let fourth = match (nonce & 0xFF) as u8 {
                0 => 1,
                value => value,
            };
            IpAddr::V4(Ipv4Addr::new(198, 18, third, fourth))
        }
        RouteFamily::Ipv6 => {
            let seg5 = ((nonce >> 32) & 0xFFFF) as u16;
            let seg6 = ((nonce >> 16) & 0xFFFF) as u16;
            let seg7 = (nonce & 0xFFFF) as u16;
            IpAddr::V6(Ipv6Addr::new(0x2001, 0x0DB8, 0, 0, seg5, seg6, seg7, 1))
        }
    };
    RouteKey::new(ip, table.to_string())
}

fn validation_comment(prefix: &str, plugin_tag: &str, _family: RouteFamily, nonce: u128) -> String {
    let mut out = String::new();
    if !prefix.is_empty() {
        out.push_str(prefix);
        out.push(';');
    }
    out.push_str(COMMENT_FIELD_PLUGIN);
    out.push('=');
    out.push_str(plugin_tag);
    out.push_str(";kind=gateway-check");
    out.push_str(";nonce=");
    out.push_str(&nonce.to_string());
    out
}

fn select_comment_domain(domains: &AHashSet<String>, persistent: bool) -> String {
    domains.iter().min().cloned().unwrap_or_else(|| {
        if persistent {
            PERSISTENT_COMMENT_DOMAIN.to_string()
        } else {
            String::new()
        }
    })
}

async fn run_manager_worker(
    tag: String,
    mut manager: RouteManager,
    mut rx: mpsc::Receiver<ManagerCommand>,
    mut shutdown_rx: oneshot::Receiver<ShutdownRequest>,
) {
    // Single-owner event loop for route state.
    // All cross-map updates are serialized here to keep transitions deterministic.
    loop {
        let command = tokio::select! {
            biased;
            shutdown = &mut shutdown_rx => {
                if let Ok(ShutdownRequest { cleanup, done }) = shutdown {
                    if let Err(e) = manager.shutdown(cleanup).await {
                        warn!(plugin = %tag, err = %e, "ros_route shutdown cleanup failed");
                    }
                    let _ = done.send(());
                }
                break;
            }
            command = rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                command
            }
        };

        match command {
            ManagerCommand::ObserveDomain {
                domain,
                replace_scope,
                addrs,
                negative_ttl_secs,
                wait,
            } => {
                let wait_for_sync = wait.is_some();
                let result = manager
                    .observe_domain(
                        domain,
                        replace_scope,
                        addrs,
                        negative_ttl_secs,
                        wait_for_sync,
                    )
                    .await;
                match (wait, result) {
                    (Some(ch), outcome) => {
                        let _ = ch.send(outcome);
                    }
                    (None, Ok(())) => {}
                    (None, Err(e)) => {
                        warn!(
                            plugin = %tag,
                            err = %e,
                            "ros_route observe failed in async mode"
                        );
                    }
                }
            }
            ManagerCommand::Sweep => {
                if let Err(e) = manager.sweep().await {
                    warn!(
                        plugin = %tag,
                        err = %e,
                        "ros_route periodic sweep failed"
                    );
                }
            }
            ManagerCommand::UpdatePersistentIps { ips } => {
                if let Err(e) = manager.update_persistent_ips(ips).await {
                    warn!(
                        plugin = %tag,
                        err = %e,
                        "ros_route persistent route maintenance failed"
                    );
                }
            }
            ManagerCommand::Reconcile => {
                if let Err(e) = manager.reconcile().await {
                    warn!(
                        plugin = %tag,
                        err = %e,
                        "ros_route periodic reconcile failed"
                    );
                } else {
                    debug!(plugin = %tag, "ros_route reconcile completed");
                }
            }
        }
    }

    debug!(plugin = %tag, "ros_route manager worker exited");
}

fn parse_dst_address(dst: &str) -> Option<(IpAddr, u8)> {
    let (ip_raw, prefix_raw) = dst.split_once('/')?;
    let ip = ip_raw.parse::<IpAddr>().ok()?;
    let prefix = prefix_raw.parse::<u8>().ok()?;
    Some((ip, prefix))
}

/// Return whether an address is covered by a managed route key.
///
/// Dynamic routes are always host prefixes, while `persistent_route` may use
/// CIDRs. Keep the comparison local so conntrack queries can request only
/// `dst-address` and never need RouterOS-side prefix expressions.
#[cfg(test)]
fn route_key_contains_ip(key: &RouteKey, candidate: IpAddr) -> bool {
    if RouteFamily::from_ip(key.ip) != RouteFamily::from_ip(candidate) {
        return false;
    }
    let (start, end) = route_key_numeric_bounds(key);
    let candidate = ip_numeric_value(candidate);
    (start..=end).contains(&candidate)
}

#[inline]
fn ip_numeric_value(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(ip) => u128::from(u32::from(ip)),
        IpAddr::V6(ip) => u128::from(ip),
    }
}

fn route_key_numeric_bounds(key: &RouteKey) -> (u128, u128) {
    let (raw, width, max) = match key.ip {
        IpAddr::V4(ip) => (u128::from(u32::from(ip)), 32u8, u128::from(u32::MAX)),
        IpAddr::V6(ip) => (u128::from(ip), 128u8, u128::MAX),
    };
    let host_bits = width.saturating_sub(key.prefix);
    let mask = if key.prefix == 0 {
        0
    } else {
        (max >> host_bits) << host_bits
    };
    let start = raw & mask;
    (start, start | (max ^ mask))
}

fn sorted_destinations_cover_route(destinations: &[u128], key: &RouteKey) -> bool {
    let (start, end) = route_key_numeric_bounds(key);
    let first_candidate = destinations.partition_point(|destination| *destination < start);
    destinations
        .get(first_candidate)
        .is_some_and(|destination| *destination <= end)
}

pub(super) fn is_default_route_dst(dst: &str) -> bool {
    dst == ROUTE_DEFAULT_V4 || dst == ROUTE_DEFAULT_V6
}

#[inline]
fn unix_now() -> u64 {
    AppClock::now_timestamp() / 1000
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::plugin::executor::ros_route::api::RouterRoute;

    #[derive(Debug)]
    struct NoopApi;

    #[async_trait::async_trait]
    impl MikrotikApi for NoopApi {
        async fn list_managed_routes(
            &self,
            _table: &str,
            _require_ipv4: bool,
            _require_ipv6: bool,
        ) -> Result<Vec<RouterRoute>> {
            unreachable!("this test only mutates local route state")
        }

        async fn find_route(
            &self,
            _key: &RouteKey,
            _comment_prefix: &str,
            _plugin_tag: &str,
        ) -> Result<Option<RouterRoute>> {
            unreachable!("this test only mutates local route state")
        }

        async fn upsert_host_route(
            &self,
            _key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
            _comment_prefix: &str,
            _plugin_tag: &str,
        ) -> Result<String> {
            unreachable!("this test only mutates local route state")
        }

        async fn validate_route_config(
            &self,
            _key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
        ) -> Result<()> {
            unreachable!("this test only mutates local route state")
        }

        async fn delete_route_by_id(&self, _id: &str, _family: RouteFamily) -> Result<()> {
            unreachable!("this test only mutates local route state")
        }

        async fn connection_destinations(
            &self,
            _family: RouteFamily,
            _destinations: &[IpAddr],
        ) -> Result<AHashSet<IpAddr>> {
            unreachable!("this test only mutates local route state")
        }

        async fn healthcheck(&self) -> Result<()> {
            unreachable!("this test only mutates local route state")
        }
    }

    #[derive(Debug, Default)]
    struct MockApiState {
        routes: Vec<RouterRoute>,
        list_requirements: Vec<(bool, bool)>,
        fail_upserts: AHashSet<IpAddr>,
        upsert_attempts: Vec<IpAddr>,
        deleted_ids: Vec<String>,
        active_connection_destinations: AHashSet<IpAddr>,
        connection_queries: Vec<(RouteFamily, Vec<IpAddr>)>,
        fail_connection_check: bool,
        fail_healthcheck: bool,
    }

    #[derive(Debug, Default)]
    struct MockApi {
        state: StdMutex<MockApiState>,
    }

    impl MockApi {
        fn with_state(state: MockApiState) -> Self {
            Self {
                state: StdMutex::new(state),
            }
        }
    }

    #[derive(Debug, Default)]
    struct PipelineApi {
        active: AtomicUsize,
        max_active: AtomicUsize,
        attempts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl MikrotikApi for PipelineApi {
        async fn list_managed_routes(
            &self,
            _table: &str,
            _require_ipv4: bool,
            _require_ipv6: bool,
        ) -> Result<Vec<RouterRoute>> {
            Ok(Vec::new())
        }

        async fn find_route(
            &self,
            _key: &RouteKey,
            _comment_prefix: &str,
            _plugin_tag: &str,
        ) -> Result<Option<RouterRoute>> {
            Ok(None)
        }

        async fn upsert_host_route(
            &self,
            key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
            _comment_prefix: &str,
            _plugin_tag: &str,
        ) -> Result<String> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_active.fetch_max(active, Ordering::AcqRel);
            self.attempts.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(format!("*{}", key.ip))
        }

        async fn validate_route_config(
            &self,
            _key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_route_by_id(&self, _id: &str, _family: RouteFamily) -> Result<()> {
            Ok(())
        }

        async fn connection_destinations(
            &self,
            _family: RouteFamily,
            _destinations: &[IpAddr],
        ) -> Result<AHashSet<IpAddr>> {
            Ok(AHashSet::new())
        }

        async fn healthcheck(&self) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl MikrotikApi for MockApi {
        async fn list_managed_routes(
            &self,
            _table: &str,
            require_ipv4: bool,
            require_ipv6: bool,
        ) -> Result<Vec<RouterRoute>> {
            let mut state = self.state.lock().expect("mock lock");
            state.list_requirements.push((require_ipv4, require_ipv6));
            Ok(state.routes.clone())
        }

        async fn find_route(
            &self,
            key: &RouteKey,
            comment_prefix: &str,
            plugin_tag: &str,
        ) -> Result<Option<RouterRoute>> {
            let owner = format!("{comment_prefix};pg={plugin_tag};");
            Ok(self
                .state
                .lock()
                .expect("mock lock")
                .routes
                .iter()
                .find(|route| {
                    route.dst_address == key.dst_address()
                        && route.routing_table == key.table
                        && route
                            .comment
                            .as_deref()
                            .is_some_and(|comment| comment.starts_with(&owner))
                })
                .cloned())
        }

        async fn upsert_host_route(
            &self,
            key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
            comment_prefix: &str,
            plugin_tag: &str,
        ) -> Result<String> {
            let mut state = self.state.lock().expect("mock lock");
            state.upsert_attempts.push(key.ip);
            if state.fail_upserts.contains(&key.ip) {
                return Err(DnsError::plugin("mock upsert failure"));
            }
            let has_foreign_duplicate = state.routes.iter().any(|route| {
                route.dst_address == key.dst_address()
                    && route.routing_table == key.table
                    && !matches!(
                        RouteCommentCodec::decode(
                            comment_prefix,
                            plugin_tag,
                            route.family,
                            &route.dst_address,
                            route.comment.as_deref().unwrap_or_default(),
                        ),
                        Ok(Some(_))
                    )
            });
            if has_foreign_duplicate {
                return Err(DnsError::plugin("mock foreign route conflict"));
            }
            Ok(format!("*{}", key.ip))
        }

        async fn validate_route_config(
            &self,
            _key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_route_by_id(&self, id: &str, _family: RouteFamily) -> Result<()> {
            let mut state = self.state.lock().expect("mock lock");
            state.deleted_ids.push(id.to_string());
            state.routes.retain(|route| route.id != id);
            Ok(())
        }

        async fn connection_destinations(
            &self,
            family: RouteFamily,
            destinations: &[IpAddr],
        ) -> Result<AHashSet<IpAddr>> {
            let mut state = self.state.lock().expect("mock lock");
            state
                .connection_queries
                .push((family, destinations.to_vec()));
            if state.fail_connection_check {
                return Err(DnsError::plugin("mock connection tracking failure"));
            }
            let active = state
                .active_connection_destinations
                .iter()
                .copied()
                .filter(|ip| {
                    RouteFamily::from_ip(*ip) == family
                        && (destinations.is_empty() || destinations.contains(ip))
                })
                .collect();
            Ok(active)
        }

        async fn healthcheck(&self) -> Result<()> {
            if self.state.lock().expect("mock lock").fail_healthcheck {
                return Err(DnsError::plugin("mock healthcheck failure"));
            }
            Ok(())
        }
    }

    fn manager_config(fixed_ttl: Option<u32>) -> RouteManagerConfig {
        AppClock::start();
        RouteManagerConfig {
            plugin_tag: "route-test".to_string(),
            routing_table: "via_proxy".to_string(),
            gateway4: Some("192.0.2.1".to_string()),
            gateway6: None,
            persistent_ips: AHashSet::new(),
            comment_prefix: "fdns".to_string(),
            distance: 100,
            min_ttl: 60,
            max_ttl: 3600,
            fixed_ttl,
            conntrack_guard: false,
        }
    }

    fn manager_with_timeless_dynamic_routes() -> RouteManager {
        RouteManager::new(Arc::new(NoopApi), manager_config(Some(0)))
    }

    fn pending_observation<'a>(
        manager: &'a RouteManager,
        domain: &str,
        replace_scope: ObservationScope,
    ) -> Option<&'a PendingObservation> {
        manager.pending_observations.iter().find(|observation| {
            observation.domain == domain && observation.replace_scope == replace_scope
        })
    }

    fn has_pending_observation(
        manager: &RouteManager,
        domain: &str,
        replace_scope: ObservationScope,
    ) -> bool {
        pending_observation(manager, domain, replace_scope).is_some()
    }

    fn owned_router_route(id: &str, key: &RouteKey) -> RouterRoute {
        RouterRoute {
            id: id.to_string(),
            family: key.family(),
            dst_address: key.dst_address(),
            routing_table: key.table.clone(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(
                "fdns;pg=route-test;kind=dynamic;dm=example.com.;exp=100;seen=100".to_string(),
            ),
            disabled: false,
        }
    }

    fn pending_delete_entry(key: RouteKey, route_id: &str) -> RouteEntry {
        RouteEntry {
            key,
            gateway: "192.0.2.1".to_string(),
            distance: 100,
            domains: AHashSet::new(),
            comment_domain: String::new(),
            domain_expiries: AHashMap::new(),
            ref_count: 0,
            persistent: false,
            expires_at_unix: 100,
            last_refresh_unix: 100,
            synced_expires_at_unix: Some(100),
            router_id: Some(route_id.to_string()),
            recovered_ownership_incomplete: false,
            sync_state: SyncState::PendingDelete,
        }
    }

    #[test]
    fn unix_now_matches_the_application_timestamp() {
        AppClock::start();
        let app_now = AppClock::now_timestamp() / 1000;
        assert!(unix_now().abs_diff(app_now) <= 1);
    }

    #[test]
    fn route_key_contains_ip_handles_ipv4_and_ipv6_prefixes() {
        let ipv4 = RouteKey::new_with_prefix(
            "198.51.100.0".parse().expect("ipv4 network"),
            24,
            "via_proxy".to_string(),
        )
        .expect("ipv4 route");
        assert!(route_key_contains_ip(
            &ipv4,
            "198.51.100.42".parse().expect("ipv4 member")
        ));
        assert!(!route_key_contains_ip(
            &ipv4,
            "198.51.101.1".parse().expect("ipv4 non-member")
        ));

        let ipv6 = RouteKey::new_with_prefix(
            "2001:db8:42::".parse().expect("ipv6 network"),
            64,
            "via_proxy".to_string(),
        )
        .expect("ipv6 route");
        assert!(route_key_contains_ip(
            &ipv6,
            "2001:db8:42::99".parse().expect("ipv6 member")
        ));
        assert!(!route_key_contains_ip(
            &ipv6,
            "2001:db8:43::1".parse().expect("ipv6 non-member")
        ));
    }

    #[test]
    fn comment_codec_escapes_domain_field_delimiters() {
        let domain = r"escaped\;pg=foreign.example.";
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 19));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let route = RouteEntry {
            key,
            gateway: "192.0.2.1".to_string(),
            distance: 100,
            domains: AHashSet::from_iter([domain.to_string()]),
            comment_domain: domain.to_string(),
            domain_expiries: AHashMap::from_iter([(domain.to_string(), 400)]),
            ref_count: 1,
            persistent: false,
            expires_at_unix: 400,
            last_refresh_unix: 100,
            synced_expires_at_unix: None,
            router_id: None,
            recovered_ownership_incomplete: false,
            sync_state: SyncState::PendingCreate,
        };

        let comment = RouteCommentCodec::encode("fdns", "route-test", &route);
        assert!(comment.contains("dm=escaped%5C%3Bpg%3Dforeign.example."));
        assert_eq!(comment.matches(";pg=").count(), 1);

        let decoded = RouteCommentCodec::decode(
            "fdns",
            "route-test",
            RouteFamily::Ipv4,
            "203.0.113.19/32",
            &comment,
        )
        .expect("decode comment")
        .expect("owned comment");
        assert_eq!(decoded.kind, RouteCommentKind::Dynamic);
        assert_eq!(decoded.comment_domain, domain);
    }

    #[test]
    fn timeless_route_is_withdrawn_when_the_domain_stops_returning_it() {
        let mut manager = manager_with_timeless_dynamic_routes();
        let domain = "example.com.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1));
        let key = RouteKey::new(ip, "via_proxy".to_string());

        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            100,
        );
        let entry = manager.routes.get(&key).expect("route should be tracked");
        assert_eq!(entry.expires_at_unix, u64::MAX);
        assert_eq!(entry.ref_count, 1);

        manager.apply_observation(domain.clone(), ObservationScope::Ipv4, Vec::new(), 101);
        assert!(!manager.domain_bindings.contains_key(&domain));
        let entry = manager
            .routes
            .get(&key)
            .expect("route state should remain for deletion");
        assert_eq!(entry.ref_count, 0);
        assert_eq!(entry.sync_state, SyncState::PendingDelete);
    }

    #[test]
    fn address_family_observations_do_not_withdraw_each_other() {
        let domain = "example.com.".to_string();
        let ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2));
        let ipv6 = "2001:db8::2".parse::<IpAddr>().expect("IPv6 address");
        let ipv4_key = RouteKey::new(ipv4, "via_proxy".to_string());
        let ipv6_key = RouteKey::new(ipv6, "via_proxy".to_string());
        let mut config = manager_config(Some(300));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ipv4,
                ttl_secs: 60,
            }],
            100,
        );
        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv6,
            vec![ObservedAddr {
                addr: ipv6,
                ttl_secs: 60,
            }],
            101,
        );

        let binding = manager
            .domain_bindings
            .get(&domain)
            .expect("dual-stack binding");
        assert_eq!(binding.ips, AHashSet::from_iter([ipv4, ipv6]));
        assert_eq!(manager.routes[&ipv4_key].ref_count, 1);
        assert_eq!(manager.routes[&ipv6_key].ref_count, 1);

        manager.apply_observation(domain.clone(), ObservationScope::Ipv4, Vec::new(), 102);

        let binding = manager
            .domain_bindings
            .get(&domain)
            .expect("IPv6 binding remains");
        assert_eq!(binding.ips, AHashSet::from_iter([ipv6]));
        assert_eq!(manager.routes[&ipv4_key].ref_count, 0);
        assert_eq!(
            manager.routes[&ipv4_key].sync_state,
            SyncState::PendingDelete
        );
        assert_eq!(manager.routes[&ipv6_key].ref_count, 1);
        assert_ne!(
            manager.routes[&ipv6_key].sync_state,
            SyncState::PendingDelete
        );
    }

    #[test]
    fn name_level_withdrawal_removes_both_address_families() {
        let domain = "nxdomain.example.".to_string();
        let ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 25));
        let ipv6 = "2001:db8::25".parse::<IpAddr>().expect("IPv6 address");
        let ipv4_key = RouteKey::new(ipv4, "via_proxy".to_string());
        let ipv6_key = RouteKey::new(ipv6, "via_proxy".to_string());
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        manager.apply_observation(
            domain.clone(),
            ObservationScope::Both,
            vec![
                ObservedAddr {
                    addr: ipv4,
                    ttl_secs: 60,
                },
                ObservedAddr {
                    addr: ipv6,
                    ttl_secs: 60,
                },
            ],
            100,
        );
        manager.apply_observation(domain.clone(), ObservationScope::Both, Vec::new(), 101);

        assert!(!manager.domain_bindings.contains_key(&domain));
        assert_eq!(
            manager.routes[&ipv4_key].sync_state,
            SyncState::PendingDelete
        );
        assert_eq!(
            manager.routes[&ipv6_key].sync_state,
            SyncState::PendingDelete
        );
    }

    #[test]
    fn observation_adds_all_answer_families_but_replaces_only_query_family() {
        let domain = "mixed-answer.example.".to_string();
        let old_ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 51));
        let new_ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 52));
        let old_ipv6 = "2001:db8::51".parse::<IpAddr>().expect("IPv6 address");
        let new_ipv6 = "2001:db8::52".parse::<IpAddr>().expect("IPv6 address");
        let old_ipv4_key = RouteKey::new(old_ipv4, "via_proxy".to_string());
        let old_ipv6_key = RouteKey::new(old_ipv6, "via_proxy".to_string());
        let new_ipv4_key = RouteKey::new(new_ipv4, "via_proxy".to_string());
        let new_ipv6_key = RouteKey::new(new_ipv6, "via_proxy".to_string());
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        manager.apply_observation(
            domain.clone(),
            ObservationScope::Both,
            vec![
                ObservedAddr {
                    addr: old_ipv4,
                    ttl_secs: 60,
                },
                ObservedAddr {
                    addr: old_ipv6,
                    ttl_secs: 60,
                },
            ],
            100,
        );
        manager.apply_observation(
            domain,
            ObservationScope::Ipv4,
            vec![
                ObservedAddr {
                    addr: new_ipv4,
                    ttl_secs: 60,
                },
                ObservedAddr {
                    addr: new_ipv6,
                    ttl_secs: 60,
                },
            ],
            101,
        );

        assert_eq!(
            manager.routes[&old_ipv4_key].sync_state,
            SyncState::PendingDelete
        );
        assert_ne!(
            manager.routes[&old_ipv6_key].sync_state,
            SyncState::PendingDelete
        );
        assert!(manager.routes.contains_key(&new_ipv4_key));
        assert!(manager.routes.contains_key(&new_ipv6_key));
    }

    #[test]
    fn pending_observations_replay_in_original_cross_family_order() {
        let domain = "pending-order.example.".to_string();
        let old_ipv6 = "2001:db8::61".parse::<IpAddr>().expect("IPv6 address");
        let new_ipv6 = "2001:db8::62".parse::<IpAddr>().expect("IPv6 address");
        let old_ipv6_key = RouteKey::new(old_ipv6, "via_proxy".to_string());
        let new_ipv6_key = RouteKey::new(new_ipv6, "via_proxy".to_string());
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        // An A response can contain an AAAA record. The later AAAA response
        // replaces only IPv6; replaying these in hash-map order could otherwise
        // reattach the stale IPv6 route after the withdrawal.
        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: old_ipv6,
                ttl_secs: u32::MAX,
            }],
            None,
            unix_now(),
        );
        manager.queue_pending_observation(
            domain,
            ObservationScope::Ipv6,
            vec![ObservedAddr {
                addr: new_ipv6,
                ttl_secs: u32::MAX,
            }],
            None,
            unix_now(),
        );

        manager.replay_pending_observations();

        assert_eq!(
            manager.routes[&old_ipv6_key].sync_state,
            SyncState::PendingDelete
        );
        assert!(manager.routes.contains_key(&new_ipv6_key));
    }

    #[test]
    fn pending_same_scope_observations_preserve_cross_family_additions() {
        let domain = "pending-same-scope.example.".to_string();
        let ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 63));
        let ipv6 = "2001:db8::63".parse::<IpAddr>().expect("IPv6 address");
        let ipv4_key = RouteKey::new(ipv4, "via_proxy".to_string());
        let ipv6_key = RouteKey::new(ipv6, "via_proxy".to_string());
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);
        let now = unix_now();

        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ipv6,
                ttl_secs: u32::MAX,
            }],
            None,
            now,
        );
        manager.queue_pending_observation(
            domain,
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ipv4,
                ttl_secs: u32::MAX,
            }],
            None,
            now,
        );

        manager.replay_pending_observations();

        assert!(manager.routes.contains_key(&ipv4_key));
        assert!(manager.routes.contains_key(&ipv6_key));
    }

    #[test]
    fn pending_same_scope_observations_replace_stale_queue_entries() {
        let domain = "pending-replacement.example.".to_string();
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);
        let now = unix_now();

        for last_octet in 1..=32 {
            manager.queue_pending_observation(
                domain.clone(),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, last_octet)),
                    ttl_secs: 60,
                }],
                None,
                now,
            );
        }

        assert_eq!(manager.pending_observations.len(), 1);
        assert_eq!(
            manager.pending_observations[0].addrs,
            vec![ObservedAddr {
                addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 32)),
                ttl_secs: 60,
            }]
        );
    }

    #[test]
    fn pending_compaction_respects_later_cross_family_replacements() {
        let domain = "pending-cross-replacement.example.".to_string();
        let stale_ipv6 = "2001:db8::71".parse::<IpAddr>().expect("IPv6 address");
        let current_ipv6 = "2001:db8::72".parse::<IpAddr>().expect("IPv6 address");
        let current_ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 72));
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);
        let now = unix_now();

        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: stale_ipv6,
                ttl_secs: 60,
            }],
            None,
            now,
        );
        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv6,
            vec![ObservedAddr {
                addr: current_ipv6,
                ttl_secs: 60,
            }],
            None,
            now,
        );
        manager.queue_pending_observation(
            domain,
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: current_ipv4,
                ttl_secs: 60,
            }],
            None,
            now,
        );

        manager.replay_pending_observations();

        assert!(
            !manager
                .routes
                .contains_key(&RouteKey::new(stale_ipv6, "via_proxy".to_string()))
        );
        assert!(
            manager
                .routes
                .contains_key(&RouteKey::new(current_ipv6, "via_proxy".to_string()))
        );
        assert!(
            manager
                .routes
                .contains_key(&RouteKey::new(current_ipv4, "via_proxy".to_string()))
        );
    }

    #[tokio::test]
    async fn sync_continues_after_one_route_fails() {
        let failing_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10));
        let good_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11));
        let api = Arc::new(MockApi::with_state(MockApiState {
            fail_upserts: AHashSet::from_iter([failing_ip]),
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        manager.apply_observation(
            "example.com.".to_string(),
            ObservationScope::Ipv4,
            vec![
                ObservedAddr {
                    addr: failing_ip,
                    ttl_secs: 60,
                },
                ObservedAddr {
                    addr: good_ip,
                    ttl_secs: 60,
                },
            ],
            100,
        );

        assert!(manager.sync_routes(100).await.is_err());
        let state = api.state.lock().expect("mock lock");
        assert!(state.upsert_attempts.contains(&failing_ip));
        assert!(state.upsert_attempts.contains(&good_ip));
        drop(state);
        let good_key = RouteKey::new(good_ip, "via_proxy".to_string());
        assert_eq!(
            manager.routes.get(&good_key).map(|route| route.sync_state),
            Some(SyncState::Synced)
        );
    }

    #[tokio::test]
    async fn route_upserts_use_bounded_pipeline() {
        let api = Arc::new(PipelineApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        let addrs = (1..=17)
            .map(|last_octet| ObservedAddr {
                addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, last_octet)),
                ttl_secs: 60,
            })
            .collect();
        manager.apply_observation(
            "pipeline.example.".to_string(),
            ObservationScope::Ipv4,
            addrs,
            100,
        );

        manager.sync_routes(100).await.expect("pipeline sync");

        assert_eq!(api.attempts.load(Ordering::Relaxed), 17);
        assert_eq!(api.max_active.load(Ordering::Acquire), UPSERT_PIPELINE_SIZE);
    }

    #[tokio::test]
    async fn pending_delete_revalidates_remote_ownership() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 12));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        manager.apply_observation(
            "example.com.".to_string(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            100,
        );
        let entry = manager.routes.get_mut(&key).expect("route");
        entry.router_id = Some("*stale".to_string());
        entry.sync_state = SyncState::PendingDelete;
        entry.ref_count = 0;

        manager.sync_routes(101).await.expect("safe deletion");
        assert!(!manager.routes.contains_key(&key));
        assert!(api.state.lock().expect("mock lock").deleted_ids.is_empty());
    }

    #[tokio::test]
    async fn conntrack_guard_defers_then_retries_route_deletion() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![owned_router_route("*guarded", &key)],
            active_connection_destinations: AHashSet::from_iter([ip]),
            ..MockApiState::default()
        }));
        let mut config = manager_config(Some(300));
        config.conntrack_guard = true;
        let mut manager = RouteManager::new(api.clone(), config);
        manager
            .routes
            .insert(key.clone(), pending_delete_entry(key.clone(), "*guarded"));

        manager.sync_routes(100).await.expect("guarded sync");
        assert!(manager.routes.contains_key(&key));
        assert!(api.state.lock().expect("mock lock").deleted_ids.is_empty());
        assert_eq!(
            api.state
                .lock()
                .expect("mock lock")
                .connection_queries
                .len(),
            1
        );

        manager.sync_routes(101).await.expect("cooldown sync");
        assert_eq!(
            api.state
                .lock()
                .expect("mock lock")
                .connection_queries
                .len(),
            1,
            "a repeated withdrawal must not re-query conntrack during cooldown"
        );

        api.state
            .lock()
            .expect("mock lock")
            .active_connection_destinations
            .clear();
        manager.sync_routes(130).await.expect("retry deletion");
        assert!(!manager.routes.contains_key(&key));
        assert_eq!(
            api.state.lock().expect("mock lock").deleted_ids,
            vec!["*guarded".to_string()]
        );
    }

    #[tokio::test]
    async fn conntrack_guard_fails_closed_and_retries_later() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 43));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![owned_router_route("*check-error", &key)],
            fail_connection_check: true,
            ..MockApiState::default()
        }));
        let mut config = manager_config(Some(300));
        config.conntrack_guard = true;
        let mut manager = RouteManager::new(api.clone(), config);
        manager.routes.insert(
            key.clone(),
            pending_delete_entry(key.clone(), "*check-error"),
        );

        assert!(manager.sync_routes(100).await.is_err());
        assert!(manager.routes.contains_key(&key));
        assert!(api.state.lock().expect("mock lock").deleted_ids.is_empty());
        assert_eq!(
            api.state
                .lock()
                .expect("mock lock")
                .connection_queries
                .len(),
            1
        );

        manager
            .sync_routes(101)
            .await
            .expect("cooldown keeps route");
        assert_eq!(
            api.state
                .lock()
                .expect("mock lock")
                .connection_queries
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn conntrack_guard_uses_snapshot_for_cidr_routes() {
        let key = RouteKey::new_with_prefix(
            "198.51.100.0".parse().expect("ip"),
            24,
            "via_proxy".to_string(),
        )
        .expect("route key");
        let active_ip = "198.51.100.42".parse().expect("ip");
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![owned_router_route("*cidr", &key)],
            active_connection_destinations: AHashSet::from_iter([active_ip]),
            ..MockApiState::default()
        }));
        let mut config = manager_config(Some(300));
        config.conntrack_guard = true;
        let mut manager = RouteManager::new(api.clone(), config);
        manager
            .routes
            .insert(key.clone(), pending_delete_entry(key.clone(), "*cidr"));

        manager.sync_routes(100).await.expect("guarded cidr sync");
        assert!(manager.routes.contains_key(&key));
        assert_eq!(
            api.state.lock().expect("mock lock").connection_queries,
            vec![(RouteFamily::Ipv4, Vec::new())]
        );
    }

    #[tokio::test]
    async fn shutdown_cleanup_bypasses_conntrack_guard() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 44));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![owned_router_route("*shutdown", &key)],
            active_connection_destinations: AHashSet::from_iter([ip]),
            ..MockApiState::default()
        }));
        let mut config = manager_config(Some(300));
        config.conntrack_guard = true;
        let mut manager = RouteManager::new(api.clone(), config);

        manager.shutdown(true).await.expect("shutdown cleanup");
        let state = api.state.lock().expect("mock lock");
        assert_eq!(state.deleted_ids, vec!["*shutdown".to_string()]);
        assert!(state.connection_queries.is_empty());
    }

    #[tokio::test]
    async fn conntrack_guard_batches_host_route_queries() {
        let mut routes = Vec::new();
        let mut entries = Vec::new();
        for offset in 1..=129u8 {
            let ip = IpAddr::V4(Ipv4Addr::new(198, 18, 0, offset));
            let key = RouteKey::new(ip, "via_proxy".to_string());
            routes.push(owned_router_route(format!("*{offset}").as_str(), &key));
            entries.push((
                key.clone(),
                pending_delete_entry(key, format!("*{offset}").as_str()),
            ));
        }
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes,
            ..MockApiState::default()
        }));
        let mut config = manager_config(Some(300));
        config.conntrack_guard = true;
        let mut manager = RouteManager::new(api.clone(), config);
        manager.routes.extend(entries);

        manager.sync_routes(100).await.expect("batched deletion");
        let query_sizes = api
            .state
            .lock()
            .expect("mock lock")
            .connection_queries
            .iter()
            .map(|(_, destinations)| destinations.len())
            .collect::<Vec<_>>();
        assert_eq!(query_sizes, vec![128, 1]);
    }

    #[tokio::test]
    async fn persistent_desired_state_survives_initialization_failure() {
        let api = Arc::new(MockApi::with_state(MockApiState {
            fail_healthcheck: true,
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api, manager_config(Some(300)));
        let desired = AHashSet::from_iter(["198.51.100.0/24".to_string()]);

        assert!(
            manager
                .update_persistent_ips(desired.clone())
                .await
                .is_err()
        );
        assert_eq!(manager.persistent_ips, desired);
    }

    #[tokio::test]
    async fn shutdown_cleanup_does_not_initialize_or_create_routes() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 34));
        let validation_ip = IpAddr::V4(Ipv4Addr::new(198, 18, 0, 34));
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![
                RouterRoute {
                    id: "*owned".to_string(),
                    family: RouteFamily::Ipv4,
                    dst_address: format!("{ip}/32"),
                    routing_table: "via_proxy".to_string(),
                    gateway: Some("192.0.2.1".to_string()),
                    distance: Some(100),
                    comment: Some(format!(
                        "fdns;pg=route-test;kind=dynamic;dm=shutdown.example.;exp={};seen=100",
                        u64::MAX
                    )),
                    disabled: false,
                },
                RouterRoute {
                    id: "*gateway-check".to_string(),
                    family: RouteFamily::Ipv4,
                    dst_address: format!("{validation_ip}/32"),
                    routing_table: "via_proxy".to_string(),
                    gateway: Some("192.0.2.1".to_string()),
                    distance: Some(100),
                    comment: Some(validation_comment(
                        "fdns",
                        "route-test",
                        RouteFamily::Ipv4,
                        34,
                    )),
                    disabled: true,
                },
            ],
            fail_healthcheck: true,
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(0)));

        manager.shutdown(true).await.expect("shutdown cleanup");

        assert!(!manager.initialized);
        let state = api.state.lock().expect("mock lock");
        assert_eq!(
            state.deleted_ids,
            vec!["*owned".to_string(), "*gateway-check".to_string()]
        );
        assert!(state.upsert_attempts.is_empty());
    }

    #[tokio::test]
    async fn priority_shutdown_drops_queued_route_writes() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 37));
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        manager.initialized = true;
        let (tx, rx) = mpsc::channel(MANAGER_QUEUE_SIZE);
        for index in 0..8 {
            tx.try_send(ManagerCommand::ObserveDomain {
                domain: format!("queued-{index}.example."),
                replace_scope: ObservationScope::Ipv4,
                addrs: vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 60,
                }],
                negative_ttl_secs: None,
                wait: None,
            })
            .expect("queue observation");
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        shutdown_tx
            .send(ShutdownRequest {
                cleanup: false,
                done: done_tx,
            })
            .expect("signal shutdown");

        run_manager_worker("route-test".to_string(), manager, rx, shutdown_rx).await;
        done_rx.await.expect("shutdown acknowledgement");

        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .is_empty()
        );
    }

    #[tokio::test]
    async fn synchronous_observation_retries_failed_initialization() {
        let domain = "sync.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 21));
        let api = Arc::new(MockApi::with_state(MockApiState {
            fail_healthcheck: true,
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        let observed = || {
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }]
        };

        assert!(
            manager
                .observe_domain(
                    domain.clone(),
                    ObservationScope::Ipv4,
                    observed(),
                    None,
                    true,
                )
                .await
                .is_err()
        );
        assert!(!manager.initialized);

        api.state.lock().expect("mock lock").fail_healthcheck = false;
        manager
            .observe_domain(domain, ObservationScope::Ipv4, observed(), None, true)
            .await
            .expect("synchronous retry should initialize and sync");

        assert!(manager.initialized);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn initialization_replays_withdrawal_after_recovering_router_state() {
        AppClock::start();
        let domain = "withdraw.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 26));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*retained".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm={domain};exp={};seen=100",
                    u64::MAX
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(0)));

        manager
            .observe_domain(
                domain.clone(),
                ObservationScope::Ipv4,
                Vec::new(),
                Some(300),
                false,
            )
            .await
            .expect("queue withdrawal before initialization");
        assert!(!manager.initialized);
        assert!(!manager.routes.contains_key(&key));

        manager
            .sweep()
            .await
            .expect("initialize and replay withdrawal");

        assert!(manager.initialized);
        assert!(!manager.routes.contains_key(&key));
        assert!(!manager.domain_bindings.contains_key(&domain));
        assert_eq!(
            api.state.lock().expect("mock lock").deleted_ids,
            vec!["*retained".to_string()]
        );
    }

    #[test]
    fn removing_persistent_anchor_preserves_dynamic_route_ownership() {
        let domain = "example.com.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 15));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut config = manager_config(Some(300));
        config.persistent_ips = AHashSet::from_iter([format!("{ip}/32")]);
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        manager.ensure_persistent_routes(100);
        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            101,
        );
        assert_eq!(manager.routes[&key].comment_domain, domain);
        let mixed_comment = RouteCommentCodec::encode("fdns", "route-test", &manager.routes[&key]);
        assert!(mixed_comment.contains(";kind=mixed;"));
        assert!(mixed_comment.contains(";exp=401;"));
        manager.persistent_ips.clear();
        manager.ensure_persistent_routes(102);

        let route = manager.routes.get(&key).expect("dynamic route remains");
        assert_eq!(route.ref_count, 1);
        assert_eq!(route.comment_domain, domain);
        assert_eq!(route.expires_at_unix, 401);
        assert!(!route.persistent);
        assert_ne!(route.sync_state, SyncState::PendingDelete);
    }

    #[tokio::test]
    async fn mixed_route_recovers_dynamic_ownership_after_persistent_source_is_removed() {
        AppClock::start();
        let now = unix_now();
        let domain = "mixed-recovery.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 43));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut source_config = manager_config(Some(300));
        source_config.persistent_ips = AHashSet::from_iter([format!("{ip}/32")]);
        let mut source = RouteManager::new(Arc::new(NoopApi), source_config);
        source.ensure_persistent_routes(now);
        source.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            now,
        );
        let comment = RouteCommentCodec::encode("fdns", "route-test", &source.routes[&key]);
        assert!(comment.contains(";kind=mixed;"));

        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*mixed".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(comment),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut recovered = RouteManager::new(api, manager_config(Some(300)));

        recovered.reconcile_from_router().await.expect("reconcile");

        let route = recovered.routes.get(&key).expect("dynamic route recovered");
        assert!(!route.persistent);
        assert_eq!(route.ref_count, 1);
        assert_eq!(route.comment_domain, domain);
        assert!(recovered.domain_bindings.contains_key(&domain));
        assert_ne!(route.sync_state, SyncState::PendingDelete);
    }

    #[test]
    fn withdrawing_last_dynamic_ref_restores_persistent_comment_marker() {
        let domain = "example.com.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 20));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut config = manager_config(Some(300));
        config.persistent_ips = AHashSet::from_iter([format!("{ip}/32")]);
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);

        manager.ensure_persistent_routes(100);
        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            101,
        );
        manager.apply_observation(domain, ObservationScope::Ipv4, Vec::new(), 102);

        let route = manager.routes.get(&key).expect("persistent route remains");
        assert_eq!(route.ref_count, 0);
        assert_eq!(route.comment_domain, PERSISTENT_COMMENT_DOMAIN);
        assert!(route.persistent);
        assert!(route.domains.is_empty());
        assert_eq!(route.expires_at_unix, PERSISTENT_EXPIRES_AT_UNIX);
        assert_ne!(route.sync_state, SyncState::PendingDelete);
        let comment = RouteCommentCodec::encode("fdns", "route-test", route);
        assert!(comment.contains(";kind=persistent;"));
    }

    #[tokio::test]
    async fn initialization_prunes_expired_queued_observations() {
        AppClock::start();
        let domain = "expired.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 16));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            None,
            1,
        );
        assert!(!manager.routes.contains_key(&key));

        manager.reconcile().await.expect("initial reconcile");

        assert!(!manager.domain_bindings.contains_key(&domain));
        assert!(!manager.routes.contains_key(&key));
        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .is_empty()
        );
    }

    #[test]
    fn expired_queued_negative_observation_is_not_replayed() {
        let domain = "stale-negative.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 35));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut manager = manager_with_timeless_dynamic_routes();
        manager.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            1,
        );
        manager.queue_pending_observation(
            domain,
            ObservationScope::Ipv4,
            Vec::new(),
            Some(30),
            100,
        );

        manager.replay_pending_observations();

        assert_eq!(manager.routes[&key].ref_count, 1);
        assert_ne!(manager.routes[&key].sync_state, SyncState::PendingDelete);
    }

    #[test]
    fn non_cacheable_queued_negative_observation_is_not_replayed() {
        AppClock::start();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 38));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        for negative_ttl_secs in [None, Some(0)] {
            let domain = "non-cacheable-negative.example.".to_string();
            let mut manager = manager_with_timeless_dynamic_routes();
            manager.apply_observation(
                domain.clone(),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 60,
                }],
                unix_now(),
            );
            manager.queue_pending_observation(
                domain,
                ObservationScope::Ipv4,
                Vec::new(),
                negative_ttl_secs,
                unix_now(),
            );

            manager.replay_pending_observations();

            assert_eq!(manager.routes[&key].ref_count, 1);
            assert_ne!(manager.routes[&key].sync_state, SyncState::PendingDelete);
        }
    }

    #[test]
    fn timeless_routes_still_bound_positive_replay_by_dns_ttl() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 41));
        let mut manager = manager_with_timeless_dynamic_routes();
        manager.queue_pending_observation(
            "ttl-bound.example.".to_string(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            None,
            100,
        );

        let pending = pending_observation(&manager, "ttl-bound.example.", ObservationScope::Ipv4)
            .expect("queued observation");
        assert_eq!(pending.replay_until_unix, 160);
    }

    #[test]
    fn positive_observation_supersedes_queued_name_level_negative() {
        let domain = "recovered.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42));
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);
        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Both,
            Vec::new(),
            Some(300),
            100,
        );

        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            None,
            101,
        );

        let ipv4 = pending_observation(&manager, &domain, ObservationScope::Ipv4)
            .expect("new positive observation");
        assert_eq!(ipv4.addrs.len(), 1);
        assert!(!ipv4.name_level_negative);
        assert!(!has_pending_observation(
            &manager,
            &domain,
            ObservationScope::Ipv6
        ));
    }

    #[test]
    fn nodata_observation_supersedes_queued_name_level_negative() {
        let domain = "nodata-recovery.example.".to_string();
        let mut config = manager_config(Some(0));
        config.gateway6 = Some("2001:db8::1".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), config);
        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Both,
            Vec::new(),
            Some(300),
            100,
        );

        manager.queue_pending_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            Vec::new(),
            Some(60),
            101,
        );

        assert!(has_pending_observation(
            &manager,
            &domain,
            ObservationScope::Ipv4
        ));
        assert!(!has_pending_observation(
            &manager,
            &domain,
            ObservationScope::Ipv6
        ));
    }

    #[tokio::test]
    async fn synchronous_non_cacheable_negative_is_applied_after_initialization() {
        AppClock::start();
        let domain = "sync-negative.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 44));
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*retained".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm={domain};exp={};seen=100",
                    u64::MAX
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(0)));

        manager
            .observe_domain(domain, ObservationScope::Ipv4, Vec::new(), None, true)
            .await
            .expect("current synchronous withdrawal should be applied once");

        assert_eq!(
            api.state.lock().expect("mock lock").deleted_ids,
            vec!["*retained".to_string()]
        );
    }

    #[tokio::test]
    async fn synchronous_zero_ttl_positive_is_applied_after_initialization() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 45));
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager
            .observe_domain(
                "sync-zero-ttl.example.".to_string(),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 0,
                }],
                None,
                true,
            )
            .await
            .expect("current synchronous positive should be applied once");

        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .contains(&ip)
        );
    }

    #[tokio::test]
    async fn synchronous_observation_bypasses_a_full_pending_backlog() {
        let backlog_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 46));
        let current_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 47));
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        let now = unix_now();

        for index in 0..MAX_PENDING_OBSERVATIONS {
            manager.queue_pending_observation(
                format!("backlog-{index}.example."),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: backlog_ip,
                    ttl_secs: 60,
                }],
                None,
                now,
            );
        }

        manager
            .observe_domain(
                "current-sync.example.".to_string(),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: current_ip,
                    ttl_secs: 60,
                }],
                None,
                true,
            )
            .await
            .expect("current synchronous observation should not be lost");

        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .contains(&current_ip)
        );
    }

    #[test]
    fn pending_observations_are_bounded_during_initialization_outage() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 39));
        let mut manager = RouteManager::new(Arc::new(NoopApi), manager_config(Some(300)));
        let now = unix_now();

        for index in 0..MAX_PENDING_OBSERVATIONS + 16 {
            manager.queue_pending_observation(
                format!("queued-{index}.example."),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 60,
                }],
                None,
                now,
            );
        }

        assert_eq!(manager.pending_observations.len(), MAX_PENDING_OBSERVATIONS);
        assert!(has_pending_observation(
            &manager,
            "queued-0.example.",
            ObservationScope::Ipv4
        ));
        assert!(!has_pending_observation(
            &manager,
            &format!("queued-{}.example.", MAX_PENDING_OBSERVATIONS),
            ObservationScope::Ipv4,
        ));
    }

    #[test]
    fn expired_pending_observations_release_backlog_capacity() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 40));
        let mut manager = RouteManager::new(Arc::new(NoopApi), manager_config(Some(300)));

        for index in 0..MAX_PENDING_OBSERVATIONS {
            manager.queue_pending_observation(
                format!("expired-{index}.example."),
                ObservationScope::Ipv4,
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 60,
                }],
                None,
                1,
            );
        }
        manager.queue_pending_observation(
            "fresh.example.".to_string(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            None,
            unix_now(),
        );

        assert_eq!(manager.pending_observations.len(), 1);
        assert!(has_pending_observation(
            &manager,
            "fresh.example.",
            ObservationScope::Ipv4,
        ));
    }

    #[tokio::test]
    async fn recovered_timeless_domain_binding_can_be_withdrawn() {
        AppClock::start();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 13));
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*13".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm=example.com.;exp={};seen=100",
                    u64::MAX
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(0)));

        manager
            .ensure_initialized()
            .await
            .expect("initialize manager");
        assert!(manager.domain_bindings.contains_key("example.com."));
        manager
            .observe_domain(
                "example.com.".to_string(),
                ObservationScope::Ipv4,
                Vec::new(),
                Some(300),
                true,
            )
            .await
            .expect("withdraw recovered timeless route");
        let key = RouteKey::new(ip, "via_proxy".to_string());
        assert!(!manager.routes.contains_key(&key));
        assert_eq!(
            api.state.lock().expect("mock lock").deleted_ids,
            vec!["*13".to_string()]
        );
    }

    #[tokio::test]
    async fn dynamic_domain_named_persistent_is_not_treated_as_route_residue() {
        AppClock::start();
        let now = unix_now();
        let expires_at = now + 300;
        let domain = PERSISTENT_COMMENT_DOMAIN;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 22));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*dynamic-persistent".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm={domain};exp={expires_at};seen={now}"
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager
            .reconcile_from_router()
            .await
            .expect("recover dynamic route");

        assert!(manager.domain_bindings.contains_key(domain));
        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
        assert_eq!(manager.routes[&key].ref_count, 1);
        assert!(api.state.lock().expect("mock lock").deleted_ids.is_empty());
    }

    #[tokio::test]
    async fn internal_anchor_name_remains_a_dynamic_qname_across_recovery() {
        AppClock::start();
        let now = unix_now();
        let domain = "__forgedns_persistent__".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 36));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut initial = RouteManager::new(Arc::new(NoopApi), manager_config(Some(300)));
        initial.apply_observation(
            domain.clone(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            now,
        );
        let initial_route = &initial.routes[&key];
        assert!(!initial_route.persistent);
        let comment = RouteCommentCodec::encode("fdns", "route-test", initial_route);
        assert!(comment.contains(";kind=dynamic;"));

        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*dynamic-anchor-name".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(comment),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut recovered = RouteManager::new(api, manager_config(Some(300)));

        recovered.reconcile_from_router().await.expect("reconcile");

        assert!(!recovered.routes[&key].persistent);
        assert_eq!(recovered.routes[&key].ref_count, 1);
        assert!(recovered.domain_bindings.contains_key(&domain));
    }

    #[tokio::test]
    async fn recovered_route_survives_representative_domain_withdrawal_until_expiry() {
        AppClock::start();
        let now = unix_now();
        let expires_at = now + 300;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 18));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*shared".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm=first.example.;exp={expires_at};seen={now}"
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api, manager_config(None));

        manager
            .reconcile_from_router()
            .await
            .expect("recover route");
        assert!(manager.routes[&key].recovered_ownership_incomplete);

        manager.apply_observation(
            "first.example.".to_string(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }],
            now + 1,
        );
        assert_eq!(manager.routes[&key].expires_at_unix, expires_at);

        manager.apply_observation(
            "first.example.".to_string(),
            ObservationScope::Ipv4,
            Vec::new(),
            now + 2,
        );

        let route = &manager.routes[&key];
        assert_eq!(route.ref_count, 0);
        assert_eq!(route.expires_at_unix, expires_at);
        assert_eq!(route.comment_domain, "first.example.");
        assert_eq!(route.sync_state, SyncState::Synced);

        manager.update_route_expiration(expires_at);
        assert_eq!(manager.routes[&key].sync_state, SyncState::PendingDelete);
    }

    #[test]
    fn repeated_observation_is_suppressed_until_comment_nears_expiry() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 14));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let mut manager = RouteManager::new(Arc::new(NoopApi), manager_config(Some(300)));
        let observation = || {
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }]
        };

        manager.apply_observation(
            "example.com.".to_string(),
            ObservationScope::Ipv4,
            observation(),
            100,
        );
        let entry = manager.routes.get_mut(&key).expect("route");
        entry.router_id = Some("*14".to_string());
        entry.sync_state = SyncState::Synced;
        entry.synced_expires_at_unix = Some(400);
        entry.last_refresh_unix = 100;

        manager.apply_observation(
            "example.com.".to_string(),
            ObservationScope::Ipv4,
            observation(),
            101,
        );
        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);

        manager.apply_observation(
            "example.com.".to_string(),
            ObservationScope::Ipv4,
            observation(),
            251,
        );
        assert_eq!(manager.routes[&key].sync_state, SyncState::Dirty);
    }

    #[tokio::test]
    async fn reconcile_accepts_manual_dynamic_deletion_until_next_observation() {
        AppClock::start();
        let now = unix_now();
        let domain = "manual-delete.example.".to_string();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 32));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(0)));
        let observation = || {
            vec![ObservedAddr {
                addr: ip,
                ttl_secs: 60,
            }]
        };

        manager.apply_observation(domain.clone(), ObservationScope::Ipv4, observation(), now);
        let route = manager.routes.get_mut(&key).expect("dynamic route");
        route.router_id = Some("*removed-by-operator".to_string());
        route.synced_expires_at_unix = Some(u64::MAX);
        route.sync_state = SyncState::Synced;

        manager.reconcile_from_router().await.expect("reconcile");

        assert!(!manager.routes.contains_key(&key));
        assert!(manager.domain_bindings[&domain].ips.contains(&ip));
        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .is_empty()
        );

        let touched =
            manager.apply_observation(domain, ObservationScope::Ipv4, observation(), now + 1);
        manager
            .sync_route_keys(touched, now + 1)
            .await
            .expect("sync next observation");

        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn reconcile_recreates_missing_configured_persistent_route() {
        AppClock::start();
        let now = unix_now();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 33));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::default());
        let mut config = manager_config(Some(0));
        config.persistent_ips.insert(format!("{ip}/32"));
        let mut manager = RouteManager::new(api.clone(), config);
        manager.ensure_persistent_routes(now);
        let route = manager.routes.get_mut(&key).expect("persistent route");
        route.router_id = Some("*removed-by-operator".to_string());
        route.synced_expires_at_unix = Some(u64::MAX);
        route.sync_state = SyncState::Synced;

        manager.reconcile_from_router().await.expect("reconcile");

        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn reconcile_removes_stale_gateway_validation_route() {
        AppClock::start();
        let ip = IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1));
        let comment = validation_comment("fdns", "route-test", RouteFamily::Ipv4, 1);
        assert!(owned_comment_has_kind(
            "fdns",
            "route-test",
            &comment,
            COMMENT_KIND_GATEWAY_CHECK
        ));
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*validation".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(comment),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");
        assert_eq!(
            api.state.lock().expect("mock lock").deleted_ids,
            vec!["*validation".to_string()]
        );
    }

    #[tokio::test]
    async fn reconcile_prunes_duplicate_owned_routes() {
        AppClock::start();
        let now = unix_now();
        let expires_at = now + 300;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 23));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let comment = format!(
            "fdns;pg=route-test;kind=dynamic;dm=duplicate.example.;exp={expires_at};seen={now}"
        );
        let route = |id: &str| RouterRoute {
            id: id.to_string(),
            family: RouteFamily::Ipv4,
            dst_address: format!("{ip}/32"),
            routing_table: "via_proxy".to_string(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(comment.clone()),
            disabled: false,
        };
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![route("*primary"), route("*duplicate")],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");

        let state = api.state.lock().expect("mock lock");
        assert_eq!(state.deleted_ids, vec!["*duplicate".to_string()]);
        assert_eq!(state.routes.len(), 1);
        assert_eq!(state.routes[0].id, "*primary");
        assert_eq!(manager.routes[&key].router_id.as_deref(), Some("*primary"));
    }

    #[tokio::test]
    async fn reconcile_prefers_live_owned_duplicate() {
        AppClock::start();
        let now = unix_now();
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 29));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let route = |id: &str, expires_at: u64, seen: u64| RouterRoute {
            id: id.to_string(),
            family: RouteFamily::Ipv4,
            dst_address: format!("{ip}/32"),
            routing_table: "via_proxy".to_string(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(format!(
                "fdns;pg=route-test;kind=dynamic;dm=duplicate.example.;exp={expires_at};seen={seen}"
            )),
            disabled: false,
        };
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![
                route("*expired", now.saturating_sub(1), now.saturating_sub(600)),
                route("*live", now + 300, now),
            ],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");

        let state = api.state.lock().expect("mock lock");
        assert_eq!(state.deleted_ids, vec!["*expired".to_string()]);
        assert_eq!(state.routes.len(), 1);
        assert_eq!(state.routes[0].id, "*live");
        assert_eq!(manager.routes[&key].router_id.as_deref(), Some("*live"));
        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
    }

    #[tokio::test]
    async fn reconcile_detects_foreign_duplicate_for_synced_owned_route() {
        AppClock::start();
        let now = unix_now();
        let expires_at = now + 300;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 30));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let route = |id: &str, comment: &str| RouterRoute {
            id: id.to_string(),
            family: RouteFamily::Ipv4,
            dst_address: format!("{ip}/32"),
            routing_table: "via_proxy".to_string(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(comment.to_string()),
            disabled: false,
        };
        let owned_comment = format!(
            "fdns;pg=route-test;kind=dynamic;dm=conflict.example.;exp={expires_at};seen={now}"
        );
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![
                route("*owned", &owned_comment),
                route("*foreign", "operator-managed"),
            ],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        assert!(manager.reconcile_from_router().await.is_err());

        assert_eq!(manager.routes[&key].sync_state, SyncState::Dirty);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn initialization_drains_unrelated_observations_after_route_conflict() {
        AppClock::start();
        let now = unix_now();
        let conflict_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 41));
        let unrelated_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42));
        let route = |id: &str, comment: &str| RouterRoute {
            id: id.to_string(),
            family: RouteFamily::Ipv4,
            dst_address: format!("{conflict_ip}/32"),
            routing_table: "via_proxy".to_string(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(comment.to_string()),
            disabled: false,
        };
        let owned_comment = format!(
            "fdns;pg=route-test;kind=dynamic;dm=conflict.example.;exp={};seen={now}",
            now + 300
        );
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![
                route("*owned", &owned_comment),
                route("*foreign", "operator-managed"),
            ],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));
        manager.queue_pending_observation(
            "unrelated.example.".to_string(),
            ObservationScope::Ipv4,
            vec![ObservedAddr {
                addr: unrelated_ip,
                ttl_secs: 300,
            }],
            None,
            now,
        );

        assert!(manager.ensure_initialized().await.is_err());

        assert!(manager.initialized);
        assert!(manager.pending_observations.is_empty());
        assert!(manager.domain_bindings.contains_key("unrelated.example."));
        assert!(
            api.state
                .lock()
                .expect("mock lock")
                .upsert_attempts
                .contains(&unrelated_ip)
        );
    }

    #[tokio::test]
    async fn reconcile_caps_recovered_timeless_route_to_current_ttl_policy() {
        AppClock::start();
        let now = unix_now();
        let seen = now.saturating_sub(100);
        let expected_expiry = seen + 300;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 31));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*timeless".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm=timeless.example.;exp={};seen={seen}",
                    u64::MAX
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");

        assert_eq!(manager.routes[&key].expires_at_unix, expected_expiry);
        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn reconcile_refreshes_disabled_owned_route() {
        AppClock::start();
        let now = unix_now();
        let expires_at = now + 300;
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 24));
        let key = RouteKey::new(ip, "via_proxy".to_string());
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*disabled".to_string(),
                family: RouteFamily::Ipv4,
                dst_address: format!("{ip}/32"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("192.0.2.1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm=disabled.example.;exp={expires_at};seen={now}"
                )),
                disabled: true,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");

        assert_eq!(manager.routes[&key].sync_state, SyncState::Synced);
        assert_eq!(
            api.state.lock().expect("mock lock").upsert_attempts,
            vec![ip]
        );
    }

    #[tokio::test]
    async fn reconcile_removes_owned_route_for_disabled_address_family() {
        AppClock::start();
        let ip = "2001:db8::17".parse::<IpAddr>().expect("IPv6 address");
        let api = Arc::new(MockApi::with_state(MockApiState {
            routes: vec![RouterRoute {
                id: "*disabled-v6".to_string(),
                family: RouteFamily::Ipv6,
                dst_address: format!("{ip}/128"),
                routing_table: "via_proxy".to_string(),
                gateway: Some("2001:db8::1".to_string()),
                distance: Some(100),
                comment: Some(format!(
                    "fdns;pg=route-test;kind=dynamic;dm=example.com.;exp={};seen=100",
                    u64::MAX
                )),
                disabled: false,
            }],
            ..MockApiState::default()
        }));
        let mut manager = RouteManager::new(api.clone(), manager_config(Some(300)));

        manager.reconcile_from_router().await.expect("reconcile");

        let state = api.state.lock().expect("mock lock");
        assert_eq!(state.list_requirements, vec![(true, false)]);
        assert_eq!(state.deleted_ids, vec!["*disabled-v6".to_string()]);
        assert!(state.routes.is_empty());
    }
}
