//! Address-list manager state machine for ros_address_list executor.
//!
//! Responsibilities:
//! - maintain desired persistent address-list entries
//! - upsert dynamic address-list entries from observed DNS answers
//! - keep ownership metadata in RouterOS comments
//! - execute idempotent create/update/delete through [`MikrotikApi`]
//!
//! Design notes:
//! - RouterOS remains the authority for dynamic expiration via native
//!   `timeout`.
//! - local state is intentionally lightweight and only suppresses redundant
//!   refresh writes; it does not attempt to mirror full remote state.
//! - persistent items are reconciled as a desired set and never enter the
//!   dynamic refresh cache.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::api::{MikrotikApi, RouterListEntry};
use crate::infra::clock::AppClock;
use crate::infra::error::{DnsError, Result};
use crate::infra::task as task_center;
use crate::plugin::executor::ros_common::ObservedAddr;
use crate::plugin::executor::ros_common::mailbox::{
    Coalesce, KeyedMailbox, PushOutcome, TryPushError,
};

/// Host prefix used for normalized IPv4 single-address entries.
const HOST_PREFIX_V4: u8 = 32;
/// Host prefix used for normalized IPv6 single-address entries.
const HOST_PREFIX_V6: u8 = 128;
/// Maximum number of distinct domains waiting for manager processing.
const MANAGER_QUEUE_SIZE: usize = 1024;
const CONTROL_QUEUE_SIZE: usize = 2;
/// Periodic interval for persistent desired-set reconciliation.
const RECONCILE_INTERVAL_SECS: u64 = 180;
/// Periodic interval for local dynamic-cache pruning.
const DYNAMIC_CACHE_PRUNE_INTERVAL_SECS: u64 = 60;
/// Maximum time allowed for graceful manager shutdown coordination.
const SHUTDOWN_TIMEOUT_SECS: u64 = 8;
/// Maximum number of RouterOS upserts issued concurrently by one observation.
const UPSERT_PIPELINE_SIZE: usize = 16;
/// Maximum time a dynamic key can go without a refresh attempt under steady
/// traffic.
const MAX_DYNAMIC_REFRESH_SUPPRESS_MS: u64 = 60_000;
/// Minimum refresh lead time before estimated RouterOS timeout expiry.
const MIN_DYNAMIC_REFRESH_LEAD_MS: u64 = 1_000;
/// Maximum refresh lead time before estimated RouterOS timeout expiry.
const MAX_DYNAMIC_REFRESH_LEAD_MS: u64 = 60_000;

/// Comment field storing the owning plugin tag.
const COMMENT_FIELD_PLUGIN: &str = "pg";
/// Comment field storing entry kind metadata.
const COMMENT_FIELD_KIND: &str = "kind";
/// Comment field storing the observed domain for dynamic entries.
const COMMENT_FIELD_DOMAIN: &str = "dm";
/// Compact comment marker for dynamic entries.
const COMMENT_KIND_DYNAMIC: &str = "D";
/// Compact comment marker for persistent entries.
const COMMENT_KIND_PERSISTENT: &str = "P";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum AddressListFamily {
    Ipv4,
    Ipv6,
}

impl AddressListFamily {
    #[inline]
    pub(super) fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }

    #[inline]
    pub(super) fn host_prefix(self) -> u8 {
        match self {
            Self::Ipv4 => HOST_PREFIX_V4,
            Self::Ipv6 => HOST_PREFIX_V6,
        }
    }

    #[inline]
    pub(super) fn is_valid_prefix(self, prefix: u8) -> bool {
        match self {
            Self::Ipv4 => prefix <= HOST_PREFIX_V4,
            Self::Ipv6 => prefix <= HOST_PREFIX_V6,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct AddressListKey {
    pub(super) family: AddressListFamily,
    pub(super) list: String,
    pub(super) address: IpAddr,
    pub(super) prefix: u8,
}

impl AddressListKey {
    pub(super) fn new(ip: IpAddr, list: String) -> Self {
        let family = AddressListFamily::from_ip(ip);
        Self {
            family,
            list,
            address: ip,
            prefix: family.host_prefix(),
        }
    }

    pub(super) fn new_with_prefix(ip: IpAddr, prefix: u8, list: String) -> Option<Self> {
        let family = AddressListFamily::from_ip(ip);
        if !family.is_valid_prefix(prefix) {
            return None;
        }
        Some(Self {
            family,
            list,
            address: normalize_network_ip(ip, prefix),
            prefix,
        })
    }

    #[inline]
    pub(super) fn normalized_value(&self) -> String {
        format!("{}/{}", self.address, self.prefix)
    }

    #[inline]
    pub(super) fn router_value(&self) -> String {
        if self.prefix == self.family.host_prefix() {
            self.address.to_string()
        } else {
            self.normalized_value()
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum OwnedCommentKind {
    Dynamic,
    Persistent,
}

impl OwnedCommentKind {
    #[inline]
    fn as_str(self) -> &'static str {
        match self {
            Self::Dynamic => COMMENT_KIND_DYNAMIC,
            Self::Persistent => COMMENT_KIND_PERSISTENT,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct OwnedCommentMeta {
    pub(super) kind: OwnedCommentKind,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DynamicRefreshState {
    /// Whether the remote entry was created without RouterOS timeout.
    timeless: bool,
    /// Timeout value written on the last successful RouterOS update.
    written_timeout_ms: u64,
    /// Local estimate of when the remote timeout will naturally expire.
    expires_at_ms: u64,
    /// Earliest local time when another refresh is worth sending.
    next_refresh_at_ms: u64,
    /// Successful-write sequence used to protect writes newer than a scan.
    generation: u64,
}

impl DynamicRefreshState {
    /// Build a suppression window after a successful dynamic write.
    ///
    /// The cache deliberately refreshes before the estimated remote expiry so
    /// periodic DNS traffic can extend entries without waiting for RouterOS to
    /// drop them first. At the same time, the suppress window is capped so very
    /// long TTLs do not completely stop background refreshes.
    fn from_write(now_ms: u64, timeout_secs: u32) -> Self {
        let timeout_ms = u64::from(timeout_secs).saturating_mul(1000);
        let expires_at_ms = now_ms.saturating_add(timeout_ms);
        let refresh_lead_ms = dynamic_refresh_lead_ms(timeout_ms);
        let near_expiry_refresh_at_ms = expires_at_ms.saturating_sub(refresh_lead_ms);
        let max_skip_refresh_at_ms = now_ms.saturating_add(MAX_DYNAMIC_REFRESH_SUPPRESS_MS);
        Self {
            timeless: false,
            written_timeout_ms: timeout_ms,
            expires_at_ms,
            next_refresh_at_ms: near_expiry_refresh_at_ms.min(max_skip_refresh_at_ms),
            generation: 0,
        }
    }

    #[inline]
    fn timeless() -> Self {
        Self {
            timeless: true,
            written_timeout_ms: 0,
            expires_at_ms: u64::MAX,
            next_refresh_at_ms: u64::MAX,
            generation: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DynamicTimeout {
    Timed(u32),
    Timeless,
}

#[derive(Debug, Clone)]
pub(super) struct AddressListManagerConfig {
    /// Plugin tag reused in RouterOS comments for ownership checks.
    pub(super) plugin_tag: String,
    /// IPv4 address-list name managed by this plugin.
    pub(super) address_list4: Option<String>,
    /// IPv6 address-list name managed by this plugin.
    pub(super) address_list6: Option<String>,
    /// Desired persistent set at startup.
    pub(super) persistent_items: AHashSet<AddressListKey>,
    /// Comment prefix used as an ownership fast-path.
    pub(super) comment_prefix: String,
    /// Minimum TTL clamp for dynamic observations.
    pub(super) min_ttl: u32,
    /// Maximum TTL clamp for dynamic observations.
    pub(super) max_ttl: u32,
    /// Optional fixed TTL override for dynamic observations.
    pub(super) fixed_ttl: Option<u32>,
    /// Hard upper bound for locally cached dynamic refresh states.
    pub(super) max_entries: usize,
}

#[derive(Debug)]
struct ObservationCommand {
    addrs: Vec<ObservedAddr>,
    waiters: Vec<oneshot::Sender<Result<()>>>,
}

impl Coalesce for ObservationCommand {
    fn coalesce(&mut self, mut newer: Self) {
        newer.waiters.append(&mut self.waiters);
        *self = newer;
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum ControlKey {
    Reconcile,
    PruneDynamicCache,
}

#[derive(Debug)]
enum ControlCommand {
    Reconcile,
    PruneDynamicCache,
}

impl Coalesce for ControlCommand {
    fn coalesce(&mut self, newer: Self) {
        *self = newer;
    }
}

#[derive(Debug)]
struct ReconcileSnapshot {
    scan_generation: u64,
    remote_dynamic: AHashSet<AddressListKey>,
}

#[derive(Debug)]
struct ShutdownRequest {
    cleanup: bool,
    done: oneshot::Sender<()>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum ObserveEnqueueError {
    Full,
    Closed,
}

#[derive(Debug, Clone)]
pub(super) struct AddressListManagerHandle {
    observations: KeyedMailbox<String, ObservationCommand>,
    controls: KeyedMailbox<ControlKey, ControlCommand>,
}

impl AddressListManagerHandle {
    pub(super) fn new() -> Self {
        Self {
            observations: KeyedMailbox::new(MANAGER_QUEUE_SIZE),
            controls: KeyedMailbox::new(CONTROL_QUEUE_SIZE),
        }
    }

    pub(super) fn try_observe(
        &self,
        domain: String,
        addrs: Vec<ObservedAddr>,
        wait: Option<oneshot::Sender<Result<()>>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        let command = ObservationCommand {
            addrs,
            waiters: wait.into_iter().collect(),
        };
        self.observations
            .try_push(domain, command)
            .map_err(|error| match error {
                TryPushError::Full(_) => ObserveEnqueueError::Full,
                TryPushError::Closed(_) => ObserveEnqueueError::Closed,
            })
    }

    pub(super) async fn observe(
        &self,
        domain: String,
        addrs: Vec<ObservedAddr>,
        wait: oneshot::Sender<Result<()>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        self.observations
            .push(
                domain,
                ObservationCommand {
                    addrs,
                    waiters: vec![wait],
                },
            )
            .await
            .map_err(|_| ObserveEnqueueError::Closed)
    }

    pub(super) fn request_reconcile(&self) -> bool {
        self.controls
            .try_push(ControlKey::Reconcile, ControlCommand::Reconcile)
            .is_ok()
    }

    fn request_prune(&self) {
        let _ = self.controls.try_push(
            ControlKey::PruneDynamicCache,
            ControlCommand::PruneDynamicCache,
        );
    }

    fn close(&self) {
        self.observations.close();
        self.controls.close();
    }

    #[cfg(test)]
    pub(super) fn queued_observations(&self) -> usize {
        self.observations.len()
    }

    #[cfg(test)]
    pub(super) fn take_reconcile_for_test(&self) -> bool {
        matches!(
            self.controls.try_recv(),
            Some((ControlKey::Reconcile, ControlCommand::Reconcile))
        )
    }
}

#[derive(Debug)]
enum WorkerCommand {
    Observe(String, ObservationCommand),
    Control(ControlCommand),
}

#[derive(Debug)]
pub(super) struct AddressListManagerRuntime {
    handle: AddressListManagerHandle,
    shutdown_tx: Option<oneshot::Sender<ShutdownRequest>>,
    /// Single-owner worker task that serializes all local state transitions.
    worker_handle: Option<JoinHandle<()>>,
    /// Local-memory cache prune loop.
    prune_task_id: Option<u64>,
    /// Periodic persistent reconcile loop.
    reconcile_task_id: Option<u64>,
}

impl AddressListManagerRuntime {
    pub(super) fn start(tag: String, manager: AddressListManager) -> Self {
        // All mutable state lives behind one worker to avoid cross-map locking
        // or request-path synchronization in the DNS hot path.
        let handle = AddressListManagerHandle::new();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker_tag = tag.clone();
        let worker_handle_mailbox = handle.clone();
        let worker_handle = Some(tokio::spawn(async move {
            run_manager_worker(worker_tag, manager, worker_handle_mailbox, shutdown_rx).await;
        }));

        // Startup reconciliation is deliberately queued onto the manager worker
        // instead of awaited during plugin init. Slow RouterOS list scans must
        // not prevent the DNS service from coming up.
        handle.request_reconcile();

        // Pruning is local-memory only. It never talks to RouterOS and exists
        // solely to keep the write-suppression cache bounded.
        let prune_handle = handle.clone();
        let prune_task_id = Some(task_center::spawn_fixed(
            format!("ros_address_list:{tag}:dynamic_cache_prune"),
            Duration::from_secs(DYNAMIC_CACHE_PRUNE_INTERVAL_SECS),
            move || {
                let prune_handle = prune_handle.clone();
                async move {
                    prune_handle.request_prune();
                }
            },
        ));

        // Reconcile also invalidates dynamic suppression state after users
        // manually remove RouterOS rows, so it runs even without persistent
        // entries.
        let reconcile_task_id = {
            let reconcile_handle = handle.clone();
            Some(task_center::spawn_fixed(
                format!("ros_address_list:{tag}:reconcile"),
                Duration::from_secs(RECONCILE_INTERVAL_SECS),
                move || {
                    let reconcile_handle = reconcile_handle.clone();
                    async move {
                        reconcile_handle.request_reconcile();
                    }
                },
            ))
        };

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            worker_handle,
            prune_task_id,
            reconcile_task_id,
        }
    }

    #[inline]
    pub(super) fn handle(&self) -> AddressListManagerHandle {
        self.handle.clone()
    }

    pub(super) async fn shutdown(mut self, cleanup: bool) {
        if let Some(task_id) = self.prune_task_id.take() {
            task_center::stop_task(task_id).await;
        }
        if let Some(task_id) = self.reconcile_task_id.take() {
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
        self.handle.close();
        let shutdown_acked = shutdown_requested
            && tokio::time::timeout(Duration::from_secs(SHUTDOWN_TIMEOUT_SECS), done_rx)
                .await
                .is_ok();
        if let Some(handle) = self.worker_handle.take() {
            if shutdown_acked {
                let _ =
                    tokio::time::timeout(Duration::from_secs(SHUTDOWN_TIMEOUT_SECS), handle).await;
            } else {
                handle.abort();
                let _ = handle.await;
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct AddressListManager {
    /// RouterOS API abstraction used by the single-owner worker.
    api: Arc<dyn MikrotikApi>,
    /// Immutable config shared across runtime decisions.
    cfg: AddressListManagerConfig,
    /// Current desired persistent set.
    persistent_items: AHashSet<AddressListKey>,
    /// Lightweight local cache that suppresses redundant dynamic refresh
    /// writes.
    dynamic_refresh_cache: AHashMap<AddressListKey, DynamicRefreshState>,
    /// Currently running background reconcile task, if any.
    reconcile_handle: Option<JoinHandle<Result<ReconcileSnapshot>>>,
    /// Monotonic local write generation for race-safe background snapshots.
    dynamic_generation: u64,
    /// One-time startup guard.
    initialized: bool,
}

impl AddressListManager {
    pub(super) fn new(api: Arc<dyn MikrotikApi>, cfg: AddressListManagerConfig) -> Self {
        Self {
            api,
            persistent_items: cfg.persistent_items.clone(),
            dynamic_refresh_cache: AHashMap::new(),
            reconcile_handle: None,
            dynamic_generation: 0,
            cfg,
            initialized: false,
        }
    }

    async fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Reconcile and cleanup immediately follow with real address-list
        // commands, so no separate identity-read permission is required.
        self.initialized = true;
        Ok(())
    }

    #[inline]
    fn effective_dynamic_timeout(&self, ttl_secs: u32) -> DynamicTimeout {
        // TTL policy is centralized here so dynamic observations and tests use
        // identical clamping semantics.
        if let Some(ttl) = self.cfg.fixed_ttl {
            return if ttl == 0 {
                DynamicTimeout::Timeless
            } else {
                DynamicTimeout::Timed(ttl)
            };
        }
        DynamicTimeout::Timed(ttl_secs.clamp(self.cfg.min_ttl, self.cfg.max_ttl))
    }

    #[inline]
    fn list_name_for(&self, family: AddressListFamily) -> Option<&str> {
        match family {
            AddressListFamily::Ipv4 => self.cfg.address_list4.as_deref(),
            AddressListFamily::Ipv6 => self.cfg.address_list6.as_deref(),
        }
    }

    #[inline]
    fn comment_for_dynamic(&self, domain: &str) -> String {
        encode_comment(
            self.cfg.comment_prefix.as_str(),
            self.cfg.plugin_tag.as_str(),
            OwnedCommentKind::Dynamic,
            Some(domain),
        )
    }

    #[inline]
    fn comment_for_persistent(&self) -> String {
        encode_comment(
            self.cfg.comment_prefix.as_str(),
            self.cfg.plugin_tag.as_str(),
            OwnedCommentKind::Persistent,
            None,
        )
    }

    fn should_refresh_dynamic_entry(
        &self,
        key: &AddressListKey,
        timeout: DynamicTimeout,
        now_ms: u64,
    ) -> bool {
        // Missing or expired cache means we have no recent successful remote write
        // to rely on, so the entry must be refreshed immediately.
        let Some(state) = self.dynamic_refresh_cache.get(key) else {
            return true;
        };
        match timeout {
            DynamicTimeout::Timeless => return !state.timeless,
            DynamicTimeout::Timed(_) if state.timeless => return true,
            DynamicTimeout::Timed(_) => {}
        }
        if now_ms >= state.expires_at_ms {
            return true;
        }

        // A longer TTL is always worth pushing immediately. Shorter TTLs are
        // intentionally ignored until the normal refresh window to avoid
        // excessive rewrite churn on frequently queried names.
        let DynamicTimeout::Timed(timeout_secs) = timeout else {
            return false;
        };
        let timeout_ms = u64::from(timeout_secs).saturating_mul(1000);
        timeout_ms > state.written_timeout_ms || now_ms >= state.next_refresh_at_ms
    }

    fn prune_dynamic_cache(&mut self, now_ms: u64) {
        // Step 1: drop obviously stale or now-persistent entries.
        self.dynamic_refresh_cache.retain(|key, state| {
            state.expires_at_ms > now_ms && !self.persistent_items.contains(key)
        });

        if self.dynamic_refresh_cache.len() <= self.cfg.max_entries {
            return;
        }

        // The cache only suppresses redundant writes, so arbitrary eviction is
        // safe and avoids sorting a large map on maintenance paths.
        while self.dynamic_refresh_cache.len() > self.cfg.max_entries {
            let Some(key) = self.dynamic_refresh_cache.keys().next().cloned() else {
                break;
            };
            self.dynamic_refresh_cache.remove(&key);
        }
    }

    fn cache_dynamic_write(&mut self, key: AddressListKey, mut state: DynamicRefreshState) {
        if !self.dynamic_refresh_cache.contains_key(&key)
            && self.dynamic_refresh_cache.len() >= self.cfg.max_entries
            && let Some(evicted) = self.dynamic_refresh_cache.keys().next().cloned()
        {
            self.dynamic_refresh_cache.remove(&evicted);
        }
        self.dynamic_generation = self.dynamic_generation.wrapping_add(1);
        state.generation = self.dynamic_generation;
        self.dynamic_refresh_cache.insert(key, state);
    }

    async fn reconcile_persistent_inner(&mut self) -> Result<AHashSet<AddressListKey>> {
        // Persistent reconcile treats RouterOS as a converged desired-set target:
        // ensure every configured persistent item exists, then remove stale owned
        // persistent entries that are no longer desired.
        let existing = self
            .api
            .list_entries(
                self.cfg.address_list4.as_deref(),
                self.cfg.address_list6.as_deref(),
            )
            .await?;
        let remote_dynamic = existing
            .iter()
            .filter(|entry| {
                decode_owned_comment(
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    entry.comment.as_deref(),
                )
                .is_some_and(|meta| meta.kind == OwnedCommentKind::Dynamic)
            })
            .map(|entry| entry.key.clone())
            .collect::<AHashSet<_>>();

        let desired_comment = self.comment_for_persistent();
        let persistent = self.persistent_items.iter().collect::<Vec<_>>();
        let mut first_error = None;
        for batch in persistent.chunks(UPSERT_PIPELINE_SIZE) {
            let results = futures::future::join_all(batch.iter().map(|key| {
                self.api.upsert_owned_entry(
                    key,
                    None,
                    desired_comment.as_str(),
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    false,
                )
            }))
            .await;
            for (key, result) in batch.iter().zip(results) {
                match result {
                    Ok(Some(())) => {}
                    Ok(None) => {
                        warn!(
                            plugin = %self.cfg.plugin_tag,
                            list = %key.list,
                            address = %key.normalized_value(),
                            "ros_address_list persistent entry conflicts with foreign address-list entry, skipping"
                        );
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        for entry in existing {
            let Some(meta) = decode_owned_comment(
                self.cfg.comment_prefix.as_str(),
                self.cfg.plugin_tag.as_str(),
                entry.comment.as_deref(),
            ) else {
                continue;
            };
            if meta.kind != OwnedCommentKind::Persistent {
                continue;
            }
            if self.persistent_items.contains(&entry.key) {
                continue;
            }
            if !self
                .is_stale_persistent_entry_still_deletable(&entry)
                .await?
            {
                continue;
            }
            self.api
                .delete_entry_by_id(&entry.id, entry.key.family)
                .await?;
        }

        Ok(remote_dynamic)
    }

    async fn is_stale_persistent_entry_still_deletable(
        &self,
        entry: &RouterListEntry,
    ) -> Result<bool> {
        let current_entries = self.api.list_entries_by_key(&entry.key).await?;
        Ok(current_entries.into_iter().any(|current| {
            current.id == entry.id
                && !self.persistent_items.contains(&current.key)
                && decode_owned_comment(
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    current.comment.as_deref(),
                )
                .is_some_and(|meta| meta.kind == OwnedCommentKind::Persistent)
        }))
    }

    fn spawn_background_reconcile(&mut self, tag: String) {
        if self
            .reconcile_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            debug!(
                plugin = %tag,
                "ros_address_list reconcile already running, skipping duplicate request"
            );
            return;
        }

        let api = self.api.clone();
        let mut cfg = self.cfg.clone();
        cfg.persistent_items = self.persistent_items.clone();
        let scan_generation = self.dynamic_generation;
        self.reconcile_handle = Some(tokio::spawn(async move {
            let mut manager = AddressListManager::new(api, cfg);
            manager
                .reconcile_persistent_inner()
                .await
                .map(|remote_dynamic| ReconcileSnapshot {
                    scan_generation,
                    remote_dynamic,
                })
        }));
    }

    async fn harvest_background_reconcile(&mut self, tag: &str) {
        if !self
            .reconcile_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return;
        }
        let Some(handle) = self.reconcile_handle.take() else {
            return;
        };
        match handle.await {
            Ok(Ok(ReconcileSnapshot {
                scan_generation,
                remote_dynamic,
            })) => {
                self.dynamic_refresh_cache.retain(|key, state| {
                    state.generation > scan_generation || remote_dynamic.contains(key)
                });
                debug!(plugin = %tag, "ros_address_list background reconcile completed");
            }
            Ok(Err(error)) => {
                warn!(
                    plugin = %tag,
                    err = %error,
                    "ros_address_list background reconcile failed"
                );
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                warn!(
                    plugin = %tag,
                    err = %error,
                    "ros_address_list background reconcile task failed"
                );
            }
        }
    }

    async fn observe_domain_inner(
        &mut self,
        domain: String,
        addrs: Vec<ObservedAddr>,
        now_ms: u64,
    ) -> Result<()> {
        // Keep the local suppression cache healthy before evaluating refreshes.
        let mut dedup = AHashMap::<AddressListKey, DynamicTimeout>::new();
        for observed in addrs {
            let family = AddressListFamily::from_ip(observed.addr);
            let Some(list) = self.list_name_for(family) else {
                continue;
            };
            let key = AddressListKey::new(observed.addr, list.to_string());
            if self.persistent_items.contains(&key) {
                continue;
            }
            let timeout = self.effective_dynamic_timeout(observed.ttl_secs.max(1));
            dedup
                .entry(key)
                .and_modify(|existing| {
                    if let (DynamicTimeout::Timed(existing_ttl), DynamicTimeout::Timed(ttl)) =
                        (existing, timeout)
                    {
                        *existing_ttl = (*existing_ttl).max(ttl);
                    }
                })
                .or_insert(timeout);
        }

        if dedup.is_empty() {
            return Ok(());
        }

        // Phase 1: collect entries that actually need a remote write, along with
        // their pre-formatted timeout strings so the borrow checker lets us hand
        // shared references to the concurrent futures below.
        let to_refresh: Vec<(AddressListKey, DynamicTimeout, Option<String>)> = dedup
            .into_iter()
            .filter_map(|(key, timeout)| {
                if !self.should_refresh_dynamic_entry(&key, timeout, now_ms) {
                    return None;
                }
                let timeout_value = match timeout {
                    DynamicTimeout::Timed(ttl) => Some(format!("{ttl}s")),
                    DynamicTimeout::Timeless => None,
                };
                Some((key, timeout, timeout_value))
            })
            .collect();

        if to_refresh.is_empty() {
            return Ok(());
        }

        let comment = self.comment_for_dynamic(domain.as_str());

        // Phase 2: pipeline upserts in bounded batches. The dependency uses a
        // bounded response channel per command, so an unbounded join_all would
        // let unusually large CDN answers create excessive in-flight work.
        let api = self.api.clone();
        let comment_str = comment.as_str();
        let comment_prefix = self.cfg.comment_prefix.clone();
        let plugin_tag = self.cfg.plugin_tag.clone();
        let mut first_error: Option<DnsError> = None;
        for batch in to_refresh.chunks(UPSERT_PIPELINE_SIZE) {
            let results =
                futures::future::join_all(batch.iter().map(|(key, timeout, timeout_value)| {
                    api.upsert_owned_entry(
                        key,
                        timeout_value.as_deref(),
                        comment_str,
                        comment_prefix.as_str(),
                        plugin_tag.as_str(),
                        matches!(timeout, DynamicTimeout::Timed(_)),
                    )
                }))
                .await;

            // Phase 3: update suppression state per result so one failure does
            // not discard successful writes from the same response.
            for ((key, timeout, _), result) in batch.iter().zip(results) {
                match result {
                    Ok(Some(())) => {
                        let state = match timeout {
                            DynamicTimeout::Timed(ttl) => {
                                DynamicRefreshState::from_write(now_ms, *ttl)
                            }
                            DynamicTimeout::Timeless => DynamicRefreshState::timeless(),
                        };
                        self.cache_dynamic_write(key.clone(), state);
                    }
                    Ok(None) => {
                        self.dynamic_refresh_cache.remove(key);
                        warn!(
                            plugin = %self.cfg.plugin_tag,
                            list = %key.list,
                            address = %key.normalized_value(),
                            "ros_address_list dynamic entry conflicts with foreign address-list entry, skipping"
                        );
                    }
                    Err(err) => {
                        self.dynamic_refresh_cache.remove(key);
                        first_error.get_or_insert(err);
                    }
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    pub(super) async fn observe_domain(
        &mut self,
        domain: String,
        addrs: Vec<ObservedAddr>,
    ) -> Result<()> {
        let tag = self.cfg.plugin_tag.clone();
        self.harvest_background_reconcile(tag.as_str()).await;
        self.observe_domain_inner(domain, addrs, now_millis()).await
    }

    #[cfg(test)]
    pub(super) async fn update_persistent_items(
        &mut self,
        items: AHashSet<AddressListKey>,
    ) -> Result<()> {
        self.ensure_initialized().await?;
        // Persistent ownership takes precedence over any cached dynamic state.
        self.persistent_items = items;
        self.prune_dynamic_cache(now_millis());
        let remote_dynamic = self.reconcile_persistent_inner().await?;
        self.dynamic_refresh_cache
            .retain(|key, _| remote_dynamic.contains(key));
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn reconcile(&mut self) -> Result<()> {
        self.ensure_initialized().await?;
        self.prune_dynamic_cache(now_millis());
        let remote_dynamic = self.reconcile_persistent_inner().await?;
        self.dynamic_refresh_cache
            .retain(|key, _| remote_dynamic.contains(key));
        Ok(())
    }

    pub(super) async fn prune_dynamic_cache_now(&mut self) -> Result<()> {
        let tag = self.cfg.plugin_tag.clone();
        self.harvest_background_reconcile(tag.as_str()).await;
        self.prune_dynamic_cache(now_millis());
        Ok(())
    }

    pub(super) async fn shutdown(&mut self, cleanup: bool) -> Result<()> {
        if let Some(handle) = self.reconcile_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        if !cleanup {
            self.dynamic_refresh_cache.clear();
            return Ok(());
        }

        // Cleanup only touches entries that match this plugin's comment ownership.
        self.ensure_initialized().await?;
        let entries = self
            .api
            .list_entries(
                self.cfg.address_list4.as_deref(),
                self.cfg.address_list6.as_deref(),
            )
            .await?;
        let owned = entries
            .into_iter()
            .filter(|entry| {
                decode_owned_comment(
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    entry.comment.as_deref(),
                )
                .is_some()
            })
            .collect::<Vec<_>>();
        let mut first_error = None;
        for batch in owned.chunks(UPSERT_PIPELINE_SIZE) {
            let results = futures::future::join_all(
                batch
                    .iter()
                    .map(|entry| self.api.delete_entry_by_id(&entry.id, entry.key.family)),
            )
            .await;
            for result in results {
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.dynamic_refresh_cache.clear();
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(super) fn dynamic_cache_len(&self) -> usize {
        self.dynamic_refresh_cache.len()
    }

    #[cfg(test)]
    pub(super) async fn observe_domain_at_for_test(
        &mut self,
        domain: String,
        addrs: Vec<ObservedAddr>,
        now_ms: u64,
    ) -> Result<()> {
        self.observe_domain_inner(domain, addrs, now_ms).await
    }

    #[cfg(test)]
    pub(super) async fn background_reconcile_for_test(&mut self) {
        let tag = self.cfg.plugin_tag.clone();
        self.spawn_background_reconcile(tag.clone());
        while self
            .reconcile_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            tokio::task::yield_now().await;
        }
        self.harvest_background_reconcile(tag.as_str()).await;
    }

    #[cfg(test)]
    pub(super) async fn prune_dynamic_cache_at_for_test(&mut self, now_ms: u64) -> Result<()> {
        self.prune_dynamic_cache(now_ms);
        Ok(())
    }
}

pub(super) fn encode_comment(
    prefix: &str,
    plugin_tag: &str,
    kind: OwnedCommentKind,
    domain: Option<&str>,
) -> String {
    // Comments intentionally stay compact because they live on RouterOS objects
    // and are parsed frequently during reconciliation and cleanup.
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
    if let Some(domain) = domain {
        out.push(';');
        out.push_str(COMMENT_FIELD_DOMAIN);
        out.push('=');
        out.push_str(domain);
    }
    out
}

pub(super) fn decode_owned_comment(
    prefix: &str,
    plugin_tag: &str,
    comment: Option<&str>,
) -> Option<OwnedCommentMeta> {
    // Prefix and plugin-tag checks provide a fast ownership filter before the
    // caller considers deleting or modifying an entry.
    let comment = comment?;
    if !prefix.is_empty() {
        if !comment.starts_with(prefix) {
            return None;
        }
        if comment.as_bytes().get(prefix.len()) != Some(&b';') {
            return None;
        }
    }

    let mut plugin_matches = false;
    let mut kind = None;
    for token in comment.split(';') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        match key.trim() {
            COMMENT_FIELD_PLUGIN if value.trim() == plugin_tag => plugin_matches = true,
            COMMENT_FIELD_KIND => {
                kind = match value.trim() {
                    COMMENT_KIND_DYNAMIC => Some(OwnedCommentKind::Dynamic),
                    COMMENT_KIND_PERSISTENT => Some(OwnedCommentKind::Persistent),
                    _ => None,
                };
            }
            _ => {}
        }
    }

    if plugin_matches {
        kind.map(|kind| OwnedCommentMeta { kind })
    } else {
        None
    }
}

async fn run_manager_worker(
    tag: String,
    mut manager: AddressListManager,
    handle: AddressListManagerHandle,
    mut shutdown_rx: oneshot::Receiver<ShutdownRequest>,
) {
    // Every state transition is serialized here. Request-path code only pushes
    // commands into the mailbox and never mutates manager state directly.
    let mut prefer_control = true;
    loop {
        let command = if prefer_control {
            tokio::select! {
                biased;
                shutdown = &mut shutdown_rx => {
                    if let Ok(ShutdownRequest { cleanup, done }) = shutdown {
                        if let Err(e) = manager.shutdown(cleanup).await {
                            warn!(plugin = %tag, err = %e, "ros_address_list shutdown cleanup failed");
                        }
                        let _ = done.send(());
                    }
                    break;
                }
                control = handle.controls.recv() => control.map(|(_, command)| WorkerCommand::Control(command)),
                observation = handle.observations.recv() => observation.map(|(domain, command)| WorkerCommand::Observe(domain, command)),
            }
        } else {
            tokio::select! {
                biased;
                shutdown = &mut shutdown_rx => {
                    if let Ok(ShutdownRequest { cleanup, done }) = shutdown {
                        if let Err(e) = manager.shutdown(cleanup).await {
                            warn!(plugin = %tag, err = %e, "ros_address_list shutdown cleanup failed");
                        }
                        let _ = done.send(());
                    }
                    break;
                }
                observation = handle.observations.recv() => observation.map(|(domain, command)| WorkerCommand::Observe(domain, command)),
                control = handle.controls.recv() => control.map(|(_, command)| WorkerCommand::Control(command)),
            }
        };
        let Some(command) = command else {
            break;
        };
        match command {
            WorkerCommand::Observe(domain, ObservationCommand { addrs, waiters }) => {
                prefer_control = true;
                let result = manager.observe_domain(domain, addrs).await;
                if waiters.is_empty() {
                    if let Err(e) = result {
                        warn!(
                            plugin = %tag,
                            err = %e,
                            "ros_address_list observe failed in async mode"
                        );
                    }
                } else {
                    let outcome = result.map_err(|error| error.to_string());
                    for waiter in waiters {
                        let result = outcome
                            .as_ref()
                            .map(|_| ())
                            .map_err(|message| DnsError::plugin(message.clone()));
                        let _ = waiter.send(result);
                    }
                }
            }
            WorkerCommand::Control(command) => {
                prefer_control = false;
                match command {
                    ControlCommand::Reconcile => {
                        manager.harvest_background_reconcile(tag.as_str()).await;
                        manager.spawn_background_reconcile(tag.clone());
                    }
                    ControlCommand::PruneDynamicCache => {
                        if let Err(e) = manager.prune_dynamic_cache_now().await {
                            warn!(
                                plugin = %tag,
                                err = %e,
                                "ros_address_list dynamic cache prune failed"
                            );
                        }
                    }
                }
            }
        }
    }

    debug!(plugin = %tag, "ros_address_list manager worker exited");
}

fn dynamic_refresh_lead_ms(timeout_ms: u64) -> u64 {
    // Refresh slightly ahead of the estimated remote expiry while keeping both
    // extremely short and extremely long TTLs within practical bounds.
    (timeout_ms / 4).clamp(MIN_DYNAMIC_REFRESH_LEAD_MS, MAX_DYNAMIC_REFRESH_LEAD_MS)
}

fn now_millis() -> u64 {
    AppClock::elapsed_millis()
}

fn normalize_network_ip(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (HOST_PREFIX_V4 - prefix)
            };
            IpAddr::V4((raw & mask).into())
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (HOST_PREFIX_V6 - prefix)
            };
            IpAddr::V6((raw & mask).into())
        }
    }
}

pub(super) fn parse_router_address(family: AddressListFamily, raw: &str) -> Option<(IpAddr, u8)> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if let Some((ip_raw, prefix_raw)) = value.split_once('/') {
        let ip = ip_raw.parse::<IpAddr>().ok()?;
        let prefix = prefix_raw.parse::<u8>().ok()?;
        if AddressListFamily::from_ip(ip) != family || !family.is_valid_prefix(prefix) {
            return None;
        }
        return Some((normalize_network_ip(ip, prefix), prefix));
    }

    let ip = value.parse::<IpAddr>().ok()?;
    if AddressListFamily::from_ip(ip) != family {
        return None;
    }
    Some((ip, family.host_prefix()))
}
