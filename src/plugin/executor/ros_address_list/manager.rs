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
use std::sync::atomic::Ordering;
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::RosMetrics;
use super::api::{MikrotikApi, RouterListEntry};
use crate::infra::clock::AppClock;
use crate::infra::error::{DnsError, Result};
use crate::infra::mikrotik::batching::join_all_bounded;
use crate::infra::mikrotik::completion::BatchCompletion;
use crate::infra::mikrotik::ip_prefix::IpPrefix;
use crate::infra::mikrotik::lease::{LeaseBook, LeaseDeadline, LeasePolicy};
use crate::infra::mikrotik::lifecycle::abort_and_reap;
use crate::infra::mikrotik::mailbox::{Coalesce, KeyedMailbox, PushOutcome, TryPushError};
use crate::infra::mikrotik::reconcile::{BackgroundReconcile, ReconcileRetry, VersionedSnapshot};
use crate::infra::mikrotik::throttle::ErrorLogThrottle;
use crate::infra::mikrotik::{ObservedAddr, SHUTDOWN_TIMEOUT};
use crate::infra::task as task_center;

/// Host prefix used for normalized IPv4 single-address entries.
const HOST_PREFIX_V4: u8 = 32;
/// Host prefix used for normalized IPv6 single-address entries.
const HOST_PREFIX_V6: u8 = 128;
/// Maximum number of distinct address-list keys waiting for manager processing.
const MANAGER_QUEUE_SIZE: usize = 1024;
const CONTROL_QUEUE_SIZE: usize = 2;
/// Periodic interval for persistent desired-set reconciliation.
const RECONCILE_INTERVAL_SECS: u64 = 180;
/// Periodic interval for local dynamic-cache pruning.
const DYNAMIC_CACHE_PRUNE_INTERVAL_SECS: u64 = 60;
/// Maximum number of RouterOS upserts issued concurrently by one observation.
const UPSERT_PIPELINE_SIZE: usize = 16;
/// Comment field storing the owning plugin tag.
const COMMENT_FIELD_PLUGIN: &str = "pg";
/// Comment field storing entry kind metadata.
const COMMENT_FIELD_KIND: &str = "kind";
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
        let prefix = IpPrefix::host(ip);
        Self {
            family,
            list,
            address: prefix.address(),
            prefix: prefix.prefix(),
        }
    }

    pub(super) fn new_with_prefix(ip: IpAddr, prefix: u8, list: String) -> Option<Self> {
        let normalized = IpPrefix::new(ip, prefix)?;
        let family = AddressListFamily::from_ip(normalized.address());
        Some(Self {
            family,
            list,
            address: normalized.address(),
            prefix: normalized.prefix(),
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
}

#[derive(Debug, Clone)]
struct AddressObservation {
    /// Absolute RouterOS timeout deadline. `None` is timeless.
    expires_at_ms: Option<u64>,
    observed_at_ms: u64,
}

#[derive(Debug)]
struct AddressListSnapshot {
    captured_at_ms: u64,
    entries: Vec<RouterListEntry>,
}

#[derive(Debug, Clone)]
struct ObservationCommand {
    observation: AddressObservation,
    completions: Vec<Arc<BatchCompletion>>,
}

impl Coalesce for ObservationCommand {
    fn coalesce(&mut self, mut newer: Self) {
        let keep_newer = match (
            self.observation.expires_at_ms,
            newer.observation.expires_at_ms,
        ) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(current), Some(next)) => next >= current,
        };
        self.completions.append(&mut newer.completions);
        if keep_newer {
            self.observation = newer.observation;
        }
    }
}

#[derive(Debug, Clone)]
struct AddressObservationPolicy {
    address_list4: Option<String>,
    address_list6: Option<String>,
    lease: LeasePolicy,
}

impl AddressObservationPolicy {
    fn from_config(config: &AddressListManagerConfig) -> Self {
        Self {
            address_list4: config.address_list4.clone(),
            address_list6: config.address_list6.clone(),
            lease: LeasePolicy::new(config.min_ttl, config.max_ttl, config.fixed_ttl),
        }
    }

    fn list_for(&self, family: AddressListFamily) -> Option<&str> {
        match family {
            AddressListFamily::Ipv4 => self.address_list4.as_deref(),
            AddressListFamily::Ipv6 => self.address_list6.as_deref(),
        }
    }

    fn commands(&self, addrs: Vec<ObservedAddr>) -> Vec<(AddressListKey, AddressObservation)> {
        self.commands_at(addrs, now_millis())
    }

    fn commands_at(
        &self,
        addrs: Vec<ObservedAddr>,
        now: u64,
    ) -> Vec<(AddressListKey, AddressObservation)> {
        let mut observations = AHashMap::<AddressListKey, AddressObservation>::new();
        for observed in addrs {
            let family = AddressListFamily::from_ip(observed.addr);
            let Some(list) = self.list_for(family) else {
                continue;
            };
            let key = AddressListKey::new(observed.addr, list.to_string());
            let deadline = self.lease.deadline(observed.ttl_secs, now);
            let observation = AddressObservation {
                expires_at_ms: deadline.unix_millis(),
                observed_at_ms: now,
            };
            observations
                .entry(key)
                .and_modify(|current| {
                    let replace = match (current.expires_at_ms, observation.expires_at_ms) {
                        (_, None) => true,
                        (None, Some(_)) => false,
                        (Some(current), Some(next)) => next >= current,
                    };
                    if replace {
                        *current = observation.clone();
                    }
                })
                .or_insert(observation);
        }
        observations.into_iter().collect()
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

#[derive(Debug, Default)]
pub(super) struct AddressListPendingWork {
    items: Vec<(AddressListKey, ObservationCommand)>,
}

#[derive(Debug)]
enum LifecycleCommand {
    Quiesce {
        targets: AHashSet<(AddressListFamily, String)>,
        done: oneshot::Sender<AddressListPendingWork>,
    },
    Activate {
        pending: AddressListPendingWork,
        done: oneshot::Sender<()>,
    },
}

impl Coalesce for ControlCommand {
    fn coalesce(&mut self, newer: Self) {
        *self = newer;
    }
}

#[derive(Debug)]
struct ShutdownRequest {
    cleanup: AddressListCleanupScope,
    done: oneshot::Sender<Result<()>>,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(super) struct AddressListCleanupScope {
    pub(super) ipv4: bool,
    pub(super) ipv6: bool,
}

impl AddressListCleanupScope {
    pub(super) const fn none() -> Self {
        Self {
            ipv4: false,
            ipv6: false,
        }
    }

    #[cfg(test)]
    pub(super) const fn all() -> Self {
        Self {
            ipv4: true,
            ipv6: true,
        }
    }

    fn is_empty(self) -> bool {
        !self.ipv4 && !self.ipv6
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum ObserveEnqueueError {
    Full,
    Closed,
}

#[derive(Debug, Clone)]
pub(super) struct AddressListManagerHandle {
    observations: KeyedMailbox<AddressListKey, ObservationCommand>,
    controls: KeyedMailbox<ControlKey, ControlCommand>,
    policy: AddressObservationPolicy,
    metrics: Option<Arc<RosMetrics>>,
    lifecycle: Option<mpsc::Sender<LifecycleCommand>>,
}

impl AddressListManagerHandle {
    fn new(
        config: &AddressListManagerConfig,
        metrics: Option<Arc<RosMetrics>>,
        lifecycle: Option<mpsc::Sender<LifecycleCommand>>,
    ) -> Self {
        Self {
            observations: KeyedMailbox::new(MANAGER_QUEUE_SIZE),
            controls: KeyedMailbox::new(CONTROL_QUEUE_SIZE),
            policy: AddressObservationPolicy::from_config(config),
            metrics,
            lifecycle,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test() -> Self {
        AppClock::start();
        Self::new(
            &AddressListManagerConfig {
                plugin_tag: "test".to_string(),
                address_list4: Some("test_v4".to_string()),
                address_list6: Some("test_v6".to_string()),
                persistent_items: AHashSet::new(),
                comment_prefix: "fdns".to_string(),
                min_ttl: 60,
                max_ttl: 3600,
                fixed_ttl: None,
            },
            None,
            None,
        )
    }

    fn refresh_pending_metric_with(&self, extra: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.pending_observations.store(
                self.observations.len().saturating_add(extra) as u64,
                Ordering::Relaxed,
            );
        }
    }

    fn record_outcome(&self, outcome: PushOutcome) {
        if matches!(outcome, PushOutcome::Coalesced)
            && let Some(metrics) = &self.metrics
        {
            metrics.coalesced_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn try_observe(
        &self,
        addrs: Vec<ObservedAddr>,
        wait: Option<oneshot::Sender<Result<()>>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        let commands = self.policy.commands(addrs);
        if commands.is_empty() {
            if let Some(waiter) = wait {
                let _ = waiter.send(Ok(()));
            }
            return Ok(PushOutcome::Inserted);
        }
        let completion = wait.map(|waiter| BatchCompletion::new(commands.len(), waiter));
        let mut outcome = PushOutcome::Coalesced;
        let mut enqueue_error = None;
        for (key, observation) in commands {
            let command = ObservationCommand {
                observation,
                completions: completion.iter().cloned().collect(),
            };
            match self.observations.try_push(key, command) {
                Ok(item_outcome @ PushOutcome::Inserted) => {
                    self.record_outcome(item_outcome);
                    outcome = PushOutcome::Inserted;
                }
                Ok(item_outcome @ PushOutcome::Coalesced) => self.record_outcome(item_outcome),
                Err(TryPushError::Full(command)) => {
                    for completion in command.completions {
                        completion.finish(&Err(DnsError::plugin(
                            "ros_address_list observation mailbox is full",
                        )));
                    }
                    enqueue_error.get_or_insert(ObserveEnqueueError::Full);
                }
                Err(TryPushError::Closed(command)) => {
                    for completion in command.completions {
                        completion.finish(&Err(DnsError::plugin(
                            "ros_address_list observation mailbox is closed",
                        )));
                    }
                    enqueue_error = Some(ObserveEnqueueError::Closed);
                }
            }
        }
        self.refresh_pending_metric_with(0);
        enqueue_error.map_or(Ok(outcome), Err)
    }

    pub(super) async fn observe(
        &self,
        addrs: Vec<ObservedAddr>,
        wait: oneshot::Sender<Result<()>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        let commands = self.policy.commands(addrs);
        if commands.is_empty() {
            let _ = wait.send(Ok(()));
            return Ok(PushOutcome::Inserted);
        }
        let completion = BatchCompletion::new(commands.len(), wait);
        let mut outcome = PushOutcome::Coalesced;
        let total = commands.len();
        for (index, (key, observation)) in commands.into_iter().enumerate() {
            let command = ObservationCommand {
                observation,
                completions: vec![completion.clone()],
            };
            match self.observations.push(key, command).await {
                Ok(item_outcome @ PushOutcome::Inserted) => {
                    self.record_outcome(item_outcome);
                    outcome = PushOutcome::Inserted;
                }
                Ok(item_outcome @ PushOutcome::Coalesced) => self.record_outcome(item_outcome),
                Err(error) => {
                    for completion in error.0.completions {
                        completion.finish(&Err(DnsError::plugin(
                            "ros_address_list observation mailbox is closed",
                        )));
                    }
                    for _ in index + 1..total {
                        completion.finish(&Err(DnsError::plugin(
                            "ros_address_list observation mailbox is closed",
                        )));
                    }
                    return Err(ObserveEnqueueError::Closed);
                }
            }
        }
        self.refresh_pending_metric_with(0);
        Ok(outcome)
    }

    pub(super) fn request_reconcile(&self) -> bool {
        self.controls
            .try_push(ControlKey::Reconcile, ControlCommand::Reconcile)
            .is_ok()
    }

    pub(super) async fn quiesce_targets(
        &self,
        targets: AHashSet<(AddressListFamily, String)>,
    ) -> AddressListPendingWork {
        let Some(lifecycle) = &self.lifecycle else {
            return AddressListPendingWork::default();
        };
        let (done, wait) = oneshot::channel();
        if lifecycle
            .send(LifecycleCommand::Quiesce { targets, done })
            .await
            .is_err()
        {
            return AddressListPendingWork::default();
        }
        wait.await.unwrap_or_default()
    }

    pub(super) async fn activate(&self, pending: AddressListPendingWork) -> Result<()> {
        let Some(lifecycle) = &self.lifecycle else {
            return Ok(());
        };
        let (done, wait) = oneshot::channel();
        lifecycle
            .send(LifecycleCommand::Activate { pending, done })
            .await
            .map_err(|_| {
                DnsError::plugin("ros_address_list manager lifecycle channel is closed")
            })?;
        wait.await
            .map_err(|_| DnsError::plugin("ros_address_list manager activation was cancelled"))
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
    Observe {
        batch: Vec<(AddressListKey, ObservationCommand)>,
        from_retry: bool,
    },
    Control(ControlCommand),
    ReconcileCompleted,
    Lifecycle(LifecycleCommand),
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
    #[cfg(test)]
    pub(super) fn start(tag: String, manager: AddressListManager) -> Self {
        Self::start_with_state(tag, manager, true)
    }

    pub(super) fn start_paused(tag: String, manager: AddressListManager) -> Self {
        Self::start_with_state(tag, manager, false)
    }

    fn start_with_state(tag: String, manager: AddressListManager, active: bool) -> Self {
        // All mutable state lives behind one worker to avoid cross-map locking
        // or request-path synchronization in the DNS hot path.
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(1);
        let handle = AddressListManagerHandle::new(
            &manager.cfg,
            manager.metrics.clone(),
            Some(lifecycle_tx),
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker_tag = tag.clone();
        let worker_handle_mailbox = handle.clone();
        let worker_handle = Some(tokio::spawn(async move {
            run_manager_worker(
                worker_tag,
                manager,
                worker_handle_mailbox,
                lifecycle_rx,
                active,
                shutdown_rx,
            )
            .await;
        }));

        // Startup reconciliation is deliberately queued onto the manager worker
        // instead of awaited during plugin init. Slow RouterOS list scans must
        // not prevent the DNS service from coming up.
        if active {
            handle.request_reconcile();
        }

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

    pub(super) async fn shutdown(mut self, cleanup: AddressListCleanupScope) -> Result<()> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let tasks = [self.prune_task_id.take(), self.reconcile_task_id.take()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for (index, task) in tasks.iter().copied().enumerate() {
            if tokio::time::timeout_at(deadline, task_center::stop_task(task))
                .await
                .is_err()
            {
                for remaining in &tasks[index..] {
                    task_center::stop_task_detached(*remaining);
                }
                self.handle.close();
                if let Some(worker) = self.worker_handle.take() {
                    abort_and_reap(worker);
                }
                return Err(DnsError::plugin(format!(
                    "ros_address_list shutdown exceeded {} seconds",
                    SHUTDOWN_TIMEOUT.as_secs()
                )));
            }
        }

        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();
        let shutdown_requested = self.shutdown_tx.take().is_some_and(|tx| {
            tx.send(ShutdownRequest {
                cleanup,
                done: done_tx,
            })
            .is_ok()
        });
        self.handle.close();
        let result = if shutdown_requested {
            match tokio::time::timeout_at(deadline, done_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(DnsError::plugin(
                    "ros_address_list shutdown worker closed before reporting cleanup",
                )),
                Err(_) => Err(DnsError::plugin(format!(
                    "ros_address_list shutdown exceeded {} seconds",
                    SHUTDOWN_TIMEOUT.as_secs()
                ))),
            }
        } else {
            Ok(())
        };
        if let Some(mut handle) = self.worker_handle.take()
            && tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
        {
            abort_and_reap(handle);
            return Err(DnsError::plugin(format!(
                "ros_address_list shutdown exceeded {} seconds while joining worker",
                SHUTDOWN_TIMEOUT.as_secs()
            )));
        }
        result
    }
}

#[derive(Debug)]
pub(super) struct AddressListManager {
    /// RouterOS API abstraction used by the single-owner worker.
    api: Arc<dyn MikrotikApi>,
    metrics: Option<Arc<RosMetrics>>,
    /// Immutable config shared across runtime decisions.
    cfg: AddressListManagerConfig,
    /// Current desired persistent set.
    persistent_items: AHashSet<AddressListKey>,
    /// Dynamic leases and successful-write refresh suppression.
    leases: LeaseBook<AddressListKey>,
    /// Single-flight background RouterOS snapshot.
    reconcile: BackgroundReconcile<AddressListSnapshot>,
    reconcile_retry: ReconcileRetry,
    /// An empty local state still requires one successful remote scan so stale
    /// persistent rows from a previous configuration can be removed.
    empty_state_needs_reconcile: bool,
}

impl AddressListManager {
    pub(super) fn new(api: Arc<dyn MikrotikApi>, cfg: AddressListManagerConfig) -> Self {
        Self {
            api,
            metrics: None,
            persistent_items: cfg.persistent_items.clone(),
            leases: LeaseBook::new(),
            reconcile: BackgroundReconcile::new(),
            reconcile_retry: ReconcileRetry::default(),
            empty_state_needs_reconcile: true,
            cfg,
        }
    }

    pub(super) fn with_metrics(
        api: Arc<dyn MikrotikApi>,
        cfg: AddressListManagerConfig,
        metrics: Arc<RosMetrics>,
    ) -> Self {
        let mut manager = Self::new(api, cfg);
        manager.metrics = Some(metrics);
        manager.refresh_managed_metric();
        manager
    }

    fn refresh_managed_metric(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.managed_entries.store(
                self.leases
                    .len()
                    .saturating_add(self.persistent_items.len()) as u64,
                Ordering::Relaxed,
            );
        }
    }

    async fn refresh_transport_metrics(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        if let Some(snapshot) = self.api.transport_snapshot().await {
            metrics
                .reconnect_total
                .store(snapshot.reconnect_total, Ordering::Relaxed);
            metrics
                .connect_attempt_total
                .store(snapshot.connect_attempt_total, Ordering::Relaxed);
            metrics
                .backoff_total
                .store(snapshot.backoff_total, Ordering::Relaxed);
            metrics
                .degraded
                .store(u64::from(snapshot.degraded), Ordering::Relaxed);
        }
    }

    #[inline]
    fn comment_for_dynamic(&self) -> String {
        encode_comment(
            self.cfg.comment_prefix.as_str(),
            self.cfg.plugin_tag.as_str(),
            OwnedCommentKind::Dynamic,
        )
    }

    #[inline]
    fn comment_for_persistent(&self) -> String {
        encode_comment(
            self.cfg.comment_prefix.as_str(),
            self.cfg.plugin_tag.as_str(),
            OwnedCommentKind::Persistent,
        )
    }

    fn should_refresh_dynamic_entry(&self, key: &AddressListKey, now_ms: u64) -> bool {
        self.leases
            .get(key)
            .is_none_or(|lease| lease.needs_sync(now_ms))
    }

    fn prune_dynamic_cache(&mut self, now_ms: u64) {
        self.leases.retain(|key, lease| {
            !lease.desired().is_expired(now_ms) && !self.persistent_items.contains(key)
        });
    }

    fn cache_dynamic_write(&mut self, key: &AddressListKey, now_ms: u64) -> bool {
        self.empty_state_needs_reconcile = true;
        let confirmed = self.leases.confirm_synced(key, now_ms);
        self.refresh_managed_metric();
        confirmed
    }

    #[cfg(test)]
    pub(super) async fn apply_reconcile_snapshot(
        &mut self,
        existing: Vec<RouterListEntry>,
        scan_generation: u64,
    ) -> Result<()> {
        self.apply_reconcile_snapshot_at(existing, scan_generation, now_millis())
            .await
    }

    async fn apply_reconcile_snapshot_at(
        &mut self,
        existing: Vec<RouterListEntry>,
        scan_generation: u64,
        captured_at_ms: u64,
    ) -> Result<()> {
        // The background task only reads RouterOS. The single state owner
        // classifies the snapshot, mutates local state, and executes the
        // resulting precise persistent diff.
        let desired_comment = self.comment_for_persistent();
        let correct_persistent = existing
            .iter()
            .filter(|entry| {
                self.persistent_items.contains(&entry.key)
                    && entry.timeout.is_none()
                    && entry.comment.as_deref() == Some(desired_comment.as_str())
                    && decode_owned_comment(
                        self.cfg.comment_prefix.as_str(),
                        self.cfg.plugin_tag.as_str(),
                        entry.comment.as_deref(),
                    )
                    .is_some_and(|meta| meta.kind == OwnedCommentKind::Persistent)
            })
            .map(|entry| entry.key.clone())
            .collect::<AHashSet<_>>();
        let persistent = self
            .persistent_items
            .iter()
            .filter(|key| !correct_persistent.contains(*key))
            .collect::<Vec<_>>();
        let results = join_all_bounded(
            persistent.iter().map(|key| {
                self.api.upsert_owned_entry(
                    key,
                    None,
                    desired_comment.as_str(),
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    false,
                )
            }),
            UPSERT_PIPELINE_SIZE,
        )
        .await;
        let mut first_error = None;
        for (key, result) in persistent.iter().zip(results) {
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
        for entry in &existing {
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
            match self.is_stale_persistent_entry_still_deletable(entry).await {
                Ok(true) => {
                    if let Err(error) = self
                        .api
                        .delete_entry_by_id(&entry.id, entry.key.family)
                        .await
                    {
                        first_error.get_or_insert(error);
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        let now = captured_at_ms;
        let remote_dynamic = existing
            .iter()
            .filter_map(|entry| {
                decode_owned_comment(
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    entry.comment.as_deref(),
                )
                .filter(|meta| meta.kind == OwnedCommentKind::Dynamic)
                .map(|_| {
                    let remote = entry
                        .timeout
                        .as_deref()
                        .and_then(parse_routeros_duration_secs)
                        .filter(|seconds| *seconds > 0)
                        .map_or(LeaseDeadline::Timeless, |seconds| {
                            LeaseDeadline::At(
                                now.saturating_add(u64::from(seconds).saturating_mul(1_000)),
                            )
                        });
                    let desired =
                        LeasePolicy::new(self.cfg.min_ttl, self.cfg.max_ttl, self.cfg.fixed_ttl)
                            .cap_recovered(remote, now);
                    (entry.key.clone(), (desired, remote))
                })
            })
            .collect::<AHashMap<_, _>>();

        // A snapshot may race successful writes. Newer generations win;
        // everything else follows actual RouterOS state, including timeless
        // rows observed after the scan started.
        self.leases.retain(|key, lease| {
            lease.desired_revision() > scan_generation || remote_dynamic.contains_key(key)
        });
        let missing_newer = self
            .leases
            .keys_with_revision_after(scan_generation)
            .into_iter()
            .filter(|key| !remote_dynamic.contains_key(key))
            .collect::<Vec<_>>();
        for key in missing_newer {
            self.leases.mark_unsynced(&key);
        }
        for (key, (desired, remote)) in remote_dynamic {
            let keep_newer = self
                .leases
                .get(&key)
                .is_some_and(|lease| lease.desired_revision() > scan_generation);
            if !keep_newer {
                self.leases
                    .recover_with_synced(key, desired, remote, now, scan_generation, now);
            }
        }
        self.prune_dynamic_cache(now);
        self.refresh_managed_metric();
        if let Some(error) = first_error {
            return Err(error);
        }
        if self.persistent_items.is_empty() && self.leases.is_empty() {
            self.empty_state_needs_reconcile = false;
        }
        Ok(())
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
        if self.reconcile.is_running() {
            debug!(
                plugin = %tag,
                "ros_address_list reconcile already running or awaiting apply, skipping duplicate request"
            );
            return;
        }

        if self.persistent_items.is_empty()
            && self.leases.is_empty()
            && !self.empty_state_needs_reconcile
        {
            debug!(
                plugin = %tag,
                "ros_address_list reconcile already confirmed empty state, skipping remote scan"
            );
            return;
        }

        let api = self.api.clone();
        let list4 = self.cfg.address_list4.clone();
        let list6 = self.cfg.address_list6.clone();
        self.reconcile.start(self.leases.revision(), async move {
            let entries = api.list_entries(list4.as_deref(), list6.as_deref()).await?;
            Ok(AddressListSnapshot {
                captured_at_ms: now_millis(),
                entries,
            })
        });
    }

    async fn wait_for_background_reconcile(&self) {
        self.reconcile.wait().await;
    }

    #[cfg(test)]
    async fn harvest_background_reconcile(&mut self, tag: &str) {
        let Some(result) = self.reconcile.take_finished().await else {
            return;
        };
        self.apply_background_reconcile_result(tag, result).await;
    }

    async fn await_background_reconcile(&mut self, tag: &str) {
        let Some(result) = self.reconcile.take().await else {
            return;
        };
        self.apply_background_reconcile_result(tag, result).await;
    }

    async fn apply_background_reconcile_result(
        &mut self,
        tag: &str,
        result: std::result::Result<
            Result<VersionedSnapshot<AddressListSnapshot>>,
            tokio::task::JoinError,
        >,
    ) {
        match result {
            Ok(Ok(VersionedSnapshot { generation, value })) => {
                match self
                    .apply_reconcile_snapshot_at(value.entries, generation, value.captured_at_ms)
                    .await
                {
                    Ok(()) => {
                        self.reconcile_retry.reset();
                        if let Some(metrics) = &self.metrics {
                            metrics
                                .last_reconcile_success_timestamp_seconds
                                .store(AppClock::now_timestamp() / 1000, Ordering::Relaxed);
                        }
                        self.refresh_transport_metrics().await;
                        debug!(plugin = %tag, "ros_address_list background reconcile completed");
                    }
                    Err(error) => {
                        if let Some(metrics) = &self.metrics {
                            metrics
                                .reconcile_error_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        self.refresh_transport_metrics().await;
                        warn!(
                            plugin = %tag,
                            err = %error,
                            "ros_address_list background reconcile diff failed"
                        );
                        self.schedule_reconcile_retry().await;
                    }
                }
            }
            Ok(Err(error)) => {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .reconcile_error_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.refresh_transport_metrics().await;
                warn!(
                    plugin = %tag,
                    err = %error,
                    "ros_address_list background reconcile failed"
                );
                self.schedule_reconcile_retry().await;
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                warn!(
                    plugin = %tag,
                    err = %error,
                    "ros_address_list background reconcile task failed"
                );
                self.schedule_reconcile_retry().await;
            }
        }
    }

    async fn schedule_reconcile_retry(&mut self) {
        self.reconcile_retry
            .schedule(self.transport_retry_delay().await);
    }

    #[cfg(test)]
    pub(super) async fn observe_domain(
        &mut self,
        _domain: String,
        addrs: Vec<ObservedAddr>,
    ) -> Result<()> {
        self.observe_at_for_test(addrs, now_millis()).await
    }

    async fn observe_address_batch(
        &mut self,
        observations: &[(AddressListKey, AddressObservation)],
    ) -> Vec<Result<()>> {
        self.observe_address_batch_at(observations, now_millis())
            .await
    }

    async fn observe_address_batch_at(
        &mut self,
        observations: &[(AddressListKey, AddressObservation)],
        now: u64,
    ) -> Vec<Result<()>> {
        self.prune_dynamic_cache(now);

        struct Prepared {
            index: usize,
            key: AddressListKey,
            timeout: DynamicTimeout,
            timeout_value: Option<String>,
            comment: String,
        }

        let mut outcomes = std::iter::repeat_with(|| None)
            .take(observations.len())
            .collect::<Vec<Option<Result<()>>>>();
        let mut prepared = Vec::new();
        for (index, (key, observation)) in observations.iter().enumerate() {
            if self.persistent_items.contains(key) {
                outcomes[index] = Some(Ok(()));
                continue;
            }
            let deadline = observation
                .expires_at_ms
                .map_or(LeaseDeadline::Timeless, LeaseDeadline::At);
            if deadline.is_expired(now) {
                outcomes[index] = Some(Ok(()));
                continue;
            }
            self.leases.observe(key.clone(), deadline, now);
            let timeout = deadline
                .remaining_secs(now)
                .map_or(DynamicTimeout::Timeless, DynamicTimeout::Timed);
            if !self.should_refresh_dynamic_entry(key, now) {
                outcomes[index] = Some(Ok(()));
                continue;
            }
            prepared.push(Prepared {
                index,
                key: key.clone(),
                timeout,
                timeout_value: match timeout {
                    DynamicTimeout::Timed(ttl) => Some(format!("{ttl}s")),
                    DynamicTimeout::Timeless => None,
                },
                comment: self.comment_for_dynamic(),
            });
        }

        let api = self.api.clone();
        let prefix = self.cfg.comment_prefix.clone();
        let plugin_tag = self.cfg.plugin_tag.clone();
        let results = join_all_bounded(
            prepared.iter().map(|item| {
                api.upsert_owned_entry(
                    &item.key,
                    item.timeout_value.as_deref(),
                    &item.comment,
                    &prefix,
                    &plugin_tag,
                    matches!(item.timeout, DynamicTimeout::Timed(_)),
                )
            }),
            UPSERT_PIPELINE_SIZE,
        )
        .await;

        for (item, result) in prepared.into_iter().zip(results) {
            outcomes[item.index] = Some(match result {
                Ok(Some(())) => {
                    self.cache_dynamic_write(&item.key, now);
                    Ok(())
                }
                Ok(None) => {
                    self.leases.remove(&item.key);
                    Ok(())
                }
                Err(error) => {
                    self.leases.remove(&item.key);
                    Err(error)
                }
            });
        }

        outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or_else(|| Ok(())))
            .collect()
    }

    #[cfg(test)]
    pub(super) async fn update_persistent_items(
        &mut self,
        items: AHashSet<AddressListKey>,
    ) -> Result<()> {
        // Persistent ownership takes precedence over any cached dynamic state.
        self.persistent_items = items;
        self.empty_state_needs_reconcile = true;
        self.prune_dynamic_cache(now_millis());
        let entries = self
            .api
            .list_entries(
                self.cfg.address_list4.as_deref(),
                self.cfg.address_list6.as_deref(),
            )
            .await?;
        self.apply_reconcile_snapshot(entries, self.leases.revision())
            .await
    }

    #[cfg(test)]
    pub(super) async fn reconcile(&mut self) -> Result<()> {
        self.prune_dynamic_cache(now_millis());
        if self.persistent_items.is_empty()
            && self.leases.is_empty()
            && !self.empty_state_needs_reconcile
        {
            return Ok(());
        }
        let entries = self
            .api
            .list_entries(
                self.cfg.address_list4.as_deref(),
                self.cfg.address_list6.as_deref(),
            )
            .await?;
        self.apply_reconcile_snapshot(entries, self.leases.revision())
            .await
    }

    pub(super) async fn prune_dynamic_cache_now(&mut self) -> Result<()> {
        self.prune_dynamic_cache(now_millis());
        self.refresh_managed_metric();
        Ok(())
    }

    async fn transport_retry_delay(&self) -> Option<Duration> {
        let snapshot = self.api.transport_snapshot().await;
        if let (Some(metrics), Some(snapshot)) = (&self.metrics, snapshot) {
            metrics
                .reconnect_total
                .store(snapshot.reconnect_total, Ordering::Relaxed);
            metrics
                .connect_attempt_total
                .store(snapshot.connect_attempt_total, Ordering::Relaxed);
            metrics
                .backoff_total
                .store(snapshot.backoff_total, Ordering::Relaxed);
            metrics
                .degraded
                .store(u64::from(snapshot.degraded), Ordering::Relaxed);
            snapshot.retry_after
        } else {
            snapshot.and_then(|snapshot| snapshot.retry_after)
        }
    }

    async fn cleanup_entry_if_still_owned(&self, entry: &RouterListEntry) -> Result<()> {
        let current = self.api.list_entries_by_key(&entry.key).await?;
        let still_owned = current.iter().any(|candidate| {
            candidate.id == entry.id
                && candidate.key == entry.key
                && decode_owned_comment(
                    self.cfg.comment_prefix.as_str(),
                    self.cfg.plugin_tag.as_str(),
                    candidate.comment.as_deref(),
                )
                .is_some()
        });
        if still_owned {
            self.api
                .delete_entry_by_id(&entry.id, entry.key.family)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn shutdown(&mut self, cleanup: AddressListCleanupScope) -> Result<()> {
        self.reconcile.cancel().await;

        if cleanup.is_empty() {
            self.leases.clear();
            return Ok(());
        }

        // Cleanup bypasses reconnect backoff but retains per-operation
        // transport timeouts.
        self.api.begin_shutdown_cleanup();
        // Cleanup only touches entries that match this plugin's comment ownership.
        let entries = self
            .api
            .list_entries(
                cleanup
                    .ipv4
                    .then_some(self.cfg.address_list4.as_deref())
                    .flatten(),
                cleanup
                    .ipv6
                    .then_some(self.cfg.address_list6.as_deref())
                    .flatten(),
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
        let results = join_all_bounded(
            owned
                .iter()
                .map(|entry| self.cleanup_entry_if_still_owned(entry)),
            UPSERT_PIPELINE_SIZE,
        )
        .await;
        let mut first_error = None;
        let mut failures = 0u64;
        for result in results {
            if let Err(error) = result {
                failures += 1;
                first_error.get_or_insert(error);
            }
        }
        if failures > 0 {
            if let Some(metrics) = &self.metrics {
                metrics
                    .cleanup_error_total
                    .fetch_add(failures, Ordering::Relaxed);
            }
            warn!(plugin = %self.cfg.plugin_tag, failures, "ros_address_list shutdown cleanup completed with failures");
        }
        self.leases.clear();
        self.refresh_managed_metric();
        self.refresh_transport_metrics().await;
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(super) fn dynamic_cache_len(&self) -> usize {
        self.leases.len()
    }

    #[cfg(test)]
    pub(super) fn lease_revision_for_test(&self) -> u64 {
        self.leases.revision()
    }

    #[cfg(test)]
    pub(super) async fn observe_domain_at_for_test(
        &mut self,
        _domain: String,
        addrs: Vec<ObservedAddr>,
        now_ms: u64,
    ) -> Result<()> {
        self.observe_at_for_test(addrs, now_ms).await
    }

    #[cfg(test)]
    async fn observe_at_for_test(&mut self, addrs: Vec<ObservedAddr>, now_ms: u64) -> Result<()> {
        let observations =
            AddressObservationPolicy::from_config(&self.cfg).commands_at(addrs, now_ms);
        self.observe_address_batch_at(&observations, now_ms)
            .await
            .into_iter()
            .find_map(std::result::Result::err)
            .map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(super) async fn background_reconcile_for_test(&mut self) {
        let tag = self.cfg.plugin_tag.clone();
        self.spawn_background_reconcile(tag.clone());
        while self.reconcile.is_running() && !self.reconcile.is_finished() {
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

pub(super) fn encode_comment(prefix: &str, plugin_tag: &str, kind: OwnedCommentKind) -> String {
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
    mut lifecycle_rx: mpsc::Receiver<LifecycleCommand>,
    mut active: bool,
    mut shutdown_rx: oneshot::Receiver<ShutdownRequest>,
) {
    // Every state transition is serialized here. Request-path code only pushes
    // commands into the mailbox and never mutates manager state directly.
    let error_logs = ErrorLogThrottle::default();
    let mut retry_observations =
        AHashMap::<AddressListKey, (tokio::time::Instant, ObservationCommand)>::new();
    loop {
        let next_retry = retry_observations
            .values()
            .map(|(retry_at, _)| *retry_at)
            .min();
        let retry_wakeup = async {
            match next_retry {
                Some(retry_at) => tokio::time::sleep_until(retry_at).await,
                None => std::future::pending::<()>().await,
            }
        };
        let reconcile_retry_at = manager.reconcile_retry.deadline();
        let reconcile_retry_wakeup = async move {
            match reconcile_retry_at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };
        let command = tokio::select! {
            biased;
            shutdown = &mut shutdown_rx => {
                if let Ok(ShutdownRequest { cleanup, done }) = shutdown {
                    let _ = done.send(manager.shutdown(cleanup).await);
                }
                break;
            }
            lifecycle = lifecycle_rx.recv() => lifecycle.map(WorkerCommand::Lifecycle),
            () = manager.wait_for_background_reconcile(), if active => {
                Some(WorkerCommand::ReconcileCompleted)
            }
            control = handle.controls.recv(), if active => control.map(|(_, command)| WorkerCommand::Control(command)),
            () = retry_wakeup, if active => {
                let now = tokio::time::Instant::now();
                let due_keys = retry_observations
                    .iter()
                    .filter(|(_, (retry_at, _))| *retry_at <= now)
                    .take(UPSERT_PIPELINE_SIZE)
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                let due = due_keys
                    .into_iter()
                    .filter_map(|key| {
                        retry_observations
                            .remove(&key)
                            .map(|(_, mut command)| {
                                if let Some(newer) = handle.observations.take(&key) {
                                    command.coalesce(newer);
                                }
                                (key, command)
                            })
                    })
                    .collect::<Vec<_>>();
                (!due.is_empty()).then_some(WorkerCommand::Observe {
                    batch: due,
                    from_retry: true,
                })
            }
            () = reconcile_retry_wakeup, if active => {
                manager.reconcile_retry.mark_due();
                Some(WorkerCommand::Control(ControlCommand::Reconcile))
            }
            observation = handle.observations.recv(), if active => {
                observation.map(|first| {
                    let mut batch = vec![first];
                    while batch.len() < UPSERT_PIPELINE_SIZE {
                        let Some(next) = handle.observations.try_recv() else {
                            break;
                        };
                        batch.push(next);
                    }
                    WorkerCommand::Observe {
                        batch,
                        from_retry: false,
                    }
                })
            }
        };
        let Some(command) = command else {
            break;
        };
        match command {
            WorkerCommand::Lifecycle(LifecycleCommand::Quiesce { targets, done }) => {
                active = false;
                manager.reconcile.cancel().await;
                let mut merged = AHashMap::<AddressListKey, ObservationCommand>::new();
                for (key, command) in handle
                    .observations
                    .drain_where(|key| targets.contains(&(key.family, key.list.clone())))
                {
                    merged
                        .entry(key)
                        .and_modify(|current| current.coalesce(command.clone()))
                        .or_insert(command);
                }
                let retry_keys = retry_observations
                    .keys()
                    .filter(|key| targets.contains(&(key.family, key.list.clone())))
                    .cloned()
                    .collect::<Vec<_>>();
                for key in retry_keys {
                    if let Some((_, command)) = retry_observations.remove(&key) {
                        merged
                            .entry(key)
                            .and_modify(|current| current.coalesce(command.clone()))
                            .or_insert(command);
                    }
                }
                let _ = done.send(AddressListPendingWork {
                    items: merged.into_iter().collect(),
                });
            }
            WorkerCommand::Lifecycle(LifecycleCommand::Activate { pending, done }) => {
                let now = now_millis();
                let policy = LeasePolicy::new(
                    manager.cfg.min_ttl,
                    manager.cfg.max_ttl,
                    manager.cfg.fixed_ttl,
                );
                for (key, mut command) in pending.items {
                    let deadline = match command.observation.expires_at_ms {
                        Some(deadline) => LeaseDeadline::At(deadline),
                        None => LeaseDeadline::Timeless,
                    };
                    let deadline =
                        policy.cap_recovered(deadline, command.observation.observed_at_ms);
                    if deadline.is_expired(now) {
                        for completion in command.completions {
                            completion.finish(&Ok(()));
                        }
                        continue;
                    }
                    command.observation.expires_at_ms = deadline.unix_millis();
                    match handle.observations.try_push(key.clone(), command) {
                        Ok(_) => {}
                        Err(TryPushError::Full(command)) => defer_address_observation(
                            &mut retry_observations,
                            tokio::time::Instant::now(),
                            key,
                            command,
                            manager.metrics.as_deref(),
                        ),
                        Err(TryPushError::Closed(command)) => {
                            let result = Err(DnsError::plugin(
                                "ros_address_list handoff observation mailbox is closed",
                            ));
                            for completion in command.completions {
                                completion.finish(&result);
                            }
                            if let Some(metrics) = &manager.metrics {
                                metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                active = true;
                handle.request_reconcile();
                let _ = done.send(());
            }
            WorkerCommand::Observe {
                mut batch,
                from_retry,
            } => {
                if !from_retry
                    && let Some(retry_at) = retry_observations
                        .values()
                        .map(|(retry_at, _)| *retry_at)
                        .min()
                {
                    for (key, command) in batch.drain(..) {
                        defer_address_observation(
                            &mut retry_observations,
                            retry_at,
                            key,
                            command,
                            handle.metrics.as_deref(),
                        );
                    }
                    handle.refresh_pending_metric_with(retry_observations.len());
                    continue;
                }
                let observations = batch
                    .iter()
                    .map(|(key, command)| (key.clone(), command.observation.clone()))
                    .collect::<Vec<_>>();
                let results = manager.observe_address_batch(&observations).await;
                let has_error = results.iter().any(|result| result.is_err());
                let retry_delay = if has_error {
                    manager.transport_retry_delay().await
                } else {
                    None
                };
                for ((key, mut command), result) in batch.drain(..).zip(results) {
                    for completion in command.completions.drain(..) {
                        completion.finish(&result);
                    }
                    if let Err(error) = &result {
                        if error_logs.should_log("observe") {
                            warn!(
                                plugin = %tag,
                                err = %error,
                                "ros_address_list observe failed in async mode"
                            );
                        }
                        if let Some(delay) = retry_delay
                            && !error.to_string().contains("state capacity")
                        {
                            defer_address_observation(
                                &mut retry_observations,
                                tokio::time::Instant::now() + delay,
                                key,
                                command,
                                handle.metrics.as_deref(),
                            );
                        }
                    }
                }
            }
            WorkerCommand::Control(command) => match command {
                ControlCommand::Reconcile => {
                    manager.spawn_background_reconcile(tag.clone());
                }
                ControlCommand::PruneDynamicCache => {
                    if let Err(e) = manager.prune_dynamic_cache_now().await
                        && error_logs.should_log("prune")
                    {
                        warn!(
                            plugin = %tag,
                            err = %e,
                            "ros_address_list dynamic cache prune failed"
                        );
                    }
                }
            },
            WorkerCommand::ReconcileCompleted => {
                manager.await_background_reconcile(tag.as_str()).await;
            }
        }
        handle.refresh_pending_metric_with(retry_observations.len());
    }

    debug!(plugin = %tag, "ros_address_list manager worker exited");
}

fn defer_address_observation(
    retries: &mut AHashMap<AddressListKey, (tokio::time::Instant, ObservationCommand)>,
    retry_at: tokio::time::Instant,
    key: AddressListKey,
    command: ObservationCommand,
    metrics: Option<&RosMetrics>,
) {
    if let Some((scheduled_at, existing)) = retries.get_mut(&key) {
        *scheduled_at = (*scheduled_at).min(retry_at);
        existing.coalesce(command);
        if let Some(metrics) = metrics {
            metrics.coalesced_total.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }
    if retries.len() < MANAGER_QUEUE_SIZE {
        retries.insert(key, (retry_at, command));
        return;
    }

    let error = Err(DnsError::plugin(
        "ros_address_list retry observation capacity reached",
    ));
    for completion in command.completions {
        completion.finish(&error);
    }
    if let Some(metrics) = metrics {
        metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
    }
}

fn parse_routeros_duration_secs(raw: &str) -> Option<u32> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let bytes = raw.as_bytes();
    let mut index = 0;
    let mut total = 0u64;
    while index < bytes.len() {
        let number_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if number_start == index {
            return None;
        }
        let value = raw[number_start..index].parse::<u64>().ok()?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let multiplier = match &raw[unit_start..index] {
            "w" => 7 * 24 * 60 * 60,
            "d" => 24 * 60 * 60,
            "h" => 60 * 60,
            "m" => 60,
            "s" => 1,
            _ => return None,
        };
        total = total.saturating_add(value.saturating_mul(multiplier));
    }
    Some(total.min(u64::from(u32::MAX)) as u32)
}

fn now_millis() -> u64 {
    AppClock::elapsed_millis()
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
        let normalized = IpPrefix::new(ip, prefix)?;
        return Some((normalized.address(), normalized.prefix()));
    }

    let ip = value.parse::<IpAddr>().ok()?;
    if AddressListFamily::from_ip(ip) != family {
        return None;
    }
    Some((ip, family.host_prefix()))
}

#[cfg(test)]
mod observation_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn routeros_duration_parser_accepts_composite_values() {
        assert_eq!(parse_routeros_duration_secs("1w2d3h4m5s"), Some(788_645));
        assert_eq!(parse_routeros_duration_secs("300s"), Some(300));
        assert_eq!(parse_routeros_duration_secs("none"), None);
        assert_eq!(parse_routeros_duration_secs("5m30"), None);
    }

    #[tokio::test]
    async fn mailbox_is_bounded_by_address_list_key() {
        let handle = AddressListManagerHandle::new_for_test();
        let addr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10));
        for index in 0..10_000 {
            handle
                .try_observe(
                    vec![ObservedAddr {
                        addr,
                        ttl_secs: 60 + (index % 300) as u32,
                    }],
                    None,
                )
                .expect("coalesced observation");
        }

        assert_eq!(handle.observations.len(), 1);
        let (key, command) = handle.observations.recv().await.expect("observation");
        assert_eq!(key.address, addr);
        // The coalesced value keeps the longest absolute expiry.
        assert!(command.observation.expires_at_ms.is_some());
    }

    #[tokio::test]
    async fn timeless_observation_cannot_be_replaced_by_timed_observation() {
        AppClock::start();
        let mut config = AddressListManagerConfig {
            plugin_tag: "test".to_string(),
            address_list4: Some("test_v4".to_string()),
            address_list6: None,
            persistent_items: AHashSet::new(),
            comment_prefix: "fdns".to_string(),
            min_ttl: 60,
            max_ttl: 3600,
            fixed_ttl: Some(0),
        };
        let handle = AddressListManagerHandle::new(&config, None, None);
        let addr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11));
        handle
            .try_observe(vec![ObservedAddr { addr, ttl_secs: 60 }], None)
            .expect("timeless observation");

        config.fixed_ttl = Some(300);
        let (key, observation) = AddressObservationPolicy::from_config(&config)
            .commands(vec![ObservedAddr { addr, ttl_secs: 60 }])
            .pop()
            .expect("timed command");
        handle
            .observations
            .try_push(
                key,
                ObservationCommand {
                    observation,
                    completions: Vec::new(),
                },
            )
            .expect("coalesced timed observation");

        let (_, command) = handle.observations.recv().await.expect("observation");
        assert_eq!(command.observation.expires_at_ms, None);
    }
}
