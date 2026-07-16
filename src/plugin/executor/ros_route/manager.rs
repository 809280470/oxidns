//! Route manager for DNS-observed RouterOS route leases.
//!
//! Dynamic state is keyed only by the destination host route. DNS answers add
//! or extend leases; absence from a later answer never withdraws a route.

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
use crate::infra::mikrotik::batching::join_all_bounded;
use crate::infra::mikrotik::completion::BatchCompletion;
use crate::infra::mikrotik::ip_prefix::{IpPrefix, host_prefix};
use crate::infra::mikrotik::lease::{LeaseBook, LeaseDeadline, LeasePolicy};
use crate::infra::mikrotik::lifecycle::abort_and_reap;
use crate::infra::mikrotik::mailbox::{Coalesce, KeyedMailbox, PushOutcome, TryPushError};
use crate::infra::mikrotik::reconcile::{BackgroundReconcile, ReconcileRetry, VersionedSnapshot};
use crate::infra::mikrotik::throttle::ErrorLogThrottle;
use crate::infra::mikrotik::{ObservedAddr, SHUTDOWN_TIMEOUT};
use crate::infra::task as task_center;

const ROUTE_DEFAULT_V4: &str = "0.0.0.0/0";
const ROUTE_DEFAULT_V6: &str = "::/0";
const MANAGER_QUEUE_SIZE: usize = 1024;
const CONTROL_QUEUE_SIZE: usize = 2;
const SWEEP_INTERVAL_SECS: u64 = 30;
const RECONCILE_INTERVAL_SECS: u64 = 180;
const CONNECTION_GUARD_RETRY_INTERVAL_SECS: u64 = SWEEP_INTERVAL_SECS;
const CONNECTION_QUERY_BATCH_SIZE: usize = 128;
const UPSERT_PIPELINE_SIZE: usize = 16;

const COMMENT_FIELD_PLUGIN: &str = "pg";
const COMMENT_FIELD_KIND: &str = "kind";
const COMMENT_FIELD_EXP: &str = "exp";
const COMMENT_FIELD_SEEN: &str = "seen";
const COMMENT_KIND_DYNAMIC: &str = "D";
const COMMENT_KIND_PERSISTENT: &str = "P";
const COMMENT_KIND_GATEWAY_CHECK: &str = "V";

#[derive(Debug, Clone)]
pub(super) struct RouteManagerConfig {
    pub(super) plugin_tag: String,
    pub(super) routing_table: String,
    pub(super) gateway4: Option<String>,
    pub(super) gateway6: Option<String>,
    pub(super) persistent_ips: AHashSet<IpPrefix>,
    pub(super) comment_prefix: String,
    pub(super) distance: u8,
    pub(super) min_ttl: u32,
    pub(super) max_ttl: u32,
    pub(super) fixed_ttl: Option<u32>,
    pub(super) conntrack_guard: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum RouteFamily {
    Ipv4,
    Ipv6,
}

impl RouteFamily {
    pub(super) fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct RouteKey {
    pub(super) ip: IpAddr,
    pub(super) prefix: u8,
    pub(super) table: String,
}

impl RouteKey {
    pub(super) fn new(ip: IpAddr, table: String) -> Self {
        Self {
            ip,
            prefix: host_prefix(ip),
            table,
        }
    }

    pub(super) fn family(&self) -> RouteFamily {
        RouteFamily::from_ip(self.ip)
    }

    pub(super) fn dst_address(&self) -> String {
        format!("{}/{}", self.ip, self.prefix)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RouteCommentKind {
    Dynamic,
    Persistent,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct RouteCommentMeta {
    pub(super) kind: RouteCommentKind,
    pub(super) expires_at_ms: LeaseDeadline,
    pub(super) last_seen_ms: u64,
}

#[derive(Debug)]
pub(super) struct RouteCommentCodec;

impl RouteCommentCodec {
    fn prefix(prefix: &str, plugin_tag: &str, kind: &str) -> String {
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
        out.push_str(kind);
        out
    }

    fn encode_persistent(prefix: &str, plugin_tag: &str) -> String {
        Self::prefix(prefix, plugin_tag, COMMENT_KIND_PERSISTENT)
    }

    fn encode_dynamic(
        prefix: &str,
        plugin_tag: &str,
        deadline: LeaseDeadline,
        last_seen_ms: u64,
    ) -> String {
        let mut out = Self::prefix(prefix, plugin_tag, COMMENT_KIND_DYNAMIC);
        let expires_at = deadline.unix_millis().map_or(0, |value| value / 1_000);
        out.push(';');
        out.push_str(COMMENT_FIELD_EXP);
        out.push('=');
        out.push_str(&expires_at.to_string());
        out.push(';');
        out.push_str(COMMENT_FIELD_SEEN);
        out.push('=');
        out.push_str(&(last_seen_ms / 1_000).to_string());
        out
    }

    pub(super) fn decode(
        prefix: &str,
        plugin_tag: &str,
        family: RouteFamily,
        dst_address: &str,
        comment: &str,
    ) -> Result<Option<RouteCommentMeta>> {
        if !prefix.is_empty()
            && (!comment.starts_with(prefix) || comment.as_bytes().get(prefix.len()) != Some(&b';'))
        {
            return Ok(None);
        }
        let mut owner = None;
        let mut kind = None;
        let mut expiry = None;
        let mut seen = None;
        for token in comment.split(';') {
            let Some((key, value)) = token.trim().split_once('=') else {
                continue;
            };
            match key.trim() {
                COMMENT_FIELD_PLUGIN => owner = Some(value.trim()),
                COMMENT_FIELD_KIND => kind = Some(value.trim()),
                COMMENT_FIELD_EXP => expiry = Some(value.trim()),
                COMMENT_FIELD_SEEN => seen = Some(value.trim()),
                _ => {}
            }
        }
        if owner != Some(plugin_tag) {
            return Ok(None);
        }
        let prefix = dst_address
            .parse::<IpPrefix>()
            .map_err(|error| DnsError::plugin(format!("ros_route invalid dst-address: {error}")))?;
        if RouteFamily::from_ip(prefix.address()) != family {
            return Err(DnsError::plugin(
                "ros_route comment address family mismatch",
            ));
        }
        match kind {
            Some(COMMENT_KIND_PERSISTENT) => Ok(Some(RouteCommentMeta {
                kind: RouteCommentKind::Persistent,
                expires_at_ms: LeaseDeadline::Timeless,
                last_seen_ms: 0,
            })),
            Some(COMMENT_KIND_DYNAMIC) if prefix.is_host() => {
                let expiry = expiry
                    .ok_or_else(|| DnsError::plugin("ros_route dynamic comment missing exp"))?
                    .parse::<u64>()
                    .map_err(|error| DnsError::plugin(format!("ros_route invalid exp: {error}")))?;
                let seen = seen
                    .ok_or_else(|| DnsError::plugin("ros_route dynamic comment missing seen"))?
                    .parse::<u64>()
                    .map_err(|error| {
                        DnsError::plugin(format!("ros_route invalid seen: {error}"))
                    })?;
                Ok(Some(RouteCommentMeta {
                    kind: RouteCommentKind::Dynamic,
                    expires_at_ms: if expiry == 0 {
                        LeaseDeadline::Timeless
                    } else {
                        LeaseDeadline::At(expiry.saturating_mul(1_000))
                    },
                    last_seen_ms: seen.saturating_mul(1_000),
                }))
            }
            Some(COMMENT_KIND_DYNAMIC) => Err(DnsError::plugin(
                "ros_route dynamic comment is only valid for host routes",
            )),
            _ => Ok(None),
        }
    }
}

fn is_validation_comment(prefix: &str, plugin_tag: &str, comment: &str) -> bool {
    comment == RouteCommentCodec::prefix(prefix, plugin_tag, COMMENT_KIND_GATEWAY_CHECK)
        || comment.starts_with(&format!(
            "{};nonce=",
            RouteCommentCodec::prefix(prefix, plugin_tag, COMMENT_KIND_GATEWAY_CHECK)
        ))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SyncState {
    PendingCreate,
    Synced,
    Dirty,
    PendingDynamicDelete,
    PendingPersistentDelete,
}

#[derive(Debug, Clone)]
struct RouteState {
    gateway: String,
    distance: u8,
    router_id: Option<String>,
    sync_state: SyncState,
}

#[derive(Debug, Clone)]
struct RouteObservation {
    deadline: LeaseDeadline,
    observed_at_ms: u64,
    completions: Vec<Arc<BatchCompletion>>,
}

impl Coalesce for RouteObservation {
    fn coalesce(&mut self, mut newer: Self) {
        newer.deadline = self.deadline.max(newer.deadline);
        newer.observed_at_ms = newer.observed_at_ms.max(self.observed_at_ms);
        newer.completions.append(&mut self.completions);
        *self = newer;
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum ControlKey {
    Sweep,
    Reconcile,
}

#[derive(Debug)]
enum ControlCommand {
    Sweep,
    Reconcile,
}

#[derive(Debug, Default)]
pub(super) struct RoutePendingWork {
    items: Vec<(RouteKey, RouteObservation)>,
}

#[derive(Debug)]
enum LifecycleCommand {
    Quiesce {
        done: oneshot::Sender<RoutePendingWork>,
    },
    Activate {
        pending: RoutePendingWork,
        done: oneshot::Sender<()>,
    },
}

impl Coalesce for ControlCommand {
    fn coalesce(&mut self, newer: Self) {
        *self = newer;
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum ObserveEnqueueError {
    Full,
    Closed,
}

#[derive(Debug, Clone)]
pub(super) struct RouteManagerHandle {
    observations: KeyedMailbox<RouteKey, RouteObservation>,
    controls: KeyedMailbox<ControlKey, ControlCommand>,
    routing_table: String,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
    policy: LeasePolicy,
    metrics: Option<Arc<RosRouteMetrics>>,
    lifecycle: Option<mpsc::Sender<LifecycleCommand>>,
}

impl RouteManagerHandle {
    fn new(
        config: &RouteManagerConfig,
        metrics: Option<Arc<RosRouteMetrics>>,
        lifecycle: Option<mpsc::Sender<LifecycleCommand>>,
    ) -> Self {
        Self {
            observations: KeyedMailbox::new(MANAGER_QUEUE_SIZE),
            controls: KeyedMailbox::new(CONTROL_QUEUE_SIZE),
            routing_table: config.routing_table.clone(),
            gateway4_enabled: config.gateway4.is_some(),
            gateway6_enabled: config.gateway6.is_some(),
            policy: LeasePolicy::new(config.min_ttl, config.max_ttl, config.fixed_ttl),
            metrics,
            lifecycle,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test() -> Self {
        AppClock::start();
        Self::new(
            &RouteManagerConfig {
                plugin_tag: "test".to_string(),
                routing_table: "main".to_string(),
                gateway4: Some("192.0.2.1".to_string()),
                gateway6: Some("2001:db8::1".to_string()),
                persistent_ips: AHashSet::new(),
                comment_prefix: "fdns".to_string(),
                distance: 100,
                min_ttl: 60,
                max_ttl: 3_600,
                fixed_ttl: None,
                conntrack_guard: false,
            },
            None,
            None,
        )
    }

    fn prepare(&self, addrs: Vec<ObservedAddr>) -> Vec<(RouteKey, LeaseDeadline, u64)> {
        let now = now_millis();
        let mut dedup = AHashMap::<RouteKey, LeaseDeadline>::new();
        for observed in addrs {
            let enabled = match observed.addr {
                IpAddr::V4(_) => self.gateway4_enabled,
                IpAddr::V6(_) => self.gateway6_enabled,
            };
            if !enabled {
                continue;
            }
            let key = RouteKey::new(observed.addr, self.routing_table.clone());
            let deadline = self.policy.deadline(observed.ttl_secs, now);
            dedup
                .entry(key)
                .and_modify(|current| *current = current.max(deadline))
                .or_insert(deadline);
        }
        dedup
            .into_iter()
            .map(|(key, deadline)| (key, deadline, now))
            .collect()
    }

    fn finish_enqueue_metric(&self, outcome: PushOutcome) {
        if matches!(outcome, PushOutcome::Coalesced)
            && let Some(metrics) = &self.metrics
        {
            metrics
                .coalesced_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.refresh_pending_metric();
    }

    fn refresh_pending_metric(&self) {
        self.refresh_pending_metric_with(0);
    }

    fn refresh_pending_metric_with(&self, extra: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.pending_observations.store(
                self.observations.len().saturating_add(extra) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }

    pub(super) fn try_observe(
        &self,
        addrs: Vec<ObservedAddr>,
        wait: Option<oneshot::Sender<Result<()>>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        let prepared = self.prepare(addrs);
        if prepared.is_empty() {
            if let Some(waiter) = wait {
                let _ = waiter.send(Ok(()));
            }
            return Ok(PushOutcome::Inserted);
        }
        let completion = wait.map(|waiter| BatchCompletion::new(prepared.len(), waiter));
        let mut total = PushOutcome::Coalesced;
        let mut error = None;
        for (key, deadline, observed_at_ms) in prepared {
            let command = RouteObservation {
                deadline,
                observed_at_ms,
                completions: completion.iter().cloned().collect(),
            };
            match self.observations.try_push(key, command) {
                Ok(outcome) => {
                    self.finish_enqueue_metric(outcome);
                    if matches!(outcome, PushOutcome::Inserted) {
                        total = outcome;
                    }
                }
                Err(TryPushError::Full(command)) => {
                    let result = Err(DnsError::plugin("ros_route observation mailbox is full"));
                    for completion in command.completions {
                        completion.finish(&result);
                    }
                    error.get_or_insert(ObserveEnqueueError::Full);
                }
                Err(TryPushError::Closed(command)) => {
                    let result = Err(DnsError::plugin("ros_route observation mailbox is closed"));
                    for completion in command.completions {
                        completion.finish(&result);
                    }
                    error = Some(ObserveEnqueueError::Closed);
                }
            }
        }
        error.map_or(Ok(total), Err)
    }

    pub(super) async fn observe(
        &self,
        addrs: Vec<ObservedAddr>,
        wait: oneshot::Sender<Result<()>>,
    ) -> std::result::Result<PushOutcome, ObserveEnqueueError> {
        let prepared = self.prepare(addrs);
        if prepared.is_empty() {
            let _ = wait.send(Ok(()));
            return Ok(PushOutcome::Inserted);
        }
        let completion = BatchCompletion::new(prepared.len(), wait);
        let total_items = prepared.len();
        let mut total = PushOutcome::Coalesced;
        for (index, (key, deadline, observed_at_ms)) in prepared.into_iter().enumerate() {
            let command = RouteObservation {
                deadline,
                observed_at_ms,
                completions: vec![completion.clone()],
            };
            match self.observations.push(key, command).await {
                Ok(outcome) => {
                    self.finish_enqueue_metric(outcome);
                    if matches!(outcome, PushOutcome::Inserted) {
                        total = outcome;
                    }
                }
                Err(error) => {
                    let result = Err(DnsError::plugin("ros_route observation mailbox is closed"));
                    for completion in error.0.completions {
                        completion.finish(&result);
                    }
                    for _ in index + 1..total_items {
                        completion.finish(&result);
                    }
                    return Err(ObserveEnqueueError::Closed);
                }
            }
        }
        Ok(total)
    }

    pub(super) fn request_reconcile(&self) -> bool {
        self.controls
            .try_push(ControlKey::Reconcile, ControlCommand::Reconcile)
            .is_ok()
    }

    pub(super) async fn quiesce(&self) -> RoutePendingWork {
        let Some(lifecycle) = &self.lifecycle else {
            return RoutePendingWork::default();
        };
        let (done, wait) = oneshot::channel();
        if lifecycle
            .send(LifecycleCommand::Quiesce { done })
            .await
            .is_err()
        {
            return RoutePendingWork::default();
        }
        wait.await.unwrap_or_default()
    }

    pub(super) async fn activate(&self, pending: RoutePendingWork) -> Result<()> {
        let Some(lifecycle) = &self.lifecycle else {
            return Ok(());
        };
        let (done, wait) = oneshot::channel();
        lifecycle
            .send(LifecycleCommand::Activate { pending, done })
            .await
            .map_err(|_| DnsError::plugin("ros_route manager lifecycle channel is closed"))?;
        wait.await
            .map_err(|_| DnsError::plugin("ros_route manager activation was cancelled"))
    }

    #[cfg(test)]
    pub(super) fn take_reconcile_for_test(&self) -> bool {
        matches!(
            self.controls.try_recv(),
            Some((ControlKey::Reconcile, ControlCommand::Reconcile))
        )
    }

    fn request_sweep(&self) {
        let _ = self
            .controls
            .try_push(ControlKey::Sweep, ControlCommand::Sweep);
    }

    fn close(&self) {
        self.observations.close();
        self.controls.close();
    }
}

#[derive(Debug)]
struct ShutdownRequest {
    cleanup: bool,
    done: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub(super) struct RouteManagerRuntime {
    handle: RouteManagerHandle,
    shutdown_tx: Option<oneshot::Sender<ShutdownRequest>>,
    worker_handle: Option<JoinHandle<()>>,
    sweep_task_id: Option<u64>,
    reconcile_task_id: Option<u64>,
}

impl RouteManagerRuntime {
    pub(super) fn start_paused(tag: String, manager: RouteManager) -> Self {
        Self::start_with_state(tag, manager, false)
    }

    fn start_with_state(tag: String, manager: RouteManager, active: bool) -> Self {
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(1);
        let handle =
            RouteManagerHandle::new(&manager.cfg, manager.metrics.clone(), Some(lifecycle_tx));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker_handle = Some(tokio::spawn(run_manager_worker(
            tag.clone(),
            manager,
            handle.clone(),
            lifecycle_rx,
            active,
            shutdown_rx,
        )));
        if active {
            handle.request_reconcile();
        }

        let sweep_handle = handle.clone();
        let sweep_task_id = Some(task_center::spawn_fixed(
            format!("ros_route:{tag}:sweep"),
            Duration::from_secs(SWEEP_INTERVAL_SECS),
            move || {
                let sweep_handle = sweep_handle.clone();
                async move { sweep_handle.request_sweep() }
            },
        ));
        let reconcile_handle = handle.clone();
        let reconcile_task_id = Some(task_center::spawn_fixed(
            format!("ros_route:{tag}:reconcile"),
            Duration::from_secs(RECONCILE_INTERVAL_SECS),
            move || {
                let reconcile_handle = reconcile_handle.clone();
                async move {
                    reconcile_handle.request_reconcile();
                }
            },
        ));
        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            worker_handle,
            sweep_task_id,
            reconcile_task_id,
        }
    }

    pub(super) fn handle(&self) -> RouteManagerHandle {
        self.handle.clone()
    }

    pub(super) async fn shutdown(self, cleanup: bool) -> Result<()> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        self.shutdown_until(cleanup, deadline).await
    }

    pub(super) async fn shutdown_until(
        mut self,
        cleanup: bool,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let tasks = [self.sweep_task_id.take(), self.reconcile_task_id.take()]
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
                    "ros_route shutdown exceeded {} seconds",
                    SHUTDOWN_TIMEOUT.as_secs()
                )));
            }
        }
        let (done_tx, done_rx) = oneshot::channel();
        let requested = self.shutdown_tx.take().is_some_and(|sender| {
            sender
                .send(ShutdownRequest {
                    cleanup,
                    done: done_tx,
                })
                .is_ok()
        });
        self.handle.close();
        let result = if requested {
            match tokio::time::timeout_at(deadline, done_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(DnsError::plugin(
                    "ros_route shutdown worker closed before reporting cleanup",
                )),
                Err(_) => Err(DnsError::plugin(format!(
                    "ros_route shutdown exceeded {} seconds",
                    SHUTDOWN_TIMEOUT.as_secs()
                ))),
            }
        } else {
            Ok(())
        };
        if let Some(mut worker) = self.worker_handle.take() {
            match tokio::time::timeout_at(deadline, &mut worker).await {
                Ok(_) => {}
                Err(_) => {
                    abort_and_reap(worker);
                    return Err(DnsError::plugin(format!(
                        "ros_route shutdown exceeded {} seconds while joining worker",
                        SHUTDOWN_TIMEOUT.as_secs()
                    )));
                }
            }
        }
        result
    }
}

#[derive(Debug)]
pub(super) struct RouteManager {
    api: Arc<dyn MikrotikApi>,
    cfg: RouteManagerConfig,
    metrics: Option<Arc<RosRouteMetrics>>,
    persistent: AHashSet<RouteKey>,
    leases: LeaseBook<RouteKey>,
    routes: AHashMap<RouteKey, RouteState>,
    connection_retry_after: AHashMap<RouteKey, u64>,
    reconcile: BackgroundReconcile<Vec<RouterRoute>>,
    reconcile_retry: ReconcileRetry,
    empty_state_needs_reconcile: bool,
    initialized: bool,
}

impl RouteManager {
    pub(super) fn new(api: Arc<dyn MikrotikApi>, cfg: RouteManagerConfig) -> Self {
        let persistent = cfg
            .persistent_ips
            .iter()
            .map(|prefix| RouteKey {
                ip: prefix.address(),
                prefix: prefix.prefix(),
                table: cfg.routing_table.clone(),
            })
            .collect();
        Self {
            api,
            metrics: None,
            persistent,
            leases: LeaseBook::new(),
            routes: AHashMap::new(),
            connection_retry_after: AHashMap::new(),
            reconcile: BackgroundReconcile::new(),
            reconcile_retry: ReconcileRetry::default(),
            empty_state_needs_reconcile: true,
            initialized: false,
            cfg,
        }
    }

    pub(super) fn with_metrics(
        api: Arc<dyn MikrotikApi>,
        cfg: RouteManagerConfig,
        metrics: Arc<RosRouteMetrics>,
    ) -> Self {
        let mut manager = Self::new(api, cfg);
        manager.metrics = Some(metrics);
        manager.refresh_managed_metric();
        manager
    }

    fn policy(&self) -> LeasePolicy {
        LeasePolicy::new(self.cfg.min_ttl, self.cfg.max_ttl, self.cfg.fixed_ttl)
    }

    fn gateway_for(&self, family: RouteFamily) -> Option<&str> {
        match family {
            RouteFamily::Ipv4 => self.cfg.gateway4.as_deref(),
            RouteFamily::Ipv6 => self.cfg.gateway6.as_deref(),
        }
    }

    fn refresh_managed_metric(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.managed_entries.store(
                self.persistent.len().saturating_add(self.leases.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }

    async fn refresh_transport_metrics(&self) {
        let Some(metrics) = &self.metrics else { return };
        let Some(snapshot) = self.api.transport_snapshot().await else {
            return;
        };
        metrics.reconnect_total.store(
            snapshot.reconnect_total,
            std::sync::atomic::Ordering::Relaxed,
        );
        metrics.connect_attempt_total.store(
            snapshot.connect_attempt_total,
            std::sync::atomic::Ordering::Relaxed,
        );
        metrics
            .backoff_total
            .store(snapshot.backoff_total, std::sync::atomic::Ordering::Relaxed);
        metrics.degraded.store(
            u64::from(snapshot.degraded),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    async fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }
        self.validate_gateways().await?;
        for key in self.persistent.clone() {
            let Some(gateway) = self.gateway_for(key.family()).map(str::to_string) else {
                continue;
            };
            self.routes.entry(key).or_insert(RouteState {
                gateway,
                distance: self.cfg.distance,
                router_id: None,
                sync_state: SyncState::PendingCreate,
            });
        }
        self.initialized = true;
        Ok(())
    }

    async fn validate_gateways(&self) -> Result<()> {
        for (family, gateway) in [
            (RouteFamily::Ipv4, self.cfg.gateway4.as_deref()),
            (RouteFamily::Ipv6, self.cfg.gateway6.as_deref()),
        ] {
            let Some(gateway) = gateway else { continue };
            let nonce = validation_nonce();
            let key = validation_route_key(family, &self.cfg.routing_table, nonce);
            let comment = format!(
                "{};nonce={nonce}",
                RouteCommentCodec::prefix(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                    COMMENT_KIND_GATEWAY_CHECK,
                )
            );
            self.api
                .validate_route_config(&key, gateway, self.cfg.distance, &comment)
                .await
                .map_err(|error| {
                    DnsError::plugin(format!(
                        "ros_route {family:?} gateway validation failed: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn observe_key(&mut self, key: RouteKey, observation: &RouteObservation) -> Result<()> {
        self.observe_batch(&[(key, observation.clone())]).await
    }

    async fn observe_batch(&mut self, observations: &[(RouteKey, RouteObservation)]) -> Result<()> {
        self.ensure_initialized().await?;
        let mut keys = Vec::with_capacity(observations.len());
        for (key, observation) in observations {
            if self.stage_observation(key.clone(), observation) {
                keys.push(key.clone());
            }
        }
        self.sync_keys(keys, now_millis()).await
    }

    fn stage_observation(&mut self, key: RouteKey, observation: &RouteObservation) -> bool {
        if self.persistent.contains(&key) {
            return false;
        }
        self.leases.observe(
            key.clone(),
            observation.deadline,
            observation.observed_at_ms,
        );
        let Some(gateway) = self.gateway_for(key.family()).map(str::to_string) else {
            self.leases.remove(&key);
            return false;
        };
        let state = self.routes.entry(key.clone()).or_insert(RouteState {
            gateway: gateway.clone(),
            distance: self.cfg.distance,
            router_id: None,
            sync_state: SyncState::PendingCreate,
        });
        state.gateway = gateway;
        state.distance = self.cfg.distance;
        if matches!(
            state.sync_state,
            SyncState::PendingDynamicDelete | SyncState::PendingPersistentDelete
        ) {
            state.sync_state = if state.router_id.is_some() {
                SyncState::Dirty
            } else {
                SyncState::PendingCreate
            };
        }
        self.connection_retry_after.remove(&key);
        true
    }

    async fn sync_keys(&mut self, keys: Vec<RouteKey>, now_ms: u64) -> Result<()> {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for key in keys {
            let Some(state) = self.routes.get(&key).cloned() else {
                continue;
            };
            match state.sync_state {
                SyncState::PendingDynamicDelete | SyncState::PendingPersistentDelete => {
                    deletes.push(key);
                }
                _ if self.persistent.contains(&key) => {
                    if !matches!(state.sync_state, SyncState::Synced) {
                        upserts.push((
                            key,
                            state,
                            RouteCommentCodec::encode_persistent(
                                &self.cfg.comment_prefix,
                                &self.cfg.plugin_tag,
                            ),
                        ));
                    }
                }
                _ => {
                    let Some(lease) = self.leases.get(&key).copied() else {
                        continue;
                    };
                    if lease.desired().is_expired(now_ms) {
                        if let Some(state) = self.routes.get_mut(&key) {
                            state.sync_state = SyncState::PendingDynamicDelete;
                        }
                        deletes.push(key);
                    } else if !matches!(state.sync_state, SyncState::Synced)
                        || lease.needs_sync(now_ms)
                    {
                        upserts.push((
                            key,
                            state,
                            RouteCommentCodec::encode_dynamic(
                                &self.cfg.comment_prefix,
                                &self.cfg.plugin_tag,
                                lease.desired(),
                                lease.last_observed_ms(),
                            ),
                        ));
                    }
                }
            }
        }

        let api = self.api.clone();
        let prefix = self.cfg.comment_prefix.clone();
        let tag = self.cfg.plugin_tag.clone();
        let results = join_all_bounded(
            upserts.iter().map(|(key, state, comment)| {
                api.upsert_host_route(key, &state.gateway, state.distance, comment, &prefix, &tag)
            }),
            UPSERT_PIPELINE_SIZE,
        )
        .await;
        let mut first_error = None;
        for ((key, _, _), result) in upserts.into_iter().zip(results) {
            match result {
                Ok(router_id) => {
                    if let Some(state) = self.routes.get_mut(&key) {
                        state.router_id = Some(router_id);
                        state.sync_state = SyncState::Synced;
                    }
                    if !self.persistent.contains(&key) {
                        self.leases.confirm_synced(&key, now_ms);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.sync_deletes(deletes, now_ms, &mut first_error).await;
        self.refresh_managed_metric();
        first_error.map_or(Ok(()), Err)
    }

    async fn sync_deletes(
        &mut self,
        keys: Vec<RouteKey>,
        now_ms: u64,
        first_error: &mut Option<DnsError>,
    ) {
        let mut dynamic = AHashMap::<RouteFamily, Vec<(RouteKey, Vec<RouterRoute>)>>::new();
        let mut immediate = Vec::new();
        for key in keys {
            let Some(state) = self.routes.get(&key) else {
                continue;
            };
            let pending_dynamic = matches!(state.sync_state, SyncState::PendingDynamicDelete);
            if pending_dynamic
                && self.cfg.conntrack_guard
                && self
                    .connection_retry_after
                    .get(&key)
                    .is_some_and(|retry| *retry > now_ms)
            {
                continue;
            }
            match self
                .api
                .find_routes(&key, &self.cfg.comment_prefix, &self.cfg.plugin_tag)
                .await
            {
                Ok(routes) if routes.is_empty() => self.forget_deleted(&key),
                Ok(routes) if pending_dynamic => {
                    dynamic.entry(key.family()).or_default().push((key, routes));
                }
                Ok(routes) => immediate.push((key, routes)),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        for (key, routes) in immediate {
            match self.delete_routes_if_still_owned(&routes).await {
                Ok(()) => self.forget_deleted(&key),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        for (family, candidates) in dynamic {
            let active = if self.cfg.conntrack_guard {
                let mut active = AHashSet::new();
                let mut failed = false;
                for chunk in candidates.chunks(CONNECTION_QUERY_BATCH_SIZE) {
                    let destinations = chunk.iter().map(|(key, _)| key.ip).collect::<Vec<_>>();
                    match self
                        .api
                        .connection_destinations(family, &destinations)
                        .await
                    {
                        Ok(found) => active.extend(found),
                        Err(error) => {
                            failed = true;
                            first_error.get_or_insert(error);
                            break;
                        }
                    }
                }
                if failed {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .connection_check_error_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    for (key, _) in candidates {
                        self.defer_connection_check(&key, now_ms);
                    }
                    continue;
                }
                active
            } else {
                AHashSet::new()
            };
            for (key, routes) in candidates {
                if active.contains(&key.ip) {
                    self.defer_connection_check(&key, now_ms);
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .delete_deferred_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    continue;
                }
                match self.delete_routes_if_still_owned(&routes).await {
                    Ok(()) => self.forget_deleted(&key),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
    }

    fn defer_connection_check(&mut self, key: &RouteKey, now_ms: u64) {
        self.connection_retry_after.insert(
            key.clone(),
            now_ms.saturating_add(CONNECTION_GUARD_RETRY_INTERVAL_SECS * 1_000),
        );
    }

    fn forget_deleted(&mut self, key: &RouteKey) {
        self.routes.remove(key);
        self.leases.remove(key);
        self.connection_retry_after.remove(key);
    }

    fn discard_unsynced_observation(&mut self, key: &RouteKey) {
        if self.leases.get(key).is_some_and(|lease| lease.has_synced()) {
            if let Some(route) = self.routes.get_mut(key) {
                route.sync_state = SyncState::Dirty;
            }
            return;
        }
        self.forget_deleted(key);
    }

    async fn sweep(&mut self) -> Result<()> {
        self.ensure_initialized().await?;
        self.harvest_reconcile().await;
        let now = now_millis();
        let expired = self.leases.expired_keys(now);
        for key in &expired {
            if let Some(state) = self.routes.get_mut(key) {
                state.sync_state = SyncState::PendingDynamicDelete;
            } else {
                self.leases.remove(key);
            }
        }
        let pending = self
            .routes
            .iter()
            .filter(|(_, state)| {
                matches!(
                    state.sync_state,
                    SyncState::PendingDynamicDelete | SyncState::PendingPersistentDelete
                )
            })
            .map(|(key, _)| key.clone())
            .collect();
        self.sync_keys(pending, now).await
    }

    async fn start_reconcile(&mut self) -> Result<()> {
        self.ensure_initialized().await?;
        if self.reconcile.is_running() {
            return Ok(());
        }
        if self.persistent.is_empty() && self.leases.is_empty() && !self.empty_state_needs_reconcile
        {
            return Ok(());
        }
        let api = self.api.clone();
        let table = self.cfg.routing_table.clone();
        let require_ipv4 = self.cfg.gateway4.is_some();
        let require_ipv6 = self.cfg.gateway6.is_some();
        self.reconcile.start(self.leases.revision(), async move {
            api.list_managed_routes(&table, require_ipv4, require_ipv6)
                .await
        });
        Ok(())
    }

    async fn harvest_reconcile(&mut self) {
        let Some(result) = self.reconcile.take_finished().await else {
            return;
        };
        match result {
            Ok(Ok(snapshot)) => match self.apply_snapshot(snapshot).await {
                Ok(()) => {
                    self.reconcile_retry.reset();
                    if let Some(metrics) = &self.metrics {
                        metrics.last_reconcile_success_timestamp_seconds.store(
                            AppClock::now_timestamp() / 1_000,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    self.refresh_transport_metrics().await;
                }
                Err(error) => self.record_reconcile_error(error).await,
            },
            Ok(Err(error)) => self.record_reconcile_error(error).await,
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                self.record_reconcile_error(DnsError::plugin(format!(
                    "ros_route reconcile task failed: {error}"
                )))
                .await
            }
        }
    }

    async fn record_reconcile_error(&mut self, error: DnsError) {
        if let Some(metrics) = &self.metrics {
            metrics
                .reconcile_error_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        warn!(plugin = %self.cfg.plugin_tag, err = %error, "ros_route reconcile failed");
        self.reconcile_retry
            .schedule(self.transport_retry_delay().await);
    }

    async fn apply_snapshot(
        &mut self,
        snapshot: VersionedSnapshot<Vec<RouterRoute>>,
    ) -> Result<()> {
        let now = now_millis();
        let mut owned = AHashMap::<RouteKey, Vec<(RouterRoute, RouteCommentMeta)>>::new();
        let mut first_error = None;
        for route in snapshot.value {
            if is_default_route_dst(&route.dst_address) {
                continue;
            }
            let Ok(prefix) = route.dst_address.parse::<IpPrefix>() else {
                continue;
            };
            let key = RouteKey {
                ip: prefix.address(),
                prefix: prefix.prefix(),
                table: self.cfg.routing_table.clone(),
            };
            let Some(comment) = route.comment.as_deref() else {
                continue;
            };
            if is_validation_comment(&self.cfg.comment_prefix, &self.cfg.plugin_tag, comment) {
                if let Err(error) = self.delete_route_if_still_owned(&route).await {
                    first_error.get_or_insert(error);
                }
                continue;
            }
            let meta = match RouteCommentCodec::decode(
                &self.cfg.comment_prefix,
                &self.cfg.plugin_tag,
                route.family,
                &route.dst_address,
                comment,
            ) {
                Ok(Some(meta)) => meta,
                _ => continue,
            };
            if self.gateway_for(key.family()).is_none() {
                if let Err(error) = self.delete_route_if_still_owned(&route).await {
                    first_error.get_or_insert(error);
                }
                self.forget_deleted(&key);
                continue;
            }
            owned.entry(key).or_default().push((route, meta));
        }

        let mut seen = AHashSet::new();
        for (key, mut candidates) in owned {
            let (route, meta) = candidates.remove(0);
            for (duplicate, _) in candidates {
                if let Err(error) = self.delete_route_if_still_owned(&duplicate).await {
                    first_error.get_or_insert(error);
                }
            }
            seen.insert(key.clone());
            let Some(gateway) = self.gateway_for(key.family()).map(str::to_string) else {
                continue;
            };
            if self.persistent.contains(&key) {
                let expected = RouteCommentCodec::encode_persistent(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                );
                let dirty = meta.kind != RouteCommentKind::Persistent
                    || route.gateway.as_deref() != Some(gateway.as_str())
                    || route.distance != Some(self.cfg.distance)
                    || route.disabled
                    || route.comment.as_deref() != Some(expected.as_str());
                self.routes.insert(
                    key,
                    RouteState {
                        gateway,
                        distance: self.cfg.distance,
                        router_id: Some(route.id),
                        sync_state: if dirty {
                            SyncState::Dirty
                        } else {
                            SyncState::Synced
                        },
                    },
                );
                continue;
            }
            if meta.kind == RouteCommentKind::Persistent {
                if self
                    .leases
                    .get(&key)
                    .is_some_and(|lease| !lease.desired().is_expired(now))
                {
                    self.routes.insert(
                        key,
                        RouteState {
                            gateway,
                            distance: self.cfg.distance,
                            router_id: Some(route.id),
                            sync_state: SyncState::Dirty,
                        },
                    );
                    continue;
                }
                self.routes.insert(
                    key,
                    RouteState {
                        gateway,
                        distance: self.cfg.distance,
                        router_id: Some(route.id),
                        sync_state: SyncState::PendingPersistentDelete,
                    },
                );
                continue;
            }
            let deadline = self
                .policy()
                .cap_recovered(meta.expires_at_ms, meta.last_seen_ms);
            let newer = self
                .leases
                .get(&key)
                .is_some_and(|lease| lease.desired_revision() > snapshot.generation);
            if deadline.is_expired(now) && !newer {
                self.routes.insert(
                    key,
                    RouteState {
                        gateway,
                        distance: self.cfg.distance,
                        router_id: Some(route.id),
                        sync_state: SyncState::PendingDynamicDelete,
                    },
                );
                continue;
            }
            if !newer {
                self.leases.recover(
                    key.clone(),
                    deadline,
                    meta.last_seen_ms,
                    snapshot.generation,
                    now,
                );
            }
            let lease = self.leases.get(&key).copied();
            let expected = lease.map(|lease| {
                RouteCommentCodec::encode_dynamic(
                    &self.cfg.comment_prefix,
                    &self.cfg.plugin_tag,
                    lease.desired(),
                    lease.last_observed_ms(),
                )
            });
            let dirty = newer
                || route.gateway.as_deref() != Some(gateway.as_str())
                || route.distance != Some(self.cfg.distance)
                || route.disabled
                || expected.as_deref() != route.comment.as_deref();
            self.routes.insert(
                key,
                RouteState {
                    gateway,
                    distance: self.cfg.distance,
                    router_id: Some(route.id),
                    sync_state: if dirty {
                        SyncState::Dirty
                    } else {
                        SyncState::Synced
                    },
                },
            );
        }

        let local_keys = self.routes.keys().cloned().collect::<Vec<_>>();
        for key in local_keys {
            if seen.contains(&key) {
                continue;
            }
            if self.persistent.contains(&key) {
                if let Some(state) = self.routes.get_mut(&key) {
                    state.router_id = None;
                    state.sync_state = SyncState::PendingCreate;
                }
                continue;
            }
            let newer = self
                .leases
                .get(&key)
                .is_some_and(|lease| lease.desired_revision() > snapshot.generation);
            if newer {
                if let Some(state) = self.routes.get_mut(&key) {
                    state.router_id = None;
                    state.sync_state = SyncState::PendingCreate;
                }
            } else {
                self.forget_deleted(&key);
            }
        }
        let keys = self.routes.keys().cloned().collect();
        if let Err(error) = self.sync_keys(keys, now).await {
            first_error.get_or_insert(error);
        }
        if self.persistent.is_empty() && self.leases.is_empty() {
            self.empty_state_needs_reconcile = false;
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn transport_retry_delay(&self) -> Option<Duration> {
        self.api
            .transport_snapshot()
            .await
            .and_then(|snapshot| snapshot.retry_after)
    }

    async fn delete_route_if_still_owned(&self, route: &RouterRoute) -> Result<bool> {
        let expected_comment = route.comment.as_deref().unwrap_or_default();
        let owned = if is_validation_comment(
            &self.cfg.comment_prefix,
            &self.cfg.plugin_tag,
            expected_comment,
        ) {
            true
        } else {
            RouteCommentCodec::decode(
                &self.cfg.comment_prefix,
                &self.cfg.plugin_tag,
                route.family,
                &route.dst_address,
                expected_comment,
            )
            .ok()
            .flatten()
            .is_some()
        };
        if !owned {
            return Ok(false);
        }
        self.api.delete_route_if_matches(route).await
    }

    async fn delete_routes_if_still_owned(&self, routes: &[RouterRoute]) -> Result<()> {
        let mut first_error = None;
        for route in routes {
            if let Err(error) = self.delete_route_if_still_owned(route).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn cleanup_route_if_still_owned(&self, route: &RouterRoute) -> Result<()> {
        self.delete_route_if_still_owned(route).await.map(|_| ())
    }

    async fn shutdown(&mut self, cleanup: bool) -> Result<()> {
        self.reconcile.cancel().await;
        if !cleanup {
            self.leases.clear();
            self.routes.clear();
            return Ok(());
        }
        self.api.begin_shutdown_cleanup();
        let rows = self
            .api
            .list_managed_routes(
                &self.cfg.routing_table,
                self.cfg.gateway4.is_some(),
                self.cfg.gateway6.is_some(),
            )
            .await?;
        let owned = rows
            .into_iter()
            .filter(|route| {
                route.comment.as_deref().is_some_and(|comment| {
                    is_validation_comment(&self.cfg.comment_prefix, &self.cfg.plugin_tag, comment)
                        || matches!(
                            RouteCommentCodec::decode(
                                &self.cfg.comment_prefix,
                                &self.cfg.plugin_tag,
                                route.family,
                                &route.dst_address,
                                comment,
                            ),
                            Ok(Some(_))
                        )
                })
            })
            .collect::<Vec<_>>();
        let results = join_all_bounded(
            owned
                .iter()
                .map(|route| self.cleanup_route_if_still_owned(route)),
            UPSERT_PIPELINE_SIZE,
        )
        .await;
        let failures = results.iter().filter(|result| result.is_err()).count();
        if failures > 0
            && let Some(metrics) = &self.metrics
        {
            metrics
                .cleanup_error_total
                .fetch_add(failures as u64, std::sync::atomic::Ordering::Relaxed);
        }
        self.leases.clear();
        self.routes.clear();
        results
            .into_iter()
            .find_map(std::result::Result::err)
            .map_or(Ok(()), Err)
    }
}

async fn run_manager_worker(
    tag: String,
    mut manager: RouteManager,
    handle: RouteManagerHandle,
    mut lifecycle_rx: mpsc::Receiver<LifecycleCommand>,
    mut active: bool,
    mut shutdown_rx: oneshot::Receiver<ShutdownRequest>,
) {
    let error_logs = ErrorLogThrottle::default();
    let mut retries = AHashMap::<RouteKey, (tokio::time::Instant, RouteObservation)>::new();
    loop {
        manager.harvest_reconcile().await;
        let next_retry = retries.values().map(|(at, _)| *at).min();
        let retry_wakeup = async {
            match next_retry {
                Some(at) => tokio::time::sleep_until(at).await,
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
        enum Event {
            Observe(Vec<(RouteKey, RouteObservation)>),
            Control(ControlCommand),
            ReconcileCompleted,
            Lifecycle(LifecycleCommand),
        }
        let event = tokio::select! {
            biased;
            shutdown = &mut shutdown_rx => {
                if let Ok(ShutdownRequest { cleanup, done }) = shutdown {
                    let _ = done.send(manager.shutdown(cleanup).await);
                }
                break;
            }
            lifecycle = lifecycle_rx.recv() => lifecycle.map(Event::Lifecycle),
            _ = manager.reconcile.wait(), if active && manager.reconcile.is_running() => Some(Event::ReconcileCompleted),
            control = handle.controls.recv(), if active => control.map(|(_, command)| Event::Control(command)),
            _ = retry_wakeup, if active => {
                let now = tokio::time::Instant::now();
                let keys = retries
                    .iter()
                    .filter(|(_, (at, _))| *at <= now)
                    .take(UPSERT_PIPELINE_SIZE)
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                let due = keys
                    .into_iter()
                    .filter_map(|key| {
                        retries.remove(&key).map(|(_, mut command)| {
                            if let Some(newer) = handle.observations.take(&key) {
                                command.coalesce(newer);
                            }
                            (key, command)
                        })
                    })
                    .collect::<Vec<_>>();
                (!due.is_empty()).then_some(Event::Observe(due))
            }
            _ = reconcile_retry_wakeup, if active => {
                manager.reconcile_retry.mark_due();
                Some(Event::Control(ControlCommand::Reconcile))
            }
            observation = handle.observations.recv(), if active => observation.map(|first| {
                let mut batch = vec![first];
                while batch.len() < UPSERT_PIPELINE_SIZE {
                    let Some(next) = handle.observations.try_recv() else { break };
                    batch.push(next);
                }
                Event::Observe(batch)
            }),
        };
        let Some(event) = event else { break };
        match event {
            Event::Lifecycle(LifecycleCommand::Quiesce { done }) => {
                active = false;
                manager.reconcile.cancel().await;
                let mut merged = AHashMap::<RouteKey, RouteObservation>::new();
                for (key, command) in handle.observations.drain_where(|_| true) {
                    merged
                        .entry(key)
                        .and_modify(|current| current.coalesce(command.clone()))
                        .or_insert(command);
                }
                for (key, (_, command)) in retries.drain() {
                    merged
                        .entry(key)
                        .and_modify(|current| current.coalesce(command.clone()))
                        .or_insert(command);
                }
                let _ = done.send(RoutePendingWork {
                    items: merged.into_iter().collect(),
                });
            }
            Event::Lifecycle(LifecycleCommand::Activate { pending, done }) => {
                let now = now_millis();
                for (key, mut command) in pending.items {
                    command.deadline = manager
                        .policy()
                        .cap_recovered(command.deadline, command.observed_at_ms);
                    if command.deadline.is_expired(now) {
                        for completion in command.completions {
                            completion.finish(&Ok(()));
                        }
                        continue;
                    }
                    match handle.observations.try_push(key.clone(), command) {
                        Ok(_) => {}
                        Err(TryPushError::Full(command)) => {
                            defer_route_observation(
                                &mut retries,
                                tokio::time::Instant::now(),
                                key,
                                command,
                                manager.metrics.as_deref(),
                            );
                        }
                        Err(TryPushError::Closed(command)) => {
                            let result = Err(DnsError::plugin(
                                "ros_route handoff observation mailbox is closed",
                            ));
                            for completion in command.completions {
                                completion.finish(&result);
                            }
                            if let Some(metrics) = &manager.metrics {
                                metrics
                                    .dropped_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
                active = true;
                handle.request_reconcile();
                let _ = done.send(());
            }
            Event::ReconcileCompleted => manager.harvest_reconcile().await,
            Event::Control(ControlCommand::Reconcile) => {
                if let Err(error) = manager.start_reconcile().await {
                    manager.record_reconcile_error(error).await;
                }
            }
            Event::Control(ControlCommand::Sweep) => {
                if let Err(error) = manager.sweep().await
                    && error_logs.should_log("sweep")
                {
                    warn!(plugin = %tag, err = %error, "ros_route sweep failed");
                }
            }
            Event::Observe(mut batch) => {
                let observations = batch
                    .iter()
                    .map(|(key, command)| (key.clone(), command.clone()))
                    .collect::<Vec<_>>();
                let result = manager.observe_batch(&observations).await;
                let retry_delay = if result.is_err() {
                    manager.transport_retry_delay().await
                } else {
                    None
                };
                for (key, mut command) in batch.drain(..) {
                    for completion in command.completions.drain(..) {
                        completion.finish(&result);
                    }
                    if let Some(delay) = retry_delay {
                        if retries.len() >= MANAGER_QUEUE_SIZE && !retries.contains_key(&key) {
                            manager.discard_unsynced_observation(&key);
                        }
                        defer_route_observation(
                            &mut retries,
                            tokio::time::Instant::now() + delay,
                            key,
                            command,
                            manager.metrics.as_deref(),
                        );
                    }
                }
                if retry_delay.is_none()
                    && let Err(error) = result
                    && error_logs.should_log("observe")
                {
                    warn!(plugin = %tag, err = %error, "ros_route observation failed");
                }
            }
        }
        handle.refresh_pending_metric_with(retries.len());
    }
    debug!(plugin = %tag, "ros_route manager worker exited");
}

fn defer_route_observation(
    retries: &mut AHashMap<RouteKey, (tokio::time::Instant, RouteObservation)>,
    retry_at: tokio::time::Instant,
    key: RouteKey,
    command: RouteObservation,
    metrics: Option<&RosRouteMetrics>,
) {
    if let Some((scheduled_at, existing)) = retries.get_mut(&key) {
        *scheduled_at = (*scheduled_at).min(retry_at);
        existing.coalesce(command);
        if let Some(metrics) = metrics {
            metrics
                .coalesced_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        return;
    }
    if retries.len() < MANAGER_QUEUE_SIZE {
        retries.insert(key, (retry_at, command));
        return;
    }

    let result = Err(DnsError::plugin(
        "ros_route retry observation capacity reached",
    ));
    for completion in command.completions {
        completion.finish(&result);
    }
    if let Some(metrics) = metrics {
        metrics
            .dropped_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn validation_nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn validation_route_key(family: RouteFamily, table: &str, nonce: u128) -> RouteKey {
    match family {
        RouteFamily::Ipv4 => {
            // RFC 2544 benchmarking range 198.18.0.0/15.
            let host = ((nonce as u32) & 0x0001_FFFF) | 0xC612_0000;
            RouteKey::new(IpAddr::V4(Ipv4Addr::from(host)), table.to_string())
        }
        RouteFamily::Ipv6 => RouteKey::new(
            // RFC 3849 documentation range 2001:db8::/32.
            IpAddr::V6(Ipv6Addr::from(
                0x2001_0DB8_0000_0000_0000_0000_0000_0000u128
                    | (nonce & 0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFu128),
            )),
            table.to_string(),
        ),
    }
}

pub(super) fn is_default_route_dst(dst: &str) -> bool {
    dst == ROUTE_DEFAULT_V4 || dst == ROUTE_DEFAULT_V6
}

fn now_millis() -> u64 {
    AppClock::now_timestamp()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;

    #[derive(Debug, Default)]
    struct MockApi {
        routes: Mutex<Vec<RouterRoute>>,
        connections: Mutex<AHashSet<IpAddr>>,
        validation_attempts: AtomicUsize,
        validation_failures: AtomicUsize,
    }

    impl MockApi {
        fn routes(&self) -> Vec<RouterRoute> {
            self.routes.lock().expect("routes").clone()
        }

        fn remove_remote(&self, key: &RouteKey) {
            self.routes.lock().expect("routes").retain(|route| {
                route.dst_address != key.dst_address() || route.routing_table != key.table
            });
        }
    }

    #[async_trait]
    impl MikrotikApi for MockApi {
        async fn list_managed_routes(
            &self,
            table: &str,
            _require_ipv4: bool,
            _require_ipv6: bool,
        ) -> Result<Vec<RouterRoute>> {
            Ok(self
                .routes()
                .into_iter()
                .filter(|route| route.routing_table == table)
                .collect())
        }

        async fn find_routes(
            &self,
            key: &RouteKey,
            comment_prefix: &str,
            plugin_tag: &str,
        ) -> Result<Vec<RouterRoute>> {
            Ok(self
                .routes()
                .into_iter()
                .filter(|route| {
                    route.dst_address == key.dst_address()
                        && route.routing_table == key.table
                        && route.comment.as_deref().is_some_and(|comment| {
                            matches!(
                                RouteCommentCodec::decode(
                                    comment_prefix,
                                    plugin_tag,
                                    route.family,
                                    &route.dst_address,
                                    comment,
                                ),
                                Ok(Some(_))
                            )
                        })
                })
                .collect())
        }

        async fn upsert_host_route(
            &self,
            key: &RouteKey,
            gateway: &str,
            distance: u8,
            comment: &str,
            _comment_prefix: &str,
            _plugin_tag: &str,
        ) -> Result<String> {
            let mut routes = self.routes.lock().expect("routes");
            if let Some(route) = routes.iter_mut().find(|route| {
                route.dst_address == key.dst_address() && route.routing_table == key.table
            }) {
                route.gateway = Some(gateway.to_string());
                route.distance = Some(distance);
                route.comment = Some(comment.to_string());
                route.disabled = false;
                return Ok(route.id.clone());
            }
            let id = format!("*{}", routes.len() + 1);
            routes.push(RouterRoute {
                id: id.clone(),
                family: key.family(),
                dst_address: key.dst_address(),
                routing_table: key.table.clone(),
                gateway: Some(gateway.to_string()),
                distance: Some(distance),
                comment: Some(comment.to_string()),
                disabled: false,
            });
            Ok(id)
        }

        async fn validate_route_config(
            &self,
            _key: &RouteKey,
            _gateway: &str,
            _distance: u8,
            _comment: &str,
        ) -> Result<()> {
            self.validation_attempts.fetch_add(1, Ordering::Relaxed);
            if self
                .validation_failures
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(DnsError::plugin("validation unavailable"));
            }
            Ok(())
        }

        async fn delete_route_if_matches(&self, expected: &RouterRoute) -> Result<bool> {
            let mut routes = self.routes.lock().expect("routes");
            let Some(index) = routes.iter().position(|route| route == expected) else {
                return Ok(false);
            };
            routes.remove(index);
            Ok(true)
        }

        async fn connection_destinations(
            &self,
            _family: RouteFamily,
            destinations: &[IpAddr],
        ) -> Result<AHashSet<IpAddr>> {
            let connections = self.connections.lock().expect("connections");
            Ok(destinations
                .iter()
                .filter(|ip| connections.contains(ip))
                .copied()
                .collect())
        }
    }

    fn config(fixed_ttl: Option<u32>, conntrack_guard: bool) -> RouteManagerConfig {
        AppClock::start();
        RouteManagerConfig {
            plugin_tag: "route-test".to_string(),
            routing_table: "policy".to_string(),
            gateway4: Some("192.0.2.1".to_string()),
            gateway6: None,
            persistent_ips: AHashSet::new(),
            comment_prefix: "fdns".to_string(),
            distance: 100,
            min_ttl: 1,
            max_ttl: 3_600,
            fixed_ttl,
            conntrack_guard,
        }
    }

    #[test]
    fn route_comment_has_no_domain_or_mixed_ownership() {
        let comment = RouteCommentCodec::encode_dynamic(
            "fdns",
            "route-test",
            LeaseDeadline::At(400_000),
            100_000,
        );
        assert_eq!(comment, "fdns;pg=route-test;kind=D;exp=400;seen=100");
        let decoded = RouteCommentCodec::decode(
            "fdns",
            "route-test",
            RouteFamily::Ipv4,
            "203.0.113.10/32",
            &comment,
        )
        .expect("decode")
        .expect("owned");
        assert_eq!(decoded.kind, RouteCommentKind::Dynamic);
        assert_eq!(decoded.expires_at_ms, LeaseDeadline::At(400_000));
    }

    #[test]
    fn old_domain_comment_is_not_owned_by_the_new_plugin_format() {
        let decoded = RouteCommentCodec::decode(
            "fdns",
            "route-test",
            RouteFamily::Ipv4,
            "203.0.113.10/32",
            "fdns;pg=route-test;kind=dynamic;dm=example.com;exp=400;seen=100",
        )
        .expect("decode");
        assert!(decoded.is_none());
    }

    #[tokio::test]
    async fn repeated_observations_share_one_route_lease() {
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), config(Some(300), false));
        let key = RouteKey::new("203.0.113.10".parse().expect("ip"), "policy".to_string());
        let now = now_millis();

        manager
            .observe_key(
                key.clone(),
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 300_000),
                    observed_at_ms: now,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("first observation");
        manager
            .observe_key(
                key,
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 600_000),
                    observed_at_ms: now + 1,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("second observation");

        assert_eq!(manager.leases.len(), 1);
        assert_eq!(api.routes().len(), 1);
        assert!(
            api.routes()[0]
                .comment
                .as_deref()
                .expect("comment")
                .contains("exp=")
        );
    }

    #[tokio::test]
    async fn reconcile_accepts_manual_dynamic_deletion_until_next_observation() {
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), config(Some(300), false));
        let key = RouteKey::new("203.0.113.11".parse().expect("ip"), "policy".to_string());
        let now = now_millis();
        let observation = RouteObservation {
            deadline: LeaseDeadline::At(now + 300_000),
            observed_at_ms: now,
            completions: Vec::new(),
        };
        manager
            .observe_key(key.clone(), &observation)
            .await
            .expect("observation");
        let generation = manager.leases.revision();
        api.remove_remote(&key);

        manager
            .apply_snapshot(VersionedSnapshot {
                generation,
                value: Vec::new(),
            })
            .await
            .expect("reconcile");
        assert!(!manager.leases.contains_key(&key));

        manager
            .observe_key(key, &observation)
            .await
            .expect("re-observation");
        assert_eq!(api.routes().len(), 1);
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_erase_a_newer_observation() {
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), config(Some(300), false));
        let key = RouteKey::new("203.0.113.21".parse().expect("ip"), "policy".to_string());
        let now = now_millis();
        manager
            .observe_key(
                key.clone(),
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 300_000),
                    observed_at_ms: now,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("initial observation");
        let scan_revision = manager.leases.revision();

        manager
            .observe_key(
                key.clone(),
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 120_000),
                    observed_at_ms: now + 1,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("newer observation");
        manager
            .apply_snapshot(VersionedSnapshot {
                generation: scan_revision,
                value: Vec::new(),
            })
            .await
            .expect("stale snapshot");

        assert!(manager.leases.contains_key(&key));
        assert_eq!(api.routes().len(), 1);
    }

    #[tokio::test]
    async fn stale_persistent_snapshot_converges_to_new_dynamic_lease() {
        let api = Arc::new(MockApi::default());
        let key = RouteKey::new("203.0.113.22".parse().expect("ip"), "policy".to_string());
        let mut cfg = config(Some(300), false);
        cfg.persistent_ips
            .insert(key.dst_address().parse().expect("prefix"));
        let mut manager = RouteManager::new(api.clone(), cfg);
        manager.ensure_initialized().await.expect("initialize");
        manager
            .sync_keys(vec![key.clone()], now_millis())
            .await
            .expect("persistent sync");
        let stale = api.routes();

        manager.persistent.remove(&key);
        let now = now_millis();
        manager
            .observe_key(
                key.clone(),
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 300_000),
                    observed_at_ms: now,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("dynamic observation");
        manager
            .apply_snapshot(VersionedSnapshot {
                generation: 0,
                value: stale,
            })
            .await
            .expect("stale persistent snapshot");

        let route = api.routes().pop().expect("route");
        let meta = RouteCommentCodec::decode(
            "fdns",
            "route-test",
            route.family,
            &route.dst_address,
            route.comment.as_deref().expect("comment"),
        )
        .expect("decode")
        .expect("owned");
        assert_eq!(meta.kind, RouteCommentKind::Dynamic);
        assert!(manager.leases.contains_key(&key));
    }

    #[tokio::test]
    async fn reconcile_does_not_delete_a_validation_row_after_ownership_changes() {
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), config(None, false));
        let key = RouteKey::new("198.18.0.10".parse().expect("ip"), "policy".to_string());
        let route = RouterRoute {
            id: "*validation".to_string(),
            family: RouteFamily::Ipv4,
            dst_address: key.dst_address(),
            routing_table: key.table.clone(),
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(format!(
                "{};nonce=1",
                RouteCommentCodec::prefix("fdns", "route-test", COMMENT_KIND_GATEWAY_CHECK)
            )),
            disabled: true,
        };
        api.routes.lock().expect("routes").push(route.clone());
        api.routes.lock().expect("routes")[0].comment = Some("operator-owned".to_string());

        manager
            .apply_snapshot(VersionedSnapshot {
                generation: 0,
                value: vec![route],
            })
            .await
            .expect("reconcile");

        assert_eq!(api.routes().len(), 1);
        assert_eq!(api.routes()[0].comment.as_deref(), Some("operator-owned"));
    }

    #[tokio::test]
    async fn refreshed_dynamic_comment_invalidates_an_older_delete_snapshot() {
        let api = Arc::new(MockApi::default());
        let manager = RouteManager::new(api.clone(), config(Some(300), false));
        let key = RouteKey::new("203.0.113.90".parse().expect("ip"), "policy".to_string());
        let old = RouterRoute {
            id: "*refresh-race".to_string(),
            family: RouteFamily::Ipv4,
            dst_address: key.dst_address(),
            routing_table: key.table,
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(RouteCommentCodec::encode_dynamic(
                "fdns",
                "route-test",
                LeaseDeadline::At(100_000),
                1_000,
            )),
            disabled: false,
        };
        api.routes.lock().expect("routes").push(old.clone());
        api.routes.lock().expect("routes")[0].comment = Some(RouteCommentCodec::encode_dynamic(
            "fdns",
            "route-test",
            LeaseDeadline::At(500_000),
            2_000,
        ));

        assert!(
            !manager
                .delete_route_if_still_owned(&old)
                .await
                .expect("conditional delete")
        );
        assert_eq!(api.routes().len(), 1);
    }

    #[tokio::test]
    async fn changed_route_parameters_invalidate_an_older_delete_snapshot() {
        let key = RouteKey::new("203.0.113.92".parse().expect("ip"), "policy".to_string());
        let expected = RouterRoute {
            id: "*parameter-race".to_string(),
            family: RouteFamily::Ipv4,
            dst_address: key.dst_address(),
            routing_table: key.table,
            gateway: Some("192.0.2.1".to_string()),
            distance: Some(100),
            comment: Some(RouteCommentCodec::encode_dynamic(
                "fdns",
                "route-test",
                LeaseDeadline::At(100_000),
                1_000,
            )),
            disabled: false,
        };

        for changed in [
            RouterRoute {
                gateway: Some("192.0.2.2".to_string()),
                ..expected.clone()
            },
            RouterRoute {
                distance: Some(101),
                ..expected.clone()
            },
            RouterRoute {
                disabled: true,
                ..expected.clone()
            },
        ] {
            let api = Arc::new(MockApi::default());
            api.routes.lock().expect("routes").push(changed);
            let manager = RouteManager::new(api.clone(), config(Some(300), false));

            assert!(
                !manager
                    .delete_route_if_still_owned(&expected)
                    .await
                    .expect("conditional delete")
            );
            assert_eq!(api.routes().len(), 1);
        }
    }

    #[tokio::test]
    async fn paused_runtime_hands_pending_observations_to_replacement() {
        let old_api = Arc::new(MockApi::default());
        let new_api = Arc::new(MockApi::default());
        let cfg = config(Some(300), false);
        let old_runtime = RouteManagerRuntime::start_paused(
            "route-handoff-old".to_string(),
            RouteManager::new(old_api.clone(), cfg.clone()),
        );
        let old_handle = old_runtime.handle();
        old_handle
            .try_observe(
                vec![ObservedAddr {
                    addr: "203.0.113.91".parse().expect("ip"),
                    ttl_secs: 300,
                }],
                None,
            )
            .expect("queue old observation");
        let pending = old_handle.quiesce().await;

        let new_runtime = RouteManagerRuntime::start_paused(
            "route-handoff-new".to_string(),
            RouteManager::new(new_api.clone(), cfg),
        );
        new_runtime
            .handle()
            .activate(pending)
            .await
            .expect("activate replacement");
        for _ in 0..64 {
            if !new_api.routes().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(old_api.routes().is_empty());
        assert_eq!(new_api.routes().len(), 1);

        old_runtime.shutdown(false).await.expect("old shutdown");
        new_runtime.shutdown(false).await.expect("new shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn initialization_failure_uses_fast_reconcile_retry() {
        let api = Arc::new(MockApi::default());
        api.validation_failures.store(2, Ordering::Release);
        let runtime = RouteManagerRuntime::start_paused(
            "route-reconcile-retry".to_string(),
            RouteManager::new(api.clone(), config(None, false)),
        );
        runtime
            .handle()
            .activate(RoutePendingWork::default())
            .await
            .expect("activate");
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(api.validation_attempts.load(Ordering::Acquire), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(api.validation_attempts.load(Ordering::Acquire), 2);

        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(api.validation_attempts.load(Ordering::Acquire) >= 3);
        tokio::time::resume();
        runtime.shutdown(false).await.expect("shutdown");
    }

    #[tokio::test]
    async fn conntrack_guard_defers_all_dynamic_duplicate_deletions() {
        let api = Arc::new(MockApi::default());
        let mut manager = RouteManager::new(api.clone(), config(Some(1), true));
        let ip = "203.0.113.12".parse().expect("ip");
        let key = RouteKey::new(ip, "policy".to_string());
        let now = now_millis();
        manager
            .observe_key(
                key.clone(),
                &RouteObservation {
                    deadline: LeaseDeadline::At(now + 300_000),
                    observed_at_ms: now,
                    completions: Vec::new(),
                },
            )
            .await
            .expect("observation");
        let mut duplicate = api.routes()[0].clone();
        duplicate.id = "*dynamic-duplicate".to_string();
        api.routes.lock().expect("routes").push(duplicate);
        manager.leases.remove(&key);
        manager.leases.observe(key.clone(), LeaseDeadline::At(0), 0);
        api.connections.lock().expect("connections").insert(ip);

        manager.sweep().await.expect("guarded sweep");
        assert_eq!(api.routes().len(), 2);

        api.connections.lock().expect("connections").clear();
        manager.connection_retry_after.clear();
        manager.sweep().await.expect("retry sweep");
        assert!(api.routes().is_empty());
    }

    #[tokio::test]
    async fn removed_persistent_route_deletes_all_duplicates_without_conntrack_guard() {
        let api = Arc::new(MockApi::default());
        let ip = "203.0.113.13".parse().expect("ip");
        let key = RouteKey::new(ip, "policy".to_string());
        let mut cfg = config(None, true);
        cfg.persistent_ips
            .insert(key.dst_address().parse().expect("prefix"));
        let mut manager = RouteManager::new(api.clone(), cfg);
        manager.ensure_initialized().await.expect("initialize");
        manager
            .sync_keys(vec![key.clone()], now_millis())
            .await
            .expect("persistent upsert");
        let mut duplicate = api.routes()[0].clone();
        duplicate.id = "*persistent-duplicate".to_string();
        api.routes.lock().expect("routes").push(duplicate);
        api.connections.lock().expect("connections").insert(ip);

        manager.persistent.remove(&key);
        manager.routes.get_mut(&key).expect("route").sync_state =
            SyncState::PendingPersistentDelete;
        manager
            .sync_keys(vec![key], now_millis())
            .await
            .expect("persistent delete");

        assert!(api.routes().is_empty());
    }
}
