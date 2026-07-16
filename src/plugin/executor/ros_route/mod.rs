// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! `ros_route` executor plugin.
//!
//! This executor is an observer-side effect stage designed to integrate with
//! OxiDNS sequence pipelines. It does not alter DNS decisions or response
//! content. Instead, it watches final downstream DNS answers and synchronizes
//! host routes into a dedicated RouterOS routing table.
//!
//! Architecture overview:
//! - continuation execution stays hot-path light and observes final A/AAAA
//!   answers.
//! - route synchronization is delegated to a single-owner background
//!   `RouteManager` state machine.
//! - RouterOS API details are isolated in `MikrotikApi` adapter
//!   implementations.
//! - route metadata is persisted in RouterOS `comment` via `RouteCommentCodec`,
//!   allowing restart recovery without local state files.
//!
//! Behavior goals:
//! - maintain `/32` (IPv4) and `/128` (IPv6) host routes in configured table.
//! - support optional always-present CIDR routes via `persistent`.
//! - load persistent route files once during plugin initialization.
//! - preserve DNS hot-path latency (`async=true` uses non-blocking queue).
//! - provide blocking write-before-return mode (`async=false`) without
//!   affecting DNS response result.
//! - avoid long-term route pollution via TTL sweep + startup reconciliation +
//!   optional shutdown cleanup.
//! - assume routing table/rule/default routes are already provisioned by users.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ahash::AHashSet;
use async_trait::async_trait;
use serde::Deserialize;
use serde_yaml_ng::Value;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tracing::warn;

use crate::config::types::PluginConfig;
use crate::core::context::DnsContext;
use crate::infra::error::{DnsError, Result};
use crate::infra::observability::metrics::{
    MetricLabel, MetricSample, MetricSink, MetricSource, register_metric_source,
    unregister_metric_source,
};
use crate::plugin::executor::{ExecStep, Executor, ExecutorNext};
use crate::plugin::{Plugin, PluginFactory, UninitializedPlugin};
use crate::proto::{Rcode, RecordType};
use crate::{continue_next, plugin_factory};

const DEFAULT_MIN_TTL: u32 = 60;
const DEFAULT_MAX_TTL: u32 = 3600;
const DEFAULT_ASYNC_MODE: bool = true;
const DEFAULT_CLEANUP_ON_SHUTDOWN: bool = true;
const DEFAULT_CONNTRACK_GUARD: bool = false;
const DEFAULT_ROUTE_DISTANCE: u8 = 100;
const DEFAULT_COMMENT_PREFIX: &str = "oxi";
const SYNC_OBSERVE_TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MikrotikConfigArgs {
    /// RouterOS API endpoint, usually `<host>:8728`.
    address: Option<String>,
    /// RouterOS login username.
    username: Option<String>,
    /// RouterOS login password.
    password: Option<String>,
    /// RouterOS API connection timeout in seconds.
    connect_timeout: Option<u64>,
    /// RouterOS API command send timeout in seconds.
    send_timeout: Option<u64>,
    /// RouterOS API response receive timeout in seconds.
    receive_timeout: Option<u64>,
    /// Optional RouterOS API-SSL configuration. Presence enables TLS.
    tls: Option<RouterOsTlsArgs>,
    /// Whether post stage waits RouterOS writes (`false`) or queues work
    /// (`true`).
    #[serde(rename = "async")]
    async_mode: Option<bool>,
    /// Dedicated RouterOS routing table for managed routes.
    routing_table: Option<String>,
    /// IPv4 gateway value for managed IPv4 routes.
    gateway4: Option<String>,
    /// IPv6 gateway value for managed IPv6 routes.
    gateway6: Option<String>,
    /// Prefix used in RouterOS route comments to mark OxiDNS-managed routes.
    /// Defaults to `oxi` when omitted.
    comment_prefix: Option<String>,
    /// Route distance written to RouterOS for managed routes.
    distance: Option<u8>,
    /// Always-present routes that should not expire with DNS TTL.
    persistent: Option<PersistentArgs>,
    /// Minimum effective TTL clamp (seconds) for observed records.
    min_ttl: Option<u32>,
    /// Maximum effective TTL clamp (seconds) for observed records.
    max_ttl: Option<u32>,
    /// Optional fixed TTL override (seconds) for dynamic observed records.
    /// `0` keeps a dynamic route until explicit cleanup or operator removal.
    fixed_ttl: Option<u32>,
    /// Whether to clean managed dynamic routes on shutdown.
    cleanup_on_shutdown: Option<bool>,
    /// Delay normal route removal while RouterOS connection tracking has a
    /// connection for the route destination.
    conntrack_guard: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PersistentArgs {
    /// Inline always-present IPs/CIDRs. Plain IP is normalized to host route.
    ips: Option<Vec<String>>,
    /// File list that provides always-present IPs.
    files: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct MikrotikConfig {
    /// RouterOS API endpoint.
    address: String,
    /// Connection settings consumed when the API transport is constructed.
    connection: Option<RouterOsConnectionConfig>,
    /// Async mode switch for post stage RouterOS writes.
    async_mode: bool,
    /// Dedicated RouterOS routing table for this plugin.
    routing_table: String,
    /// Optional IPv4 gateway.
    gateway4: Option<String>,
    /// Optional IPv6 gateway.
    gateway6: Option<String>,
    /// Always-present routes in normalized CIDR format (`ip/prefix`).
    persistent_ips: AHashSet<String>,
    /// Managed route comment prefix.
    comment_prefix: String,
    /// Route distance written to RouterOS.
    distance: u8,
    /// Minimum effective TTL clamp in seconds.
    min_ttl: u32,
    /// Maximum effective TTL clamp in seconds.
    max_ttl: u32,
    /// Optional fixed TTL override in seconds. `0` never expires by time.
    fixed_ttl: Option<u32>,
    /// Shutdown cleanup behavior for dynamic routes.
    cleanup_on_shutdown: bool,
    /// Delay normal route removal while a matching RouterOS connection exists.
    conntrack_guard: bool,
}

#[derive(Debug)]
struct ExtractedObservation {
    addrs: Vec<ObservedAddr>,
}

impl MikrotikConfigArgs {
    fn into_config(self, emit_warnings: bool) -> Result<MikrotikConfig> {
        let address = required_non_empty(self.address, "address")?;
        let username = required_non_empty(self.username, "username")?;
        let password = required_non_empty(self.password, "password")?;
        let api_timeouts = MikrotikApiTimeouts::from_secs(
            timeout_secs(
                self.connect_timeout,
                "connect_timeout",
                DEFAULT_CONNECT_TIMEOUT_SECS,
            )?,
            timeout_secs(self.send_timeout, "send_timeout", DEFAULT_SEND_TIMEOUT_SECS)?,
            timeout_secs(
                self.receive_timeout,
                "receive_timeout",
                DEFAULT_RECEIVE_TIMEOUT_SECS,
            )?,
        );
        let connection = RouterOsConnectionConfig::new(
            address.clone(),
            username,
            password,
            api_timeouts,
            self.tls,
        )?;
        let routing_table = required_non_empty(self.routing_table, "routing_table")?;
        let comment_prefix = optional_non_empty(self.comment_prefix)
            .unwrap_or_else(|| DEFAULT_COMMENT_PREFIX.to_string());
        validate_comment_token("comment_prefix", &comment_prefix)?;
        let distance = self.distance.unwrap_or(DEFAULT_ROUTE_DISTANCE);

        let gateway4 = optional_non_empty(self.gateway4);
        let gateway6 = optional_non_empty(self.gateway6);
        if gateway4.is_none() && gateway6.is_none() {
            return Err(DnsError::plugin(
                "ros_route requires at least one of gateway4 or gateway6",
            ));
        }

        let min_ttl = self.min_ttl.unwrap_or(DEFAULT_MIN_TTL);
        let max_ttl = self.max_ttl.unwrap_or(DEFAULT_MAX_TTL);
        if min_ttl > max_ttl {
            return Err(DnsError::plugin(format!(
                "ros_route ttl range is invalid: min_ttl({min_ttl}) > max_ttl({max_ttl})"
            )));
        }
        // `0` deliberately means a dynamic route that never expires by time.
        let fixed_ttl = self.fixed_ttl;
        let parsed_persistent =
            parse_persistent_ips(self.persistent, gateway4.is_some(), gateway6.is_some())?;
        let ignored_by_gateway = parsed_persistent.ignored_by_gateway;
        if emit_warnings && ignored_by_gateway > 0 {
            warn!(
                ignored = ignored_by_gateway,
                "ros_route persistent ignored entries without corresponding gateway family"
            );
        }
        let ignored_default_route = parsed_persistent.ignored_default_route;
        if emit_warnings && ignored_default_route > 0 {
            warn!(
                ignored = ignored_default_route,
                "ros_route persistent ignored default-route entries (/0)"
            );
        }

        Ok(MikrotikConfig {
            address,
            connection: Some(connection),
            async_mode: self.async_mode.unwrap_or(DEFAULT_ASYNC_MODE),
            routing_table,
            gateway4,
            gateway6,
            persistent_ips: parsed_persistent.all_ips,
            comment_prefix,
            distance,
            min_ttl,
            max_ttl,
            fixed_ttl,
            cleanup_on_shutdown: self
                .cleanup_on_shutdown
                .unwrap_or(DEFAULT_CLEANUP_ON_SHUTDOWN),
            conntrack_guard: self.conntrack_guard.unwrap_or(DEFAULT_CONNTRACK_GUARD),
        })
    }
}

mod api;
mod manager;

use self::api::{
    DEFAULT_CONNECT_TIMEOUT_SECS, DEFAULT_RECEIVE_TIMEOUT_SECS, DEFAULT_SEND_TIMEOUT_SECS,
    MikrotikApi, MikrotikApiTimeouts, MikrotikRsClient,
};
use self::manager::{
    ObserveEnqueueError, RouteManager, RouteManagerConfig, RouteManagerHandle, RouteManagerRuntime,
    RoutePendingWork,
};
use crate::infra::mikrotik::ip_prefix::IpPrefix;
use crate::infra::mikrotik::lifecycle::{ActiveInstanceRegistry, WriterGate};
use crate::infra::mikrotik::transport::{RouterOsConnectionConfig, RouterOsTlsArgs};
use crate::infra::mikrotik::{ObservedAddr, SHUTDOWN_TIMEOUT, collect_observed_addrs};

#[derive(Debug)]
struct MikrotikExecutor {
    tag: String,
    instance_id: u64,
    active_registered: AtomicBool,
    writer_gate: Arc<WriterGate>,
    manager_active: Arc<AtomicBool>,
    metrics: Arc<RosRouteMetrics>,
    config: MikrotikConfig,
    manager: Option<RouteManager>,
    manager_handle: Option<RouteManagerHandle>,
    runtime: Mutex<Option<RouteManagerRuntime>>,
}

#[derive(Debug)]
struct RosRouteMetrics {
    tag: String,
    observe_total: AtomicU64,
    dropped_total: AtomicU64,
    sync_error_total: AtomicU64,
    sync_timeout_total: AtomicU64,
    delete_deferred_total: AtomicU64,
    connection_check_error_total: AtomicU64,
    pending_observations: AtomicU64,
    managed_entries: AtomicU64,
    coalesced_total: AtomicU64,
    reconnect_total: AtomicU64,
    connect_attempt_total: AtomicU64,
    backoff_total: AtomicU64,
    reconcile_error_total: AtomicU64,
    last_reconcile_success_timestamp_seconds: AtomicU64,
    degraded: AtomicU64,
    cleanup_error_total: AtomicU64,
}

#[derive(Debug, Clone)]
struct ActiveRouteInstance {
    instance_id: u64,
    namespace: RouteOwnershipNamespace,
    metrics: Arc<RosRouteMetrics>,
    /// Lifecycle channel used to pause, drain, and activate the single writer
    /// during commit or rollback.
    manager_handle: Option<RouteManagerHandle>,
    writer_gate: Arc<WriterGate>,
    manager_active: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RouteOwnershipNamespace {
    address: String,
    routing_table: String,
    comment_prefix: String,
}

impl RouteOwnershipNamespace {
    fn from_config(config: &MikrotikConfig) -> Self {
        Self {
            address: config.address.clone(),
            routing_table: config.routing_table.clone(),
            comment_prefix: config.comment_prefix.clone(),
        }
    }
}

static NEXT_ROUTE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn active_route_instances() -> &'static ActiveInstanceRegistry<ActiveRouteInstance> {
    static INSTANCES: OnceLock<ActiveInstanceRegistry<ActiveRouteInstance>> = OnceLock::new();
    INSTANCES.get_or_init(ActiveInstanceRegistry::new)
}

fn route_lifecycle_transition() -> &'static AsyncMutex<()> {
    static TRANSITION: OnceLock<AsyncMutex<()>> = OnceLock::new();
    TRANSITION.get_or_init(|| AsyncMutex::new(()))
}

async fn register_prepared_route_instance(
    tag: &str,
    instance_id: u64,
    namespace: RouteOwnershipNamespace,
    metrics: Arc<RosRouteMetrics>,
    manager_handle: Option<RouteManagerHandle>,
) -> Result<(Arc<WriterGate>, Arc<AtomicBool>)> {
    let _transition = route_lifecycle_transition().lock().await;
    register_metric_source(metrics.clone())?;
    // Candidate servers may accept requests while the runtime is being built.
    // Admit their observations into the bounded mailbox, but keep the manager
    // paused until the runtime manager commits the candidate.
    let writer_gate = WriterGate::new(true);
    let manager_active = Arc::new(AtomicBool::new(false));
    active_route_instances().push(
        tag,
        ActiveRouteInstance {
            instance_id,
            namespace,
            metrics,
            manager_handle: manager_handle.clone(),
            writer_gate: writer_gate.clone(),
            manager_active: manager_active.clone(),
        },
    );
    Ok((writer_gate, manager_active))
}

async fn commit_prepared_route_instance(tag: &str, instance_id: u64) {
    let _transition = route_lifecycle_transition().lock().await;
    let Some(instance) =
        active_route_instances().find(tag, |instance| instance.instance_id == instance_id)
    else {
        return;
    };
    if instance.manager_active.load(Ordering::Acquire) {
        return;
    }
    if active_route_instances()
        .find(tag, |other| {
            other.instance_id != instance_id
                && other.namespace == instance.namespace
                && other.manager_active.load(Ordering::Acquire)
        })
        .is_some()
    {
        warn!(plugin = %tag, "ros_route commit deferred because the previous manager is still active");
        return;
    }
    if let Some(handle) = &instance.manager_handle {
        match handle.activate(RoutePendingWork::default()).await {
            Ok(()) => instance.manager_active.store(true, Ordering::Release),
            Err(error) => {
                warn!(plugin = %tag, err = %error, "ros_route failed to commit prepared manager")
            }
        }
    }
}

/// Unregister one runtime and return whether its ownership namespace may be
/// cleaned up.
///
/// Candidate runtimes are initialized before the previous runtime is
/// destroyed. Tracking all active instances prevents the old runtime from
/// cleaning RouterOS state that a compatible replacement owns. A replacement
/// using a different RouterOS address, routing table, or comment prefix does
/// not suppress cleanup of the old namespace. The stack also restores the
/// previous metric source when candidate initialization later rolls back.
async fn release_active_route_instance(tag: &str, instance_id: u64) -> bool {
    release_active_route_instance_until(
        tag,
        instance_id,
        tokio::time::Instant::now() + SHUTDOWN_TIMEOUT,
    )
    .await
}

async fn release_active_route_instance_until(
    tag: &str,
    instance_id: u64,
    deadline: tokio::time::Instant,
) -> bool {
    let _transition = route_lifecycle_transition().lock().await;
    let Some((
        cleanup_allowed,
        metric_replacement,
        remove_metric,
        removed_handle,
        removed_writer_gate,
        removed_manager_active,
        transfer,
    )) = active_route_instances().release(
        tag,
        |instance| instance.instance_id == instance_id,
        |removed, instances, was_metric_owner| {
            removed.writer_gate.deactivate();
            let is_last = instances.is_empty();
            let removed_active = removed.manager_active.load(Ordering::Acquire);
            let cleanup_allowed = removed_active
                && !instances
                    .iter()
                    .any(|instance| instance.namespace == removed.namespace);
            let metric_replacement = was_metric_owner
                .then(|| instances.last().map(|instance| instance.metrics.clone()))
                .flatten();
            let transfer = instances
                .iter()
                .rev()
                .find(|instance| {
                    instance.namespace == removed.namespace
                        && instance.manager_active.load(Ordering::Acquire) != removed_active
                })
                .cloned();
            let remove_metric = was_metric_owner && is_last;
            (
                cleanup_allowed,
                metric_replacement,
                remove_metric,
                removed.manager_handle.clone(),
                removed.writer_gate.clone(),
                removed.manager_active.clone(),
                transfer,
            )
        },
    )
    else {
        return false;
    };

    let (pending, handoff_ready) = if transfer.is_some() {
        let handoff_deadline = deadline
            .checked_sub(Duration::from_secs(1))
            .unwrap_or(deadline);
        if tokio::time::timeout_at(handoff_deadline, removed_writer_gate.wait_idle())
            .await
            .is_err()
        {
            warn!(plugin = %tag, "ros_route writer drain exceeded shutdown deadline");
            (RoutePendingWork::default(), false)
        } else if let Some(handle) = removed_handle {
            match tokio::time::timeout_at(handoff_deadline, handle.quiesce()).await {
                Ok(pending) => (pending, true),
                Err(_) => {
                    warn!(plugin = %tag, "ros_route manager quiesce exceeded shutdown deadline");
                    (RoutePendingWork::default(), false)
                }
            }
        } else {
            (RoutePendingWork::default(), true)
        }
    } else {
        (RoutePendingWork::default(), false)
    };
    removed_manager_active.store(false, Ordering::Release);
    if handoff_ready
        && let Some(transfer) = transfer
        && let Some(handle) = &transfer.manager_handle
    {
        match tokio::time::timeout_at(deadline, handle.activate(pending)).await {
            Ok(Ok(())) => {
                transfer.manager_active.store(true, Ordering::Release);
                handle.request_reconcile();
            }
            Ok(Err(error)) => {
                warn!(plugin = %tag, err = %error, "ros_route failed to transfer manager ownership")
            }
            Err(_) => {
                warn!(plugin = %tag, "ros_route manager activation exceeded shutdown deadline")
            }
        }
    }
    if let Some(metrics) = metric_replacement {
        let _ = register_metric_source(metrics);
    } else if remove_metric {
        unregister_metric_source(tag);
    }
    cleanup_allowed
}

impl RosRouteMetrics {
    fn new(tag: String) -> Self {
        Self {
            tag,
            observe_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            sync_error_total: AtomicU64::new(0),
            sync_timeout_total: AtomicU64::new(0),
            delete_deferred_total: AtomicU64::new(0),
            connection_check_error_total: AtomicU64::new(0),
            pending_observations: AtomicU64::new(0),
            managed_entries: AtomicU64::new(0),
            coalesced_total: AtomicU64::new(0),
            reconnect_total: AtomicU64::new(0),
            connect_attempt_total: AtomicU64::new(0),
            backoff_total: AtomicU64::new(0),
            reconcile_error_total: AtomicU64::new(0),
            last_reconcile_success_timestamp_seconds: AtomicU64::new(0),
            degraded: AtomicU64::new(0),
            cleanup_error_total: AtomicU64::new(0),
        }
    }
}

impl MetricSource for RosRouteMetrics {
    fn tag(&self) -> &str {
        &self.tag
    }

    fn plugin_type(&self) -> &'static str {
        "ros_route"
    }

    fn collect(&self, sink: &mut dyn MetricSink) {
        let labels = [MetricLabel::new("plugin_tag", self.tag.as_str())];
        sink.emit(MetricSample::counter(
            "ros_route_observe_total",
            "Total address observations submitted to the RouterOS route manager.",
            &labels,
            self.observe_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_route_dropped_total",
            "Total route observations dropped because the manager queue was unavailable.",
            &labels,
            self.dropped_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_route_sync_error_total",
            "Total synchronous route observations that failed without changing DNS output.",
            &labels,
            self.sync_error_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_route_sync_timeout_total",
            "Total synchronous route observations that timed out without changing DNS output.",
            &labels,
            self.sync_timeout_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_route_delete_deferred_total",
            "Total route deletions deferred because a matching RouterOS connection exists.",
            &labels,
            self.delete_deferred_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_route_connection_check_error_total",
            "Total RouterOS connection-tracking queries that failed during route deletion.",
            &labels,
            self.connection_check_error_total.load(Ordering::Relaxed),
        ));
        for (name, help, value) in [
            (
                "ros_route_pending_observations",
                "Current coalesced route observations waiting for processing.",
                self.pending_observations.load(Ordering::Relaxed),
            ),
            (
                "ros_route_managed_entries",
                "Current route entries retained by the manager.",
                self.managed_entries.load(Ordering::Relaxed),
            ),
            (
                "ros_route_last_reconcile_success_timestamp_seconds",
                "Unix timestamp of the last successful route reconcile.",
                self.last_reconcile_success_timestamp_seconds
                    .load(Ordering::Relaxed),
            ),
            (
                "ros_route_degraded",
                "Whether the RouterOS transport is currently degraded.",
                self.degraded.load(Ordering::Relaxed),
            ),
        ] {
            sink.emit(MetricSample::gauge(name, help, &labels, value));
        }
        for (name, help, value) in [
            (
                "ros_route_coalesced_total",
                "Total route observations merged into an existing mailbox key.",
                self.coalesced_total.load(Ordering::Relaxed),
            ),
            (
                "ros_route_reconnect_total",
                "Total successful RouterOS transport reconnections.",
                self.reconnect_total.load(Ordering::Relaxed),
            ),
            (
                "ros_route_connect_attempt_total",
                "Total RouterOS transport connection attempts.",
                self.connect_attempt_total.load(Ordering::Relaxed),
            ),
            (
                "ros_route_backoff_total",
                "Total RouterOS transport backoff schedules.",
                self.backoff_total.load(Ordering::Relaxed),
            ),
            (
                "ros_route_reconcile_error_total",
                "Total failed route reconcile attempts.",
                self.reconcile_error_total.load(Ordering::Relaxed),
            ),
            (
                "ros_route_cleanup_error_total",
                "Total route entries that failed shutdown cleanup.",
                self.cleanup_error_total.load(Ordering::Relaxed),
            ),
        ] {
            sink.emit(MetricSample::counter(name, help, &labels, value));
        }
    }
}

#[async_trait]
impl Plugin for MikrotikExecutor {
    fn tag(&self) -> &str {
        &self.tag
    }

    async fn init(&mut self, _context: &crate::plugin::PluginInitContext<'_>) -> Result<()> {
        if self.manager.is_none() || self.manager_handle.is_some() {
            return Ok(());
        }

        let Some(manager) = self.manager.take() else {
            return Ok(());
        };

        let runtime = RouteManagerRuntime::start_paused(self.tag.clone(), manager);
        let manager_handle = runtime.handle();
        let (writer_gate, manager_active) = match register_prepared_route_instance(
            &self.tag,
            self.instance_id,
            RouteOwnershipNamespace::from_config(&self.config),
            self.metrics.clone(),
            Some(manager_handle.clone()),
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                let _ = runtime.shutdown(false).await;
                return Err(error);
            }
        };
        let mut runtime = Some(runtime);
        if let Ok(mut slot) = self.runtime.lock() {
            *slot = runtime.take();
        }
        if let Some(runtime) = runtime {
            release_active_route_instance(&self.tag, self.instance_id).await;
            let _ = runtime.shutdown(false).await;
            return Err(DnsError::plugin(
                "ros_route runtime lock is poisoned during initialization",
            ));
        }
        self.manager_handle = Some(manager_handle);
        self.writer_gate = writer_gate;
        self.manager_active = manager_active;
        self.active_registered.store(true, Ordering::Release);
        Ok(())
    }

    async fn commit(&self) {
        if self.active_registered.load(Ordering::Acquire) {
            commit_prepared_route_instance(&self.tag, self.instance_id).await;
        }
    }

    async fn destroy(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let is_last_instance = if self.active_registered.swap(false, Ordering::AcqRel) {
            release_active_route_instance_until(&self.tag, self.instance_id, deadline).await
        } else {
            false
        };
        if let Some(runtime) = self.runtime.lock().ok().and_then(|mut slot| slot.take()) {
            return runtime
                .shutdown_until(
                    self.config.cleanup_on_shutdown && is_last_instance,
                    deadline,
                )
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for MikrotikExecutor {
    fn with_next(&self) -> bool {
        true
    }

    async fn execute(&self, context: &mut DnsContext) -> Result<ExecStep> {
        self.execute_with_next(context, None).await
    }

    async fn execute_with_next(
        &self,
        context: &mut DnsContext,
        next: Option<ExecutorNext>,
    ) -> Result<ExecStep> {
        let writer_permit = self
            .active_registered
            .load(Ordering::Acquire)
            .then(|| self.writer_gate.enter())
            .flatten();
        let step = continue_next!(next, context)?;
        let Some(_writer_permit) = writer_permit else {
            return Ok(step);
        };
        let Some(handle) = self.manager_handle.as_ref() else {
            return Ok(step);
        };

        let Some(ExtractedObservation { addrs }) = extract_observation(context, &self.config)
        else {
            return Ok(step);
        };
        self.metrics.observe_total.fetch_add(1, Ordering::Relaxed);

        if self.config.async_mode {
            match handle.try_observe(addrs, None) {
                Ok(_) => {}
                Err(ObserveEnqueueError::Full) => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        plugin = %self.tag,
                        "ros_route observe queue is full, observation dropped"
                    );
                }
                Err(ObserveEnqueueError::Closed) => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        plugin = %self.tag,
                        "ros_route manager channel closed, observation dropped"
                    );
                }
            }
            return Ok(step);
        }

        let (wait_tx, wait_rx) = oneshot::channel::<Result<()>>();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(SYNC_OBSERVE_TIMEOUT_SECS);
        let send_outcome = tokio::time::timeout_at(deadline, handle.observe(addrs, wait_tx)).await;
        match send_outcome {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {
                self.metrics
                    .sync_error_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    "ros_route manager channel closed in sync mode, DNS response is kept unchanged"
                );
                return Ok(step);
            }
            Err(_) => {
                self.metrics
                    .sync_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    timeout_secs = SYNC_OBSERVE_TIMEOUT_SECS,
                    "ros_route observe enqueue timed out in sync mode, DNS response is kept unchanged"
                );
                return Ok(step);
            }
        }

        let wait_outcome = tokio::time::timeout_at(deadline, wait_rx).await;
        match wait_outcome {
            Ok(Ok(Ok(()))) => Ok(step),
            Ok(Ok(Err(e))) => {
                self.metrics
                    .sync_error_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    err = %e,
                    "ros_route observe failed in sync mode, DNS response is kept unchanged"
                );
                Ok(step)
            }
            Ok(Err(_)) => {
                self.metrics
                    .sync_error_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    "ros_route manager dropped sync observe response, DNS response is kept unchanged"
                );
                Ok(step)
            }
            Err(_) => {
                self.metrics
                    .sync_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    timeout_secs = SYNC_OBSERVE_TIMEOUT_SECS,
                    "ros_route observe timed out in sync mode, DNS response is kept unchanged"
                );
                Ok(step)
            }
        }
    }
}

#[derive(Debug, Clone)]
#[plugin_factory("ros_route")]
pub struct MikrotikFactory;

impl PluginFactory for MikrotikFactory {
    fn create(
        &self,
        plugin_config: &PluginConfig,
        _init_context: &crate::plugin::PluginInitContext<'_>,
    ) -> Result<UninitializedPlugin> {
        validate_comment_token("plugin tag", plugin_config.tag.as_str())?;
        let mut config = parse_plugin_config(plugin_config.args.clone(), true)?;
        let connection = config
            .connection
            .take()
            .ok_or_else(|| DnsError::plugin("ros_route connection config already consumed"))?;
        let api = Arc::new(MikrotikRsClient::new(connection)) as Arc<dyn MikrotikApi>;

        let manager_cfg = RouteManagerConfig {
            plugin_tag: plugin_config.tag.clone(),
            routing_table: config.routing_table.clone(),
            gateway4: config.gateway4.clone(),
            gateway6: config.gateway6.clone(),
            persistent_ips: config
                .persistent_ips
                .iter()
                .map(|raw| raw.parse::<IpPrefix>())
                .collect::<std::result::Result<AHashSet<_>, _>>()?,
            comment_prefix: config.comment_prefix.clone(),
            distance: config.distance,
            min_ttl: config.min_ttl,
            max_ttl: config.max_ttl,
            fixed_ttl: config.fixed_ttl,
            conntrack_guard: config.conntrack_guard,
        };
        let metrics = Arc::new(RosRouteMetrics::new(plugin_config.tag.clone()));
        let manager = RouteManager::with_metrics(api, manager_cfg, metrics.clone());

        Ok(UninitializedPlugin::Executor(Box::new(MikrotikExecutor {
            tag: plugin_config.tag.clone(),
            instance_id: NEXT_ROUTE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            active_registered: AtomicBool::new(false),
            writer_gate: WriterGate::new(false),
            manager_active: Arc::new(AtomicBool::new(false)),
            metrics,
            config,
            manager: Some(manager),
            manager_handle: None,
            runtime: Mutex::new(None),
        })))
    }
}

fn extract_observation(
    context: &mut DnsContext,
    config: &MikrotikConfig,
) -> Option<ExtractedObservation> {
    let question = context.request.first_question()?;
    match question.qtype() {
        RecordType::A | RecordType::AAAA => {}
        _ => return None,
    }

    let response = context.response()?;
    if response.rcode() != Rcode::NoError {
        return None;
    }
    let addrs = collect_observed_addrs(&context.request, response, |ip| match ip {
        IpAddr::V4(_) => config.gateway4.is_some(),
        IpAddr::V6(_) => config.gateway6.is_some(),
    });
    (!addrs.is_empty()).then_some(ExtractedObservation { addrs })
}

fn parse_plugin_config(args: Option<Value>, emit_warnings: bool) -> Result<MikrotikConfig> {
    let Some(args) = args else {
        return Err(DnsError::plugin("ros_route plugin requires args"));
    };
    let raw = serde_yaml_ng::from_value::<MikrotikConfigArgs>(args)
        .map_err(|e| DnsError::plugin(format!("failed to parse ros_route config: {e}")))?;
    raw.into_config(emit_warnings)
}

/// Require non-empty string config fields and keep trimmed value.
fn required_non_empty(value: Option<String>, field: &str) -> Result<String> {
    let Some(value) = value else {
        return Err(DnsError::plugin(format!("ros_route '{field}' is required")));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DnsError::plugin(format!(
            "ros_route '{field}' cannot be empty"
        )));
    }
    Ok(trimmed.to_string())
}

/// Convert optional string to trimmed non-empty value.
fn optional_non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[inline]
fn contains_comment_delimiter(value: &str) -> bool {
    value.contains(';') || value.contains('=')
}

fn validate_comment_token(field: &str, value: &str) -> Result<()> {
    if contains_comment_delimiter(value) {
        return Err(DnsError::plugin(format!(
            "ros_route '{field}' cannot contain ';' or '='"
        )));
    }
    Ok(())
}

fn timeout_secs(value: Option<u64>, field: &str, default_secs: u64) -> Result<u64> {
    match value {
        Some(0) => Err(DnsError::plugin(format!(
            "ros_route '{field}' must be greater than 0 seconds"
        ))),
        Some(value) => Ok(value),
        None => Ok(default_secs),
    }
}

#[derive(Debug, Default)]
struct ParsedPersistentRoutes {
    all_ips: AHashSet<String>,
    inline_ips: AHashSet<String>,
    files: Vec<String>,
    ignored_by_gateway: usize,
    ignored_default_route: usize,
}

/// Parse always-present route list from inline args and optional files.
///
/// Accepted item formats:
/// - `1.1.1.1`
/// - `2001:db8::1`
/// - generic CIDR: `1.1.1.0/24`, `2001:db8::/64`
///
/// Entries whose IP family has no corresponding configured gateway are skipped.
fn parse_persistent_ips(
    persistent: Option<PersistentArgs>,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<ParsedPersistentRoutes> {
    let mut parsed = ParsedPersistentRoutes::default();
    let Some(route) = persistent else {
        return Ok(parsed);
    };

    if let Some(ips) = route.ips {
        for (index, item) in ips.into_iter().enumerate() {
            let source = format!("persistent.ips[{index}]");
            let cidr = parse_persistent_ip_item(item.as_str(), source.as_str())?;
            if is_default_route_cidr(cidr.as_str()) {
                parsed.ignored_default_route = parsed.ignored_default_route.saturating_add(1);
                continue;
            }
            if !is_persistent_ip_family_enabled(
                cidr.as_str(),
                gateway4_enabled,
                gateway6_enabled,
                source.as_str(),
            )? {
                parsed.ignored_by_gateway = parsed.ignored_by_gateway.saturating_add(1);
                continue;
            }
            parsed.inline_ips.insert(cidr.clone());
            parsed.all_ips.insert(cidr);
        }
    }

    parsed.files = parse_persistent_files(route.files)?;
    let (file_ips, ignored_from_files, ignored_default_from_files) =
        load_persistent_ips_from_files(
            parsed.files.as_slice(),
            gateway4_enabled,
            gateway6_enabled,
        )?;
    parsed.ignored_by_gateway = parsed.ignored_by_gateway.saturating_add(ignored_from_files);
    parsed.ignored_default_route = parsed
        .ignored_default_route
        .saturating_add(ignored_default_from_files);
    parsed.all_ips.extend(file_ips);

    Ok(parsed)
}

fn parse_persistent_files(files: Option<Vec<String>>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let Some(files) = files else {
        return Ok(out);
    };
    for (index, file_raw) in files.into_iter().enumerate() {
        let file = file_raw.trim();
        if file.is_empty() {
            return Err(DnsError::plugin(format!(
                "ros_route persistent.files[{index}] cannot be empty"
            )));
        }
        out.push(file.to_string());
    }
    Ok(out)
}

fn load_persistent_ips_from_content(
    source_prefix: &str,
    content: &str,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<(AHashSet<String>, usize, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_gateway = 0usize;
    let mut ignored_default_route = 0usize;

    for (line_no, line) in content.lines().enumerate() {
        let token = line.split('#').next().unwrap_or_default().trim();
        if token.is_empty() {
            continue;
        }

        let source = format!("{source_prefix} line {}", line_no + 1);
        let cidr = parse_persistent_ip_item(token, source.as_str())?;
        if is_default_route_cidr(cidr.as_str()) {
            ignored_default_route = ignored_default_route.saturating_add(1);
            continue;
        }
        if !is_persistent_ip_family_enabled(
            cidr.as_str(),
            gateway4_enabled,
            gateway6_enabled,
            source.as_str(),
        )? {
            ignored_by_gateway = ignored_by_gateway.saturating_add(1);
            continue;
        }
        out.insert(cidr);
    }

    Ok((out, ignored_by_gateway, ignored_default_route))
}

pub(super) fn load_persistent_ips_from_files(
    files: &[String],
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<(AHashSet<String>, usize, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_gateway = 0usize;
    let mut ignored_default_route = 0usize;

    for (index, file) in files.iter().enumerate() {
        let content = fs::read_to_string(file).map_err(|e| {
            DnsError::plugin(format!(
                "ros_route failed to read persistent route file '{file}': {e}"
            ))
        })?;
        let source_prefix = format!("persistent.files[{index}]");
        let (loaded, ignored_by_gateway_delta, ignored_default_delta) =
            load_persistent_ips_from_content(
                source_prefix.as_str(),
                &content,
                gateway4_enabled,
                gateway6_enabled,
            )?;
        out.extend(loaded);
        ignored_by_gateway = ignored_by_gateway.saturating_add(ignored_by_gateway_delta);
        ignored_default_route = ignored_default_route.saturating_add(ignored_default_delta);
    }

    Ok((out, ignored_by_gateway, ignored_default_route))
}

/// Parse one persistent item and normalize into `ip/prefix`.
///
/// Rules:
/// - plain IPv4/IPv6 becomes `/32` or `/128`
/// - CIDR keeps its configured prefix and is normalized to network address
fn parse_persistent_ip_item(raw: &str, source: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(DnsError::plugin(format!("ros_route {source} is empty")));
    }

    if let Some((ip_raw, prefix_raw)) = value.split_once('/') {
        let ip = ip_raw.trim().parse::<IpAddr>().map_err(|e| {
            DnsError::plugin(format!("ros_route {source} has invalid ip '{ip_raw}': {e}"))
        })?;
        let prefix = prefix_raw.trim().parse::<u8>().map_err(|e| {
            DnsError::plugin(format!(
                "ros_route {source} has invalid prefix '{prefix_raw}': {e}"
            ))
        })?;
        let max_prefix = if ip.is_ipv4() { 32 } else { 128 };
        if prefix > max_prefix {
            return Err(DnsError::plugin(format!(
                "ros_route {source} has invalid prefix /{prefix} for {ip}, max /{max_prefix}"
            )));
        }
        let network_ip = normalize_network_ip(ip, prefix);
        return Ok(format!("{network_ip}/{prefix}"));
    }

    let ip = value.parse::<IpAddr>().map_err(|e| {
        DnsError::plugin(format!("ros_route {source} has invalid ip '{value}': {e}"))
    })?;
    let prefix = if ip.is_ipv4() { 32 } else { 128 };
    Ok(format!("{ip}/{prefix}"))
}

fn normalize_network_ip(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(raw & mask))
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(raw & mask))
        }
    }
}

#[inline]
fn is_default_route_cidr(cidr: &str) -> bool {
    cidr == "0.0.0.0/0" || cidr == "::/0"
}

/// Check whether this persistent route's family is enabled by gateway config.
///
/// Returns `Ok(false)` when family gateway is not configured so caller can skip
/// the item without failing plugin startup.
fn is_persistent_ip_family_enabled(
    cidr: &str,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
    source: &str,
) -> Result<bool> {
    let (ip_raw, _) = cidr.split_once('/').ok_or_else(|| {
        DnsError::plugin(format!(
            "ros_route {source} has invalid normalized route '{cidr}'"
        ))
    })?;
    let ip = ip_raw.parse::<IpAddr>().map_err(|e| {
        DnsError::plugin(format!(
            "ros_route {source} has invalid normalized route '{cidr}': {e}"
        ))
    })?;

    match ip {
        IpAddr::V4(_) if !gateway4_enabled => Ok(false),
        IpAddr::V6(_) if !gateway6_enabled => Ok(false),
        _ => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::proto::rdata::{A, AAAA, CNAME, SOA};
    use crate::proto::{DNSClass, Message, Name, Question, RData, Record};

    fn observation_config() -> MikrotikConfig {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
gateway6: "2001:db8::1"
"#,
        )
        .expect("yaml");
        parse_plugin_config(Some(args), false).expect("config")
    }

    fn context_with_rcode(qtype: RecordType, rcode: Rcode) -> DnsContext {
        let mut request = Message::new();
        request.add_question(Question::new(
            Name::from_ascii("example.com.").expect("domain"),
            qtype,
            DNSClass::IN,
        ));
        let response = request.response(rcode);
        let mut context = DnsContext::new(
            "127.0.0.1:5353".parse::<SocketAddr>().expect("client"),
            request,
        );
        context.set_response(response);
        context
    }

    fn context_with_nodata(qtype: RecordType) -> DnsContext {
        context_with_rcode(qtype, Rcode::NoError)
    }

    #[test]
    fn fixed_ttl_zero_is_accepted() {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
fixed_ttl: 0
"#,
        )
        .expect("yaml");
        let parsed = parse_plugin_config(Some(args), false).expect("config");
        assert_eq!(parsed.fixed_ttl, Some(0));
    }

    #[test]
    fn config_rejects_old_persistent_route_key() {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
persistent_route:
  ips:
    - "192.0.2.10"
"#,
        )
        .expect("yaml");
        let error = parse_plugin_config(Some(args), false).expect_err("old key");
        assert!(error.to_string().contains("persistent_route"));
    }

    #[test]
    fn config_keeps_plaintext_when_tls_is_omitted() {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "router.example:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
"#,
        )
        .expect("yaml");
        let parsed = parse_plugin_config(Some(args), false).expect("config");
        let debug = format!("{:?}", parsed.connection.expect("connection"));
        assert!(debug.contains("tls: None"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn conntrack_guard_defaults_to_disabled_and_can_be_enabled() {
        let base = r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
"#;
        let default_args = serde_yaml_ng::from_str::<Value>(base).expect("yaml");
        assert!(
            !parse_plugin_config(Some(default_args), false)
                .expect("default config")
                .conntrack_guard
        );

        let enabled_args =
            serde_yaml_ng::from_str::<Value>(&format!("{base}conntrack_guard: true\n"))
                .expect("yaml");
        assert!(
            parse_plugin_config(Some(enabled_args), false)
                .expect("enabled config")
                .conntrack_guard
        );
    }

    #[test]
    fn config_requires_a_gateway() {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
"#,
        )
        .expect("yaml");
        assert!(parse_plugin_config(Some(args), false).is_err());
    }

    #[test]
    fn config_defaults_comment_prefix_to_oxi() {
        let args = serde_yaml_ng::from_str::<Value>(
            r#"
address: "127.0.0.1:8728"
username: "api"
password: "secret"
routing_table: "policy"
gateway4: "192.0.2.1"
"#,
        )
        .expect("yaml");
        let parsed = parse_plugin_config(Some(args), false).expect("route config");
        assert_eq!(parsed.comment_prefix, "oxi");
    }

    #[test]
    fn observation_ignores_non_address_queries_and_nodata() {
        let config = observation_config();
        let mut txt_context = context_with_nodata(RecordType::TXT);
        assert!(extract_observation(&mut txt_context, &config).is_none());
        let mut any_context = context_with_nodata(RecordType::ANY);
        assert!(extract_observation(&mut any_context, &config).is_none());

        let mut a_context = context_with_nodata(RecordType::A);
        assert!(extract_observation(&mut a_context, &config).is_none());
    }

    #[test]
    fn nodata_for_disabled_query_family_is_ignored() {
        let mut config = observation_config();
        config.gateway4 = None;
        let mut context = context_with_nodata(RecordType::A);

        assert!(extract_observation(&mut context, &config).is_none());
    }

    #[test]
    fn observation_collects_all_answer_addresses_without_cname_ttl_cap() {
        let config = observation_config();
        let mut context = context_with_rcode(RecordType::A, Rcode::NoError);
        let response = context.response_mut().expect("response");
        response.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").expect("owner"),
            30,
            RData::CNAME(CNAME(
                Name::from_ascii("edge.example.com.").expect("target"),
            )),
        ));
        response.add_answer(Record::from_rdata(
            Name::from_ascii("edge.example.com.").expect("owner"),
            300,
            RData::A(A(Ipv4Addr::new(203, 0, 113, 27))),
        ));
        response.add_answer(Record::from_rdata(
            Name::from_ascii("unrelated.example.com.").expect("owner"),
            600,
            RData::A(A(Ipv4Addr::new(203, 0, 113, 28))),
        ));
        response.add_answer(Record::from_rdata(
            Name::from_ascii("unrelated.example.com.").expect("owner"),
            120,
            RData::AAAA(AAAA(Ipv6Addr::new(0x2001, 0xDB8, 0, 0, 0, 0, 0, 27))),
        ));

        let observation = extract_observation(&mut context, &config).expect("CNAME observation");

        assert_eq!(observation.addrs.len(), 3);
        assert!(observation.addrs.contains(&ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 27)),
            ttl_secs: 300,
        }));
        assert!(observation.addrs.contains(&ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 28)),
            ttl_secs: 600,
        }));
        assert!(observation.addrs.contains(&ObservedAddr {
            addr: IpAddr::V6(Ipv6Addr::new(0x2001, 0xDB8, 0, 0, 0, 0, 0, 27)),
            ttl_secs: 120,
        }));
    }

    #[test]
    fn nxdomain_does_not_withdraw_existing_leases() {
        let config = observation_config();
        let mut context = context_with_rcode(RecordType::AAAA, Rcode::NXDomain);

        assert!(extract_observation(&mut context, &config).is_none());
    }

    #[test]
    fn nxdomain_with_mismatched_question_is_ignored() {
        let config = observation_config();
        let mut context = context_with_rcode(RecordType::A, Rcode::NXDomain);
        let response = context.response_mut().expect("response");
        response.questions_mut().clear();
        response.add_question(Question::new(
            Name::from_ascii("other.example.").expect("other domain"),
            RecordType::A,
            DNSClass::IN,
        ));

        assert!(extract_observation(&mut context, &config).is_none());
    }

    #[test]
    fn negative_soa_ttl_does_not_create_a_withdrawal_observation() {
        let config = observation_config();
        let mut context = context_with_rcode(RecordType::A, Rcode::NXDomain);
        context
            .response_mut()
            .expect("response")
            .add_authority(Record::from_rdata(
                Name::from_ascii("example.com.").expect("zone"),
                120,
                RData::SOA(SOA::new(
                    Name::from_ascii("ns.example.com.").expect("mname"),
                    Name::from_ascii("hostmaster.example.com.").expect("rname"),
                    1,
                    3600,
                    600,
                    86400,
                    30,
                )),
            ));

        assert!(extract_observation(&mut context, &config).is_none());
    }

    #[tokio::test]
    async fn same_tag_runtime_coordinates_cleanup_by_ownership_namespace() {
        let sequence = NEXT_ROUTE_INSTANCE_ID.fetch_add(6, Ordering::Relaxed);
        let namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "policy".to_string(),
            comment_prefix: "fdns".to_string(),
        };
        let success_tag = format!("route-reload-success-{sequence}");
        let old_metrics = Arc::new(RosRouteMetrics::new(success_tag.clone()));
        let new_metrics = Arc::new(RosRouteMetrics::new(success_tag.clone()));
        let (_, old_active) = register_prepared_route_instance(
            &success_tag,
            sequence,
            namespace.clone(),
            old_metrics,
            None,
        )
        .await
        .expect("old runtime");
        old_active.store(true, Ordering::Release);
        let (_, replacement_active) = register_prepared_route_instance(
            &success_tag,
            sequence + 1,
            namespace.clone(),
            new_metrics,
            None,
        )
        .await
        .expect("replacement runtime");
        assert!(!release_active_route_instance(&success_tag, sequence).await);
        replacement_active.store(true, Ordering::Release);
        assert!(release_active_route_instance(&success_tag, sequence + 1).await);

        let rollback_tag = format!("route-reload-rollback-{sequence}");
        let old_metrics = Arc::new(RosRouteMetrics::new(rollback_tag.clone()));
        let candidate_metrics = Arc::new(RosRouteMetrics::new(rollback_tag.clone()));
        let (_, old_active) = register_prepared_route_instance(
            &rollback_tag,
            sequence + 2,
            namespace.clone(),
            old_metrics,
            None,
        )
        .await
        .expect("old runtime");
        old_active.store(true, Ordering::Release);
        register_prepared_route_instance(
            &rollback_tag,
            sequence + 3,
            namespace,
            candidate_metrics,
            None,
        )
        .await
        .expect("candidate runtime");
        assert!(!release_active_route_instance(&rollback_tag, sequence + 3).await);
        assert!(release_active_route_instance(&rollback_tag, sequence + 2).await);

        let migration_tag = format!("route-reload-migration-{sequence}");
        let old_namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "old-policy".to_string(),
            comment_prefix: "old-fdns".to_string(),
        };
        let new_namespace = RouteOwnershipNamespace {
            address: "192.0.2.11:8728".to_string(),
            routing_table: "new-policy".to_string(),
            comment_prefix: "new-fdns".to_string(),
        };
        let (_, old_active) = register_prepared_route_instance(
            &migration_tag,
            sequence + 4,
            old_namespace,
            Arc::new(RosRouteMetrics::new(migration_tag.clone())),
            None,
        )
        .await
        .expect("old namespace");
        old_active.store(true, Ordering::Release);
        let (_, new_active) = register_prepared_route_instance(
            &migration_tag,
            sequence + 5,
            new_namespace,
            Arc::new(RosRouteMetrics::new(migration_tag.clone())),
            None,
        )
        .await
        .expect("new namespace");
        assert!(release_active_route_instance(&migration_tag, sequence + 4).await);
        new_active.store(true, Ordering::Release);
        assert!(release_active_route_instance(&migration_tag, sequence + 5).await);
    }

    #[tokio::test]
    async fn failed_compatible_reload_requests_immediate_restore_reconcile() {
        let sequence = NEXT_ROUTE_INSTANCE_ID.fetch_add(2, Ordering::Relaxed);
        let tag = format!("route-reload-restore-{sequence}");
        let namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "policy".to_string(),
            comment_prefix: "fdns".to_string(),
        };
        let old_handle = RouteManagerHandle::new_for_test();

        let (_, old_active) = register_prepared_route_instance(
            &tag,
            sequence,
            namespace.clone(),
            Arc::new(RosRouteMetrics::new(tag.clone())),
            Some(old_handle.clone()),
        )
        .await
        .expect("old runtime");
        old_active.store(true, Ordering::Release);
        register_prepared_route_instance(
            &tag,
            sequence + 1,
            namespace,
            Arc::new(RosRouteMetrics::new(tag.clone())),
            None,
        )
        .await
        .expect("candidate runtime");

        assert!(!release_active_route_instance(&tag, sequence + 1).await);
        assert!(old_handle.take_reconcile_for_test());
        assert!(release_active_route_instance(&tag, sequence).await);
    }

    #[tokio::test]
    async fn compatible_release_bounds_writer_drain_by_shutdown_deadline() {
        let sequence = NEXT_ROUTE_INSTANCE_ID.fetch_add(2, Ordering::Relaxed);
        let tag = format!("route-release-deadline-{sequence}");
        let namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "policy".to_string(),
            comment_prefix: "fdns".to_string(),
        };
        let (old_gate, old_active) = register_prepared_route_instance(
            &tag,
            sequence,
            namespace.clone(),
            Arc::new(RosRouteMetrics::new(tag.clone())),
            None,
        )
        .await
        .expect("old runtime");
        old_active.store(true, Ordering::Release);
        let permit = old_gate.enter().expect("in-flight writer");
        let (_, replacement_active) = register_prepared_route_instance(
            &tag,
            sequence + 1,
            namespace,
            Arc::new(RosRouteMetrics::new(tag.clone())),
            None,
        )
        .await
        .expect("replacement runtime");

        let released = tokio::time::timeout(
            Duration::from_millis(200),
            release_active_route_instance_until(
                &tag,
                sequence,
                tokio::time::Instant::now() + Duration::from_millis(20),
            ),
        )
        .await
        .expect("release must respect deadline");
        assert!(!released);

        drop(permit);
        replacement_active.store(true, Ordering::Release);
        assert!(release_active_route_instance(&tag, sequence + 1).await);
    }
}
