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
//! - support optional always-present CIDR routes via `persistent_route`.
//! - periodically reload persistent route files and keep route table in sync.
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

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use serde::Deserialize;
use serde_yaml_ng::Value;
use tokio::fs as tokio_fs;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::config::types::PluginConfig;
use crate::core::context::DnsContext;
use crate::core::response::{NegativeResponseKind, ResponseDisposition, classify_response};
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
const DEFAULT_COMMENT_PREFIX: &str = "fdns";
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
    /// Prefix used in RouterOS route comments to mark ForgeDNS-managed routes.
    /// Defaults to `fdns` when omitted.
    comment_prefix: Option<String>,
    /// Route distance written to RouterOS for managed routes.
    distance: Option<u8>,
    /// Always-present routes that should not expire with DNS TTL.
    persistent_route: Option<PersistentRouteArgs>,
    /// Minimum effective TTL clamp (seconds) for observed records.
    min_ttl: Option<u32>,
    /// Maximum effective TTL clamp (seconds) for observed records.
    max_ttl: Option<u32>,
    /// Optional fixed TTL override (seconds) for dynamic observed records.
    /// `0` keeps a dynamic route until a later observation withdraws it.
    fixed_ttl: Option<u32>,
    /// Whether to clean managed dynamic routes on shutdown.
    cleanup_on_shutdown: Option<bool>,
    /// Delay normal route removal while RouterOS connection tracking has a
    /// connection for the route destination.
    conntrack_guard: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PersistentRouteArgs {
    /// Inline always-present IPs/CIDRs. Plain IP is normalized to host route.
    ips: Option<Vec<String>>,
    /// File list that provides always-present IPs.
    files: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct MikrotikConfig {
    /// RouterOS API endpoint.
    address: String,
    /// RouterOS login username.
    username: String,
    /// RouterOS login password.
    password: String,
    /// RouterOS API operation timeouts.
    api_timeouts: MikrotikApiTimeouts,
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
    /// Inline persistent routes in normalized CIDR format.
    persistent_inline_ips: AHashSet<String>,
    /// Persistent route source files for periodic reload.
    persistent_files: Vec<String>,
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
    domain: String,
    /// Address family that this response is allowed to replace or withdraw.
    replace_scope: ObservationScope,
    addrs: Vec<ObservedAddr>,
    /// RFC 2308 lifetime for a negative response. Used only to bound replay
    /// while RouterOS initialization is unavailable.
    negative_ttl_secs: Option<u32>,
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
        let parsed_persistent = parse_persistent_ips(
            self.persistent_route,
            gateway4.is_some(),
            gateway6.is_some(),
        )?;
        let ignored_by_gateway = parsed_persistent.ignored_by_gateway;
        if emit_warnings && ignored_by_gateway > 0 {
            warn!(
                ignored = ignored_by_gateway,
                "ros_route persistent_route ignored entries without corresponding gateway family"
            );
        }
        let ignored_default_route = parsed_persistent.ignored_default_route;
        if emit_warnings && ignored_default_route > 0 {
            warn!(
                ignored = ignored_default_route,
                "ros_route persistent_route ignored default-route entries (/0)"
            );
        }

        Ok(MikrotikConfig {
            address,
            username,
            password,
            api_timeouts,
            async_mode: self.async_mode.unwrap_or(DEFAULT_ASYNC_MODE),
            routing_table,
            gateway4,
            gateway6,
            persistent_ips: parsed_persistent.all_ips,
            persistent_inline_ips: parsed_persistent.inline_ips,
            persistent_files: parsed_persistent.files,
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
    ManagerCommand, ObservationScope, PersistentReloadConfig, RouteManager, RouteManagerConfig,
    RouteManagerRuntime,
};
use crate::plugin::executor::ros_common::{ObservedAddr, collect_answer_addrs};

#[derive(Debug)]
struct MikrotikExecutor {
    tag: String,
    instance_id: u64,
    active_registered: AtomicBool,
    metrics: Arc<RosRouteMetrics>,
    config: MikrotikConfig,
    manager: Option<RouteManager>,
    command_tx: Option<mpsc::Sender<ManagerCommand>>,
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
}

#[derive(Debug)]
struct ActiveRouteInstance {
    instance_id: u64,
    namespace: RouteOwnershipNamespace,
    metrics: Arc<RosRouteMetrics>,
    /// Used to request that the prior compatible runtime immediately restore
    /// its desired RouterOS state when a replacement candidate rolls back.
    command_tx: Option<mpsc::Sender<ManagerCommand>>,
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

fn active_route_instances() -> &'static Mutex<AHashMap<String, Vec<ActiveRouteInstance>>> {
    static INSTANCES: OnceLock<Mutex<AHashMap<String, Vec<ActiveRouteInstance>>>> = OnceLock::new();
    INSTANCES.get_or_init(|| Mutex::new(AHashMap::new()))
}

fn register_active_route_instance(
    tag: &str,
    instance_id: u64,
    namespace: RouteOwnershipNamespace,
    metrics: Arc<RosRouteMetrics>,
    command_tx: Option<mpsc::Sender<ManagerCommand>>,
) -> Result<()> {
    register_metric_source(metrics.clone())?;
    let mut active = active_route_instances()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    active
        .entry(tag.to_string())
        .or_default()
        .push(ActiveRouteInstance {
            instance_id,
            namespace,
            metrics,
            command_tx,
        });
    Ok(())
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
fn release_active_route_instance(tag: &str, instance_id: u64) -> bool {
    let (cleanup_allowed, metric_replacement, remove_metric, restore_tx) = {
        let mut active = active_route_instances()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(instances) = active.get_mut(tag) else {
            return false;
        };
        let Some(index) = instances
            .iter()
            .position(|instance| instance.instance_id == instance_id)
        else {
            return false;
        };
        let was_metric_owner = index + 1 == instances.len();
        let removed = instances.remove(index);
        let is_last = instances.is_empty();
        let cleanup_allowed = !instances
            .iter()
            .any(|instance| instance.namespace == removed.namespace);
        let metric_replacement = was_metric_owner
            .then(|| instances.last().map(|instance| instance.metrics.clone()))
            .flatten();
        // A candidate is appended after the running instance. If that newest
        // candidate is destroyed during a failed reload, it may already have
        // reconciled changed gateway/distance metadata. Ask the compatible
        // surviving runtime to restore its desired state immediately instead
        // of waiting for the periodic 180-second reconcile.
        let restore_tx = was_metric_owner
            .then(|| {
                instances
                    .iter()
                    .rev()
                    .find(|instance| instance.namespace == removed.namespace)
                    .and_then(|instance| instance.command_tx.clone())
            })
            .flatten();
        let remove_metric = was_metric_owner && is_last;
        if is_last {
            active.remove(tag);
        }
        (
            cleanup_allowed,
            metric_replacement,
            remove_metric,
            restore_tx,
        )
    };

    if let Some(metrics) = metric_replacement {
        let _ = register_metric_source(metrics);
    } else if remove_metric {
        unregister_metric_source(tag);
    }
    if let Some(tx) = restore_tx
        && tx.try_send(ManagerCommand::Reconcile).is_err()
    {
        warn!(
            plugin = %tag,
            "ros_route failed to enqueue immediate reconcile after reload rollback"
        );
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
            "Total domain observations submitted to the RouterOS route manager.",
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
    }
}

#[async_trait]
impl Plugin for MikrotikExecutor {
    fn tag(&self) -> &str {
        &self.tag
    }

    async fn init(&mut self, _context: &crate::plugin::PluginInitContext<'_>) -> Result<()> {
        if self.manager.is_none() || self.command_tx.is_some() {
            return Ok(());
        }

        let Some(manager) = self.manager.take() else {
            return Ok(());
        };

        let persistent_reload = Some(PersistentReloadConfig {
            inline_ips: self.config.persistent_inline_ips.clone(),
            files: self.config.persistent_files.clone(),
            initial_ips: self.config.persistent_ips.clone(),
            gateway4_enabled: self.config.gateway4.is_some(),
            gateway6_enabled: self.config.gateway6.is_some(),
        });
        let runtime = RouteManagerRuntime::start(self.tag.clone(), manager, persistent_reload);
        let command_tx = runtime.sender();
        self.command_tx = Some(command_tx.clone());
        if let Ok(mut slot) = self.runtime.lock() {
            *slot = Some(runtime);
        }
        register_active_route_instance(
            &self.tag,
            self.instance_id,
            RouteOwnershipNamespace::from_config(&self.config),
            self.metrics.clone(),
            Some(command_tx),
        )?;
        self.active_registered.store(true, Ordering::Release);
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        let is_last_instance = self.active_registered.swap(false, Ordering::AcqRel)
            && release_active_route_instance(&self.tag, self.instance_id);
        if let Some(runtime) = self.runtime.lock().ok().and_then(|mut slot| slot.take()) {
            runtime
                .shutdown(self.config.cleanup_on_shutdown && is_last_instance)
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
        let step = continue_next!(next, context)?;
        if !self.active_registered.load(Ordering::Acquire) {
            return Ok(step);
        }
        let Some(tx) = self.command_tx.as_ref() else {
            return Ok(step);
        };

        let Some(ExtractedObservation {
            domain,
            replace_scope,
            addrs,
            negative_ttl_secs,
        }) = extract_observation(context, &self.config)
        else {
            return Ok(step);
        };
        self.metrics.observe_total.fetch_add(1, Ordering::Relaxed);

        if self.config.async_mode {
            match tx.try_send(ManagerCommand::ObserveDomain {
                domain,
                replace_scope,
                addrs,
                negative_ttl_secs,
                wait: None,
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        plugin = %self.tag,
                        "ros_route observe queue is full, observation dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
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
        let send_cmd = ManagerCommand::ObserveDomain {
            domain,
            replace_scope,
            addrs,
            negative_ttl_secs,
            wait: Some(wait_tx),
        };
        let send_outcome = tokio::time::timeout(
            Duration::from_secs(SYNC_OBSERVE_TIMEOUT_SECS),
            tx.send(send_cmd),
        )
        .await;
        match send_outcome {
            Ok(Ok(())) => {}
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

        let wait_outcome =
            tokio::time::timeout(Duration::from_secs(SYNC_OBSERVE_TIMEOUT_SECS), wait_rx).await;
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
        let config = parse_plugin_config(plugin_config.args.clone(), true)?;
        let api = Arc::new(MikrotikRsClient::new(
            config.address.clone(),
            config.username.clone(),
            config.password.clone(),
            config.api_timeouts,
        )) as Arc<dyn MikrotikApi>;

        let manager_cfg = RouteManagerConfig {
            plugin_tag: plugin_config.tag.clone(),
            routing_table: config.routing_table.clone(),
            gateway4: config.gateway4.clone(),
            gateway6: config.gateway6.clone(),
            persistent_ips: config.persistent_ips.clone(),
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
            metrics,
            config,
            manager: Some(manager),
            command_tx: None,
            runtime: Mutex::new(None),
        })))
    }
}

fn extract_observation(
    context: &mut DnsContext,
    config: &MikrotikConfig,
) -> Option<ExtractedObservation> {
    let question = context.request.first_question()?;
    let domain = question.name().normalized().to_string();
    let replace_scope = match question.qtype() {
        RecordType::A => ObservationScope::Ipv4,
        RecordType::AAAA => ObservationScope::Ipv6,
        _ => return None,
    };

    let response = context.response()?;
    // A response for another request must never update this request's route
    // bindings. Keep this cheap identity check even though positive answers no
    // longer need CNAME-chain reconstruction.
    if response
        .first_question()
        .is_some_and(|response_question| response_question != question)
    {
        return None;
    }

    if response.rcode() == Rcode::NoError {
        // RouterOS observers deliberately use the simple Answer-section
        // semantics shared with ros_address_list: every enabled A/AAAA answer
        // is useful. The queried type controls only what may be withdrawn.
        let addrs = collect_answer_addrs(response, |ip| match ip {
            IpAddr::V4(_) => config.gateway4.is_some(),
            IpAddr::V6(_) => config.gateway6.is_some(),
        });
        if !addrs.is_empty() {
            return Some(ExtractedObservation {
                domain,
                replace_scope,
                addrs,
                negative_ttl_secs: None,
            });
        }
    }

    match classify_response(response, Some(question)) {
        ResponseDisposition::DefinitiveNegative(NegativeResponseKind::NoData) => {
            Some(ExtractedObservation {
                domain,
                replace_scope,
                addrs: Vec::new(),
                negative_ttl_secs: response.negative_ttl_from_soa(),
            })
        }
        // NXDOMAIN is authoritative for the name, not only the queried record
        // type, so withdraw both address families. This is essential when
        // fixed_ttl=0 because no time-based cleanup will happen later.
        ResponseDisposition::DefinitiveNegative(NegativeResponseKind::NxDomain) => {
            Some(ExtractedObservation {
                domain,
                replace_scope: ObservationScope::Both,
                addrs: Vec::new(),
                negative_ttl_secs: response.negative_ttl_from_soa(),
            })
        }
        _ => None,
    }
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
    persistent_route: Option<PersistentRouteArgs>,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<ParsedPersistentRoutes> {
    let mut parsed = ParsedPersistentRoutes::default();
    let Some(route) = persistent_route else {
        return Ok(parsed);
    };

    if let Some(ips) = route.ips {
        for (index, item) in ips.into_iter().enumerate() {
            let source = format!("persistent_route.ips[{index}]");
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

    parsed.files = parse_persistent_route_files(route.files)?;
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

fn parse_persistent_route_files(files: Option<Vec<String>>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let Some(files) = files else {
        return Ok(out);
    };
    for (index, file_raw) in files.into_iter().enumerate() {
        let file = file_raw.trim();
        if file.is_empty() {
            return Err(DnsError::plugin(format!(
                "ros_route persistent_route.files[{index}] cannot be empty"
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
        let source_prefix = format!("persistent_route.files[{index}]");
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

pub(super) async fn load_persistent_ips_from_files_async(
    files: &[String],
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<(AHashSet<String>, usize, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_gateway = 0usize;
    let mut ignored_default_route = 0usize;

    for (index, file) in files.iter().enumerate() {
        let content = tokio_fs::read_to_string(file).await.map_err(|e| {
            DnsError::plugin(format!(
                "ros_route failed to read persistent route file '{file}': {e}"
            ))
        })?;
        let source_prefix = format!("persistent_route.files[{index}]");
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
    fn observation_ignores_non_address_queries_and_scopes_nodata() {
        let config = observation_config();
        let mut txt_context = context_with_nodata(RecordType::TXT);
        assert!(extract_observation(&mut txt_context, &config).is_none());
        let mut any_context = context_with_nodata(RecordType::ANY);
        assert!(extract_observation(&mut any_context, &config).is_none());

        let mut a_context = context_with_nodata(RecordType::A);
        let observation =
            extract_observation(&mut a_context, &config).expect("A NODATA observation");
        assert_eq!(observation.replace_scope, ObservationScope::Ipv4);
        assert!(observation.addrs.is_empty());
        assert_eq!(observation.negative_ttl_secs, None);
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

        assert_eq!(observation.replace_scope, ObservationScope::Ipv4);
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
    fn nxdomain_withdraws_both_address_families() {
        let config = observation_config();
        let mut context = context_with_rcode(RecordType::AAAA, Rcode::NXDomain);

        let observation =
            extract_observation(&mut context, &config).expect("AAAA NXDOMAIN observation");

        assert_eq!(observation.domain, "example.com");
        assert_eq!(observation.replace_scope, ObservationScope::Both);
        assert!(observation.addrs.is_empty());
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
    fn negative_observation_carries_soa_ttl_for_queued_replay() {
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

        let observation = extract_observation(&mut context, &config).expect("NXDOMAIN observation");

        assert_eq!(observation.negative_ttl_secs, Some(30));
    }

    #[test]
    fn same_tag_runtime_coordinates_cleanup_by_ownership_namespace() {
        let sequence = NEXT_ROUTE_INSTANCE_ID.fetch_add(6, Ordering::Relaxed);
        let namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "policy".to_string(),
            comment_prefix: "fdns".to_string(),
        };
        let success_tag = format!("route-reload-success-{sequence}");
        let old_metrics = Arc::new(RosRouteMetrics::new(success_tag.clone()));
        let new_metrics = Arc::new(RosRouteMetrics::new(success_tag.clone()));
        register_active_route_instance(
            &success_tag,
            sequence,
            namespace.clone(),
            old_metrics,
            None,
        )
        .expect("old runtime");
        register_active_route_instance(
            &success_tag,
            sequence + 1,
            namespace.clone(),
            new_metrics,
            None,
        )
        .expect("replacement runtime");
        assert!(!release_active_route_instance(&success_tag, sequence));
        assert!(release_active_route_instance(&success_tag, sequence + 1));

        let rollback_tag = format!("route-reload-rollback-{sequence}");
        let old_metrics = Arc::new(RosRouteMetrics::new(rollback_tag.clone()));
        let candidate_metrics = Arc::new(RosRouteMetrics::new(rollback_tag.clone()));
        register_active_route_instance(
            &rollback_tag,
            sequence + 2,
            namespace.clone(),
            old_metrics,
            None,
        )
        .expect("old runtime");
        register_active_route_instance(
            &rollback_tag,
            sequence + 3,
            namespace,
            candidate_metrics,
            None,
        )
        .expect("candidate runtime");
        assert!(!release_active_route_instance(&rollback_tag, sequence + 3));
        assert!(release_active_route_instance(&rollback_tag, sequence + 2));

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
        register_active_route_instance(
            &migration_tag,
            sequence + 4,
            old_namespace,
            Arc::new(RosRouteMetrics::new(migration_tag.clone())),
            None,
        )
        .expect("old namespace");
        register_active_route_instance(
            &migration_tag,
            sequence + 5,
            new_namespace,
            Arc::new(RosRouteMetrics::new(migration_tag.clone())),
            None,
        )
        .expect("new namespace");
        assert!(release_active_route_instance(&migration_tag, sequence + 4));
        assert!(release_active_route_instance(&migration_tag, sequence + 5));
    }

    #[test]
    fn failed_compatible_reload_requests_immediate_restore_reconcile() {
        let sequence = NEXT_ROUTE_INSTANCE_ID.fetch_add(2, Ordering::Relaxed);
        let tag = format!("route-reload-restore-{sequence}");
        let namespace = RouteOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            routing_table: "policy".to_string(),
            comment_prefix: "fdns".to_string(),
        };
        let (old_tx, mut old_rx) = mpsc::channel(1);

        register_active_route_instance(
            &tag,
            sequence,
            namespace.clone(),
            Arc::new(RosRouteMetrics::new(tag.clone())),
            Some(old_tx),
        )
        .expect("old runtime");
        register_active_route_instance(
            &tag,
            sequence + 1,
            namespace,
            Arc::new(RosRouteMetrics::new(tag.clone())),
            None,
        )
        .expect("candidate runtime");

        assert!(!release_active_route_instance(&tag, sequence + 1));
        assert!(matches!(old_rx.try_recv(), Ok(ManagerCommand::Reconcile)));
        assert!(release_active_route_instance(&tag, sequence));
    }
}
