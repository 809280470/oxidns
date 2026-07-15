// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! `ros_address_list` executor plugin.
//!
//! This executor is an observer-side effect stage designed to integrate with
//! OxiDNS sequence pipelines. It does not alter DNS decisions or response
//! content. Instead, it watches final downstream DNS answers and synchronizes
//! IPs into RouterOS address lists.
//!
//! Architecture overview:
//! - continuation pre-stage stays hot-path light.
//! - continuation post-stage extracts normalized query domain and unique A/AAAA
//!   IPs.
//! - address-list synchronization is delegated to a single-owner background
//!   manager state machine.
//! - RouterOS API details are isolated in `MikrotikApi` adapter
//!   implementations.
//! - ownership metadata is persisted in RouterOS `comment` so cleanup can
//!   safely distinguish OxiDNS-managed entries from foreign entries.
//!
//! Behavior goals:
//! - maintain IPv4/IPv6 dynamic host entries in configured address lists.
//! - support optional always-present IP/CIDR entries via `persistent`.
//! - use RouterOS native `timeout` for dynamic expiration maintenance.
//! - preserve DNS hot-path latency (`async=true` uses non-blocking queue).
//! - provide blocking write-before-return mode (`async=false`) without
//!   affecting DNS response result.
//! - load persistent file-backed entries at startup and keep them fixed until
//!   the plugin is reloaded.

use std::fs;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ahash::AHashSet;
use async_trait::async_trait;
use serde::Deserialize;
use serde_yaml_ng::Value;
use tokio::sync::oneshot;
use tracing::warn;

use self::api::{
    DEFAULT_CONNECT_TIMEOUT_SECS, DEFAULT_RECEIVE_TIMEOUT_SECS, DEFAULT_SEND_TIMEOUT_SECS,
    MikrotikApi, MikrotikApiTimeouts, MikrotikRsClient,
};
use self::manager::{
    AddressListCleanupScope, AddressListFamily, AddressListKey, AddressListManager,
    AddressListManagerConfig, AddressListManagerHandle, AddressListManagerRuntime,
    ObserveEnqueueError,
};
use crate::config::types::PluginConfig;
use crate::core::context::DnsContext;
use crate::infra::error::{DnsError, Result};
use crate::infra::observability::metrics::{
    MetricLabel, MetricSample, MetricSink, MetricSource, register_metric_source,
    unregister_metric_source,
};
use crate::plugin::executor::ros_common::lifecycle::ActiveInstanceRegistry;
use crate::plugin::executor::ros_common::transport::{RouterOsConnectionConfig, RouterOsTlsArgs};
use crate::plugin::executor::ros_common::{
    ObservedAddr, collect_answer_addrs, response_question_matches_request,
};
use crate::plugin::executor::{ExecStep, Executor, ExecutorNext};
use crate::plugin::{Plugin, PluginFactory, UninitializedPlugin};
use crate::proto::Rcode;
use crate::{continue_next, plugin_factory};

mod api;
mod manager;

/// Default lower TTL clamp for dynamic address-list entries.
const DEFAULT_MIN_TTL: u32 = 60;
/// Default upper TTL clamp for dynamic address-list entries.
const DEFAULT_MAX_TTL: u32 = 3600;
/// Default execution mode keeps RouterOS writes off the DNS request path.
const DEFAULT_ASYNC_MODE: bool = true;
/// Default shutdown behavior removes plugin-owned RouterOS entries.
const DEFAULT_CLEANUP_ON_SHUTDOWN: bool = true;
/// Default comment prefix used to mark OxiDNS-owned RouterOS rows.
const DEFAULT_COMMENT_PREFIX: &str = "fdns";
const DEFAULT_MAX_ENTRIES: usize = 65_536;
/// Maximum time sync mode waits for one observe command to finish.
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
    /// IPv4 address-list name for observed IPv4 answers.
    address_list4: Option<String>,
    /// IPv6 address-list name for observed IPv6 answers.
    address_list6: Option<String>,
    /// Prefix used in RouterOS comments to mark OxiDNS-managed entries.
    /// Defaults to `fdns` when omitted.
    comment_prefix: Option<String>,
    /// Always-present address-list items that should never expire.
    persistent: Option<PersistentArgs>,
    /// Minimum effective TTL clamp (seconds) for observed records.
    min_ttl: Option<u32>,
    /// Maximum effective TTL clamp (seconds) for observed records.
    max_ttl: Option<u32>,
    /// Optional fixed TTL override (seconds) for dynamic observed records.
    /// `0` means do not set RouterOS timeout.
    fixed_ttl: Option<u32>,
    /// Whether to clean managed address-list entries on shutdown.
    cleanup_on_shutdown: Option<bool>,
    /// Maximum number of dynamic refresh states retained locally.
    max_entries: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PersistentArgs {
    /// Inline always-present IPs/CIDRs. Plain IP is normalized to host entry.
    ips: Option<Vec<String>>,
    /// File list that provides always-present IPs/CIDRs.
    files: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct MikrotikConfig {
    /// RouterOS API endpoint, retained for reload ownership coordination.
    address: String,
    /// Connection settings consumed when the API transport is constructed.
    connection: Option<RouterOsConnectionConfig>,
    /// Async mode switch for post stage writes.
    async_mode: bool,
    /// IPv4 address-list name managed by this plugin.
    address_list4: Option<String>,
    /// IPv6 address-list name managed by this plugin.
    address_list6: Option<String>,
    /// Full persistent desired set after merging inline and file sources.
    persistent_items: AHashSet<AddressListKey>,
    /// Prefix used in RouterOS comments to mark plugin ownership.
    comment_prefix: String,
    /// Minimum TTL clamp for dynamic entries.
    min_ttl: u32,
    /// Maximum TTL clamp for dynamic entries.
    max_ttl: u32,
    /// Optional fixed TTL override for dynamic entries.
    /// `0` means do not set RouterOS timeout.
    fixed_ttl: Option<u32>,
    /// Whether shutdown should remove owned entries from RouterOS.
    cleanup_on_shutdown: bool,
    /// Hard upper bound for the local dynamic refresh cache.
    max_entries: usize,
}

impl MikrotikConfigArgs {
    /// Validate user-facing config and normalize it into a runtime-ready form.
    ///
    /// This is also where persistent input sources are parsed into normalized
    /// `AddressListKey` values so the manager does not need to re-interpret
    /// human-facing YAML at runtime.
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
        let address_list4 = optional_non_empty(self.address_list4);
        let address_list6 = optional_non_empty(self.address_list6);
        if address_list4.is_none() && address_list6.is_none() {
            return Err(DnsError::plugin(
                "ros_address_list requires at least one of address_list4 or address_list6",
            ));
        }

        let comment_prefix = optional_non_empty(self.comment_prefix)
            .unwrap_or_else(|| DEFAULT_COMMENT_PREFIX.to_string());
        validate_comment_token("comment_prefix", &comment_prefix)?;

        let min_ttl = self.min_ttl.unwrap_or(DEFAULT_MIN_TTL);
        let max_ttl = self.max_ttl.unwrap_or(DEFAULT_MAX_TTL);
        if min_ttl > max_ttl {
            return Err(DnsError::plugin(format!(
                "ros_address_list ttl range is invalid: min_ttl({min_ttl}) > max_ttl({max_ttl})"
            )));
        }
        let fixed_ttl = self.fixed_ttl;
        let max_entries = self.max_entries.unwrap_or(DEFAULT_MAX_ENTRIES);
        if max_entries == 0 {
            return Err(DnsError::plugin(
                "ros_address_list max_entries must be greater than zero",
            ));
        }

        let parsed_persistent = parse_persistent_items(
            self.persistent,
            address_list4.as_deref(),
            address_list6.as_deref(),
        )?;
        if emit_warnings && parsed_persistent.ignored_by_family > 0 {
            warn!(
                ignored = parsed_persistent.ignored_by_family,
                "ros_address_list persistent ignored entries without corresponding address list family"
            );
        }

        Ok(MikrotikConfig {
            address,
            connection: Some(connection),
            async_mode: self.async_mode.unwrap_or(DEFAULT_ASYNC_MODE),
            address_list4,
            address_list6,
            persistent_items: parsed_persistent.all_items,
            comment_prefix,
            min_ttl,
            max_ttl,
            fixed_ttl,
            cleanup_on_shutdown: self
                .cleanup_on_shutdown
                .unwrap_or(DEFAULT_CLEANUP_ON_SHUTDOWN),
            max_entries,
        })
    }
}

#[derive(Debug)]
struct RosMetrics {
    tag: String,
    observe_total: AtomicU64,
    dropped_total: AtomicU64,
    sync_error_total: AtomicU64,
    sync_timeout_total: AtomicU64,
    pending_observations: AtomicU64,
    managed_entries: AtomicU64,
    coalesced_total: AtomicU64,
    capacity_rejected_total: AtomicU64,
    reconnect_total: AtomicU64,
    connect_attempt_total: AtomicU64,
    backoff_total: AtomicU64,
    reconcile_error_total: AtomicU64,
    last_reconcile_success_timestamp_seconds: AtomicU64,
    degraded: AtomicU64,
    cleanup_error_total: AtomicU64,
}

#[derive(Debug)]
struct ActiveAddressListInstance {
    instance_id: u64,
    namespace: AddressListOwnershipNamespace,
    metrics: Arc<RosMetrics>,
    manager_handle: Option<AddressListManagerHandle>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AddressListOwnershipNamespace {
    address: String,
    address_list4: Option<String>,
    address_list6: Option<String>,
    comment_prefix: String,
}

impl AddressListOwnershipNamespace {
    fn from_config(config: &MikrotikConfig) -> Self {
        Self {
            address: config.address.clone(),
            address_list4: config.address_list4.clone(),
            address_list6: config.address_list6.clone(),
            comment_prefix: config.comment_prefix.clone(),
        }
    }

    fn shares_owner_root(&self, other: &Self) -> bool {
        self.address == other.address && self.comment_prefix == other.comment_prefix
    }

    fn shares_any_list(&self, other: &Self) -> bool {
        self.shares_owner_root(other)
            && ((self.address_list4.is_some() && self.address_list4 == other.address_list4)
                || (self.address_list6.is_some() && self.address_list6 == other.address_list6))
    }

    fn cleanup_scope(&self, remaining: &[ActiveAddressListInstance]) -> AddressListCleanupScope {
        let ipv4 = self.address_list4.is_some()
            && !remaining.iter().any(|instance| {
                self.shares_owner_root(&instance.namespace)
                    && self.address_list4 == instance.namespace.address_list4
            });
        let ipv6 = self.address_list6.is_some()
            && !remaining.iter().any(|instance| {
                self.shares_owner_root(&instance.namespace)
                    && self.address_list6 == instance.namespace.address_list6
            });
        AddressListCleanupScope { ipv4, ipv6 }
    }
}

static NEXT_ADDRESS_LIST_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn active_address_list_instances() -> &'static ActiveInstanceRegistry<ActiveAddressListInstance> {
    static INSTANCES: OnceLock<ActiveInstanceRegistry<ActiveAddressListInstance>> = OnceLock::new();
    INSTANCES.get_or_init(ActiveInstanceRegistry::new)
}

fn register_active_address_list_instance(
    tag: &str,
    instance_id: u64,
    namespace: AddressListOwnershipNamespace,
    metrics: Arc<RosMetrics>,
    manager_handle: Option<AddressListManagerHandle>,
) -> Result<()> {
    register_metric_source(metrics.clone())?;
    active_address_list_instances().push(
        tag,
        ActiveAddressListInstance {
            instance_id,
            namespace,
            metrics,
            manager_handle,
        },
    );
    Ok(())
}

fn release_active_address_list_instance(tag: &str, instance_id: u64) -> AddressListCleanupScope {
    let Some((cleanup_scope, metric_replacement, remove_metric, restore_handle)) =
        active_address_list_instances().release(
            tag,
            |instance| instance.instance_id == instance_id,
            |removed, instances, was_metric_owner| {
                let is_last = instances.is_empty();
                let cleanup_scope = removed.namespace.cleanup_scope(instances);
                let metric_replacement = was_metric_owner
                    .then(|| instances.last().map(|instance| instance.metrics.clone()))
                    .flatten();
                let restore_handle = was_metric_owner
                    .then(|| {
                        instances
                            .iter()
                            .rev()
                            .find(|instance| instance.namespace.shares_any_list(&removed.namespace))
                            .and_then(|instance| instance.manager_handle.clone())
                    })
                    .flatten();
                let remove_metric = was_metric_owner && is_last;
                (
                    cleanup_scope,
                    metric_replacement,
                    remove_metric,
                    restore_handle,
                )
            },
        )
    else {
        return AddressListCleanupScope::none();
    };

    if let Some(metrics) = metric_replacement {
        let _ = register_metric_source(metrics);
    } else if remove_metric {
        unregister_metric_source(tag);
    }
    if let Some(handle) = restore_handle
        && !handle.request_reconcile()
    {
        warn!(
            plugin = %tag,
            "ros_address_list failed to enqueue immediate reconcile after reload rollback"
        );
    }
    cleanup_scope
}

impl RosMetrics {
    fn new(tag: String) -> Self {
        Self {
            tag,
            observe_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            sync_error_total: AtomicU64::new(0),
            sync_timeout_total: AtomicU64::new(0),
            pending_observations: AtomicU64::new(0),
            managed_entries: AtomicU64::new(0),
            coalesced_total: AtomicU64::new(0),
            capacity_rejected_total: AtomicU64::new(0),
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

impl MetricSource for RosMetrics {
    fn tag(&self) -> &str {
        &self.tag
    }

    fn plugin_type(&self) -> &'static str {
        "ros_address_list"
    }

    fn collect(&self, sink: &mut dyn MetricSink) {
        let labels = [MetricLabel::new("plugin_tag", self.tag.as_str())];
        sink.emit(MetricSample::counter(
            "ros_address_list_observe_total",
            "Total domain observations submitted to the RouterOS address-list manager.",
            &labels,
            self.observe_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_address_list_dropped_total",
            "Total observations dropped in async mode (queue full or channel closed).",
            &labels,
            self.dropped_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_address_list_sync_error_total",
            "Total sync-mode observations that failed at the RouterOS manager.",
            &labels,
            self.sync_error_total.load(Ordering::Relaxed),
        ));
        sink.emit(MetricSample::counter(
            "ros_address_list_sync_timeout_total",
            "Total sync-mode observations that timed out enqueueing or waiting.",
            &labels,
            self.sync_timeout_total.load(Ordering::Relaxed),
        ));
        for (name, help, value) in [
            (
                "ros_address_list_pending_observations",
                "Current coalesced address-list observations waiting for processing.",
                self.pending_observations.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_managed_entries",
                "Current address-list entries retained by the manager.",
                self.managed_entries.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_last_reconcile_success_timestamp_seconds",
                "Unix timestamp of the last successful address-list reconcile.",
                self.last_reconcile_success_timestamp_seconds
                    .load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_degraded",
                "Whether the RouterOS transport is currently degraded.",
                self.degraded.load(Ordering::Relaxed),
            ),
        ] {
            sink.emit(MetricSample::gauge(name, help, &labels, value));
        }
        for (name, help, value) in [
            (
                "ros_address_list_coalesced_total",
                "Total address-list observations merged into an existing mailbox key.",
                self.coalesced_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_capacity_rejected_total",
                "Total address-list observations rejected by queue or state capacity.",
                self.capacity_rejected_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_reconnect_total",
                "Total successful RouterOS transport reconnections.",
                self.reconnect_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_connect_attempt_total",
                "Total RouterOS transport connection attempts.",
                self.connect_attempt_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_backoff_total",
                "Total RouterOS transport backoff schedules.",
                self.backoff_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_reconcile_error_total",
                "Total failed address-list reconcile attempts.",
                self.reconcile_error_total.load(Ordering::Relaxed),
            ),
            (
                "ros_address_list_cleanup_error_total",
                "Total address-list entries that failed shutdown cleanup.",
                self.cleanup_error_total.load(Ordering::Relaxed),
            ),
        ] {
            sink.emit(MetricSample::counter(name, help, &labels, value));
        }
    }
}

#[derive(Debug)]
struct MikrotikExecutor {
    /// Plugin tag from the global registry.
    tag: String,
    instance_id: u64,
    active_registered: AtomicBool,
    /// Shared observability counters.
    metrics: Arc<RosMetrics>,
    /// Fully validated immutable runtime config.
    config: MikrotikConfig,
    /// Pre-built manager consumed during `init()`.
    manager: Option<AddressListManager>,
    /// Coalescing mailbox handle exposed after the background runtime starts.
    manager_handle: Option<AddressListManagerHandle>,
    /// Runtime handle stored so `destroy()` can stop worker tasks.
    runtime: Mutex<Option<AddressListManagerRuntime>>,
}

#[async_trait]
impl Plugin for MikrotikExecutor {
    fn tag(&self) -> &str {
        &self.tag
    }

    async fn init(&mut self, _context: &crate::plugin::PluginInitContext<'_>) -> Result<()> {
        // `init()` may be called more than once by the plugin framework.
        // Keep it idempotent and only build the runtime once.
        if self.manager.is_none() || self.manager_handle.is_some() {
            return Ok(());
        }

        let Some(manager) = self.manager.take() else {
            return Ok(());
        };

        let runtime = AddressListManagerRuntime::start(self.tag.clone(), manager);
        let manager_handle = runtime.handle();
        if let Err(error) = register_active_address_list_instance(
            &self.tag,
            self.instance_id,
            AddressListOwnershipNamespace::from_config(&self.config),
            self.metrics.clone(),
            Some(manager_handle.clone()),
        ) {
            runtime.shutdown(AddressListCleanupScope::none()).await;
            return Err(error);
        }
        let mut runtime = Some(runtime);
        if let Ok(mut slot) = self.runtime.lock() {
            *slot = runtime.take();
        }
        if let Some(runtime) = runtime {
            release_active_address_list_instance(&self.tag, self.instance_id);
            runtime.shutdown(AddressListCleanupScope::none()).await;
            return Err(DnsError::plugin(
                "ros_address_list runtime lock is poisoned during initialization",
            ));
        }
        self.manager_handle = Some(manager_handle);
        self.active_registered.store(true, Ordering::Release);
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        let cleanup_scope = if self.active_registered.swap(false, Ordering::AcqRel) {
            release_active_address_list_instance(&self.tag, self.instance_id)
        } else {
            AddressListCleanupScope::none()
        };
        if let Some(runtime) = self.runtime.lock().ok().and_then(|mut slot| slot.take()) {
            let cleanup_scope = if self.config.cleanup_on_shutdown {
                cleanup_scope
            } else {
                AddressListCleanupScope::none()
            };
            runtime.shutdown(cleanup_scope).await;
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for MikrotikExecutor {
    fn with_next(&self) -> bool {
        true
    }

    #[hotpath::measure]
    async fn execute(&self, context: &mut DnsContext) -> Result<ExecStep> {
        self.execute_with_next(context, None).await
    }

    #[hotpath::measure]
    async fn execute_with_next(
        &self,
        context: &mut DnsContext,
        next: Option<ExecutorNext>,
    ) -> Result<ExecStep> {
        let step = continue_next!(next, context)?;
        // If the runtime never started, the plugin stays side-effect free.
        if !self.active_registered.load(Ordering::Acquire) {
            return Ok(step);
        }
        let Some(handle) = self.manager_handle.as_ref() else {
            return Ok(step);
        };

        // This executor only reacts to successful final answers containing A/AAAA data.
        let Some((domain, addrs)) = extract_observation(context, &self.config) else {
            return Ok(step);
        };
        self.metrics.observe_total.fetch_add(1, Ordering::Relaxed);

        if self.config.async_mode {
            // Async mode keeps RouterOS I/O fully off the request path.
            match handle.try_observe(domain, addrs, None) {
                Ok(_) => {}
                Err(ObserveEnqueueError::Full) => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        plugin = %self.tag,
                        "ros_address_list observe queue is full, observation dropped"
                    );
                }
                Err(ObserveEnqueueError::Closed) => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        plugin = %self.tag,
                        "ros_address_list manager channel closed, observation dropped"
                    );
                }
            }
            return Ok(step);
        }

        // Sync mode still preserves DNS behavior on RouterOS failures. The only
        // difference is that we wait for the manager to attempt the write.
        let (wait_tx, wait_rx) = oneshot::channel::<Result<()>>();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(SYNC_OBSERVE_TIMEOUT_SECS);
        let send_outcome =
            tokio::time::timeout_at(deadline, handle.observe(domain, addrs, wait_tx)).await;
        match send_outcome {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {
                self.metrics
                    .sync_error_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    "ros_address_list manager channel closed in sync mode, DNS response is kept unchanged"
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
                    "ros_address_list observe enqueue timed out in sync mode, DNS response is kept unchanged"
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
                    "ros_address_list observe failed in sync mode, DNS response is kept unchanged"
                );
                Ok(step)
            }
            Ok(Err(_)) => {
                self.metrics
                    .sync_error_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    plugin = %self.tag,
                    "ros_address_list manager dropped sync observe response, DNS response is kept unchanged"
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
                    "ros_address_list observe timed out in sync mode, DNS response is kept unchanged"
                );
                Ok(step)
            }
        }
    }
}

#[derive(Debug, Clone)]
#[plugin_factory("ros_address_list")]
pub struct MikrotikFactory;

impl PluginFactory for MikrotikFactory {
    fn create(
        &self,
        plugin_config: &PluginConfig,
        _init_context: &crate::plugin::PluginInitContext<'_>,
    ) -> Result<UninitializedPlugin> {
        // Plugin tag is reused inside RouterOS comment ownership metadata.
        validate_comment_token("plugin tag", plugin_config.tag.as_str())?;
        let mut config = parse_plugin_config(plugin_config.args.clone(), true)?;
        let connection = config.connection.take().ok_or_else(|| {
            DnsError::plugin("ros_address_list connection config already consumed")
        })?;
        let api = Arc::new(MikrotikRsClient::new(connection)) as Arc<dyn MikrotikApi>;

        let manager_cfg = AddressListManagerConfig {
            plugin_tag: plugin_config.tag.clone(),
            address_list4: config.address_list4.clone(),
            address_list6: config.address_list6.clone(),
            persistent_items: config.persistent_items.clone(),
            comment_prefix: config.comment_prefix.clone(),
            min_ttl: config.min_ttl,
            max_ttl: config.max_ttl,
            fixed_ttl: config.fixed_ttl,
            max_entries: config.max_entries,
        };
        let metrics = Arc::new(RosMetrics::new(plugin_config.tag.clone()));
        let manager = AddressListManager::with_metrics(api, manager_cfg, metrics.clone());

        Ok(UninitializedPlugin::Executor(Box::new(MikrotikExecutor {
            tag: plugin_config.tag.clone(),
            instance_id: NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            active_registered: AtomicBool::new(false),
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
) -> Option<(String, Vec<ObservedAddr>)> {
    // The first question is the authoritative domain label written to the
    // RouterOS comment for dynamic entries. This is intentionally lightweight:
    // we do not inspect CNAME chains or reconstruct canonical names here.

    let response = context.response()?;
    if response.rcode() != Rcode::NoError {
        return None;
    }

    if !response_question_matches_request(&context.request, response) {
        return None;
    }

    let domain = context
        .request
        .first_question()
        .map(|question| question.name().normalized().to_string())?;

    let addrs = collect_answer_addrs(response, |ip| match ip {
        IpAddr::V4(_) => config.address_list4.is_some(),
        IpAddr::V6(_) => config.address_list6.is_some(),
    });
    if addrs.is_empty() {
        return None;
    }
    Some((domain, addrs))
}

fn parse_plugin_config(args: Option<Value>, emit_warnings: bool) -> Result<MikrotikConfig> {
    let Some(args) = args else {
        return Err(DnsError::plugin("ros_address_list plugin requires args"));
    };
    let raw = serde_yaml_ng::from_value::<MikrotikConfigArgs>(args)
        .map_err(|e| DnsError::plugin(format!("failed to parse ros_address_list config: {e}")))?;
    raw.into_config(emit_warnings)
}

fn required_non_empty(value: Option<String>, field: &str) -> Result<String> {
    let Some(value) = value else {
        return Err(DnsError::plugin(format!(
            "ros_address_list '{field}' is required"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DnsError::plugin(format!(
            "ros_address_list '{field}' cannot be empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn optional_non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn timeout_secs(value: Option<u64>, field: &str, default_secs: u64) -> Result<u64> {
    match value {
        Some(0) => Err(DnsError::plugin(format!(
            "ros_address_list '{field}' must be greater than 0 seconds"
        ))),
        Some(value) => Ok(value),
        None => Ok(default_secs),
    }
}

#[inline]
fn contains_comment_delimiter(value: &str) -> bool {
    value.contains(';') || value.contains('=')
}

fn validate_comment_token(field: &str, value: &str) -> Result<()> {
    if contains_comment_delimiter(value) {
        return Err(DnsError::plugin(format!(
            "ros_address_list '{field}' cannot contain ';' or '='"
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ParsedPersistentItems {
    /// Final desired set after merging inline and file sources.
    all_items: AHashSet<AddressListKey>,
    /// Count of items skipped because that family is not configured.
    ignored_by_family: usize,
}

/// Parse `persistent` config into normalized address-list keys.
///
/// The parser performs all expensive normalization and validation at startup:
/// plain IPs become host prefixes, CIDRs are masked to network form, and each
/// item is bound to the correct IPv4/IPv6 address-list name.
fn parse_persistent_items(
    persistent: Option<PersistentArgs>,
    address_list4: Option<&str>,
    address_list6: Option<&str>,
) -> Result<ParsedPersistentItems> {
    let mut parsed = ParsedPersistentItems::default();
    let Some(persistent) = persistent else {
        return Ok(parsed);
    };

    if let Some(ips) = persistent.ips {
        for (index, item) in ips.into_iter().enumerate() {
            let source = format!("persistent.ips[{index}]");
            let key = parse_persistent_item(
                item.as_str(),
                source.as_str(),
                address_list4,
                address_list6,
            )?;
            match key {
                Some(key) => {
                    parsed.all_items.insert(key);
                }
                None => {
                    parsed.ignored_by_family = parsed.ignored_by_family.saturating_add(1);
                }
            }
        }
    }

    let files = parse_persistent_files(persistent.files)?;
    let (file_items, ignored_by_family) =
        load_persistent_items_from_files(files.as_slice(), address_list4, address_list6)?;
    parsed.ignored_by_family = parsed.ignored_by_family.saturating_add(ignored_by_family);
    parsed.all_items.extend(file_items);
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
                "ros_address_list persistent.files[{index}] cannot be empty"
            )));
        }
        out.push(file.to_string());
    }
    Ok(out)
}

/// Parse one file body into normalized persistent items.
///
/// Files use the same item grammar as inline YAML. Empty lines and `#` comments
/// are ignored. Family-mismatched entries are skipped rather than failing
/// startup so shared source files can contain both IPv4 and IPv6 items.
fn load_persistent_items_from_content(
    source_prefix: &str,
    content: &str,
    address_list4: Option<&str>,
    address_list6: Option<&str>,
) -> Result<(AHashSet<AddressListKey>, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_family = 0usize;

    for (line_no, line) in content.lines().enumerate() {
        let token = line.split('#').next().unwrap_or_default().trim();
        if token.is_empty() {
            continue;
        }

        let source = format!("{source_prefix} line {}", line_no + 1);
        match parse_persistent_item(token, source.as_str(), address_list4, address_list6)? {
            Some(key) => {
                out.insert(key);
            }
            None => {
                ignored_by_family = ignored_by_family.saturating_add(1);
            }
        }
    }

    Ok((out, ignored_by_family))
}

fn load_persistent_items_from_files(
    files: &[String],
    address_list4: Option<&str>,
    address_list6: Option<&str>,
) -> Result<(AHashSet<AddressListKey>, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_family = 0usize;

    for (index, file) in files.iter().enumerate() {
        let content = fs::read_to_string(file).map_err(|e| {
            DnsError::plugin(format!(
                "ros_address_list failed to read persistent file '{file}': {e}"
            ))
        })?;
        let source_prefix = format!("persistent.files[{index}]");
        let (loaded, ignored_delta) = load_persistent_items_from_content(
            source_prefix.as_str(),
            &content,
            address_list4,
            address_list6,
        )?;
        out.extend(loaded);
        ignored_by_family = ignored_by_family.saturating_add(ignored_delta);
    }

    Ok((out, ignored_by_family))
}

/// Parse one human-facing persistent item and bind it to the correct list.
///
/// Return `Ok(None)` when the item is valid but its IP family has no configured
/// target list, allowing callers to ignore mixed-family source files cleanly.
fn parse_persistent_item(
    raw: &str,
    source: &str,
    address_list4: Option<&str>,
    address_list6: Option<&str>,
) -> Result<Option<AddressListKey>> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(DnsError::plugin(format!(
            "ros_address_list {source} is empty"
        )));
    }

    let (ip, prefix) = if let Some((ip_raw, prefix_raw)) = value.split_once('/') {
        let ip = ip_raw.trim().parse::<IpAddr>().map_err(|e| {
            DnsError::plugin(format!(
                "ros_address_list {source} has invalid ip '{ip_raw}': {e}"
            ))
        })?;
        let prefix = prefix_raw.trim().parse::<u8>().map_err(|e| {
            DnsError::plugin(format!(
                "ros_address_list {source} has invalid prefix '{prefix_raw}': {e}"
            ))
        })?;
        (ip, prefix)
    } else {
        let ip = value.parse::<IpAddr>().map_err(|e| {
            DnsError::plugin(format!(
                "ros_address_list {source} has invalid ip '{value}': {e}"
            ))
        })?;
        let family = AddressListFamily::from_ip(ip);
        (ip, family.host_prefix())
    };

    let family = AddressListFamily::from_ip(ip);
    let list = match family {
        AddressListFamily::Ipv4 => address_list4,
        AddressListFamily::Ipv6 => address_list6,
    };
    let Some(list) = list else {
        return Ok(None);
    };

    AddressListKey::new_with_prefix(ip, prefix, list.to_string())
        .ok_or_else(|| {
            DnsError::plugin(format!(
                "ros_address_list {source} has invalid prefix /{prefix} for {ip}"
            ))
        })
        .map(Some)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::sync::atomic::AtomicUsize;

    use ahash::AHashMap;

    use super::*;
    use crate::infra::clock::AppClock;
    use crate::plugin::executor::ros_address_list::api::RouterListEntry;
    use crate::plugin::executor::ros_address_list::manager::{
        OwnedCommentKind, decode_owned_comment, encode_comment,
    };
    use crate::proto::rdata::{A, AAAA};
    use crate::proto::{DNSClass, Message, Name, Question, RData, Rcode, Record, RecordType};

    #[derive(Debug, Default)]
    struct MockApiState {
        entries: AHashMap<String, RouterListEntry>,
        next_id: u64,
        fail_next_upsert: bool,
        fail_healthcheck: bool,
        list_entries_calls: u64,
        list_entries_delay: Option<Duration>,
        convert_persistent_to_dynamic_after_list: bool,
        convert_owned_to_foreign_after_list: bool,
        upsert_v4: u64,
        upsert_v6: u64,
        update_ops: u64,
    }

    #[derive(Debug, Clone)]
    struct MockMikrotikApi {
        state: Arc<Mutex<MockApiState>>,
    }

    impl Default for MockMikrotikApi {
        fn default() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockApiState::default())),
            }
        }
    }

    impl MockMikrotikApi {
        fn storage_key(key: &AddressListKey) -> String {
            format!("{:?}:{}:{}", key.family, key.list, key.normalized_value())
        }

        fn seed_entry(&self, entry: RouterListEntry) {
            if let Ok(mut state) = self.state.lock() {
                state.entries.insert(Self::storage_key(&entry.key), entry);
            }
        }

        fn entry_count(&self) -> usize {
            self.state
                .lock()
                .map(|state| state.entries.len())
                .unwrap_or_default()
        }

        fn list_entries_calls(&self) -> u64 {
            self.state
                .lock()
                .map(|state| state.list_entries_calls)
                .unwrap_or_default()
        }
    }

    #[derive(Debug, Default)]
    struct PipelineMikrotikApi {
        active: AtomicUsize,
        max_active: AtomicUsize,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl MikrotikApi for PipelineMikrotikApi {
        async fn list_entries(
            &self,
            _list4: Option<&str>,
            _list6: Option<&str>,
        ) -> Result<Vec<RouterListEntry>> {
            Ok(Vec::new())
        }

        async fn list_entries_by_key(&self, _key: &AddressListKey) -> Result<Vec<RouterListEntry>> {
            Ok(Vec::new())
        }

        async fn upsert_owned_entry(
            &self,
            _key: &AddressListKey,
            _timeout: Option<&str>,
            _comment: &str,
            _comment_prefix: &str,
            _plugin_tag: &str,
            _refresh_timeout: bool,
        ) -> Result<Option<()>> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_active.fetch_max(active, Ordering::AcqRel);
            self.attempts.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(Some(()))
        }

        async fn delete_entry_by_id(&self, _id: &str, _family: AddressListFamily) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl MikrotikApi for MockMikrotikApi {
        async fn list_entries(
            &self,
            list4: Option<&str>,
            list6: Option<&str>,
        ) -> Result<Vec<RouterListEntry>> {
            let (fail_scan, delay) = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
                state.list_entries_calls = state.list_entries_calls.saturating_add(1);
                (state.fail_healthcheck, state.list_entries_delay)
            };
            if fail_scan {
                return Err(DnsError::plugin("mock address-list scan failure"));
            }
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }

            let state = self
                .state
                .lock()
                .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
            let entries = state
                .entries
                .values()
                .filter(|entry| match entry.key.family {
                    AddressListFamily::Ipv4 => list4 == Some(entry.key.list.as_str()),
                    AddressListFamily::Ipv6 => list6 == Some(entry.key.list.as_str()),
                })
                .cloned()
                .collect::<Vec<_>>();
            drop(state);

            let mut state = self
                .state
                .lock()
                .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
            if state.convert_persistent_to_dynamic_after_list {
                state.convert_persistent_to_dynamic_after_list = false;
                if let Some(entry) = state.entries.values_mut().find(|entry| {
                    decode_owned_comment("oxidns", "mk", entry.comment.as_deref())
                        .is_some_and(|meta| meta.kind == OwnedCommentKind::Persistent)
                }) {
                    entry.comment = Some(encode_comment(
                        "oxidns",
                        "mk",
                        OwnedCommentKind::Dynamic,
                        Some("race.example"),
                    ));
                }
            }
            if state.convert_owned_to_foreign_after_list {
                state.convert_owned_to_foreign_after_list = false;
                if let Some(entry) = state.entries.values_mut().next() {
                    entry.comment = Some("operator-owned".to_string());
                }
            }

            Ok(entries)
        }

        async fn list_entries_by_key(&self, key: &AddressListKey) -> Result<Vec<RouterListEntry>> {
            let state = self
                .state
                .lock()
                .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
            Ok(state
                .entries
                .values()
                .filter(|entry| entry.key == *key)
                .cloned()
                .collect())
        }

        async fn upsert_owned_entry(
            &self,
            key: &AddressListKey,
            timeout: Option<&str>,
            comment: &str,
            comment_prefix: &str,
            plugin_tag: &str,
            refresh_timeout: bool,
        ) -> Result<Option<()>> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
            if state.fail_next_upsert {
                state.fail_next_upsert = false;
                return Err(DnsError::plugin("mock upsert failure"));
            }

            let existing = state
                .entries
                .values()
                .filter(|entry| entry.key == *key)
                .cloned()
                .collect::<Vec<_>>();
            let mut owned = existing
                .iter()
                .filter(|entry| {
                    decode_owned_comment(comment_prefix, plugin_tag, entry.comment.as_deref())
                        .is_some()
                })
                .cloned()
                .collect::<Vec<_>>();
            let has_foreign = existing.len() > owned.len();
            if owned.is_empty() && has_foreign {
                return Ok(None);
            }

            if let Some(mut entry) = owned.pop() {
                let timeout_changed = entry.timeout.as_deref() != timeout;
                let comment_changed = entry.comment.as_deref() != Some(comment);
                if refresh_timeout || timeout_changed || comment_changed {
                    entry.timeout = timeout.map(str::to_string);
                    entry.comment = Some(comment.to_string());
                    state.update_ops = state.update_ops.saturating_add(1);
                    state.entries.insert(Self::storage_key(key), entry);
                }
                return Ok(Some(()));
            }

            state.next_id = state.next_id.saturating_add(1);
            let id = format!("*{}", state.next_id);
            match key.family {
                AddressListFamily::Ipv4 => state.upsert_v4 = state.upsert_v4.saturating_add(1),
                AddressListFamily::Ipv6 => state.upsert_v6 = state.upsert_v6.saturating_add(1),
            }
            state.entries.insert(
                Self::storage_key(key),
                RouterListEntry {
                    id,
                    key: key.clone(),
                    timeout: timeout.map(str::to_string),
                    comment: Some(comment.to_string()),
                },
            );
            Ok(Some(()))
        }

        async fn delete_entry_by_id(&self, id: &str, _family: AddressListFamily) -> Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| DnsError::plugin("mock api lock poisoned"))?;
            let key = state
                .entries
                .iter()
                .find(|(_, entry)| entry.id == id)
                .map(|(key, _)| key.clone());
            if let Some(key) = key {
                state.entries.remove(&key);
            }
            Ok(())
        }
    }

    fn default_cfg(tag: &str) -> AddressListManagerConfig {
        AppClock::start();
        AddressListManagerConfig {
            plugin_tag: tag.to_string(),
            address_list4: Some("oxidns_ipv4".to_string()),
            address_list6: Some("oxidns_ipv6".to_string()),
            persistent_items: AHashSet::new(),
            comment_prefix: "oxidns".to_string(),
            min_ttl: DEFAULT_MIN_TTL,
            max_ttl: DEFAULT_MAX_TTL,
            fixed_ttl: None,
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    fn make_context() -> DnsContext {
        let mut request = Message::new();
        request.add_question(Question::new(
            Name::from_ascii("example.com.").unwrap(),
            RecordType::A,
            DNSClass::IN,
        ));
        DnsContext::new("127.0.0.1:5353".parse::<SocketAddr>().unwrap(), request)
    }

    fn response_with_records(records: Vec<Record>) -> Message {
        let mut resp = Message::new();
        resp.set_rcode(Rcode::NoError);
        for record in records {
            resp.answers_mut().push(record);
        }
        resp
    }

    #[test]
    fn observation_with_mismatched_response_question_is_ignored() {
        let config = MikrotikConfig {
            address: "127.0.0.1:8728".to_string(),
            connection: None,
            async_mode: true,
            address_list4: Some("oxidns_ipv4".to_string()),
            address_list6: None,
            persistent_items: AHashSet::new(),
            comment_prefix: "oxidns".to_string(),
            min_ttl: DEFAULT_MIN_TTL,
            max_ttl: DEFAULT_MAX_TTL,
            fixed_ttl: None,
            cleanup_on_shutdown: false,
            max_entries: DEFAULT_MAX_ENTRIES,
        };
        let mut context = make_context();
        let mut response = response_with_records(vec![a_record(Ipv4Addr::new(192, 0, 2, 1), 60)]);
        response.add_question(Question::new(
            Name::from_ascii("other.example.").expect("name"),
            RecordType::A,
            DNSClass::IN,
        ));
        context.set_response(response);

        assert!(extract_observation(&mut context, &config).is_none());
    }

    #[tokio::test]
    async fn dynamic_upserts_use_bounded_pipeline() {
        let api = Arc::new(PipelineMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("pipeline"));
        let addrs = (1..=17)
            .map(|last_octet| ObservedAddr {
                addr: IpAddr::V4(Ipv4Addr::new(203, 0, 113, last_octet)),
                ttl_secs: 60,
            })
            .collect();

        manager
            .observe_domain("pipeline.example.".to_string(), addrs)
            .await
            .expect("pipeline writes");

        assert_eq!(api.attempts.load(Ordering::Relaxed), 17);
        assert_eq!(api.max_active.load(Ordering::Acquire), 16);
    }

    #[tokio::test]
    async fn persistent_reconcile_uses_bounded_pipeline() {
        let api = Arc::new(PipelineMikrotikApi::default());
        let mut cfg = default_cfg("persistent-pipeline");
        cfg.persistent_items = (1..=17)
            .map(|last| {
                AddressListKey::new(
                    IpAddr::V4(Ipv4Addr::new(203, 0, 113, last)),
                    "oxidns_ipv4".to_string(),
                )
            })
            .collect();
        let mut manager = AddressListManager::new(api.clone(), cfg);

        manager.background_reconcile_for_test().await;

        assert_eq!(api.attempts.load(Ordering::Relaxed), 17);
        assert_eq!(api.max_active.load(Ordering::Acquire), 16);
    }

    #[test]
    fn observation_mailbox_keeps_distinct_addresses_from_same_domain() {
        let handle = AddressListManagerHandle::new_for_test();
        let first = vec![ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            ttl_secs: 60,
        }];
        let latest = vec![ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
            ttl_secs: 300,
        }];

        assert!(
            handle
                .try_observe("busy.example.".to_string(), first, None)
                .is_ok()
        );
        assert!(
            handle
                .try_observe("busy.example.".to_string(), latest, None)
                .is_ok()
        );
        assert_eq!(handle.queued_observations(), 2);
    }

    fn a_record(ip: Ipv4Addr, ttl: u32) -> Record {
        Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            ttl,
            RData::A(A(ip)),
        )
    }

    fn aaaa_record(ip: Ipv6Addr, ttl: u32) -> Record {
        Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            ttl,
            RData::AAAA(AAAA(ip)),
        )
    }

    fn build_executor_for_test(
        tag: &str,
        async_mode: bool,
        cleanup_on_shutdown: bool,
        address_list4: Option<&str>,
        address_list6: Option<&str>,
        api: Arc<dyn MikrotikApi>,
    ) -> MikrotikExecutor {
        AppClock::start();
        let config = MikrotikConfig {
            address: "127.0.0.1:8728".to_string(),
            connection: None,
            async_mode,
            address_list4: address_list4.map(|v| v.to_string()),
            address_list6: address_list6.map(|v| v.to_string()),
            persistent_items: AHashSet::new(),
            comment_prefix: "oxidns".to_string(),
            min_ttl: DEFAULT_MIN_TTL,
            max_ttl: DEFAULT_MAX_TTL,
            fixed_ttl: None,
            cleanup_on_shutdown,
            max_entries: DEFAULT_MAX_ENTRIES,
        };
        let manager_cfg = AddressListManagerConfig {
            plugin_tag: tag.to_string(),
            address_list4: config.address_list4.clone(),
            address_list6: config.address_list6.clone(),
            persistent_items: config.persistent_items.clone(),
            comment_prefix: config.comment_prefix.clone(),
            min_ttl: config.min_ttl,
            max_ttl: config.max_ttl,
            fixed_ttl: config.fixed_ttl,
            max_entries: config.max_entries,
        };
        MikrotikExecutor {
            tag: tag.to_string(),
            instance_id: NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            active_registered: AtomicBool::new(false),
            metrics: Arc::new(RosMetrics::new(tag.to_string())),
            config,
            manager: Some(AddressListManager::new(api, manager_cfg)),
            manager_handle: None,
            runtime: Mutex::new(None),
        }
    }

    async fn yield_until(description: &str, mut predicate: impl FnMut() -> bool) {
        for _ in 0..64 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met after yielding: {description}");
    }

    #[test]
    fn config_validation_requires_address_list() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
"#,
        )
        .unwrap();
        let err = parse_plugin_config(Some(cfg), false).unwrap_err();
        assert!(err.to_string().contains("address_list4 or address_list6"));
    }

    #[test]
    fn config_validation_rejects_old_route_fields() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
routing_table: "oxidns_dynamic"
"#,
        )
        .unwrap();
        let err = parse_plugin_config(Some(cfg), false).unwrap_err();
        assert!(err.to_string().contains("routing_table"));
    }

    #[test]
    fn config_validation_rejects_old_persistent_route_key() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
persistent_route:
  ips:
    - "1.1.1.1"
"#,
        )
        .unwrap();
        let err = parse_plugin_config(Some(cfg), false).unwrap_err();
        assert!(err.to_string().contains("persistent_route"));
    }

    #[test]
    fn config_validation_defaults_comment_prefix() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
"#,
        )
        .unwrap();
        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        assert_eq!(parsed.comment_prefix, DEFAULT_COMMENT_PREFIX);
        assert_eq!(parsed.max_entries, DEFAULT_MAX_ENTRIES);
        assert_eq!(
            parsed.connection.as_ref().expect("connection").timeouts,
            MikrotikApiTimeouts::default()
        );
    }

    #[test]
    fn config_validation_accepts_routeros_api_timeouts() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
connect_timeout: 10
send_timeout: 11
receive_timeout: 60
address_list4: "oxidns_ipv4"
"#,
        )
        .unwrap();
        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        assert_eq!(
            parsed.connection.as_ref().expect("connection").timeouts,
            MikrotikApiTimeouts::from_secs(10, 11, 60)
        );
    }

    #[test]
    fn config_validation_enables_verified_routeros_tls() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "router.example:8729"
username: "user"
password: "sensitive-credential"
tls: {}
address_list4: "oxidns_ipv4"
"#,
        )
        .unwrap();

        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        let debug = format!("{:?}", parsed.connection.expect("connection"));
        assert!(debug.contains("Secure"));
        assert!(debug.contains("router.example"));
        assert!(!debug.contains("sensitive-credential"));
    }

    #[test]
    fn config_validation_keeps_plaintext_when_tls_is_omitted() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "router.example:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
"#,
        )
        .unwrap();

        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        let debug = format!("{:?}", parsed.connection.expect("connection"));
        assert!(debug.contains("tls: None"));
    }

    #[test]
    fn config_validation_rejects_zero_routeros_api_timeout() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
receive_timeout: 0
address_list4: "oxidns_ipv4"
"#,
        )
        .unwrap();
        let err = parse_plugin_config(Some(cfg), false).unwrap_err();
        assert!(err.to_string().contains("receive_timeout"));
    }

    #[test]
    fn config_validation_accepts_positive_max_entries_and_rejects_zero() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
max_entries: 2048
"#,
        )
        .unwrap();
        assert_eq!(
            parse_plugin_config(Some(cfg), false).unwrap().max_entries,
            2048
        );

        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
max_entries: 0
"#,
        )
        .unwrap();
        let error = parse_plugin_config(Some(cfg), false).unwrap_err();
        assert!(error.to_string().contains("max_entries"));
    }

    #[test]
    fn config_validation_allows_zero_fixed_ttl() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
fixed_ttl: 0
"#,
        )
        .unwrap();
        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        assert_eq!(parsed.fixed_ttl, Some(0));
    }

    #[test]
    fn config_validation_ignores_persistent_item_without_family_list() {
        let cfg = serde_yaml_ng::from_str::<Value>(
            r#"
address: "1.1.1.1:8728"
username: "user"
password: "pass"
address_list4: "oxidns_ipv4"
persistent:
  ips:
    - "2001:db8::1"
"#,
        )
        .unwrap();
        let parsed = parse_plugin_config(Some(cfg), false).unwrap();
        assert!(parsed.persistent_items.is_empty());
    }

    #[test]
    fn persistent_file_content_is_loaded_and_normalized() {
        let files = parse_persistent_files(Some(vec!["persistent.txt".to_string()])).unwrap();
        let (loaded, ignored_by_family) = load_persistent_items_from_content(
            "persistent.files[0]",
            r#"
# comments are ignored
1.1.1.1
2001:db8::/64
0.0.0.0/0
"#,
            Some("oxidns_ipv4"),
            Some("oxidns_ipv6"),
        )
        .unwrap();

        assert_eq!(files, vec!["persistent.txt".to_string()]);
        assert!(loaded.contains(&AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            "oxidns_ipv4".to_string()
        )));
        assert!(
            loaded.contains(
                &AddressListKey::new_with_prefix(
                    IpAddr::V6("2001:db8::".parse().unwrap()),
                    64,
                    "oxidns_ipv6".to_string()
                )
                .unwrap()
            )
        );
        assert!(
            loaded.contains(
                &AddressListKey::new_with_prefix(
                    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                    0,
                    "oxidns_ipv4".to_string()
                )
                .unwrap()
            )
        );
        assert_eq!(ignored_by_family, 0);
    }

    #[test]
    fn comment_codec_roundtrip() {
        let comment = encode_comment(
            "oxidns",
            "mk",
            OwnedCommentKind::Dynamic,
            Some("example.com"),
        );
        let meta = decode_owned_comment("oxidns", "mk", Some(comment.as_str())).unwrap();
        assert_eq!(meta.kind, OwnedCommentKind::Dynamic);
    }

    #[tokio::test]
    async fn dynamic_observation_creates_address_list_entry() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        manager
            .observe_domain(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
                    ttl_secs: 120,
                }],
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state.entries.values().next().unwrap();
        assert_eq!(entry.key.list, "oxidns_ipv4");
        assert_eq!(entry.timeout.as_deref(), Some("120s"));
    }

    #[tokio::test]
    async fn dynamic_observation_with_zero_fixed_ttl_creates_timeless_entry() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("mk");
        cfg.fixed_ttl = Some(0);
        let mut manager = AddressListManager::new(api.clone(), cfg);
        manager
            .observe_domain(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 2)),
                    ttl_secs: 120,
                }],
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state.entries.values().next().unwrap();
        assert_eq!(entry.key.list, "oxidns_ipv4");
        assert_eq!(entry.timeout, None);
    }

    #[tokio::test]
    async fn repeated_dynamic_observation_refreshes_timeout() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        let observed = ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)),
            ttl_secs: 120,
        };
        manager
            .observe_domain("example.com".to_string(), vec![observed])
            .await
            .unwrap();
        manager
            .observe_domain(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: observed.addr,
                    ttl_secs: 300,
                }],
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state.entries.values().next().unwrap();
        assert_eq!(entry.timeout.as_deref(), Some("300s"));
        assert!(state.update_ops >= 1);
    }

    #[tokio::test]
    async fn repeated_dynamic_observation_with_same_ttl_is_suppressed_before_refresh_window() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        let observed = ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(3, 3, 3, 3)),
            ttl_secs: 300,
        };
        manager
            .observe_domain_at_for_test("example.com".to_string(), vec![observed], 0)
            .await
            .unwrap();
        manager
            .observe_domain_at_for_test("example.com".to_string(), vec![observed], 10_000)
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        assert_eq!(state.upsert_v4, 1);
        assert_eq!(state.update_ops, 0);
    }

    #[tokio::test]
    async fn shorter_ttl_does_not_force_early_refresh() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        let ip = IpAddr::V4(Ipv4Addr::new(4, 4, 4, 4));
        manager
            .observe_domain_at_for_test(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 300,
                }],
                0,
            )
            .await
            .unwrap();
        manager
            .observe_domain_at_for_test(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: ip,
                    ttl_secs: 60,
                }],
                10_000,
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state.entries.values().next().unwrap();
        assert_eq!(entry.timeout.as_deref(), Some("300s"));
        assert_eq!(state.update_ops, 0);
    }

    #[tokio::test]
    async fn failed_refresh_clears_cache_and_next_observation_retries_immediately() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        let observed = ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(5, 5, 5, 5)),
            ttl_secs: 120,
        };
        manager
            .observe_domain_at_for_test("example.com".to_string(), vec![observed], 0)
            .await
            .unwrap();
        {
            let mut state = api.state.lock().unwrap();
            state.fail_next_upsert = true;
        }
        let err = manager
            .observe_domain_at_for_test("example.com".to_string(), vec![observed], 90_000)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("mock upsert failure"));
        assert_eq!(manager.dynamic_cache_len(), 0);

        manager
            .observe_domain_at_for_test("example.com".to_string(), vec![observed], 90_000)
            .await
            .unwrap();
        let state = api.state.lock().unwrap();
        assert!(state.update_ops >= 1);
    }

    #[tokio::test]
    async fn persistent_entry_is_created_without_timeout() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("mk");
        cfg.persistent_items.insert(
            AddressListKey::new_with_prefix(
                IpAddr::V4(Ipv4Addr::new(100, 64, 1, 0)),
                24,
                "oxidns_ipv4".to_string(),
            )
            .unwrap(),
        );
        let mut manager = AddressListManager::new(api.clone(), cfg);

        manager.reconcile().await.unwrap();

        let state = api.state.lock().unwrap();
        let entry = state.entries.values().next().unwrap();
        assert_eq!(entry.timeout, None);
        let meta = decode_owned_comment("oxidns", "mk", entry.comment.as_deref()).unwrap();
        assert_eq!(meta.kind, OwnedCommentKind::Persistent);
    }

    #[tokio::test]
    async fn unchanged_persistent_reconcile_does_not_upsert() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(100, 64, 1, 9)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*unchanged".to_string(),
            key: key.clone(),
            timeout: None,
            comment: Some(encode_comment(
                "oxidns",
                "mk",
                OwnedCommentKind::Persistent,
                None,
            )),
        });
        let mut cfg = default_cfg("mk");
        cfg.persistent_items.insert(key);
        let mut manager = AddressListManager::new(api.clone(), cfg);

        manager.reconcile().await.unwrap();

        let state = api.state.lock().unwrap();
        assert_eq!(state.upsert_v4, 0);
        assert_eq!(state.update_ops, 0);
    }

    #[tokio::test]
    async fn empty_reconcile_removes_stale_persistent_then_skips_redundant_scan() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(100, 64, 1, 10)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*stale-persistent".to_string(),
            key,
            timeout: None,
            comment: Some(encode_comment(
                "oxidns",
                "mk",
                OwnedCommentKind::Persistent,
                None,
            )),
        });
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));

        manager.background_reconcile_for_test().await;

        assert_eq!(api.entry_count(), 0);
        assert_eq!(api.list_entries_calls(), 1);

        manager.background_reconcile_for_test().await;
        assert_eq!(api.list_entries_calls(), 1);
    }

    #[tokio::test]
    async fn startup_reconcile_is_applied_as_soon_as_background_scan_finishes() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("startup-reconcile");
        cfg.persistent_items.insert(AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(100, 64, 1, 11)),
            "oxidns_ipv4".to_string(),
        ));
        let manager = AddressListManager::new(api.clone(), cfg);
        let runtime = AddressListManagerRuntime::start("startup-reconcile".to_string(), manager);

        tokio::time::timeout(Duration::from_millis(500), async {
            while api.entry_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("startup reconcile result should be applied without waiting for a timer tick");

        runtime.shutdown(AddressListCleanupScope::none()).await;
    }

    #[tokio::test]
    async fn persistent_update_replaces_removed_entries() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("mk");
        cfg.persistent_items.insert(
            AddressListKey::new_with_prefix(
                IpAddr::V4(Ipv4Addr::new(100, 64, 2, 0)),
                24,
                "oxidns_ipv4".to_string(),
            )
            .unwrap(),
        );
        let mut manager = AddressListManager::new(api.clone(), cfg);
        manager.reconcile().await.unwrap();

        let mut updated = AHashSet::new();
        updated.insert(
            AddressListKey::new_with_prefix(
                IpAddr::V4(Ipv4Addr::new(100, 64, 3, 0)),
                24,
                "oxidns_ipv4".to_string(),
            )
            .unwrap(),
        );
        manager.update_persistent_items(updated).await.unwrap();

        let state = api.state.lock().unwrap();
        assert!(
            state
                .entries
                .values()
                .all(|entry| entry.key.address == IpAddr::V4(Ipv4Addr::new(100, 64, 3, 0)))
        );
    }

    #[tokio::test]
    async fn reconcile_revalidates_stale_persistent_before_delete() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(15, 15, 15, 15)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*401".to_string(),
            key: key.clone(),
            timeout: None,
            comment: Some(encode_comment(
                "oxidns",
                "mk",
                OwnedCommentKind::Persistent,
                None,
            )),
        });
        {
            let mut state = api.state.lock().unwrap();
            state.convert_persistent_to_dynamic_after_list = true;
        }

        let mut cfg = default_cfg("mk");
        cfg.persistent_items.insert(AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(15, 15, 15, 16)),
            "oxidns_ipv4".to_string(),
        ));
        let mut manager = AddressListManager::new(api.clone(), cfg);
        manager.reconcile().await.unwrap();

        let state = api.state.lock().unwrap();
        let entry = state
            .entries
            .get(&MockMikrotikApi::storage_key(&key))
            .unwrap();
        let meta = decode_owned_comment("oxidns", "mk", entry.comment.as_deref()).unwrap();
        assert_eq!(entry.id, "*401");
        assert_eq!(entry.timeout, None);
        assert_eq!(meta.kind, OwnedCommentKind::Dynamic);
    }

    #[tokio::test]
    async fn persistent_entry_wins_over_dynamic_timeout() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)),
            "oxidns_ipv4".to_string(),
        );
        let mut cfg = default_cfg("mk");
        cfg.persistent_items.insert(key.clone());
        let mut manager = AddressListManager::new(api.clone(), cfg);
        manager.reconcile().await.unwrap();

        manager
            .observe_domain(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)),
                    ttl_secs: 60,
                }],
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state
            .entries
            .get(&MockMikrotikApi::storage_key(&key))
            .unwrap();
        assert_eq!(entry.timeout, None);
    }

    #[tokio::test]
    async fn foreign_entry_conflict_is_left_untouched() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*200".to_string(),
            key: key.clone(),
            timeout: Some("300s".to_string()),
            comment: Some("oxidns;pg=other;kind=dynamic;dm=foreign.example".to_string()),
        });
        let mut manager = AddressListManager::new(api.clone(), default_cfg("mk"));
        manager
            .observe_domain(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
                    ttl_secs: 60,
                }],
            )
            .await
            .unwrap();

        let state = api.state.lock().unwrap();
        let entry = state
            .entries
            .get(&MockMikrotikApi::storage_key(&key))
            .unwrap();
        assert_eq!(entry.id, "*200");
        assert_eq!(entry.timeout.as_deref(), Some("300s"));
    }

    #[tokio::test]
    async fn dynamic_cache_prune_removes_expired_entries() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut manager = AddressListManager::new(api, default_cfg("mk"));
        manager
            .observe_domain_at_for_test(
                "example.com".to_string(),
                vec![ObservedAddr {
                    addr: IpAddr::V4(Ipv4Addr::new(7, 7, 7, 7)),
                    ttl_secs: 60,
                }],
                0,
            )
            .await
            .unwrap();
        assert_eq!(manager.dynamic_cache_len(), 1);

        manager
            .prune_dynamic_cache_at_for_test(61_000)
            .await
            .unwrap();
        assert_eq!(manager.dynamic_cache_len(), 0);
    }

    #[tokio::test]
    async fn dynamic_refresh_cache_never_exceeds_configured_capacity() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("cache-cap");
        cfg.max_entries = 2;
        let mut manager = AddressListManager::new(api, cfg);
        let addrs = (1..=3)
            .map(|last| ObservedAddr {
                addr: IpAddr::V4(Ipv4Addr::new(198, 51, 100, last)),
                ttl_secs: 300,
            })
            .collect();

        manager
            .observe_domain_at_for_test("capacity.example".to_string(), addrs, 0)
            .await
            .unwrap();

        assert_eq!(manager.dynamic_cache_len(), 2);
    }

    #[tokio::test]
    async fn reconcile_preserves_remote_dynamic_over_capacity_and_rejects_new_key() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("over-capacity");
        cfg.max_entries = 2;
        let mut manager = AddressListManager::new(api.clone(), cfg);
        let first = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1));
        manager
            .observe_domain_at_for_test(
                "first.example".to_string(),
                vec![ObservedAddr {
                    addr: first,
                    ttl_secs: 300,
                }],
                0,
            )
            .await
            .unwrap();

        for last in [2, 3] {
            let key = AddressListKey::new(
                IpAddr::V4(Ipv4Addr::new(198, 51, 100, last)),
                "oxidns_ipv4".to_string(),
            );
            api.seed_entry(RouterListEntry {
                id: format!("*remote-{last}"),
                key,
                timeout: Some("300s".to_string()),
                comment: Some(encode_comment(
                    "oxidns",
                    "over-capacity",
                    OwnedCommentKind::Dynamic,
                    Some("remote.example"),
                )),
            });
        }

        manager.reconcile().await.unwrap();
        assert_eq!(manager.dynamic_cache_len(), 3);

        let rejected = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4)),
            "oxidns_ipv4".to_string(),
        );
        manager
            .observe_domain_at_for_test(
                "new.example".to_string(),
                vec![ObservedAddr {
                    addr: rejected.address,
                    ttl_secs: 300,
                }],
                1_000,
            )
            .await
            .unwrap();
        assert_eq!(manager.dynamic_cache_len(), 3);
        assert!(
            !api.state
                .lock()
                .unwrap()
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&rejected))
        );
    }

    #[tokio::test]
    async fn reconcile_accepts_manual_dynamic_deletion_until_next_observation() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut cfg = default_cfg("manual-delete");
        cfg.fixed_ttl = Some(0);
        let mut manager = AddressListManager::new(api.clone(), cfg);
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 77));
        let key = AddressListKey::new(ip, "oxidns_ipv4".to_string());
        let observed = vec![ObservedAddr {
            addr: ip,
            ttl_secs: 300,
        }];

        manager
            .observe_domain_at_for_test("manual.example".to_string(), observed.clone(), 0)
            .await
            .unwrap();
        assert_eq!(manager.dynamic_cache_len(), 1);
        api.state
            .lock()
            .unwrap()
            .entries
            .remove(&MockMikrotikApi::storage_key(&key));

        manager.background_reconcile_for_test().await;
        assert_eq!(manager.dynamic_cache_len(), 0);

        manager
            .observe_domain_at_for_test("manual.example".to_string(), observed, 1_000)
            .await
            .unwrap();
        assert_eq!(manager.dynamic_cache_len(), 1);
        assert!(
            api.state
                .lock()
                .unwrap()
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&key))
        );
    }

    #[tokio::test]
    async fn execute_returns_next() {
        let api = Arc::new(MockMikrotikApi::default()) as Arc<dyn MikrotikApi>;
        let mut executor =
            build_executor_for_test("mk", true, false, Some("oxidns_ipv4"), None, api);
        let _ = executor.init_for_test().await;
        let mut ctx = make_context();
        let step = executor.execute(&mut ctx).await.unwrap();
        assert!(matches!(step, ExecStep::Next));
        let _ = executor.destroy().await;
    }

    #[tokio::test]
    async fn continuation_skips_unconfigured_family() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut executor = build_executor_for_test(
            "mk",
            true,
            false,
            None,
            Some("oxidns_ipv6"),
            api.clone() as Arc<dyn MikrotikApi>,
        );
        let _ = executor.init_for_test().await;
        let mut ctx = make_context();
        ctx.set_response(response_with_records(vec![
            a_record(Ipv4Addr::new(1, 1, 1, 1), 300),
            aaaa_record(Ipv6Addr::LOCALHOST, 300),
        ]));
        executor.execute_with_next(&mut ctx, None).await.unwrap();
        yield_until("ipv6 entry upsert", || {
            api.state.lock().unwrap().upsert_v6 >= 1
        })
        .await;

        {
            let state = api.state.lock().unwrap();
            assert_eq!(state.upsert_v4, 0);
            assert!(state.upsert_v6 >= 1);
        }
        let _ = executor.destroy().await;
    }

    #[tokio::test]
    async fn async_false_waits_and_keeps_dns_result_on_add_failure() {
        let api = Arc::new(MockMikrotikApi::default());
        {
            let mut state = api.state.lock().unwrap();
            state.fail_next_upsert = true;
        }
        let mut executor = build_executor_for_test(
            "mk",
            false,
            false,
            Some("oxidns_ipv4"),
            None,
            api as Arc<dyn MikrotikApi>,
        );
        let _ = executor.init_for_test().await;

        let mut ctx = make_context();
        ctx.set_response(response_with_records(vec![a_record(
            Ipv4Addr::new(10, 0, 0, 1),
            300,
        )]));
        executor.execute_with_next(&mut ctx, None).await.unwrap();
        assert!(ctx.response().is_some());
        let _ = executor.destroy().await;
    }

    #[tokio::test]
    async fn async_true_uses_background_manager() {
        let api = Arc::new(MockMikrotikApi::default());
        let mut executor = build_executor_for_test(
            "mk",
            true,
            false,
            Some("oxidns_ipv4"),
            None,
            api.clone() as Arc<dyn MikrotikApi>,
        );
        let _ = executor.init_for_test().await;
        let mut ctx = make_context();
        ctx.set_response(response_with_records(vec![a_record(
            Ipv4Addr::new(6, 6, 6, 6),
            300,
        )]));
        executor.execute_with_next(&mut ctx, None).await.unwrap();
        yield_until("background manager entry creation", || {
            api.entry_count() > 0
        })
        .await;
        assert!(api.entry_count() > 0);
        let _ = executor.destroy().await;
    }

    #[tokio::test]
    async fn startup_reconcile_failure_does_not_block_dns_execution() {
        let api = Arc::new(MockMikrotikApi::default());
        {
            let mut state = api.state.lock().unwrap();
            state.fail_healthcheck = true;
        }
        let mut executor = build_executor_for_test(
            "mk_startup",
            true,
            false,
            Some("oxidns_ipv4"),
            None,
            api.clone() as Arc<dyn MikrotikApi>,
        );
        executor.init_for_test().await.unwrap();

        let mut ctx = make_context();
        ctx.set_response(response_with_records(vec![a_record(
            Ipv4Addr::new(13, 13, 13, 13),
            300,
        )]));
        executor.execute_with_next(&mut ctx, None).await.unwrap();
        assert!(ctx.response().is_some());

        yield_until("dynamic write after startup reconcile failure", || {
            api.entry_count() > 0
        })
        .await;
        let _ = executor.destroy().await;
    }

    #[tokio::test]
    async fn startup_reconcile_scan_does_not_delay_sync_observation() {
        let api = Arc::new(MockMikrotikApi::default());
        {
            let mut state = api.state.lock().unwrap();
            state.list_entries_delay = Some(Duration::from_secs(1));
        }
        let mut executor = build_executor_for_test(
            "mk_sync_startup",
            false,
            false,
            Some("oxidns_ipv4"),
            None,
            api.clone() as Arc<dyn MikrotikApi>,
        );
        executor.init_for_test().await.unwrap();

        let mut ctx = make_context();
        ctx.set_response(response_with_records(vec![a_record(
            Ipv4Addr::new(14, 14, 14, 14),
            300,
        )]));
        tokio::time::timeout(
            Duration::from_millis(200),
            executor.execute_with_next(&mut ctx, None),
        )
        .await
        .expect("sync observation should not wait for startup reconcile scan")
        .unwrap();

        {
            let state = api.state.lock().unwrap();
            assert!(state.upsert_v4 >= 1);
        }
        let _ = executor.destroy().await;
    }

    #[test]
    fn same_tag_instances_coordinate_cleanup_and_reload_restore() {
        let sequence = NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(2, Ordering::Relaxed);
        let tag = format!("address-list-reload-{sequence}");
        let namespace = AddressListOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            address_list4: Some("managed-v4".to_string()),
            address_list6: None,
            comment_prefix: "fdns".to_string(),
        };
        let old_handle = AddressListManagerHandle::new_for_test();

        register_active_address_list_instance(
            tag.as_str(),
            sequence,
            namespace.clone(),
            Arc::new(RosMetrics::new(tag.clone())),
            Some(old_handle.clone()),
        )
        .expect("old runtime");
        register_active_address_list_instance(
            tag.as_str(),
            sequence + 1,
            namespace,
            Arc::new(RosMetrics::new(tag.clone())),
            None,
        )
        .expect("replacement runtime");

        assert_eq!(
            release_active_address_list_instance(tag.as_str(), sequence + 1),
            AddressListCleanupScope::none()
        );
        assert!(old_handle.take_reconcile_for_test());
        assert_eq!(
            release_active_address_list_instance(tag.as_str(), sequence),
            AddressListCleanupScope {
                ipv4: true,
                ipv6: false,
            }
        );
    }

    #[test]
    fn partially_overlapping_reload_cleans_only_unclaimed_address_lists() {
        let sequence = NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(2, Ordering::Relaxed);
        let tag = format!("address-list-partial-reload-{sequence}");
        let old_namespace = AddressListOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            address_list4: Some("shared-v4".to_string()),
            address_list6: Some("old-v6".to_string()),
            comment_prefix: "fdns".to_string(),
        };
        let new_namespace = AddressListOwnershipNamespace {
            address: "192.0.2.10:8728".to_string(),
            address_list4: Some("shared-v4".to_string()),
            address_list6: Some("new-v6".to_string()),
            comment_prefix: "fdns".to_string(),
        };

        register_active_address_list_instance(
            &tag,
            sequence,
            old_namespace,
            Arc::new(RosMetrics::new(tag.clone())),
            None,
        )
        .expect("old runtime");
        register_active_address_list_instance(
            &tag,
            sequence + 1,
            new_namespace,
            Arc::new(RosMetrics::new(tag.clone())),
            None,
        )
        .expect("replacement runtime");

        assert_eq!(
            release_active_address_list_instance(&tag, sequence),
            AddressListCleanupScope {
                ipv4: false,
                ipv6: true,
            }
        );
        assert_eq!(
            release_active_address_list_instance(&tag, sequence + 1),
            AddressListCleanupScope::all()
        );
    }

    #[tokio::test]
    async fn compatible_reload_defers_cleanup_to_last_address_list_instance() {
        let api = Arc::new(MockMikrotikApi::default());
        let tag = format!(
            "address-list-cleanup-{}",
            NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(1, Ordering::Relaxed)
        );
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 88)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*reload-owned".to_string(),
            key: key.clone(),
            timeout: Some("300s".to_string()),
            comment: Some(encode_comment(
                "oxidns",
                tag.as_str(),
                OwnedCommentKind::Dynamic,
                Some("reload.example"),
            )),
        });
        let mut old = build_executor_for_test(
            tag.as_str(),
            true,
            true,
            Some("oxidns_ipv4"),
            None,
            api.clone(),
        );
        let mut replacement = build_executor_for_test(
            tag.as_str(),
            true,
            true,
            Some("oxidns_ipv4"),
            None,
            api.clone(),
        );
        old.init_for_test().await.unwrap();
        replacement.init_for_test().await.unwrap();

        old.destroy().await.unwrap();
        assert!(
            api.state
                .lock()
                .unwrap()
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&key))
        );

        replacement.destroy().await.unwrap();
        assert!(
            !api.state
                .lock()
                .unwrap()
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&key))
        );
    }

    #[tokio::test]
    async fn shutdown_cleanup_removes_only_owned_entries() {
        let api = Arc::new(MockMikrotikApi::default());
        let tag = format!(
            "mk-cleanup-{}",
            NEXT_ADDRESS_LIST_INSTANCE_ID.fetch_add(1, Ordering::Relaxed)
        );
        let owned_key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(11, 11, 11, 11)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*301".to_string(),
            key: owned_key.clone(),
            timeout: Some("300s".to_string()),
            comment: Some(encode_comment(
                "oxidns",
                tag.as_str(),
                OwnedCommentKind::Dynamic,
                Some("example.com"),
            )),
        });
        api.seed_entry(RouterListEntry {
            id: "*302".to_string(),
            key: AddressListKey::new(
                IpAddr::V4(Ipv4Addr::new(12, 12, 12, 12)),
                "oxidns_ipv4".to_string(),
            ),
            timeout: Some("300s".to_string()),
            comment: Some("oxidns;pg=other;kind=dynamic;dm=foreign.example".to_string()),
        });

        let mut executor = build_executor_for_test(
            tag.as_str(),
            true,
            true,
            Some("oxidns_ipv4"),
            None,
            api.clone() as Arc<dyn MikrotikApi>,
        );
        let _ = executor.init_for_test().await;
        let _ = executor.destroy().await;

        let state = api.state.lock().unwrap();
        assert!(
            !state
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&owned_key))
        );
        assert_eq!(state.entries.len(), 1);
    }

    #[tokio::test]
    async fn shutdown_cleanup_revalidates_ownership_before_delete() {
        let api = Arc::new(MockMikrotikApi::default());
        let key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(11, 11, 11, 13)),
            "oxidns_ipv4".to_string(),
        );
        api.seed_entry(RouterListEntry {
            id: "*ownership-race".to_string(),
            key: key.clone(),
            timeout: Some("300s".to_string()),
            comment: Some(encode_comment(
                "oxidns",
                "cleanup-race",
                OwnedCommentKind::Dynamic,
                Some("example.com"),
            )),
        });
        api.state
            .lock()
            .unwrap()
            .convert_owned_to_foreign_after_list = true;
        let mut manager = AddressListManager::new(api.clone(), {
            let mut cfg = default_cfg("cleanup-race");
            cfg.comment_prefix = "oxidns".to_string();
            cfg
        });

        manager
            .shutdown(AddressListCleanupScope::all())
            .await
            .unwrap();

        assert!(
            api.state
                .lock()
                .unwrap()
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&key))
        );
    }

    #[tokio::test]
    async fn shutdown_cleanup_can_target_only_an_unclaimed_address_family() {
        let api = Arc::new(MockMikrotikApi::default());
        let ipv4_key = AddressListKey::new(
            IpAddr::V4(Ipv4Addr::new(11, 11, 11, 14)),
            "oxidns_ipv4".to_string(),
        );
        let ipv6_key = AddressListKey::new(
            IpAddr::V6("2001:db8::14".parse().expect("ipv6")),
            "oxidns_ipv6".to_string(),
        );
        for (id, key) in [("*owned-v4", &ipv4_key), ("*owned-v6", &ipv6_key)] {
            api.seed_entry(RouterListEntry {
                id: id.to_string(),
                key: key.clone(),
                timeout: Some("300s".to_string()),
                comment: Some(encode_comment(
                    "oxidns",
                    "partial-cleanup",
                    OwnedCommentKind::Dynamic,
                    Some("example.com"),
                )),
            });
        }
        let mut manager = AddressListManager::new(api.clone(), default_cfg("partial-cleanup"));

        manager
            .shutdown(AddressListCleanupScope {
                ipv4: false,
                ipv6: true,
            })
            .await
            .expect("partial cleanup");

        let state = api.state.lock().expect("mock lock");
        assert!(
            state
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&ipv4_key))
        );
        assert!(
            !state
                .entries
                .contains_key(&MockMikrotikApi::storage_key(&ipv6_key))
        );
    }
}
