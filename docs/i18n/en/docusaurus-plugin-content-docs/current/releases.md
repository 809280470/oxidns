---
title: Release Notes
sidebar_position: 4
---

import ReleaseCard from '@site/src/components/ReleaseCard';

# Release Notes

## 2026-07

<div className="release-stack">
   <ReleaseCard version="v1.5.1" badge="Patch Release" date="2026-07-22" defaultOpen>
       **Release Scope**

       - Patch Release. v1.5.1 focuses on matcher runtime control, upgrade operations, and WebUI quality. It expands temporary matcher switching into tri-state base-result controls, adds force and post-upgrade cleanup controls, and delivers a concentrated set of localization, polling, log-viewer, and plugin-card fixes.
       - Existing YAML configurations upgrade directly, but the matcher runtime management API has a breaking change. Clients using that API must migrate before upgrading.

       **Changes**

       - `feat/fix(matcher)`: replace the runtime switch with `normal`, `always_false`, and `always_true` modes. Both fixed modes skip the matcher implementation and fix its base Boolean value; each `$tag` or `!$tag` reference then applies its own outer negation, so positive and negated results remain opposites. `sequence`, `any_match`, and query recorder now track both the fixed mode and effective match result, with regression coverage for shared controls.
       - `feat(upgrade)`: let the management API and WebUI set `force` to reinstall a release even when it is already current. Add `cleanup` to control removal of download caches and backups after a successful upgrade. The WebUI persists both preferences and generates equivalent CLI commands. Cleanup releases the upgrade lock first and reports cleanup failures without changing a successful apply result.
       - `fix(webui/i18n)`: complete Chinese and English localization for RouterOS, plugin definitions, metrics, configuration history, and console components, including locale-aware date formatting. Add coverage auditing to prevent missing English translations or fallback to Chinese.
       - `fix(webui/runtime)`: schedule runtime polling according to page visibility while retaining background metric collection. Isolate responses, metric baselines, and update-check caches between backend connections; fetch matcher state only on initial load or explicit refresh; reset QPS sampling after long gaps.
       - `feat(webui/logs)`: add persisted timestamp formats, optional elapsed-time display, adaptive duration units, and compact target paths. Unify plugin configuration and metric cards around an adaptive grid, with better RouterOS write-result, timestamp-metric, and system-memory presentation.
       - `perf(build)`: use size-oriented release optimization, fat LTO, one codegen unit, and symbol stripping. Limit Tokio, QUIC, and TLS dependencies to required features, and attempt UPX compression for minimal and standard release artifacts without making compression failure block publication. Exclude development-only benchmarks, site documentation, and WebUI sources from the crates.io source package to stay clear of the registry size limit.
       - `deps/ci/release`: update `wincode`, `syn`, other Rust dependencies, and GitHub Actions. Temporarily apply the RouterOS unbounded-response-channel fix through a Git patch to prevent protocol-event loss under burst traffic. The patch is not published separately, and crates.io publication uses `--no-verify` until upstream ships the fix. GitHub Release and Telegram announcements now share version-heading-validated release notes; announcements target the configured topic and are pinned automatically.

       **Compatibility and Upgrade Notes**

       - The root crate version is `1.5.1`; publishable workspace crate versions remain unchanged, and the release tag should be `v1.5.1`. The RouterOS patch is not published as a separate crate.
       - v1.5.0 YAML configurations upgrade directly. No configuration fields are added, renamed, or given new defaults. Run `oxidns check -c <config-file>` before replacing the binary.
       - **Matcher API migration**: `POST /api/plugins/<matcher_tag>/enable` and `/disable` are removed. Use `POST /api/plugins/<matcher_tag>/mode` with `{ "mode": "normal|always_false|always_true" }`. The `GET /status` response replaces `enabled` with `mode`. Unmigrated automation and third-party controllers will receive a 404 or fail response decoding.
       - Fixed matcher modes exist only in the current runtime and reset to `normal` after an application reload or process restart. The mode is shared by matcher tag, while reference semantics remain local: with `always_false`, `$tag` misses and `!$tag` matches; `always_true` produces the opposite results. Control the `any_match` matcher itself when the whole composition must be fixed.
       - The WebUI defaults to deleting download caches and backups after a successful upgrade. Disable post-upgrade cleanup when local rollback files must be retained. Use `force` only to repair a damaged installation or redeploy the same version, after confirming the intended bundle and platform.
       - Minimal and standard artifacts may be UPX-compressed. Environments with binary scanners, allowlists, or integrity baselines should verify the release asset digest again and smoke-test startup, upgrade, and rollback before production replacement.
   </ReleaseCard>

   <ReleaseCard version="v1.5.0" badge="Minor Release" date="2026-07-19">
       **Release Scope**

       - Minor Release. v1.5.0 centers on RouterOS policy synchronization and live operations: it adds the `ros_route` static policy-route plugin, comprehensively rebuilds `ros_address_list`, and adds management API plus WebUI runtime controls for matchers and providers.
       - It also adds a configurable `response` executor, timezone-aware `time` matching, query-recorder space reclamation, stronger CNAME response selection, advanced WebUI configuration, and broader OpenWrt, ARMv7, and container delivery support.

       **Changes**

       - `feat(routeros)`: add `ros_route` to synchronize observed DNS A/AAAA addresses into per-IP static routes in a selected RouterOS routing table, with IPv4/IPv6 gateways, distance, TTL leases, persistent IP/CIDR entries, optional conntrack-delayed removal, startup recovery, and bounded shutdown cleanup.
       - `refactor(routeros)`: share TLS/API-SSL transport, bounded parallel batching, key-coalescing queues, leases, reconciliation, retry, and lifecycle primitives across `ros_address_list` and `ros_route`. RouterOS outages no longer block DNS startup; synchronous mode is limited by `wait_timeout` and never changes the DNS response on failure; deletion revalidates internal ID, target key, and ownership comment to avoid touching foreign entries.
       - `feat(runtime_control)`: all matchers gain live status, enable, and disable controls; providers gain serialized reload with conflicting concurrent requests rejected. The management API, logs, and WebUI plugin detail panels expose these controls, while builds without the API feature keep them disabled.
       - `feat(response/matcher)`: add a `response` executor that builds Answer, Authority, and Additional sections from zone-record templates with RCODE, flags, and `{qname}`/`{qclass}` placeholders. The `time` matcher gains IANA timezones, multiple periods, overnight windows, weekdays, and month-day constraints.
       - `refactor(query_recorder)`: coordinate readers, writers, and maintenance sharing one SQLite database. Retention cleanup and manual history clearing now delete in batches, truncate WAL, migrate legacy databases to incremental auto-vacuum, reclaim disk space, and keep the in-memory tail consistent with stored results.
       - `fix(dns/forward/cache)`: add a shared query-aware response classifier. Concurrent upstream selection distinguishes complete positives, definitive negatives, and incomplete aliases; bare CNAME responses no longer win early, vote as negatives, or populate address caches. Cache dump/load, lazy refresh, TTL, and persisted-age behavior are hardened while repeated CNAME scans are reduced.
       - `feat(webui)`: add collapsible advanced configuration fields while preserving explicit defaults, `false`, `0`, and empty objects. Replace Monaco with a locally hosted CodeMirror YAML editor with stronger validation, completion, array, and time-period serialization. Improve DNS traffic, process memory, and unavailable-metrics dashboard states.
       - `feat(release/operations)`: add an ARMv7 release target; let the installer deploy `luci-app-oxidns` and its Chinese package on OpenWrt; move the container to an Alpine build stage plus BusyBox musl runtime; split upgrade discovery, digest verification, archive handling, and binary/WebUI installation while correcting full/slim target selection.
       - `fix(config/api/health)`: reject path-unsafe plugin tags and the reserved quick-setup namespace, consistently URL-encode plugin API routes, and report health only after plugin initialization. Synchronize new `response` and RouterOS configuration, features, and build-info capabilities.
       - `deps/ci/docs`: update dependencies and GitHub Actions, including the `oxidns-proto` nightly-Clippy fix. Clarify server, plugin, infra, upgrade, and provider/V2Ray module boundaries and update bilingual API, plugin, OpenWrt, installation, and operations documentation.

       **Compatibility and Upgrade Notes**

       - The root crate version is `1.5.0`; `oxidns-proto` is updated to `0.1.4`; the release tag should be `v1.5.0`.
       - Most v1.4.0 configurations upgrade directly; all newly introduced capabilities are optional. Run `oxidns check` before upgrading. Unsafe plugin tags and reserved quick-setup tags now fail validation and must be renamed together with all references.
       - **RouterOS ownership migration**: the default `ros_address_list.comment_prefix` changes from `fdns` to `oxi`. To continue recognizing, refreshing, or cleaning entries created by older releases, explicitly keep `comment_prefix: fdns` in the existing plugin configuration; handle the old namespace before switching to `oxi`.
       - `ros_address_list` adds optional `tls`, `wait_timeout`, and `queue_capacity`; existing address-list, persistent, and TTL settings remain valid. `cleanup_on_shutdown` still defaults to `true`, and application reload shuts down the old instance first. Deployments requiring policy continuity should evaluate `false` and must not run two processes with the same tag, comment prefix, and target list.
       - `ros_route` is new and requires a pre-created RouterOS routing table/rule plus at least one gateway. `fixed_ttl: 0` creates dynamic routes that never expire naturally, and dynamic leases have no entry-count cap; assess RouterOS routing-table and OxiDNS memory capacity first. Custom builds can select `plugin-ros-address-list` or `plugin-ros-route`; `plugin-mikrotik` remains the aggregate feature.
       - Legacy query-recorder databases migrate to incremental auto-vacuum on the first retention cleanup or manual history clear. The first migration/reclaim of a large database may create noticeable disk I/O, so schedule it outside peak traffic and keep free space available.
       - `forward.concurrent: 1` still does not retry upstreams that were not started. An incomplete CNAME response can still be returned when no better result exists, but is not cached as an address answer. Legacy CNAME-only address entries in cache dumps are discarded during load or hit validation.
       - The container now uses a musl/BusyBox runtime while retaining CA and timezone data. Container, OpenWrt, and ARMv7 deployments should verify startup arguments, mounts, timezone behavior, and upgrade/rollback procedures before production replacement.
   </ReleaseCard>
</div>

## Archive

Detailed notes are partitioned by month so the current page remains bounded:

- [June 2026](releases/2026-06.md)
- [May 2026](releases/2026-05.md)
- [April 2026](releases/2026-04.md)
- [March 2026](releases/2026-03.md)

Before upgrading, read the target release and every intervening “Configuration and Upgrade Notes” section. Historical notes describe behavior at that time; current parameters come from the code and documentation at the matching release tag.
