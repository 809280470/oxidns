---
title: Roadmap
sidebar_position: 5
---

import RoadmapTimeline, { RoadmapItem } from '@site/src/components/RoadmapTimeline';

# Roadmap

OxiDNS's complete development roadmap since v0.1.0. Upcoming work appears at the top; completed milestones and past versions follow in reverse chronological order.

<RoadmapTimeline>

<RoadmapItem type="future" label="Planned" title="DHCP Service and DNS Integration" desc="Unify lease management, address allocation, and local DNS naming" num={5}>

Add an optional DHCP service covering address pools, static leases, lease persistence, and common DHCP options. Hostname/IP data from leases will be exposed safely to local DNS policy, while the management API and WebUI provide lease inspection, configuration, and essential administrative actions. The DHCP runtime will remain decoupled from the DNS plugin pipeline and integrate through an explicit data interface rather than placing lease state on the request hot path.

</RoadmapItem>

<RoadmapItem type="future" label="Planned" title="Cache Plugin Refactor and Performance Work" desc="Reshape cache architecture, reduce hit-path cost, and improve stability at scale" num={4}>

Further separate cache lookup, admission, TTL decisions, lazy refresh, eviction maintenance, persistence, and metrics so DNS cache semantics remain distinct from generic storage primitives. The performance work will target message cloning, temporary allocations, timestamp write amplification, and lock contention on cache hits; improve same-key refresh deduplication, batched expiration, and sampled-LRU behavior; and add focused benchmarks and regressions for hit, miss, stale, write, concurrent refresh, and large-capacity workloads.

</RoadmapItem>

<RoadmapItem type="future" label="Soon" title="Plugin API Expansion & WebUI Wiring" desc="Fill in management APIs for existing plugins; wire them into the WebUI" num={2}>

Apply the rule "per-entity / status / action → API; counters / histograms / low-cardinality gauges → metrics" and fill in runtime management APIs (upstream probe, job pause / run-now, rule enumeration, hot-client buckets, cache top-N, …) for existing plugins such as `forward`, `cron`, `download`, `script`, `ip_selector`, `cache`, and `rate_limiter`. Wire each endpoint into a corresponding WebUI detail panel and round out Prometheus metrics along the same boundary to improve observability and day-2 operations.

</RoadmapItem>

<RoadmapItem type="active" label="In progress" title="Standard-mode WebUI" desc="A form-driven, turnkey experience built on standard scenario templates">

Provide a standardized configuration entry point for users who do not want to edit YAML directly. Common scenarios such as ad blocking, anti-poisoning, family filtering, and split-tunnel acceleration will be configured through forms, toggles, and presets, with a path into the advanced configuration mode for complete rule editing. The goal is to reduce initial setup and day-to-day maintenance cost without weakening OxiDNS's policy model.

</RoadmapItem>

<RoadmapItem type="version" label="2026-07-17" title="MikroTik RouterOS Plugin Pair" desc="ros_address_list and ros_route synchronize address lists and static routes">

Completed the `ros_address_list` and `ros_route` RouterOS executors. `ros_address_list` synchronizes A/AAAA addresses from DNS responses into RouterOS address lists, while `ros_route` installs per-address static routes in a selected routing table. They share asynchronous queues, batching, TTL leases, reconnect, reconciliation, deduplication, shutdown cleanup, and metrics. The route plugin also supports conntrack-aware deferred deletion to reduce the risk of removing routes that still carry active connections.

</RoadmapItem>

<RoadmapItem type="version" label="2026-07-02" title="OpenWrt LuCI App" desc="Use luci-app-oxidns to install the core, manage the service, edit config, and view logs from LuCI">

Added [`luci-app-oxidns`](https://github.com/svenshi/luci-app-oxidns): OpenWrt users can install the OxiDNS core, manage the init service, edit configuration, and view logs from LuCI under `Services -> OxiDNS`. The LuCI app does not embed the OxiDNS core; on first install it downloads and verifies the official Linux musl release archive from GitHub Releases. Future core upgrades continue to use OxiDNS's built-in upgrade capability.

</RoadmapItem>

<RoadmapItem type="version" title="IP Optimization" desc="Parallel latency testing of A/AAAA addresses; return the lowest-latency IP" version="v1.2.0" date="2026-06-03">

Test multiple A/AAAA addresses from a DNS response in parallel and return the lowest-latency IP to the client, improving real-world access speed. This capability shipped with v1.2.0.

</RoadmapItem>

<RoadmapItem type="version" title="Self-learning Domain Set" desc="learn_domain auto-captures query names into a persistent dynamic_domain_set; rules managed from the WebUI" version="v1.2.0" date="2026-06-03">

A new `learn_domain` executor paired with the `dynamic_domain_set` provider: the executor captures matching query names from the resolver pipeline and writes them to `dynamic_domain_set`, which persists to disk and hot-reloads — no manual rule list maintenance. The WebUI adds a Detail tab for `dynamic_domain_set` to browse, add, remove, and clear rules in place, and every config field of `learn_domain` / `dynamic_domain_set` ships with field-level documentation.

</RoadmapItem>

<RoadmapItem type="version" title="Custom Builds" desc="minimal / standard / full bundle presets; minimal binary ~40% of full" version="v1.2.0" date="2026-06-03">

Split compilation by plugin module — users fork the repo, pick only the plugins they need, and produce a lean custom binary.

`minimal` / `standard` / `full` presets are live. Every protocol stack and management surface is feature-gated — `api` / `webui` / `metrics`, `server-dot/doh/doq/doh3`, `upstream-dot/doh/doq/doh3`, plus MikroTik, query_recorder, ipset/nftset, cron, script, upgrade, download, http_request, reverse_lookup, geo providers, and adguard_rule. `AppController` / `LogBuffer` now live under `src/infra/` as runtime infrastructure, so `minimal` excludes hyper / rustls / quinn and lands at ~40% of `full` (≈ 8.9 MB vs 21 MB).

</RoadmapItem>

<RoadmapItem type="version" title="Stability iterations" desc="nftset/ipset fixes; WebUI polish; Monaco self-hosted; provider memory optimization" version="v1.1.x" date="2026-05">

Fixed `nftset` interval encoding (EINVAL) and `ipset` byte-order; query_recorder history clearing; WebUI mobile polish; Monaco editor self-hosted; provider/matcher memory further optimized.

</RoadmapItem>

<RoadmapItem type="version" title="Env config; upgrade rework" desc="${ENV_VAR} placeholders in config; upgrade rework (Windows); aggregate stats" version="v1.1.0" date="2026-05-25">

Config `${ENV_VAR}` placeholders; `upgrade` fully supports Windows in-place update; `query_recorder` aggregate stats and rankings.

</RoadmapItem>

<RoadmapItem type="version" title="WebUI launch" desc="Live logs, config history with rollback, plugin metrics, execution flow visualization" version="v1.0.0" date="2026-05-20">

Full WebUI first release: live log viewer, config history (save / apply / rollback), per-plugin metrics, cache management, `query_recorder` execution waterfall, offline config editing.

</RoadmapItem>

<RoadmapItem type="version" title="query_recorder rewrite" desc="Streaming background pipeline; matcher hit stats and execution path tracing" version="v0.5.0" date="2026-04-27">

`query_recorder` rebuilt as a streaming pipeline; added execution path stats; unified time handling with `jiff`.

</RoadmapItem>

<RoadmapItem type="version" title="Provider optimization; CLI upgrade" desc="Provider memory and reload optimized; any_match plugin; one-command self-update" version="v0.4.0" date="2026-04-19">

Reduced provider memory; added `any_match`, `upgrade` CLI subcommand; HTTP/3 `Alt-Svc` advertisement.

</RoadmapItem>

<RoadmapItem type="version" title="http_request plugin" desc="HTTP calls inside the DNS pipeline; wire buffer reuse on hot path" version="v0.3.0" date="2026-04-14">

New `http_request` plugin; introduced wire-buffer object pool to cut per-query allocations.

</RoadmapItem>

<RoadmapItem type="version" title="Plugin expansion" desc="script / download / adguard_rule plugins; startup reload; SOCKS5 proxy" version="v0.2.0" date="2026-04-02">

Added `script` executor, `download` (SOCKS5 supported), `adguard_rule` matching, and `reload` hot-reload executor.

</RoadmapItem>

<RoadmapItem type="version" title="Initial release" desc="Core UDP/TCP DNS proxy, multi-upstream forwarding, local caching" version="v0.1.0" date="2026-03-28">

First public release: dual-stack UDP/TCP, multi-upstream forwarding, local caching, base rule-matching framework.

</RoadmapItem>

</RoadmapTimeline>

<div style={{borderLeft: '4px solid var(--ifm-color-primary)', background: 'rgba(15, 118, 110, 0.06)', borderRadius: '0 12px 12px 0', padding: '0.9rem 1.2rem', marginTop: '2rem'}}>
  <p style={{margin: 0, lineHeight: 1.75}}><strong>Long-term direction: plugin ecosystem</strong></p>
  <ul style={{margin: '0.5rem 0 0', paddingLeft: '1.25rem', lineHeight: 1.75}}>
    <li><strong>WebAssembly plugins</strong>: Explore WASM-based third-party plugins so developers can write and distribute plugins in any language without modifying OxiDNS, with sandboxing by default.</li>
    <li><strong>Dynamic library plugins</strong>: Explore native plugin loading via shared libraries (.so / .dylib) for the highest-performance scenarios, with independent compile and distribute, loaded at runtime.</li>
  </ul>
</div>
