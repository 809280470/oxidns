---
title: Performance Benchmark
sidebar_position: 8
---

# Performance Benchmark

This page presents a periodic benchmark snapshot of OxiDNS `oxidns 1.5.1 (full)` and mosdns `v5.3.4-0-gb732318`, measured with dnsperf `2.15.1`. It is updated for meaningful architecture, request-path, methodology, or milestone changes—not for every release.

Generated: `2026-07-23T12:14:30.417665+08:00`

## Method

- repeats per point: `3`; measured duration: `8s`; warmup: `2s`
- outstanding-query levels: `1,4,16,64,256,1024`
- aggregation: median across repeats; stable capacity excludes points with loss above 0.1%
- server CPU is aggregate process CPU (100% = one fully occupied logical CPU); memory is sampled RSS
- engines run one at a time and their order alternates by repeat/load point

## Environment

- `timestamp=2026-07-23T12:14:30.417665+08:00`
- `hostname=oxidns`
- `kernel=Linux-7.0.14-4-pve-x86_64-with-glibc2.36`
- `cpu=Model name:                              Intel(R) N100`
- `logical_cpus=4`
- `memory=Mem:           512Mi        63Mi        25Mi        88Ki       423Mi       448Mi`
- `git_head=n/a (release artifact benchmark)`
- `git_describe=n/a (release artifact benchmark)`
- `oxidns_version=oxidns 1.5.1 (full)`
- `oxidns_sha256=8cff1b81a6518f4436308750fb24700fd1389d747d171adaca809d1110e73518`
- `mosdns_version=v5.3.4-0-gb732318`
- `mosdns_sha256=5357fbb83c89f0a7acad275b72c33aa70d4c720cb5590525660132b10cee8af9`
- `dnsperf_version=2.15.1`

## How to read the metrics

- **QPS / throughput: higher is better**, provided loss and tail latency remain acceptable.
- **p50/p95/p99/max latency: lower is better**. p99 exposes queueing and long-tail stalls better than an average.
- **Packet loss: lower is better**. A point is stable here only when median loss is at most 0.1%.
- **CPU: lower is better at the same throughput**. CPU alone is not a speed score; 100% means one fully occupied logical CPU.
- **RSS memory: lower is better for the same workload**. It is the process's resident physical memory.
- A flat QPS curve combined with rising p99 means the engine has reached saturation.

## Charts

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/throughput.svg" alt="Maximum stable throughput by scenario" />
  </div>
  <div className="col col--4">
    <p><strong>Higher is better</strong></p>
    <p>A taller bar means more DNS requests completed per second at a stable point. Check p99 and loss as well; a peak reached with excessive loss is not usable capacity.</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/scaling.svg" alt="Throughput scaling by concurrency" />
  </div>
  <div className="col col--4">
    <p><strong>Rising without flattening early is better</strong></p>
    <p>QPS should continue to rise as concurrency increases. A flat curve marks the throughput ceiling; if p99 rises at the same time, the engine is queueing and saturated.</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/tail-latency.svg" alt="p99 tail latency under load" />
  </div>
  <div className="col col--4">
    <p><strong>Lower is better</strong></p>
    <p>Lower p99 means the slowest part of normal traffic remains controlled. A curve that stays flat as concurrency grows is preferable to one that climbs sharply.</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/cpu.svg" alt="CPU at maximum stable throughput" />
  </div>
  <div className="col col--4">
    <p><strong>Lower is better at equal throughput</strong></p>
    <p>100% is one fully occupied logical CPU. Read CPU together with QPS: using more CPU can be reasonable when it produces proportionally more throughput.</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/memory.svg" alt="Resident memory at maximum stable throughput" />
  </div>
  <div className="col col--4">
    <p><strong>Lower is better for the same workload</strong></p>
    <p>RSS is physical memory resident for the process. A shorter bar means lower memory pressure when scenario, rules, and load are equivalent.</p>
  </div>
</div>

## Maximum stable point by scenario

| Scenario | OxiDNS QPS | mosdns QPS | OxiDNS p99 | mosdns p99 | OxiDNS CPU | mosdns CPU | OxiDNS RSS | mosdns RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 02-cache-hotpath | 119,538.7 | 102,457.7 | 2.047 ms | 2.943 ms | 204.5% | 179.6% | 14.7 MiB | 23.1 MiB |
| 06-local-answers | 113,114.7 | 109,150.1 | 1.983 ms | 2.559 ms | 204.5% | 154.7% | 13.7 MiB | 23.1 MiB |
| 08-domain-set | 136,180.9 | 36,359.3 | 1.727 ms | 12.031 ms | 189.6% | 323.0% | 34.6 MiB | 44.4 MiB |
| 43-composite-provider-chain | 137,398.4 | 25,925.3 | 1.855 ms | 6.399 ms | 194.7% | 344.2% | 35.1 MiB | 43.9 MiB |
| 47-server-local-udp | 116,888.4 | 109,793.7 | 1.951 ms | 2.431 ms | 199.6% | 149.8% | 13.7 MiB | 22.8 MiB |

## Objective assessment

- **The lead on simple local paths is modest, not universal.** Maximum stable throughput versus mosdns is warm cache +16.7%, local answers +3.6%, minimal UDP +6.5%. Local answers and the minimal UDP path differ by only single-digit percentages, and some mid-concurrency points are close.
- **The difference is substantial on complex rule paths.** OxiDNS maximum stable throughput is domain set 3.75×, composite provider chain 5.30×; p99 is domain set 85.6% lower, composite provider chain 71.0% lower. The largest gains therefore come from dataset lookup and the composite provider/matcher chain, not merely the UDP framework.
- **Resident memory is consistently lower.** OxiDNS RSS is lower in every measured scenario, by about 20.0%–40.6%.
- **CPU must be read together with throughput.** At the local-answer and minimal-UDP stable points, OxiDNS uses more CPU for only a modest throughput increase, so those results do not support a blanket CPU-efficiency claim. On the complex paths it delivers both higher throughput and lower CPU.
- **The strength of the conclusion is limited.** Three repeats per point are suitable for a periodic engineering comparison, but they do not replace larger samples, confidence intervals, or cross-machine replication. Treat these values as a profile for this host and workload, not a universal capacity promise.

## Representativeness assessment

This matrix represents the stable local UDP request path: minimal listener overhead, local answers, warm-cache behavior, dataset lookup, and a composite provider/matcher chain. Its load sweep exposes scaling, saturation, and queueing instead of relying on one peak-QPS number.

It does not cover cold start or reload cost, TCP/DoT/DoH/DoQ, cache-miss-heavy traffic, public upstream quality, multi-machine network effects, or host-integrated side effects such as ipset/nftset. Production-capacity work needs a separate load-generator host and dedicated matrices for those paths.

## Interpretation limits

This is a same-host loopback comparison. It is representative of local request-path cost and concurrency scaling on the recorded machine, not public-upstream quality or production capacity on other hardware. External-forward scenarios must be reported separately because upstream/network variance can dominate engine cost.
