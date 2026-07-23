---
title: 性能测试
sidebar_position: 8
---

# 性能测试

本页展示 OxiDNS `oxidns 1.5.1 (full)` 与 mosdns `v5.3.4-0-gb732318` 的阶段性实测快照，dnsperf 版本为 `2.15.1`。仅在架构、关键请求路径、测试口径或重要里程碑发生明显变化时更新，不要求每个版本重复测试。

本轮数据采集于 `2026-07-23T12:14:30.417665+08:00`。测试参数见[完整原始报告](/benchmarks/staged/report.txt)。每个指标取多次重复的中位数；最大稳定吞吐只接受丢包率不高于 0.1% 的点。进程 CPU 的 100% 表示占满一个逻辑核。

## 被测环境

* CPU：`Model name:                              Intel(R) N100`，逻辑核 `4`
* 内存：`Mem:           512Mi        63Mi        25Mi        88Ki       423Mi       448Mi`
* OxiDNS：`oxidns 1.5.1 (full)`，SHA-256 `8cff1b81a6518f4436308750fb24700fd1389d747d171adaca809d1110e73518`
* mosdns：`v5.3.4-0-gb732318`，SHA-256 `5357fbb83c89f0a7acad275b72c33aa70d4c720cb5590525660132b10cee8af9`
* dnsperf：`2.15.1`

## 指标怎么看

* **QPS / 吞吐量：越高越好**，但前提是丢包率和尾延迟仍在可接受范围内。
* **p50、p95、p99、最大延迟：越低越好**。p99 表示 99% 已完成请求的响应时间不超过该值，比平均值更容易看出排队和长尾卡顿。
* **丢包率：越低越好**。本报告只有在丢包率中位数不超过 0.1% 时，才把该并发点计为“稳定”。
* **CPU：相同吞吐量下越低越好**。不能脱离 QPS 单看 CPU；如果使用更多 CPU 换来了明显更高吞吐，仍可能是合理结果。这里 100% 表示占满一个逻辑核。
* **RSS 内存：相同负载下越低越好**，表示测试过程中进程实际驻留在物理内存中的容量。
* 看折线图时，理想状态是并发增加后 QPS 继续上升，同时 p99 和丢包保持稳定；如果 QPS 已经走平而 p99 快速升高，说明服务已经进入饱和区。

## 吞吐与并发扩展

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/throughput.svg" alt="各场景最大稳定吞吐量柱状图" />
  </div>
  <div className="col col--4">
    <p><strong>越高越好</strong></p>
    <p>柱子越高表示稳定状态下每秒完成的 DNS 请求越多。仍需结合 p99 和丢包判断，不能把高丢包下的峰值当作有效容量。</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/scaling.svg" alt="并发扩展折线图" />
  </div>
  <div className="col col--4">
    <p><strong>上升且不过早走平更好</strong></p>
    <p>并发增加时 QPS 应继续上升。曲线走平表示接近吞吐上限；此时若 p99 同时快速升高，说明已经进入排队和饱和区。</p>
  </div>
</div>

## 尾延迟

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/tail-latency.svg" alt="p99 尾延迟折线图" />
  </div>
  <div className="col col--4">
    <p><strong>越低越好</strong></p>
    <p>p99 越低，绝大多数请求的最慢部分越可控。随着并发增加仍保持平缓的曲线，比只看平均延迟更可靠。</p>
  </div>
</div>

## CPU 与内存

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/cpu.svg" alt="CPU 占用柱状图" />
  </div>
  <div className="col col--4">
    <p><strong>相同吞吐量下越低越好</strong></p>
    <p>100% 等于占满一个逻辑核。CPU 必须和 QPS 配合看：CPU 更高但吞吐提升更大并不一定更差，也可进一步比较每万 QPS 的 CPU 成本。</p>
  </div>
</div>

<div className="row margin-bottom--lg">
  <div className="col col--8">
    <img src="/img/benchmarks/staged/memory.svg" alt="RSS 内存柱状图" />
  </div>
  <div className="col col--4">
    <p><strong>相同负载下越低越好</strong></p>
    <p>RSS 表示进程实际驻留的物理内存。柱子越低，常驻内存压力越小；比较时应确保场景、规则数据和负载一致。</p>
  </div>
</div>

## 各场景最大稳定点

| 场景 | OxiDNS QPS | mosdns QPS | OxiDNS p99 | mosdns p99 | OxiDNS CPU | mosdns CPU | OxiDNS RSS | mosdns RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 02-cache-hotpath | 119,538.7 | 102,457.7 | 2.047 ms | 2.943 ms | 204.5% | 179.6% | 14.7 MiB | 23.1 MiB |
| 06-local-answers | 113,114.7 | 109,150.1 | 1.983 ms | 2.559 ms | 204.5% | 154.7% | 13.7 MiB | 23.1 MiB |
| 08-domain-set | 136,180.9 | 36,359.3 | 1.727 ms | 12.031 ms | 189.6% | 323.0% | 34.6 MiB | 44.4 MiB |
| 43-composite-provider-chain | 137,398.4 | 25,925.3 | 1.855 ms | 6.399 ms | 194.7% | 344.2% | 35.1 MiB | 43.9 MiB |
| 47-server-local-udp | 116,888.4 | 109,793.7 | 1.951 ms | 2.431 ms | 199.6% | 149.8% | 13.7 MiB | 22.8 MiB |

## 客观评价

* **简单本地路径优势温和，并非全面拉开差距。** 最大稳定吞吐相对 mosdns 分别为：缓存热路径 +16.7%、本地回答 +3.6%、最小 UDP 路径 +6.5%。其中本地回答和最小 UDP 路径只有个位数差距；部分中等并发点两者接近，因此不应概括成所有负载下都有大幅领先。
* **复杂规则路径差异明显。** OxiDNS 最大稳定吞吐相对 mosdns：域名集合为 3.75 倍、复合 provider 链为 5.30 倍；p99 降幅分别为：域名集合低 85.6%、复合 provider 链低 71.0%。这说明本轮优势主要集中在真实数据集查询和复合 provider/matcher 链，而不是只来自 UDP 协议框架。
* **常驻内存更低。** 五个场景中 OxiDNS 的 RSS 均低于 mosdns，降幅约为 20.0%–40.6%。
* **CPU 结果需要结合吞吐解释。** 在本地回答和最小 UDP 路径的最大稳定点，OxiDNS 使用了更多 CPU，但吞吐只小幅提高；这些场景下不能仅凭 QPS 宣称效率全面更好。复杂规则路径中 OxiDNS 则同时取得更高吞吐和更低 CPU。
* **结论强度有限。** 每点 3 次重复适合作为阶段性工程对比，但不足以替代更大样本、置信区间和跨机器复测。这里的数值应理解为该主机和该工作负载下的性能轮廓，不是通用容量承诺。


## 代表性判断

本矩阵对**稳定的本地 UDP 请求路径**具有代表性：最小监听器、本地回答、热缓存、真实域名集合查询和复合 provider/matcher 链被分开测试，并通过多档并发展示扩展、饱和与排队，而不是只比较一个峰值 QPS。

它不能代表冷启动/热重载、TCP/DoT/DoH/DoQ、以缓存未命中为主的流量、公网上游质量、跨机网络开销，或 ipset/nftset 等宿主机副作用。生产容量测试还应使用独立压测机，并为这些路径建立单独矩阵。

## 口径限制

本轮是同机 loopback 对比，适合观察本地请求路径成本、并发扩展和排队，不代表其他硬件上的生产容量，也不把公网转发上游波动混进默认结论。可下载：[完整报告](/benchmarks/staged/report.txt)、[聚合 TSV](/benchmarks/staged/summary.tsv)、[逐轮 JSON](/benchmarks/staged/summary.raw.json)、[环境快照](/benchmarks/staged/environment.json)。
