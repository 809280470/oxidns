---
title: 路线图
sidebar_position: 5
---

import RoadmapTimeline, { RoadmapItem } from '@site/src/components/RoadmapTimeline';

# 路线图

以下是 OxiDNS 自 v0.1.0 发布以来的完整开发路线图。最新计划在最上方，已完成里程碑和历史版本按时间倒序排列。

<RoadmapTimeline>

<RoadmapItem type="future" label="规划中" title="DHCP 服务与 DNS 联动" desc="提供租约管理、地址分配和本地域名解析的一体化网络服务" num={5}>

新增可选 DHCP 服务，覆盖地址池、静态租约、租约持久化和常用 DHCP options；将租约中的 hostname/IP 作为本地 DNS 数据安全地提供给解析策略，并通过管理 API 与 WebUI 查看租约、调整配置和执行必要的管理动作。实现上保持 DHCP runtime 与 DNS 插件管线解耦，通过明确的数据接口完成联动，避免把租约状态直接耦合进请求热路径。

</RoadmapItem>

<RoadmapItem type="future" label="规划中" title="Cache 插件重构与性能优化" desc="重整缓存架构，降低命中热路径开销并改善大容量场景稳定性" num={4}>

进一步拆分 Cache 插件的查询、准入、TTL 决策、lazy refresh、淘汰维护、持久化与指标边界，使 DNS 缓存语义和通用存储原语保持清晰分层；重点减少 cache hit 中的消息 clone、临时分配、时间戳写放大和锁竞争，优化相同 key 的并发刷新去重、批量过期清理与 sampled LRU 行为，并补齐 hit / miss / stale / write、并发刷新和大容量缓存的专项 benchmark 与回归测试。

</RoadmapItem>

<RoadmapItem type="future" label="敬请期待" title="插件 API 扩展与 WebUI 接入" desc="为已有插件补齐管理 API，WebUI 接入新增端点与详情面板" num={2}>

按"个体枚举 / 状态查询 / 动作触发"归 API、"计数器 / 直方图 / 低基数 gauge"归 metrics 的划分原则，为 `forward`、`cron`、`download`、`script`、`ip_selector`、`cache`、`rate_limiter` 等已有插件补齐运行时管理 API（上游探测、任务暂停 / 立即执行、规则枚举、热点客户端、缓存 top-N 等），并在 WebUI 的插件详情面板里接入对应端点；同时按上述边界补全 Prometheus 指标，提升整体可观测性与运维效率。

</RoadmapItem>

<RoadmapItem type="active" label="进行中" title="标准模式 WebUI" desc="基于标准场景模板，提供表单化、开箱即用的配置体验">

面向不想直接编写 YAML 的用户，提供一套标准化配置入口。常见场景（去广告、防污染、家庭过滤、分流加速等）通过表单、开关和预置模板完成，同时保留进入高级配置模式继续编辑完整规则的入口。目标是在不削弱 OxiDNS 策略能力的前提下，显著降低首次安装和日常维护门槛。

</RoadmapItem>

<RoadmapItem type="version" label="2026-07-17" title="MikroTik RouterOS 双插件" desc="ros_address_list 与 ros_route 完成地址列表和静态路由同步">

完成 `ros_address_list` 与 `ros_route` 两个 RouterOS executor。`ros_address_list` 将 DNS 响应中的 A/AAAA 地址同步到 RouterOS address-list，`ros_route` 将目标地址同步为指定 routing table 中的逐 IP 静态路由。两者共用异步队列、批处理、TTL lease、重连、reconcile、去重、关闭清理和指标体系；路由插件还支持基于 conntrack 的延迟删除，降低活跃连接被提前移除路由的风险。

</RoadmapItem>

<RoadmapItem type="version" label="2026-07-02" title="OpenWrt LuCI 插件" desc="通过 luci-app-oxidns 在 LuCI 中安装内核、托管服务、编辑配置并查看日志">

新增 [`luci-app-oxidns`](https://github.com/svenshi/luci-app-oxidns)：OpenWrt 用户可以在 LuCI 的 `Services -> OxiDNS` 中安装 OxiDNS core、管理 init 服务、编辑配置并查看日志。LuCI 插件不内置 OxiDNS 内核，首次安装时会从官方 GitHub Releases 下载并校验 Linux musl release archive；后续内核升级继续使用 OxiDNS 自带升级能力。

</RoadmapItem>

<RoadmapItem type="version" title="IP 优选" desc="对多个 A/AAAA 地址并行测速，自动返回延迟最低的 IP" version="v1.2.0"  date="2026-06-03">

对 DNS 响应中的多个 A/AAAA 地址并行测速，自动选出延迟最低的 IP 返回给客户端，提升实际访问速度。该能力已随 v1.2.0 发布。

</RoadmapItem>

<RoadmapItem type="version" title="自学习域名集" desc="learn_domain 自动收集查询域名，写入持久化的 dynamic_domain_set，WebUI 可视化管理规则" version="v1.2.0" date="2026-06-03">

新增 `learn_domain` 执行器与 `dynamic_domain_set` provider 组合：执行器在查询流中按规则自动捕获域名，写入 `dynamic_domain_set` 持久化文件并热生效，无需手动维护规则列表。WebUI 为 `dynamic_domain_set` 增加 Detail 标签页，可直接查看、增删、清空规则；`learn_domain` / `dynamic_domain_set` 的每个配置字段均补齐了字段级说明。

</RoadmapItem>

<RoadmapItem type="version" title="编译定制化" desc="三档组合包（minimal / standard / full），minimal 二进制约为 full 的 40%" version="v1.2.0"  date="2026-06-03">

按功能模块拆分编译，用户 fork 仓库后可自由组合所需插件，构建精简的定制版本。

`minimal` / `standard` / `full` 三档落地；所有协议栈与管理面已 feature 化——`api` / `webui` / `metrics`、`server-dot/doh/doq/doh3`、`upstream-dot/doh/doq/doh3`，以及 MikroTik、query_recorder、ipset/nftset、cron、script、upgrade、download、http_request、reverse_lookup、geo provider、adguard_rule 均可单独裁剪。`AppController` / `LogBuffer` 作为运行基础设施位于 `src/infra/`，`minimal` 排除 hyper / rustls / quinn，release 二进制约为 `full` 的 40%（≈ 8.9 MB vs 21 MB）。

</RoadmapItem>

<RoadmapItem type="version" title="稳定迭代" desc="nftset/ipset 修复；WebUI 完善；Monaco 本地化；provider 性能" version="v1.1.x" date="2026-05">

修复 `nftset` interval 编码（EINVAL）和 `ipset` 字节序；query_recorder 历史清空；WebUI 移动端完善；Monaco 本地自托管；provider/matcher 内存优化。

</RoadmapItem>

<RoadmapItem type="version" title="env 配置；升级改进" desc="配置环境变量占位符；升级流程重构（Windows 支持）；聚合统计" version="v1.1.0" date="2026-05-25">

配置支持 `${ENV_VAR}` 占位符；`upgrade` 全面支持 Windows 原地升级；`query_recorder` 聚合统计与排行榜。

</RoadmapItem>

<RoadmapItem type="version" title="WebUI 正式发布" desc="实时日志、配置历史回滚、插件指标、执行流瀑布图" version="v1.0.0" date="2026-05-20">

WebUI 全功能首发：实时运行日志、配置历史（保存 / 应用 / 回滚）、插件指标面板、缓存管理、`query_recorder` 执行流可视化、离线配置编辑。

</RoadmapItem>

<RoadmapItem type="version" title="query_recorder 重构" desc="流式后台管道；matcher 命中统计与执行路径追踪" version="v0.5.0" date="2026-04-27">

`query_recorder` 重构为流式管道，降低主路径开销；新增执行路径统计；引入 `jiff` 统一时间处理。

</RoadmapItem>

<RoadmapItem type="version" title="provider 优化；升级 CLI" desc="provider 内存与 reload 优化；any_match 插件；一键自更新" version="v0.4.0" date="2026-04-19">

大幅降低 provider 内存占用；新增 `any_match`、`upgrade` CLI 子命令；HTTP/3 `Alt-Svc` 通告。

</RoadmapItem>

<RoadmapItem type="version" title="http_request 插件" desc="DNS 流水线中发起 HTTP 请求；查询热路径 wire buffer 复用" version="v0.3.0" date="2026-04-14">

新增 `http_request` 插件；引入 wire buffer 对象池，减少查询热路径内存分配。

</RoadmapItem>

<RoadmapItem type="version" title="插件扩展" desc="script / download / adguard_rule 插件；startup reload；SOCKS5 代理下载" version="v0.2.0" date="2026-04-02">

新增 `script` 执行器、`download` 文件拉取（支持 SOCKS5）、`adguard_rule` 域名匹配、`reload` 热重载执行器。

</RoadmapItem>

<RoadmapItem type="version" title="初始发布" desc="基础 UDP/TCP DNS 代理、多上游转发、本地缓存核心" version="v0.1.0" date="2026-03-28">

基础 DNS 代理功能上线：UDP/TCP 双栈监听、多上游转发、本地缓存、规则匹配基础框架。

</RoadmapItem>

</RoadmapTimeline>

<div style={{borderLeft: '4px solid var(--ifm-color-primary)', background: 'rgba(15, 118, 110, 0.06)', borderRadius: '0 12px 12px 0', padding: '0.9rem 1.2rem', marginTop: '2rem'}}>
  <p style={{margin: 0, lineHeight: 1.75}}><strong>关于插件生态的长期方向</strong></p>
  <ul style={{margin: '0.5rem 0 0', paddingLeft: '1.25rem', lineHeight: 1.75}}>
    <li><strong>WebAssembly 插件</strong>：探索通过 WASM 支持第三方插件，允许开发者用任意语言按规范开发和分发插件，无需修改 OxiDNS 主体代码，并天然获得沙箱隔离保障。</li>
    <li><strong>动态链接库插件</strong>：探索通过动态库（.so / .dylib）加载机制支持原生插件，适合对性能要求极高的场景，开发者可独立编译和分发，OxiDNS 在运行时按需加载。</li>
  </ul>
</div>
