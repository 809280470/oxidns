# OxiDNS v1.5.0

## 🚀 发布概览

- v1.5.0 是一次以 RouterOS 策略同步和运行时运维能力为主线的 Minor Release：新增 `ros_route`，完整重构 `ros_address_list`，并为 matcher/provider 增加管理 API 与 WebUI 运行时控制。
- 本次还带来可配置 `response` 执行器、时区感知的 `time` matcher、query recorder 空间回收、CNAME 响应裁决加固、WebUI 高级配置，以及 OpenWrt、ARMv7 和容器发布改进。
- 升级前请特别检查两项兼容变化：不安全或保留的 plugin tag 现在会被拒绝；`ros_address_list.comment_prefix` 默认值由 `fdns` 改为 `oxi`。

## ✨ 主要亮点

- 新增 `ros_route`：将 DNS A/AAAA 观察结果同步为 RouterOS 指定 routing table 中的逐 IP 静态路由，支持双栈网关、distance、TTL lease、persistent IP/CIDR、conntrack 延迟删除和启动恢复。
- RouterOS 双插件共用 TLS/API-SSL、有限并行批处理、有界去重队列、reconcile、重试和受限关闭清理；RouterOS 不可达不会阻塞 DNS 启动，删除前会重新确认 ownership，降低误删风险。
- matcher 可在管理 API 与 WebUI 中查询状态、启用或禁用；provider 可触发串行化 reload，并明确拒绝重复并发操作。
- 新增 `response` 执行器与增强的 `time` matcher；支持 zone record 模板、RCODE/flags、请求占位符、IANA 时区、跨午夜时间段及 weekday/monthday 条件。
- query recorder 的 retention cleanup 与手动清空现在会协调并发数据库访问、批量删除、截断 WAL、迁移旧库并实际回收磁盘空间。
- `forward` 与 `cache` 采用共享的 query-aware 响应分类：裸 CNAME 不再错误胜出、参与负响应共识或写入地址缓存，cache dump/load、lazy refresh 与 TTL 处理同步加固。
- WebUI 增加高级配置折叠与显式值保留，YAML 编辑器迁移到本地 CodeMirror，并增强流量、内存与指标不可用状态展示。
- 新增 ARMv7 release asset；安装脚本支持 OpenWrt LuCI 应用生命周期；Docker 使用 Alpine 构建阶段和 BusyBox musl 运行时；升级模块加强摘要校验和 full/slim 产物选择。

## ⚠️ 升级说明

- 大多数 v1.4.0 配置可以直接升级；新增功能均为可选。替换二进制前请先用 v1.5.0 执行 `oxidns check -c <配置文件>`。
- 不安全的 plugin tag 和保留的 quick-setup namespace 现在会被配置校验拒绝；命中时请重命名 tag，并同步更新所有 matcher/executor/provider 引用。
- RouterOS 用户：若要继续识别、刷新或清理由旧版本创建的 address-list 条目，请显式保留 `comment_prefix: fdns`。如直接使用新的默认值 `oxi`，旧 namespace 条目不会被新实例接管，需要自行迁移或清理。
- `cleanup_on_shutdown` 仍默认为 `true`。应用 reload 会先关闭旧 RouterOS 插件实例；要求策略连续性的环境请评估设为 `false`，并避免两个进程同时使用相同 tag、comment prefix 和目标列表/路由表。
- 使用 `ros_route.fixed_ttl: 0` 时，动态路由不会自然过期且没有数量上限；请先评估 RouterOS 路由表和 OxiDNS 内存容量，并准备明确的回收策略。
- query recorder 大库在首次 retention cleanup 或手动清空时可能执行 auto-vacuum 迁移和空间回收，建议避开业务高峰并预留磁盘空间。
- 旧 cache dump 可以继续使用；其中不完整的 CNAME-only 地址缓存项会在加载或命中校验时被丢弃。
- 容器运行时已改为 musl/BusyBox。容器、OpenWrt 和 ARMv7 用户应先验证启动参数、挂载、时区以及升级/回滚流程。

## 📦 下载与校验

- 根据平台和 bundle 选择对应 archive；常规部署使用 full 或 standard，最小能力部署使用 minimal。
- 替换生产环境二进制前，请使用 GitHub Release assets 提供的 digest 校验文件完整性。
