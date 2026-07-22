# OxiDNS v1.5.1

## 🚀 发布概览

- v1.5.1 是一次聚焦 matcher 运行时控制、升级运维和 WebUI 质量的 Patch Release：新增三态 matcher 基础结果控制、强制重装与升级后清理，并集中修复国际化、轮询、日志及插件卡片展示。
- v1.5.0 YAML 配置可以直接升级，但 matcher 运行时管理 API 已移除旧的 `/enable`、`/disable` 接口和 `enabled` 响应字段；依赖该 API 的客户端必须先完成迁移。

## ✨ 主要亮点

- matcher 运行时模式扩展为 `normal`、`always_false` 和 `always_true`；两个固定模式跳过内部逻辑并固定基础布尔值，正向与取反引用随后仍分别应用取反，因此结果始终相反。
- 管理 API 与 WebUI 支持强制重新安装当前版本，并可选择在成功升级后清理下载缓存和备份；清理前会释放升级锁，清理失败不会改变升级成功结果。
- 补齐 RouterOS、插件定义、指标和控制台的中英文翻译与日期本地化，并加入 i18n 覆盖审计，避免英文界面回退到中文。
- WebUI 轮询按页面可见性调度并隔离不同后端的运行时状态、指标基线和升级检查；长时间中断后会重置 QPS 采样。
- 日志查看器新增时间戳偏好、可选耗时、自适应单位和紧凑 target；插件配置、指标与系统内存展示同步统一。
- release 构建采用体积优先优化、fat LTO 和符号剥离，缩减 Tokio/TLS feature，并在 minimal/standard 产物流程中尝试 UPX 压缩。
- crates.io 源码包排除仅供开发的 benchmark、站点文档和 WebUI 源码，避免触及 registry 包体限制。
- RouterOS 暂时通过 Git patch 使用 unbounded response channel 修复，避免突发流量下丢失协议事件；该 patch 不单独发布，crates.io 发布在上游修复前暂用 `--no-verify`。
- GitHub Release 与 Telegram 公告现在共用经过版本标题校验的发布说明，Telegram 公告会发送到指定 topic 并自动置顶。

## ⚠️ 升级说明

- 现有 v1.5.0 YAML 配置可以直接升级，本次没有新增或重命名配置字段。替换二进制前建议运行 `oxidns check -c <配置文件>`。
- Matcher API 客户端必须改用 `POST /api/plugins/<matcher_tag>/mode`，请求体为 `{ "mode": "normal|always_false|always_true" }`；`GET /status` 现在返回 `mode`。旧接口会返回 404。
- matcher 固定模式不会写入 YAML，应用 reload 或进程重启后恢复为 `normal`；同一 tag 的所有引用共享基础值，但每个 `$tag` / `!$tag` 引用仍保留自身的取反语义。
- WebUI 默认在成功升级后删除下载缓存与备份；如需保留本地回滚文件，请关闭“升级后清理”。强制升级会重新安装当前版本，执行前请确认目标 bundle 和平台。
- minimal/standard 产物可能经过 UPX 压缩；使用二进制扫描、白名单或完整性基线的环境应重新验证 release asset digest，并先完成启动与回滚演练。

## 📦 下载与校验

- 根据平台和 bundle 选择对应 archive；常规部署使用 full 或 standard，最小能力部署使用 minimal。
- 替换生产环境二进制前，请使用 GitHub Release assets 提供的 digest 校验文件完整性。
