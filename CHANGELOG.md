# 更新日志

本项目的所有重要变更都会记录在此文件。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [1.0.0] - 2026-07-29

首个正式发布版。在 0.1.0 的基础上完成了一轮全面 review 与修复，新增 GitHub Actions 自动构建多架构镜像（amd64 + arm64）并自动创建 GitHub Release。

### 修复

- `Outbox` 增加 `max_delivery_attempts` 上限，避免在 5xx 风暴期间 `notification_dead_letters` 永远 0；新增
  `fping_monitor_notification_attempts_total{result="success|retry|dead"}` 计数。
- `StateManager` 在 `DOWN → UP` 且 `incident_id is None` 时不再发出"孤立"的 `host_recovered` 事件。
- `Scheduler` 在大面积故障窗口解除后，对窗口内被抑制的 `host_down` 主动补发一次。
- `StateManager.upsert_targets` 把被移除的 host 保留 5 分钟，再次出现时复用旧 `HostState`，
  避免刚发出的 `host_down` 还没收到响应就被丢弃。
- `Application.reload_config` 限制为只刷 targets 列表，其它字段若改动会在日志 WARN 提醒用户重启。
- `WebhookNotifier` 检测占位 token（`replace-me` / `changeme` / 长度 < 16），命中后立即 dead 并打印 ERROR。
- `config._parse_notification` 校验 `url.scheme in {http, https}` 且 `netloc` 非空。

### 新增

- 新增 `fping_monitor_target_config_reload_total{result="success|failed"}` 与
  `fping_monitor_last_successful_config_reload_timestamp_seconds`。
- 新增 `fping_monitor_storage_operation_total{operation,result}`，记录 outbox 读写结果。
- 新增 `.github/workflows/ci.yml`：`pytest -q` + `ruff check`。
- 新增 `.github/workflows/release.yml`：push tag `v*` 时构建多架构镜像并创建 GitHub Release。
- 新增 `CHANGELOG.md`。
- 新增 `.gitignore` 中 `*.db` `*.db-wal` `*.db-shm` `.coverage` `htmlcov/` `dist/` `build/` `.env` `*.local.yaml` `*.bak`。
- 新增 `uv.lock`。

### 变更

- `fping_binary` 默认路径由 `/usr/sbin/fping` 改为 `/usr/bin/fping`（apt 包实际路径；macOS Homebrew 仍在
  `/usr/local/sbin/fping`）。
- `__main__.py` 在 `notification.monitor_instance` 仍是示例默认 `monitor-a` 时打印 WARN，提醒多实例部署时
  改成唯一标识。
- `arch.md` / `docs/architecture.md` / `docs/architecture.html` / `docs/usage.md` 中 fping 路径与目录结构同步。
- `arch.md` §13 删除"Pydantic 2"表述，改为"dataclass + 手工校验"；§12 删除不存在的 `targets.py`。
- `arch.md` §9.2 指标列表修正：`notification_attempts_total` 的 result 改为 `success|retry|dead`。

## [0.1.0] - 2026-07-28

首次稳定版本：fping 批量探测 + 状态机 + Prometheus 指标 + SQLite Outbox + Webhook 告警 +
HTTP 端点 + Docker/Podman 镜像。

[1.0.0]: https://github.com/your-org/fping-monitor/releases/tag/v1.0.0
[0.1.0]: https://github.com/your-org/fping-monitor/compare/v0.1.0...v1.0.0