# fping-monitor 使用文档

本文面向运维与二次开发，介绍如何部署、运行、验证与排错。设计动机与原则见 [`arch.md`](../arch.md)，代码结构与调用链见 [`docs/architecture.md`](architecture.md)。

## 1. 简介

`fping-monitor` 是一个轻量的主机在线监控程序：

- 使用 `fping` 批量 ICMP 探测，约 1000 台主机的规模下，单实例 CPU 占用 < 15%、内存 < 200MB。
- 通过 HTTP 暴露 `/metrics` 与 `/healthz` `/readyz`，可被 Prometheus 直接抓取。
- 主机状态变化时调用外部 Webhook 接口（`host_down` / `host_recovered`），未发送事件在 SQLite 中持久化，进程崩溃后能继续投递。
- Docker / Podman 友好：多阶段构建、`setcap cap_net_raw+ep`、非 root 运行、只读根文件系统。

适用场景：内网/办公网主机存活监控、需要区分"延迟+丢包+在线状态"的基础拨测、对告警可靠性有要求的小型团队。

不适用：对大规模（>10k）主机的横向扩展、SNMP/Trap、TCP 业务拨测（当前未实现，可作为后续增强）。

## 2. 环境要求

| 项目 | 版本 / 说明 |
| ---- | ----------- |
| Python | 3.12+（开发与本地运行） |
| `fping` | 5.x；容器镜像内置 5.1，宿主机需保持兼容 |
| Docker | 24+（推荐 25/26） |
| Podman | 4.5+（rootless 也支持，需注意 `NET_RAW` 能力映射） |
| 磁盘 | 监控数据卷预留 200MB；按通知频率可上调 |
| 内存 | 容器建议 256MB，复杂场景上调至 512MB |
| 网络 | 出向：ICMP 到被监控目标；入向：9100/tcp 供 Prometheus 抓取 |

## 3. 快速开始

### 3.1 本地 venv（开发与排错）

```bash
# 1. 拉取代码
git clone <repo-url> fping-monitor && cd fping-monitor

# 2. 创建虚拟环境
uv venv .venv --python 3.12
# 如果网络需要代理：
# export http_proxy=http://127.0.0.1:10809
# export https_proxy=http://127.0.0.1:10809

# 3. 安装依赖（运行时 + 测试）
uv pip install -e ".[dev]"
# 也可用 .venv/bin/python -m pip install -e ".[dev]"

# 4. 准备配置（先把示例复制一份再修改）
cp config/config.example.yaml config/local.yaml

# 5. 启动（注意：默认 storage.path 是 /var/lib/...，需要可写）
#    开发环境建议临时改成 ./state.db：
#      python -c "import re,sys;p='config/local.yaml';t=open(p).read();open(p,'w').write(re.sub(r'path: /var/lib/fping-monitor/state.db','path: ./state.db',t))"
.venv/bin/python -m fping_monitor --config config/local.yaml
```

启动成功后会看到：

```text
fping-monitor 启动完成: targets=2 interval=10s
metrics http 服务已启动: 0.0.0.0:9100
```

打开另一个终端验证：

```bash
curl -s http://127.0.0.1:9100/healthz       # {"status":"ok"}
curl -s http://127.0.0.1:9100/readyz        # {"ready": true, "reason": "ok"}
curl -s http://127.0.0.1:9100/metrics | head # 看到 fping_monitor_* 指标
```

### 3.2 Docker

```bash
docker compose up --build
```

`docker-compose.yml` 关键点：

- `cap_add: NET_RAW` 与 `cap_drop: ALL`：赋予 ICMP 能力同时限制其它特权。
- `security_opt: no-new-privileges:true`：禁止权限提升。
- `read_only: true` + `tmpfs /tmp`：根目录只写。
- 持久卷 `monitor-data` 挂到 `/var/lib/fping-monitor`，存放 SQLite。

通过 `http://<host>:9100/metrics` 暴露给 Prometheus。

也可直接拉取 GHCR 上的**公开镜像**（无需 `docker login`）：

```bash
docker run --rm --cap-add NET_RAW --read-only \
    -p 9100:9100 \
    -v "$PWD/config:/etc/fping-monitor:ro" \
    -v fping-data:/var/lib/fping-monitor \
    ghcr.io/wengerchan/fping-monitor:v1.0.0
```

镜像为公开包，多架构标签会按 host 架构自动选择。

### 3.3 Podman

rootful：

```bash
podman compose up --build
```

rootless 注意事项：

- 默认 rootless 容器**没有** `CAP_NET_RAW`，必须靠镜像内的 `setcap cap_net_raw+ep /usr/bin/fping` 兜底（已在 `Dockerfile` 中处理）。
- 一些 rootless 后端（如 crun + pasta）在 ICMP 上可能有兼容性差异，请优先使用 Docker 或 rootful Podman 验证后再上 rootless。
- 数据卷使用 `podman volume create monitor-data` 然后挂载；不要直接 bind 宿主目录，否则 SELinux 可能阻止容器访问。

## 4. 配置详解

YAML 文件结构与默认值见 `config/config.example.yaml`。下面说明每一段的常见调参点。

### 4.1 文件位置与重载

- 通过 `--config <path>` 指定，默认无回退。
- 程序启动后**重新加载**通过 `SIGHUP`：

  ```bash
  kill -HUP $(pidof python)   # PID 视实际启动方式而定
  ```

  重载只会更新 `targets` 列表，不会改其它字段。其它字段（如 `probe.interval_seconds`）需要重启程序。

### 4.2 server

```yaml
server:
  listen: 0.0.0.0
  port: 9100
```

- `listen`：建议只对内网开放（如 `127.0.0.1` 或内网 IP），结合 Prometheus 侧做白名单。
- `port`：HTTP 端点端口；Prometheus 抓取目标配置要与之一致。

### 4.3 probe

```yaml
probe:
  interval_seconds: 10
  timeout_ms: 1000
  packets: 3
  batch_size: 200
  batch_jitter_ms: 0
  fping_binary: /usr/bin/fping # apt 包实际路径；macOS Homebrew 在 /usr/local/sbin/fping
```

- `interval_seconds`：每轮探测的整体间隔。建议 ≥ 5s；过短会显著抬升 ICMP 流量。
- `timeout_ms`：fping 单包超时。
- `packets`：每目标每轮发送的包数；与 RTT/丢包率统计相关。
- `batch_size`：单次 fping 调用的目标数；命令行过长会被 exec 限制。`200` 是经验值。
- `batch_jitter_ms`：每批之间插入的随机 sleep，错峰使用。
- `fping_binary`：容器内默认 `/usr/bin/fping`（apt 包路径）；宿主机直接运行时可能不同（macOS Homebrew 一般在 `/usr/local/sbin/fping`）。

### 4.4 state

```yaml
state:
  down_after_failures: 3
  up_after_successes: 3
  mass_failure_ratio: 0.5
```

- `down_after_failures`：连续 N 轮失败才确认 DOWN。值越大越不容易误报，但告警延迟增加。
- `up_after_successes`：连续 N 轮成功才确认 UP。值越大越不容易"假恢复"，但恢复延迟增加。
- `mass_failure_ratio`：当本轮失败比例 ≥ 该值且总目标数 ≥ 5 时，抑制逐台 DOWN 告警并触发"大面积故障"指标。0.5 适合内网；公网高丢包环境可以调到 0.7。

### 4.5 notification

```yaml
notification:
  enabled: true
  url: https://alert.example.internal/api/v1/events
  timeout_seconds: 5
  max_attempts: 8
  max_backoff_seconds: 60
  token_env: ALERT_API_TOKEN
  monitor_instance: monitor-a
```

- `enabled`：false 时仍会写 outbox，但 worker 不启动；本地调试常用。
- `url`：外部告警接口完整 URL；`POST` 携带 `Content-Type: application/json`。
- `token_env`：从哪个环境变量读取 Bearer Token（容器中用 `docker compose` 的 `environment` 或 secret 注入，**不要**写死在 yaml）。
- `monitor_instance`：写入 `event.monitor_instance` 字段，便于接收端按部署单元聚合。
- `max_attempts` × `max_backoff_seconds`：单条事件最多重试的总时长粗算 = `2**max_attempts` 秒，封顶 60s。8 次约 4 分钟。

### 4.6 storage

```yaml
storage:
  path: /var/lib/fping-monitor/state.db
```

- 容器中必须保证父目录可写。`Dockerfile` 已 `mkdir -p` 并 `chown`。
- 备份该文件即可完整保留未发送的告警。

### 4.7 targets

```yaml
targets:
  - id: core-switch-01
    address: 10.10.0.1
    labels:
      site: shanghai
      group: network
```

- `id`：必须匹配正则 `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`，全局唯一。
- `address`：IP 或可解析的主机名。`fping` 直接接受 hostname 字符串。
- `labels`：键必须在白名单 `site / group / env / role` 内；值是任意字符串（建议也限定在小集合）。
- 整个 YAML 中重复 `id` 或 `id` 格式非法都会在启动时报错并退出。

## 5. HTTP 端点

| 路径 | 方法 | 状态码 | 说明 |
| ---- | ---- | ------ | ---- |
| `/metrics` | GET | 200 | Prometheus 抓取；`Content-Type: text/plain; version=0.0.4` |
| `/healthz` | GET | 200 | 进程存活；用于容器/K8s 的 liveness 探针 |
| `/readyz`  | GET | 200/503 | 已加载目标且最近一轮探测在 `max(interval*2, 60)` 秒内完成 |
| 其他 | GET | 404 | 固定 `{"error":"not found"}` |

`/readyz` 的响应体形如 `{"ready": false, "reason": "尚未完成任何探测轮次"}`，便于定位。

## 6. Prometheus 集成

`/metrics` 输出全部 `fping_monitor_*` 指标。完整定义见 [`arch.md §9`](../arch.md)，本节只给出常用查询。

### 6.1 抓取配置

```yaml
scrape_configs:
  - job_name: fping-monitor
    static_configs:
      - targets: ['fping-monitor.internal:9100']
    scrape_interval: 15s
    scrape_timeout: 5s
```

### 6.2 常用 PromQL

```promql
# 当前所有 DOWN 主机的 id
fping_monitor_host_up == 0

# 最近 1 分钟内 RTT 抖动的目标
rate(fping_monitor_probe_latency_seconds_sum[1m]) > 0.05

# 监控自身是否活跃
time() - fping_monitor_last_probe_completion_timestamp_seconds

# 通知积压
fping_monitor_notification_queue_size
fping_monitor_notification_oldest_pending_age_seconds > 300
```

### 6.3 自监控告警规则

仓库提供一份示例：[`deploy/prometheus_alerts.yml`](../deploy/prometheus_alerts.yml)。在 Prometheus 中通过 `rule_files` 引入：

```yaml
rule_files:
  - /etc/prometheus/rules/fping-monitor.yml
```

示例中包含 8 条规则：

- `FpingMonitorAbsent` — 抓取目标消失
- `FpingMonitorProbeStalled` — 探测循环停滞
- `FpingMonitorProbeErrors` — 探测轮次连续错误
- `FpingMonitorNotificationBacklog` — 通知积压
- `FpingMonitorNotificationDeadLetters` — 出现 dead letter
- `FpingMonitorStorageUnhealthy` — SQLite 不可达
- `FpingMonitorNoTargets` — 未加载任何目标
- `FpingMonitorMassFailureProtection` — 大面积故障保护激活

## 7. 通知协议

故障事件（POST 到 `notification.url`）：

```json
{
  "event_id": "5e8c...",
  "incident_id": "3a0f...",
  "event_type": "host_down",
  "host_id": "core-switch-01",
  "address": "10.10.0.1",
  "occurred_at": "2026-07-28T02:15:30Z",
  "confirmed_at": "2026-07-28T02:15:50Z",
  "last_success_at": "2026-07-28T02:15:20Z",
  "consecutive_failures": 3,
  "packet_loss_ratio": 1.0,
  "probe_type": "icmp",
  "monitor_instance": "monitor-a",
  "labels": {"site": "shanghai", "group": "network"}
}
```

恢复事件 `event_type="host_recovered"`，其它字段一致；`incident_id` 与同一故障轮次的 `host_down` 相同。

接收端建议：

- 按 `event_id` 做幂等去重（同一事件可能因网络抖动被重发）。
- 解析 `consecutive_failures` 与 `packet_loss_ratio` 决定推送渠道（IM/电话/邮件）。
- 返回 `2xx` 即视为成功；`4xx` 视为永久失败，发送方会标记 dead；`5xx` / 超时会按指数退避重试（`max_attempts` 内）。

## 8. 运维操作

### 8.1 配置热重载（SIGHUP）

```bash
# 1. 修改 config/local.yaml
# 2. 找到进程 PID（容器内：docker inspect / podman inspect 取 PID 1；宿主机：pidof python）
# 3. 发送 SIGHUP
kill -HUP <pid>
```

热重载的行为：

- **targets 列表**：增量生效；新增的主机从 UNKNOWN 开始探测，移除的主机在 5 分钟内进入
  "保留窗口"，若在窗口内再次出现会复用旧 `HostState`（包括 incident_id），避免刚发出的 host_down 还没
  收到响应就被丢弃。
- **其它字段**（probe / state / notification / storage / server）：**不会**热生效；reload 时若检测到差异，
  会在日志打印 `WARN`，需要重启程序。
- **可观测**：`fping_monitor_target_config_reload_total{result}` 与
  `fping_monitor_last_successful_config_reload_timestamp_seconds` 会反映重载结果。

### 8.2 优雅停机

`SIGINT` 或 `SIGTERM` 会触发 `Application.shutdown()`：停止调度、停止 HTTP、关闭 outbox worker、关闭 HTTP 客户端。容器中 `docker stop` 默认 10s 超时；不够时调 `--time 30`。

### 8.3 数据备份

`/var/lib/fping-monitor/state.db` 是 SQLite WAL 模式文件；备份时建议：

```bash
sqlite3 /var/lib/fping-monitor/state.db ".backup /backup/state-$(date +%F).db"
```

或者直接 `cp` 文件（WAL 会先做 checkpoint）。**不要**在进程未关闭时拷贝 `state.db-wal`/`state.db-shm` 后单独处理。

### 8.4 升级

1. `docker compose pull` 或重新 `docker compose build`。
2. 启动新版本（`docker compose up -d`）。
3. 新版本对老 SQLite 文件**向前兼容**（schema 未变），不需要迁移。
4. 观察 `fping_monitor_storage_healthy` 与 `fping_monitor_notification_queue_size`，确认没有积压。

## 9. 常见排错

### 9.1 启动失败：`PermissionError: '/var/lib/fping-monitor'`

容器中父目录权限不对。检查：

- `docker compose.yml` 中 `volumes` 是否挂载到 `/var/lib/fping-monitor`。
- 宿主机目录被 SELinux/AppArmor 限制时加上 `:z` 后缀，或用命名卷。

本地 venv 下：把 `storage.path` 改成 `./state.db` 再启动。

### 9.2 所有目标都是 DOWN / `fping` 报错

- 容器内 `setcap` 失败 → `docker exec <c> getcap /usr/bin/fping`，应看到 `cap_net_raw+ep`。
- rootless Podman 未生效 → 改用 rootful，或确认底层 runtime 是 crun+slirp4netns。
- fping 二进制找不到 → `fping_binary` 配置；本地 venv 时 macOS 默认在 `/usr/local/sbin/fping`。

### 9.3 `/readyz` 一直 503

- 启动不到 1 个 `interval_seconds` 还没有完成第一轮：等待，或缩小 `interval_seconds`。
- 程序反复 crash → 看 stdout 日志；常见原因有 OOM、配置错误。
- `/metrics` 中 `fping_monitor_probe_round_in_progress==1` 持续很久：fping 卡住；通过 `fping_last_exit_code` 排查。

### 9.4 SIGHUP 不生效

- 容器内 PID 1 是 `tini`，需要 `docker kill -s HUP <c>` 而不是 `docker exec kill …`。
- 只改了 `probe.*` 或 `state.*`：这些字段不支持热加载，请改完配置后重启。

### 9.5 通知接口返回 401/403

- 容器未注入 `ALERT_API_TOKEN`（或自定义 `token_env`）环境变量；启动时 `WebhookNotifier` 会检查
  是否为占位值（如 `replace-me`、`changeme`、长度 < 16），命中后会立即把事件标记 dead 并打 ERROR。
- 接收方按 IP 白名单限制：把容器出口 IP 加白。
- 临时绕过：把 `notification.enabled` 设为 false，但配置可改其他参数。

### 9.6 通知积压 / dead letter 增加

- 接收端 5xx 持续：查 `fping_monitor_notification_oldest_pending_age_seconds` 与 `notification_dead_letters`。
- 收件端超时：调大 `notification.timeout_seconds` 与 `max_attempts`。
- 队列中混杂了大量旧事件：可手动 `UPDATE outbox SET status='delivered' WHERE id IN (...)`（**慎用**）。
- 队列里**永远**不出 dead letter，但 `notification_attempts_total{result="retry"}` 持续增长：
  说明 webhook 一直返回 5xx 且单条事件重试总数未到 `max_delivery_attempts`；此时需要尽快修复 webhook。

### 9.7 启动期大流量告警

冷启动时所有目标都是 `UNKNOWN`；一旦网络不可达会立刻 DOWN，导致大量 `host_down` 通知。

- 临时方案：先关闭 `notification.enabled`，等状态稳定后再开启。
- 长期方案：与告警接收方约定"首条 host_down 延迟 X 秒入栈"或维护"已知不可达主机"白名单。

### 9.8 端口冲突 / 服务起不来

- `9100` 已被占用：改 `server.port`，同步调整 Prometheus scrape target。
- `PermissionError: /var/lib/fping-monitor` 启动失败：检查 compose 中 `volumes` 是否挂载到容器内
  `/var/lib/fping-monitor`，并确认宿主机目录被 chown 给 `10001:10001`（或用命名卷）。

### 9.9 SQLite 锁 / database is locked

- 同一 outbox 数据库被多个进程打开：只能运行**一个** `fping-monitor` 进程共用 SQLite 文件。
- WAL checkpoint 阻塞：默认配置应足够；如出现 `database is locked` 持续 > 30s，重启进程并在
  下游 webhook 恢复正常后清空 outbox 中的积压。

### 9.10 容器 OOM / CPU 占用高

- `docker stats` 看 RSS：若持续逼近 `mem_limit`，先调小 `state.batch_size` / 增加 `interval_seconds`。
- 1000 主机 + 10s 间隔在 1 核 256MB 已可稳定运行；若部署到 2 核以下机器，CPU 会成为瓶颈，
  可把 `interval_seconds` 调到 20s。

## 10. 容量与性能

| 指标 | 1000 主机 / 10s 间隔 | 备注 |
| ---- | -------------------- | ---- |
| fping PPS | ~300 | 与 `packets × targets / interval` 成正比 |
| 内存 | < 200MB | 主要是状态对象 + outbox |
| Prometheus 时序数 | ~8–10k | 1000 主机 × ~8 series + 程序自身 |
| CPU | 5–15% 单核 | ICMP 处理极轻 |
| 通知数据库 | < 1MB / 天 | 视 down 频率 |

可观测性要点：

- 部署前用 `deploy/prometheus_alerts.yml` 在 Prometheus 端接入告警；
  `FpingMonitorProbeStalled` / `FpingMonitorNotificationDeadLetters` / `FpingMonitorMassFailureProtection`
  是三条最重要的规则。
- 大面积故障保护：单批失败比例 ≥ `state.mass_failure_ratio` 且总数 ≥ 5 时抑制逐台 DOWN，并在窗口解除后
  对仍处于 DOWN 且 incident_id 为 None 的主机补发一次 `host_down`。
- outbox 单条事件的最大尝试次数 = `notification.max_attempts * 4`；超过即 dead。

如果未来扩展到 10k+ 主机，建议先按 `site` 或 `group` 拆实例，再考虑分布式方案。

## 11. 开发与测试

### 11.1 跑测试

```bash
uv pip install -e ".[dev]"
.venv/bin/python -m pytest -q
```

当前约 72 个测试，覆盖配置解析、fping 输出解析、状态机、指标、通知、outbox、HTTP 端点、调度器、装配、大面积故障保护、状态机恢复、URL/token 校验。

### 11.2 调试 fping 输出

可以直接执行解析器单测，传入真实 `fping` 输出：

```python
from fping_monitor.probes import parse_fping_output
print(parse_fping_output("10.0.0.1 : 0.12 0.15 0.10", {"10.0.0.1": "host-a"}))
```

或者本地执行 `fping -C 3 -q -t 1000 -p 200 10.0.0.1` 后把 stdout 粘到 `parse_fping_output` 中。

### 11.3 注入假探针

为避免每改一处都跑真 fping，可以在测试或本地脚本中给 `Scheduler.probe_fn` 赋一个 lambda：

```python
from fping_monitor.scheduler import Scheduler
scheduler.probe_fn = lambda targets, *a, **kw: {
    t.id: ProbeResult(t.id, success=True, latency_seconds=0.001, packet_loss_ratio=0.0)
    for t in targets
}
```

### 11.4 行为约束

- 严控 `fping_monitor_*` 标签基数；新增 label 需要在 [`metrics.py`](../src/fping_monitor/metrics.py) 中显式声明。
- 避免引入复杂装饰器、动态注册、嵌套三元；新代码沿用普通 dataclass + 显式 `if/elif`。
- 中文注释：docstring 与解释性注释使用中文；标识符与协议字段保持英文。

### 11.5 静态检查与格式化（可选）

```bash
# ruff 是 CI 必跑，本地建议也装上
.venv/bin/python -m pip install ruff
.venv/bin/python -m ruff check src tests
```
