# fping 主机监控程序架构设计

## 1. 项目目标

开发一个使用 Python 实现的主机在线监控程序，主要能力如下：

- 监控约 1000 台主机；
- 使用 `fping` 批量探测主机在线状态、往返延时和丢包；
- 支持 Docker 和 Podman 容器化部署；
- 主机确认 DOWN 后调用外部 HTTP 接口告警；
- 主机恢复后调用外部接口发送恢复通知；
- 按 Prometheus 标准通过 `/metrics` 暴露状态和延时数据；
- 保证代码简单、清晰、容易维护。

## 2. 核心原则

### 2.1 批量探测

使用 `fping` 一次探测一批目标，不为每台主机单独启动 `ping` 进程。

### 2.2 探测结果和主机状态分离

一次探测失败只代表本轮失败，不立即将主机判定为 DOWN。主机状态由连续成功或失败次数确认。

### 2.3 只在状态变化时通知

- `UP -> DOWN`：发送故障通知；
- `DOWN -> UP`：发送恢复通知；
- 状态没有发生变化时，不重复发送通知。

### 2.4 告警不能阻塞探测

通知由独立的异步 worker 处理。外部告警接口超时或异常时，不能阻塞后续主机探测。

### 2.5 Prometheus 使用 Pull 模式

程序提供 `/metrics`，由 Prometheus 主动抓取。延时历史由 Prometheus 保存，监控程序不重复建设时序数据库。

### 2.6 最小权限运行

容器中的 Python 程序以非 root 用户运行，只为 `fping` 提供 ICMP 所需的 `NET_RAW` 能力。

### 2.7 优先保证代码可读性

代码以清晰、直接、容易调试为第一目标，不刻意使用复杂或“高大上”的语法。

具体要求：

- 优先使用普通函数、简单类、明确的数据结构和顺序控制流；
- 避免过度抽象，不为尚未出现的需求提前设计复杂框架；
- 避免元类、复杂装饰器、动态生成类、猴子补丁和隐式注册机制；
- 避免难以阅读的多层列表推导式、嵌套三元表达式和过长的方法链；
- 异步代码只用于确实存在 I/O 并发的地方，例如子进程、HTTP 通知和 HTTP 服务；
- 不为了“全部异步化”而把简单的配置处理和状态计算改成异步函数；
- 函数保持职责单一，名称应直接说明用途；
- 关键状态变化使用清晰的 `if/elif` 表达，不使用难以理解的技巧压缩代码；
- 类型标注用于帮助理解接口，不追求复杂的泛型和类型体操；
- 优先使用 Python 标准库，只有依赖能明显降低复杂度时才引入第三方库；
- 注释主要解释业务约束和不明显的原因，不逐行复述代码；
- 新功能应优先沿用现有结构，避免同一问题出现多套实现方式。

## 3. 总体架构

```text
                       hosts.yaml / CMDB API
                                |
                                v
+--------------------------------------------------------------+
|                     fping-monitor 容器                       |
|                                                              |
|  +---------------+    +----------------+                     |
|  | TargetManager |--->| ProbeScheduler |                     |
|  | 加载/刷新主机  |    | 分组、错峰调度  |                     |
|  +---------------+    +-------+--------+                     |
|                              |                               |
|                              v                               |
|                      +---------------+                       |
|                      |  FpingProber  |                       |
|                      | 批量 ICMP 探测 |                       |
|                      +-------+-------+                       |
|                              |                               |
|                              v                               |
|                      +---------------+                       |
|                      | StateManager  |                       |
|                      | 去抖和状态机   |                       |
|                      +-------+-------+                       |
|                              | 状态变化事件                   |
|              +---------------+----------------+              |
|              |                                |              |
|              v                                v              |
|      +---------------+                +---------------+      |
|      | MetricsStore  |                | Notification  |      |
|      | 内存指标快照   |                | Webhook/重试   |      |
|      +-------+-------+                +-------+-------+      |
|              |                                |              |
|              |                                v              |
|              |                        +---------------+      |
|              |                        | SQLite Outbox |      |
|              |                        | 待发事件持久化 |      |
|              |                        +---------------+      |
|              v                                               |
|      GET /metrics                                             |
|      GET /healthz                                             |
|      GET /readyz                                              |
+--------------+-----------------------------------------------+
               | Prometheus Pull
               v
        Prometheus / Grafana
```

现阶段采用单容器、单实例架构。1000 台主机不需要引入 Redis、Kafka 或分布式任务系统。

## 4. 探测方案

### 4.1 首选 fping

`fping` 适合一次探测大量目标。Python 负责启动子进程、传入目标和解析结果。

建议通过 `asyncio.create_subprocess_exec()` 调用 `fping`，不要使用 `shell=True`，避免 shell 注入和参数转义问题。

示意命令：

```bash
fping -C 3 -q -t 1000 -p 200
```

参数含义：

- `-C 3`：每台主机发送 3 个包，并输出每次 RTT；
- `-q`：汇总输出；
- `-t 1000`：单次响应超时 1000 毫秒；
- `-p 200`：同一目标的探测包间隔 200 毫秒。

实际实现时需要根据镜像内安装的 `fping` 版本确认参数和输出格式，并固定主要版本。

### 4.2 解析要求

输出解析器必须覆盖：

- 全部响应；
- 部分丢包；
- 全部超时；
- DNS 解析失败；
- IPv4 和后续可能增加的 IPv6；
- `fping` 权限错误；
- 子进程异常退出；
- 子进程整体超时。

解析器需要使用真实输出样例编写单元测试。

### 4.3 TCP/HTTP 备用探针

ICMP 被防火墙禁止时，主机可能仍然在线。后续可以为特定目标配置 TCP 或 HTTP 探针。

需要保持语义清晰：

- ICMP 表示网络层可达性；
- TCP 表示指定端口可连接；
- HTTP 表示应用可以正常响应。

不同探针应输出独立指标，不应默认把 TCP 成功直接改写为 ICMP 成功。

## 5. 探测调度

### 5.1 建议初始参数

- 探测周期：10 秒；
- 每轮每台主机发送 3 个包；
- 单包超时：1000 毫秒；
- 连续 3 轮失败确认 DOWN；
- 连续 3 轮成功确认恢复；
- Prometheus 抓取周期：15 秒。

该配置通常需要约 20 至 30 秒确认主机 DOWN，并需要连续 3 个成功探测周期确认恢复。

### 5.2 MVP 调度方式

第一版每个周期将所有目标交给一次或若干次 `fping` 批量执行：

```text
读取目标
  -> 执行批量 fping
  -> 解析结果
  -> 更新状态和指标
  -> 等待下一周期
```

可以配置 `batch_size`，例如每批 200 台，避免命令行过长，也便于控制执行时间。

### 5.3 后续错峰调度

如果全量探测造成周期性流量尖峰，可以按稳定哈希将目标分组。例如分成 10 组，每秒探测约 100 台，每台仍保持约 10 秒探测周期。

调度模块从一开始就按“批次”设计，但第一版不实现复杂时间轮。

## 6. 主机状态机

### 6.1 状态

主机状态包括：

- `UNKNOWN`：程序刚启动，尚未确认状态；
- `UP`：已确认在线；
- `DOWN`：已确认离线。

```text
UNKNOWN
  |-- 连续成功达到阈值 --> UP
  `-- 连续失败达到阈值 --> DOWN

UP
  `-- 连续失败 3 轮 ----> DOWN，并产生 DOWN 事件

DOWN
  `-- 连续成功 3 轮 ----> UP，并产生 RECOVERY 事件
```

### 6.2 每台主机的运行状态

建议保存：

```text
status
consecutive_successes
consecutive_failures
last_probe_at
last_success_at
state_changed_at
last_latency_seconds
packet_loss_ratio
incident_id
```

第一版可以使用普通 `dataclass` 表达这些数据，不需要复杂继承体系。

### 6.3 冷启动规则

- 服务启动后，所有目标初始状态为 `UNKNOWN`；
- 首轮失败不立即告警；
- 达到 DOWN 阈值后才发送故障通知；
- 首次确认 UP 不发送恢复通知；
- 只有此前产生过 DOWN 事件的主机，恢复时才发送恢复通知。

### 6.4 抖动控制

第一版通过以下方式减少抖动：

- DOWN 和 UP 都需要连续 3 轮结果确认，避免单次波动触发状态变化；
- 同一状态不重复通知；
- 可以配置通知冷却时间。

暂不增加复杂的 `FLAPPING` 状态。如果实际运行中存在频繁抖动，再增加短时间状态变化计数和通知抑制。

## 7. 大面积故障保护

监控程序所在主机或网络出口发生故障时，可能出现大量目标同时失败。此时不能立即发送数百条主机 DOWN 通知。

建议实现批量故障保护：

- 一轮中超过指定比例的目标同时失败，例如 50%；
- 或多个预先配置的基准目标也同时不可达；
- 则认为可能是监控节点、网关或网络出口故障；
- 暂停逐主机 DOWN 通知；
- 只发送一条“监控节点或网络出口异常”通知；
- 继续探测和更新指标；
- 网络恢复并重新确认状态后，再决定是否产生单主机事件。

阈值必须可配置，并通过故障注入测试验证。

## 8. 通知模块

### 8.1 通知接口

通知模块采用简单接口，方便以后扩展其他通知渠道：

```python
class Notifier(Protocol):
    async def send(self, event: AlertEvent) -> None:
        ...
```

第一版实现 `WebhookNotifier`。

### 8.2 事件格式

故障事件示例：

```json
{
  "event_id": "唯一事件ID",
  "incident_id": "一次故障的唯一ID",
  "event_type": "host_down",
  "host_id": "core-switch-01",
  "address": "10.10.0.1",
  "occurred_at": "2026-07-28T02:15:30Z",
  "confirmed_at": "2026-07-28T02:15:50Z",
  "last_success_at": "2026-07-28T02:15:20Z",
  "consecutive_failures": 3,
  "packet_loss_ratio": 1.0,
  "probe_type": "icmp",
  "monitor_instance": "monitor-a"
}
```

恢复事件使用新的 `event_id`，但复用同一个 `incident_id`。

### 8.3 SQLite Outbox

通知流程：

```text
状态发生变化
  -> 先写入 SQLite outbox
  -> 异步通知 worker 读取待发送事件
  -> 调用外部 HTTP API
       |-- 成功：标记 delivered
       `-- 失败：记录原因并延迟重试
```

SQLite 使用 WAL 模式。容器需要为数据库目录挂载持久卷。

### 8.4 重试规则

- HTTP 连接和读取超时：3 至 5 秒；
- 使用简单的指数退避，例如 1、2、4、8、16、30 秒；
- `429` 和 `5xx` 响应重试；
- 大多数其他 `4xx` 响应不重试；
- 达到最大次数后标记为 dead letter；
- dead letter 数量通过 Prometheus 暴露；
- 接收端按 `event_id` 实现幂等去重。

告警认证令牌从环境变量或容器 secret 读取，不写入配置和日志。

### 8.5 Alertmanager 选项

如果现有环境已经部署 Prometheus Alertmanager，可以考虑由 Prometheus 规则和 Alertmanager 完成告警分组、静默、抑制和路由。

如果业务明确要求调用指定接口，则保留程序内的 Webhook Outbox。两种模式应通过配置选择，避免同一故障被重复通知。

## 9. Prometheus 指标

Prometheus 指标使用秒作为延时单位。

### 9.1 主机级指标

```text
# TYPE fping_monitor_host_up gauge
fping_monitor_host_up{target="core-switch-01"} 1

# TYPE fping_monitor_probe_success gauge
fping_monitor_probe_success{target="core-switch-01",probe="icmp"} 1

# TYPE fping_monitor_probe_latency_seconds gauge
fping_monitor_probe_latency_seconds{target="core-switch-01",probe="icmp"} 0.0124

# TYPE fping_monitor_probe_packet_loss_ratio gauge
fping_monitor_probe_packet_loss_ratio{target="core-switch-01",probe="icmp"} 0.0

# TYPE fping_monitor_host_state_changes_total counter
fping_monitor_host_state_changes_total{target="core-switch-01",state="down"} 2
```

指标语义：

- `probe_success`：最近一轮探测是否成功；
- `host_up`：状态机确认后的稳定状态；
- `probe_latency_seconds`：最近一次成功响应的 RTT；
- `probe_packet_loss_ratio`：本轮丢包比例，范围为 0 至 1。

主机当前延时使用 Gauge，不为每台主机创建 Histogram，避免产生过多时间序列。

### 9.2 程序自身指标

程序本身必须是可监控对象，不能只监控目标主机。自监控指标应能回答以下问题：

- 进程是否还活着；
- 探测循环是否仍在正常运行；
- 最近一轮探测何时完成、耗时多久；
- `fping` 是否持续报错或超时；
- 实际加载了多少目标；
- 通知是否积压、失败或进入 dead letter；
- SQLite 是否可用；
- 配置最近一次加载是否成功；
- 当前程序版本和启动时间是什么。

建议暴露以下指标：

```text
# 固定值为 1，带有版本等低基数构建信息
fping_monitor_build_info{version="1.0.0",python_version="3.12.0",fping_version="5.x"} 1

# 进程启动时间
fping_monitor_start_time_seconds

# 当前加载的目标数量
fping_monitor_targets

# 探测轮次总数
fping_monitor_probe_rounds_total{result="success|partial|error"}

# 每轮探测耗时，适合使用 Histogram
fping_monitor_probe_round_duration_seconds

# 最近一轮探测开始和完成时间
fping_monitor_last_probe_start_timestamp_seconds
fping_monitor_last_probe_completion_timestamp_seconds

# 当前是否有探测轮次正在执行，0 或 1
fping_monitor_probe_round_in_progress

# fping 子进程执行次数和结果
fping_monitor_fping_process_total{result="success|timeout|error"}

# fping 子进程退出码；没有执行过时不暴露或使用约定值
fping_monitor_fping_last_exit_code

# 单个探测结果累计数量
fping_monitor_probe_results_total{result="up|timeout|resolve_error|process_error"}

# 通知发送和队列状态
fping_monitor_notification_attempts_total{result="success|retry|dead"}
fping_monitor_notification_queue_size
fping_monitor_notification_oldest_pending_age_seconds
fping_monitor_notification_dead_letters

# 配置加载结果和最近成功时间
fping_monitor_target_config_reload_total{result="success|failed"}
fping_monitor_last_successful_config_reload_timestamp_seconds

# SQLite 操作和健康状态
fping_monitor_storage_operation_total{operation="read|write",result="success|error"}
fping_monitor_storage_healthy

# 大面积故障保护是否激活，0 或 1
fping_monitor_mass_failure_protection_active
fping_monitor_mass_failure_events_total
```

此外，`prometheus-client` 默认提供 Python 进程指标，例如 CPU、内存、打开文件描述符、垃圾回收和进程启动时间。除非部署环境另有统一采集方案，应保留这些默认指标。

### 9.3 自监控指标设计约束

- 指标名称统一使用 `fping_monitor_` 前缀；
- Counter 使用 `_total` 后缀；
- 时间戳使用 Unix timestamp 秒；
- 持续时间和延时统一使用秒；
- 状态值使用 Gauge 的 0 或 1；
- 不把异常文本、文件路径、event ID 或 target ID 加入程序级指标标签；
- `result`、`operation` 等标签必须是代码中预定义的有限枚举；
- 探测循环失败时仍应尽量保持 HTTP 指标端点可用，以便 Prometheus 看见故障；
- 指标更新失败不能影响主探测逻辑。

### 9.4 程序内部 watchdog

仅有 `/healthz` 返回 200 不能证明探测任务仍在工作。程序需要维护简单的内部 watchdog 状态：

- 记录最近一轮探测开始时间；
- 记录最近一轮探测完成时间；
- 记录探测任务是否仍存在且未异常退出；
- 记录通知 worker 是否仍存在且未异常退出；
- 如果后台任务意外退出，主程序应记录错误并主动退出，让容器重启策略接管；
- 如果单轮探测超过配置的最大执行时间，应终止 `fping` 子进程，将本轮标记为失败，并继续下一轮；
- 不实现复杂的线程或进程监管框架，使用明确的 asyncio task 状态检查即可。

### 9.5 推荐 Prometheus 告警规则

部署时应同时提供一份示例告警规则。阈值可以通过部署配置调整。

```yaml
groups:
  - name: fping-monitor-self
    rules:
      - alert: FpingMonitorAbsent
        expr: absent(up{job="fping-monitor"} == 1)
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "fping-monitor 指标端点不可用"

      - alert: FpingMonitorProbeStalled
        expr: time() - fping_monitor_last_probe_completion_timestamp_seconds > 30
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "fping-monitor 探测循环已经停止或严重延迟"

      - alert: FpingMonitorProbeErrors
        expr: increase(fping_monitor_probe_rounds_total{result="error"}[5m]) >= 3
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "fping-monitor 连续出现探测轮次错误"

      - alert: FpingMonitorNotificationBacklog
        expr: fping_monitor_notification_queue_size > 0
          and fping_monitor_notification_oldest_pending_age_seconds > 300
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "fping-monitor 通知队列持续积压"

      - alert: FpingMonitorNotificationDeadLetters
        expr: fping_monitor_notification_dead_letters > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "fping-monitor 存在无法发送的通知"

      - alert: FpingMonitorStorageUnhealthy
        expr: fping_monitor_storage_healthy == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "fping-monitor SQLite 存储不可用"

      - alert: FpingMonitorNoTargets
        expr: fping_monitor_targets == 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "fping-monitor 当前没有加载任何监控目标"

      - alert: FpingMonitorMassFailureProtection
        expr: fping_monitor_mass_failure_protection_active == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "fping-monitor 检测到大面积探测失败"
```

`FpingMonitorProbeStalled` 的阈值应大于正常探测周期。默认探测周期为 10 秒时，可以从 30 秒开始；如果用户修改探测周期，告警规则也应同步调整。

程序无法在自己完全停止时发送自我告警，因此 `FpingMonitorAbsent` 必须由外部 Prometheus 负责判断。生产环境还应监控运行容器的重启次数和宿主机状态。

### 9.6 标签基数控制

允许的标签应保持有限和稳定：

- `target`：稳定的主机 ID；
- `probe`：`icmp`、`tcp` 或 `http`；
- 可选 `site` 和 `group`，但值集合必须较小。

禁止将以下内容作为标签：

- 时间戳；
- event ID 和 incident ID；
- 完整错误信息；
- URL 和 HTTP 响应正文；
- 任意用户输入；
- 经常变化的动态值。

如果 IP 地址可能变化，优先只使用稳定的 `target` 标签。

## 10. HTTP 端点

程序至少提供：

- `/metrics`：Prometheus 指标；
- `/healthz`：HTTP 进程是否存活；
- `/readyz`：是否成功加载目标且调度器正常完成探测。

`/readyz` 应检查：

- 目标配置已加载；
- 最近两个探测周期内完成过探测；
- 调度任务仍在运行；
- SQLite 可读写。

如果后续增加 `/status`，该接口只允许受控内网访问，且不能替代 Prometheus 指标。

## 11. 配置模型

建议使用 YAML 配置，并允许通过环境变量覆盖敏感项。

```yaml
server:
  listen: 0.0.0.0
  port: 9100

probe:
  interval_seconds: 10
  timeout_ms: 1000
  packets: 3
  batch_size: 200
  batch_jitter_ms: 500

state:
  down_after_failures: 3
  up_after_successes: 3
  mass_failure_ratio: 0.5

notification:
  enabled: true
  url: https://alert.example.internal/api/v1/events
  timeout_seconds: 5
  max_attempts: 8
  max_backoff_seconds: 60
  token_env: ALERT_API_TOKEN

storage:
  path: /var/lib/fping-monitor/state.db

targets:
  - id: core-switch-01
    address: 10.10.0.1
    labels:
      site: shanghai
      group: network

  - id: app-server-01
    address: 10.10.1.10
    labels:
      site: shanghai
      group: application
```

配置校验要求：

- target ID 唯一；
- IP 地址或 hostname 格式合法；
- 探测周期和阈值必须大于零；
- 标签名称来自白名单；
- 禁止标签名与内部标签冲突；
- 不允许把以 `-` 开头的目标传给 `fping`；
- 配置错误时启动失败并给出明确错误信息。

## 12. Python 模块划分

```text
src/fping_monitor/
|-- __main__.py          # 程序入口和信号处理
|-- app.py               # 依赖装配和生命周期
|-- config.py            # dataclass + 手工校验
|-- models.py            # Target、ProbeResult、AlertEvent
|-- scheduler.py         # 批次调度
|-- state.py             # 主机状态机
|-- storage.py           # SQLite outbox 和状态持久化
|-- metrics.py           # Prometheus 指标
|-- api.py               # /metrics、/healthz、/readyz
|-- logging.py           # 日志配置
|-- outbox.py            # OutboxNotifier / OutboxWorker
|-- notifications.py     # WebhookNotifier + 重试
|   `-- tcp.py           # 可选 TCP 探针
`-- notifications/
    |-- base.py
    |-- webhook.py
    `-- worker.py
```

模块保持小而明确。只有在多个实现确实需要共享接口时才抽象基类或 Protocol。

## 13. 技术栈

- Python 3.12 或更新的稳定版本；
- `asyncio`：子进程、HTTP 和服务生命周期；
- `prometheus-client`：指标暴露；
- `httpx`：异步 HTTP 通知；
- `dataclass` + 手工校验：配置解析（target/阈值/标签白名单），无 Pydantic；
- PyYAML：YAML 配置；
- Python `sqlite3`：通知 outbox；
- Python 标准库 `logging`，需要 JSON 日志时再增加结构化格式；
- pytest 和 pytest-asyncio：测试。

不要仅为了统一风格引入大型 Web 框架、ORM、依赖注入框架或任务队列。

## 14. 容器部署

### 14.1 Compose 关键配置

```yaml
services:
  monitor:
    image: fping-monitor:latest
    cap_add:
      - NET_RAW
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    user: "10001:10001"
    read_only: true
    tmpfs:
      - /tmp
    volumes:
      - ./config:/etc/fping-monitor:ro
      - monitor-data:/var/lib/fping-monitor
    ports:
      - "9100:9100"
    restart: unless-stopped
```

`cap_drop: ALL` 与 `cap_add: NET_RAW` 的组合需要分别在 Docker、rootful Podman 和 rootless Podman 中验证。

### 14.2 文件 capability 备选方案

可以在镜像构建时执行：

```bash
setcap cap_net_raw+ep /usr/bin/fping
```

但文件 capability 可能受镜像构建方式、存储驱动和 rootless Podman 影响，因此仍需实际测试。

### 14.3 资源起点

建议初始限制：

- CPU：1 核；
- 内存：256 MB；
- 如果增加大量 TCP/HTTP 探针，可提高至 512 MB。

上线前应使用接近 1000 个目标进行压力和故障测试。

## 15. 持久化边界

程序本地只保存：

- 待发送通知；
- 通知发送结果；
- 当前 incident ID；
- 可选的确认状态；
- 少量状态转换时间。

程序不保存延时历史，延时和在线状态的历史查询由 Prometheus 负责。

程序重启后：

- 未发送事件继续重试；
- 恢复状态后先重新探测校验；
- 如果停机时间过长，则旧状态视为陈旧；
- 不因读取到旧 DOWN 状态而立即重复通知。

## 16. 日志要求

日志输出到标准输出，方便 Docker 和 Podman 收集。

日志至少包含：

- 时间；
- 日志级别；
- 模块；
- target ID；
- 探测批次 ID；
- 状态变化；
- 通知 event ID；
- 错误类别。

不得记录：

- 告警接口认证令牌；
- 完整敏感请求头；
- 不必要的响应正文；
- 1000 台主机每一轮的成功日志。

正常探测使用汇总日志，单主机详细日志仅在失败或调试模式下输出。

## 17. 测试策略

### 17.1 单元测试

重点覆盖：

- `fping` 各类输出解析；
- 连续失败确认 DOWN；
- 连续成功确认恢复；
- 冷启动不发送恢复通知；
- 相同状态不重复通知；
- 大面积失败保护；
- HTTP 状态码重试判断；
- 配置校验；
- Prometheus 指标值更新。

### 17.2 集成测试

- 使用 fake `fping` 可执行文件返回固定输出；
- 使用本地测试 HTTP 服务模拟告警接口；
- 模拟超时、连接失败、429 和 5xx；
- 验证 SQLite 中的 pending、delivered 和 dead-letter 状态；
- 验证容器重启后待发通知继续处理。

### 17.3 容器测试

分别验证：

- Docker；
- rootful Podman；
- rootless Podman；
- `NET_RAW` 权限；
- 只读根文件系统；
- 非 root 用户；
- SIGTERM 优雅退出；
- 持久卷权限；
- 1000 个目标的执行时间和资源占用。

## 18. 实施阶段

### 第一阶段：MVP

- 静态 YAML 目标；
- 批量 `fping`；
- RTT、丢包和本轮成功状态；
- UP/DOWN 状态机；
- `/metrics`、`/healthz` 和 `/readyz`；
- 基础程序自监控指标，包括目标数、最近探测完成时间、轮次结果和构建信息；
- 状态变化调用 webhook；
- Docker/Podman 镜像；
- 输出解析器和状态机单元测试。

### 第二阶段：可靠性

- SQLite outbox；
- 通知重试和幂等；
- 大面积故障保护；
- 后台任务 watchdog，异常退出时让容器重启；
- 通知队列、SQLite、配置加载和大面积故障保护指标；
- 示例 Prometheus 自监控告警规则；
- 优雅退出；
- 配置重载；
- 故障注入测试。

### 第三阶段：按需增强

- CMDB 动态同步；
- TCP/HTTP 备用探针；
- 双监控节点；
- Alertmanager 集成；
- Grafana 看板；
- IPv6；
- 多地域分片。

## 19. 暂不采用的设计

当前规模下暂不采用：

- Redis；
- Kafka；
- Celery；
- Kubernetes Operator；
- 自研分布式选主；
- 每台主机一个进程或一个容器；
- 在程序内存储完整延时历史；
- 为所有模块设计复杂插件系统；
- 过度使用抽象工厂、元类或动态注册。

只有在实际规模、可靠性或业务需求证明有必要时，才增加这些复杂度。

## 20. 最终技术决策

- 探测引擎：`fping` 批量 ICMP；
- 开发语言：Python；
- 并发模型：`asyncio`，只用于 I/O；
- 状态管理：内存中的简单状态机；
- 告警可靠性：SQLite Outbox 和异步 Webhook worker；
- 指标：`prometheus-client` Pull 模式；
- 部署：单个 Docker/Podman 容器，非 root 加 `NET_RAW`；
- 历史数据：由 Prometheus 保存；
- 初始周期：10 秒；
- DOWN 确认：连续 3 轮失败；
- 恢复确认：连续 3 轮成功；
- 告警风暴保护：大面积失败时抑制逐主机通知；
- 编码风格：可读性优先，使用直接、普通、容易维护的 Python 写法。
