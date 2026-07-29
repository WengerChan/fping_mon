# fping-monitor 架构图

本文档用图把 `arch.md` 的文字设计与 `src/fping_monitor/` 下的真实代码对应起来。每张 Mermaid 图的节点都使用了相对路径（`src/fping_monitor/...`）作为可点击链接，渲染后会直接跳到代码位置。

阅读建议：先看 [1. 部署/进程视图](#1-部署进程视图) 与 [2. 容器内模块视图](#2-容器内模块视图) 把握整体；再按需深入 [3. 探测主循环](#3-探测主循环调用链) / [4. 通知投递](#4-通知投递调用链) / [5. HTTP 端点](#5-http-与健康检查视图) / [6. 状态机](#6-状态机迁移图)；最后用 [7. 模块速查表](#7-模块速查表) 找到具体实现。

## 1. 部署/进程视图

```mermaid
flowchart LR
    subgraph container["fping-monitor 容器 (非 root + NET_RAW)"]
        P["Python 主进程<br/>python -m fping_monitor"]
        F["/usr/bin/fping 5.x<br/>(setcap cap_net_raw+ep)"]
        DB[("/var/lib/fping-monitor/state.db<br/>SQLite WAL")]
    end

    subgraph host["宿主机 / 同网段"]
        PROM["Prometheus<br/>(抓取 :9100/metrics)"]
        ALERT["告警接口<br/>POST /api/v1/events"]
    end

    T["被监控目标 × N<br/>(ICMP)"]

    P -- "asyncio.to_thread" --> F
    P <-- "读写" --> DB
    P -- "/metrics 9100/tcp" --> PROM
    P -- "HTTPS POST + Bearer" --> ALERT
    F -- "ICMP echo" --> T
    PROM -. "rules → Alertmanager" .-> ALERT
```

要点：

- fping 需要 `CAP_NET_RAW` 才能发 ICMP；通过 `setcap` 让非 root 用户也能用，无需 root 进程。
- SQLite 状态文件必须挂载持久卷，否则容器重启会丢失未发送告警。
- Prometheus 走 Pull；如果业务侧已经部署 Alertmanager，可让它负责告警去重、分组与抑制。

## 2. 容器内模块视图

```mermaid
flowchart TB
    main["__main__.py: main()"] --> app_run
    app_run["app.py: Application.run()"] --> sched_loop
    app_run --> http_start
    app_run --> worker_start
    app_run --> stats_task

    sched_loop["scheduler.py: Scheduler.run_forever()"] --> probe_once
    http_start["api.py: MetricsHTTPServer.start()"]
    worker_start["outbox.py: OutboxWorker.start()"]
    stats_task["app.py: Application._publish_outbox_stats()"]

    probe_once["scheduler.py: Scheduler.probe_once()"] --> prober
    probe_once --> metrics_probe
    probe_once --> state_apply
    probe_once --> metrics_state
    probe_once --> notify_send

    prober["probes.py: probe_targets()"] --> fping["fping 子进程<br/>-C 3 -q -t 1000 -p 200"]
    metrics_probe["metrics.py: MetricsStore.update_probe_results()"]
    state_apply["state.py: StateManager.apply()"]
    metrics_state["metrics.py: MetricsStore.update_host_state() / record_state_change()"]
    notify_send["outbox.py: OutboxNotifier.send()"] --> outbox_db["storage.py: Outbox.enqueue()"]

    worker_loop["outbox.py: OutboxWorker.run_forever()"] --> drain
    drain["outbox.py: OutboxWorker.drain_once()"] --> deliver
    deliver["outbox.py: OutboxWorker._deliver()"] --> webhook
    webhook["notifications.py: WebhookNotifier.send()"] --> remote["外部告警接口"]
```

模块速记（按职责）：

| 角色 | 主要文件 |
| ---- | -------- |
| 入口与装配 | [`__main__.py`](../../src/fping_monitor/__main__.py), [`app.py`](../../src/fping_monitor/app.py) |
| 探测 | [`probes.py`](../../src/fping_monitor/probes.py) |
| 状态机 | [`state.py`](../../src/fping_monitor/state.py) |
| 指标 | [`metrics.py`](../../src/fping_monitor/metrics.py) |
| 通知传输 | [`notifications.py`](../../src/fping_monitor/notifications.py) |
| Outbox | [`storage.py`](../../src/fping_monitor/storage.py), [`outbox.py`](../../src/fping_monitor/outbox.py) |
| HTTP 端点 | [`api.py`](../../src/fping_monitor/api.py) |
| 数据模型 | [`models.py`](../../src/fping_monitor/models.py) |
| 配置 | [`config.py`](../../src/fping_monitor/config.py) |
| 日志 | [`logging.py`](../../src/fping_monitor/logging.py) |

## 3. 探测主循环调用链

```mermaid
sequenceDiagram
    autonumber
    participant M as __main__: main()
    participant A as app: Application
    participant S as scheduler: Scheduler
    participant P as probes: probe_targets
    participant F as fping 子进程
    participant MS as metrics: MetricsStore
    participant ST as state: StateManager
    participant ON as outbox: OutboxNotifier
    participant DB as storage: Outbox

    M->>A: bootstrap(config) → Application.run()
    A->>S: scheduler.run_forever()
    loop 每 interval_seconds
        S->>S: probe_once()
        S->>MS: mark_probe_round_start()
        loop 按 batch_size 切片
            S->>P: asyncio.to_thread(probe_targets, batch)
            P->>F: subprocess.run(fping -C 3 -q -t 1000 -p 200 …)
            F-->>P: stdout (每目标一行)
            P-->>S: dict[target_id, ProbeResult]
            S->>MS: update_probe_results(results)
            S->>MS: record_fping_process(...)
        end
        S->>ST: apply(all_results)
        ST-->>S: list[StateChange]
        S->>MS: record_state_change / update_host_state
        Note over S: 计算 failed_ratio<br/>仅当 total >= 5 时启用 mass-failure 保护
        alt mass_failure=False
            loop 每个 change.event
                S->>ON: await send(event)
                ON->>DB: enqueue(event)  [to_thread]
            end
        else mass_failure=True
            Note over S: 跳过所有 host_down 通知<br/>只更新状态与指标
        end
        S->>MS: mark_probe_round_complete(duration, result)
    end
```

要点：

- `probe_targets` 通过 `asyncio.to_thread` 跑在默认线程池，不阻塞事件循环。
- fping 子进程有整体超时 (`overall_timeout`)，避免极端情况下拖死整轮。
- `mass_failure` 保护默认只在 ≥ 5 台主机的批次上触发，避免单台离线被误判为监控节点问题。
- 状态机 `apply` 在一帧内顺序消费所有结果；如果需要"每轮 N 个成功才确认 UP"，把多轮结果连续灌入即可。

## 4. 通知投递调用链

```mermaid
sequenceDiagram
    autonumber
    participant S as scheduler: Scheduler.probe_once
    participant ON as outbox: OutboxNotifier
    participant DB as storage: Outbox (SQLite WAL)
    participant W as outbox: OutboxWorker
    participant H as notifications: WebhookNotifier
    participant R as 告警接收端

    S->>ON: await send(event)
    ON->>DB: enqueue(event) [asyncio.to_thread]
    Note over DB: 唯一索引 event_id 保证幂等

    loop worker poll tick
        W->>DB: claim_due() (status=pending, next_attempt_at<=now)
        DB-->>W: list[StoredEvent] (in_flight)
        loop 每个 row
            W->>H: await transport.send(event)
            alt 2xx
                H-->>W: OK
                W->>DB: mark_delivered(row_id)
            else 4xx (不可重试)
                H-->>W: raise NotificationFailed
                W->>DB: mark_dead(row_id, error)
            else 5xx / transport (可重试)
                loop attempts < max_attempts
                    H-->>W: retryable
                end
                H-->>W: raise RetriesExhausted
                W->>DB: mark_retry(row_id, backoff, error)
            end
        end
    end
    H->>R: HTTPS POST (Bearer $ALERT_API_TOKEN)
```

要点：

- `NotificationFailed`：单次响应即放弃（典型 4xx），不再排队。
- `RetriesExhausted`：5xx / 传输错误在线用尽 `max_attempts` 后抛出，worker 写入更长 backoff 再次调度。
- 进程崩溃时遗留的 `in_flight` 行会在 `Outbox.__init__` 里通过 `reclaim_in_flight()` 回到 `pending`，不丢事件。
- `_publish_outbox_stats` 每 5 秒刷新 outbox 指标（队列长度、dead 数、最旧 pending 时长）。

## 5. HTTP 与健康检查视图

```mermaid
flowchart LR
    subgraph h["api.py: MetricsHTTPServer (后台线程)"]
        h_metrics["GET /metrics"] --> gen["prometheus_client.generate_latest(REGISTRY)"]
        h_health["GET /healthz"] --> ok["固定 200<br/>{\"status\":\"ok\"}"]
        h_ready["GET /readyz"] --> rc["app: _build_ready_check() 闭包"]
        rc --> check1{"round_count > 0 ?"}
        check1 -- 否 --> notready["503 reason=尚未完成任何探测轮次"]
        check1 -- 是 --> check2{"now - last_round_completed_at<br/><= max(interval*2, 60) ?"}
        check2 -- 否 --> stale["503 reason=最近一轮探测距今…"]
        check2 -- 是 --> ready["200 reason=ok"]
    end

    P["Prometheus"] -- "scrape :9100/metrics" --> h_metrics
    K["K8s/容器健康检查"] -- "HTTP :9100/healthz" --> h_health
    LB["LB / 探针"] -- "HTTP :9100/readyz" --> h_ready
```

要点：

- `/healthz` 只证明 HTTP 进程在跑，不反映探测是否健康。
- `/readyz` 才会判断"最近一轮是否在 2× interval 内完成"；`docker compose` 的 healthcheck 与 Prometheus 的 `up` 指标应当指向 `/healthz`。
- 当 `notification.enabled=false` 时，outbox 表依然存在但 worker 不启动；此时 `fping_monitor_notification_queue_size` 始终为 0。

## 6. 状态机迁移图

```mermaid
stateDiagram-v2
    [*] --> UNKNOWN
    UNKNOWN --> UP: 连续 3 次成功<br/>(up_after_successes)
    UNKNOWN --> DOWN: 连续 3 次失败<br/>(down_after_failures)
    UP --> DOWN: 连续 3 次失败
    DOWN --> UP: 连续 3 次成功

    note right of UP
        进入 UP 时若 previous=UNKNOWN
        -> 不产生任何通知（冷启动）
    end note

    note right of DOWN
        进入 DOWN: 分配/复用 incident_id<br/>产生 event_type=host_down
    end note

    note left of DOWN
        DOWN → UP: 复用 incident_id<br/>产生 event_type=host_recovered
    end note
```

实现位置：[`state.py:StateManager.apply`](../../src/fping_monitor/state.py)。

## 7. 模块速查表

| 模块 | 文件 | 关键导出 | 角色 |
| ---- | ---- | -------- | ---- |
| 配置加载 | [`config.py`](../../src/fping_monitor/config.py) | `load_config`, `AppConfig`, `ConfigError` | 解析 YAML，校验 target/threshold/标签白名单 |
| 领域模型 | [`models.py`](../../src/fping_monitor/models.py) | `Target`, `ProbeResult`, `AlertEvent` | 跨模块共享的纯 dataclass |
| 日志 | [`logging.py`](../../src/fping_monitor/logging.py) | `configure_logging` | 统一 stdout JSON-ish 格式 |
| fping 探针 | [`probes.py`](../../src/fping_monitor/probes.py) | `parse_fping_output`, `probe_targets`, `build_fping_command` | 子进程调用与解析 |
| 状态机 | [`state.py`](../../src/fping_monitor/state.py) | `StateManager`, `HostState`, `StateChange`, `HostStatus` | 连续成功/失败计数 + 状态迁移 |
| Prometheus 指标 | [`metrics.py`](../../src/fping_monitor/metrics.py) | `MetricsStore`, `get_default_store` | 全部 `fping_monitor_*` 指标 |
| 通知传输 | [`notifications.py`](../../src/fping_monitor/notifications.py) | `WebhookNotifier`, `NotificationFailed`, `RetriesExhausted` | Webhook POST + 重试 |
| SQLite Outbox | [`storage.py`](../../src/fping_monitor/storage.py) | `Outbox`, `StoredEvent` | WAL outbox 单表 |
| Outbox 适配层 | [`outbox.py`](../../src/fping_monitor/outbox.py) | `OutboxNotifier`, `OutboxWorker` | 把事件写盘 + 后台消费 |
| 调度器 | [`scheduler.py`](../../src/fping_monitor/scheduler.py) | `Scheduler` | 主循环、mass-failure 抑制、状态机调用 |
| HTTP 端点 | [`api.py`](../../src/fping_monitor/api.py) | `MetricsHTTPServer`, `make_handler` | `/metrics` `/healthz` `/readyz` |
| 应用装配 | [`app.py`](../../src/fping_monitor/app.py) | `Application`, `bootstrap`, `_build_ready_check` | 全部依赖装配 + SIGHUP reload |
| 入口 | [`__main__.py`](../../src/fping_monitor/__main__.py) | `main` | argparse + 信号处理 |

## 8. 阅读建议

1. 想搞清楚"如何配置" → 看 [`config.py`](../../src/fping_monitor/config.py) 与 `config/config.example.yaml`。
2. 想搞清楚"如何探测" → 看 [`probes.py`](../../src/fping_monitor/probes.py) + 真实 `fping 5.x` 文档。
3. 想搞清楚"如何判断上下线" → 看 [`state.py`](../../src/fping_monitor/state.py) 的 `apply` / `_next_status`。
4. 想搞清楚"如何保证通知不丢" → 看 [`storage.py`](../../src/fping_monitor/storage.py) + [`outbox.py`](../../src/fping_monitor/outbox.py)。
5. 想搞清楚"启动流程" → 看 [`app.py:bootstrap`](../../src/fping_monitor/app.py) + [`__main__.py:main`](../../src/fping_monitor/__main__.py)。

更深入的设计动机写在 [`arch.md`](../../arch.md)，使用说明写在 [`docs/usage.md`](usage.md)。
