"""监控程序共用的领域模型。

整个项目只使用普通 dataclass，不引入 ORM / Pydantic 等框架，方便阅读
和测试构造。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Target:
    """单个被监控的主机。

    `id` 是稳定的标识符，会出现在指标、通知和日志中，必须唯一；
    `address` 是传给 fping 的目标（IP 或可解析的域名）；`labels`
    是低基数的静态标签，可在后续扩展中作为指标标签使用。
    """

    id: str
    address: str
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ProbeResult:
    """一台主机一轮探测的结果。

    `success` 表示本轮是否至少收到一个应答包。`latency_seconds` 是
    本轮应答的平均 RTT（秒），若全部丢失则为 None。
    `packet_loss_ratio` 取值范围 0.0 ~ 1.0。
    """

    target_id: str
    success: bool
    latency_seconds: Optional[float] = None
    packet_loss_ratio: float = 1.0
    # 失败原因，取值： "timeout" / "resolve_error" / "process_error"
    error: Optional[str] = None


@dataclass
class AlertEvent:
    """一个状态变化事件，交给通知模块处理。

    `event_type` 取值 "host_down" 或 "host_recovered"。`incident_id`
    在同一轮 down/recovered 之间保持一致，便于接收端关联；
    `event_id` 每次调用都不同。`confirmed_at` 是状态机判定发生
    变化的时间；`last_success_at` 是变化前最后一次成功探测的时间
    （首次事件时可为 None）。
    """

    event_id: str
    incident_id: str
    event_type: str
    target: Target
    occurred_at: str
    confirmed_at: str
    last_success_at: Optional[str]
    consecutive_failures: int
    packet_loss_ratio: float
    probe_type: str = "icmp"
    monitor_instance: str = "monitor-a"
