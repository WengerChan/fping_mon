"""Prometheus 指标 Facade。

所有指标名、标签键、标签允许值都在 :class:`MetricsStore` 中预先
定义，业务代码通过该类更新指标，而不是直接使用 prometheus_client，
便于控制标签基数和后续扩展。

通过 :func:`get_default_store` 获取进程级单例；测试可以自己构造
带独立 CollectorRegistry 的实例。
"""

from __future__ import annotations

import platform
import subprocess
import time
from typing import Iterable, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from .models import ProbeResult
from .state import HostState, HostStatus, StateChange


# 程序级 Counter 的 `result` 标签允许值
_PROBE_RESULT_VALUES = ("up", "timeout", "resolve_error", "process_error")
_ROUND_RESULT_VALUES = ("success", "partial", "error")
_FPING_RESULT_VALUES = ("success", "timeout", "error")
_NOTIFICATION_RESULT_VALUES = ("success", "retry", "dead")


def _probe_round_duration_buckets() -> tuple[float, ...]:
    # 1000 主机 5s 一轮，多数轮次应在 1 秒以内完成；这里覆盖到 30s
    return (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


class MetricsStore:
    """拥有程序对外暴露的所有指标。

    每个 MetricsStore 对应一个 CollectorRegistry。生产代码用
    :func:`get_default_store` 拿进程级单例；测试可以传自定义 registry
    避免冲突。
    """

    def __init__(
        self,
        registry: CollectorRegistry,
        *,
        version: str,
        fping_binary: str,
    ) -> None:
        self._registry = registry
        # ---- 主机级指标 ----------------------------------------
        self.host_up = Gauge(
            "fping_monitor_host_up",
            "1 表示主机已确认在线，0 表示已确认离线或未知",
            labelnames=("target",),
            registry=registry,
        )
        self.probe_success = Gauge(
            "fping_monitor_probe_success",
            "本轮探测是否成功（1=成功，0=失败）",
            labelnames=("target", "probe"),
            registry=registry,
        )
        self.probe_latency_seconds = Gauge(
            "fping_monitor_probe_latency_seconds",
            "最近一次成功应答的 RTT（秒）",
            labelnames=("target", "probe"),
            registry=registry,
        )
        self.probe_packet_loss_ratio = Gauge(
            "fping_monitor_probe_packet_loss_ratio",
            "最近一轮的丢包比例（0.0 ~ 1.0）",
            labelnames=("target", "probe"),
            registry=registry,
        )
        self.host_state_changes_total = Counter(
            "fping_monitor_host_state_changes_total",
            "主机已确认状态变化的总次数",
            labelnames=("target", "state"),
            registry=registry,
        )

        # ---- 程序自身指标 -----------------------------------------
        self.targets_loaded = Gauge(
            "fping_monitor_targets",
            "当前已加载的监控目标数量",
            registry=registry,
        )
        self.build_info = Gauge(
            "fping_monitor_build_info",
            "描述构建信息的静态标签，值恒为 1",
            labelnames=("version", "python_version", "fping_version"),
            registry=registry,
        )
        self.start_time_seconds = Gauge(
            "fping_monitor_start_time_seconds",
            "进程启动时间（Unix 时间戳，秒）",
            registry=registry,
        )
        self.last_probe_start_timestamp_seconds = Gauge(
            "fping_monitor_last_probe_start_timestamp_seconds",
            "最近一轮探测开始的 Unix 时间戳（秒）",
            registry=registry,
        )
        self.last_probe_completion_timestamp_seconds = Gauge(
            "fping_monitor_last_probe_completion_timestamp_seconds",
            "最近一轮探测完成的 Unix 时间戳（秒）",
            registry=registry,
        )
        self.probe_round_in_progress = Gauge(
            "fping_monitor_probe_round_in_progress",
            "探测轮次进行中为 1，否则为 0",
            registry=registry,
        )
        self.probe_rounds_total = Counter(
            "fping_monitor_probe_rounds_total",
            "按结果分类的探测轮次计数",
            labelnames=("result",),
            registry=registry,
        )
        self.probe_round_duration_seconds = Histogram(
            "fping_monitor_probe_round_duration_seconds",
            "单轮探测的耗时（秒）",
            buckets=_probe_round_duration_buckets(),
            registry=registry,
        )
        self.fping_process_total = Counter(
            "fping_monitor_fping_process_total",
            "按结果分类的 fping 子进程调用次数",
            labelnames=("result",),
            registry=registry,
        )
        self.fping_last_exit_code = Gauge(
            "fping_monitor_fping_last_exit_code",
            "最近一次 fping 进程的退出码",
            registry=registry,
        )
        self.probe_results_total = Counter(
            "fping_monitor_probe_results_total",
            "按分类统计的每目标探测结果数",
            labelnames=("result",),
            registry=registry,
        )
        self.mass_failure_protection_active = Gauge(
            "fping_monitor_mass_failure_protection_active",
            "当最近一轮触发了大面积故障保护时为 1，否则为 0",
            registry=registry,
        )
        self.mass_failure_events_total = Counter(
            "fping_monitor_mass_failure_events_total",
            "触发大面积故障保护的探测轮次总数",
            registry=registry,
        )
        self.notification_queue_size = Gauge(
            "fping_monitor_notification_queue_size",
            "通知 outbox 中尚未发送的行数",
            registry=registry,
        )
        self.notification_oldest_pending_age_seconds = Gauge(
            "fping_monitor_notification_oldest_pending_age_seconds",
            "最旧待发送通知的存活时长（秒），无待发送时为 0",
            registry=registry,
        )
        self.notification_dead_letters = Gauge(
            "fping_monitor_notification_dead_letters",
            "outbox 中已被标记为 dead 的行数",
            registry=registry,
        )
        self.storage_healthy = Gauge(
            "fping_monitor_storage_healthy",
            "outbox 数据库可达时为 1，否则为 0",
            registry=registry,
        )
        self.notification_attempts_total = Counter(
            "fping_monitor_notification_attempts_total",
            "按结果分类的通知发送尝试次数",
            labelnames=("result",),
            registry=registry,
        )
        self.target_config_reload_total = Counter(
            "fping_monitor_target_config_reload_total",
            "按结果分类的 SIGHUP 配置重载次数",
            labelnames=("result",),
            registry=registry,
        )
        self.last_successful_config_reload_timestamp_seconds = Gauge(
            "fping_monitor_last_successful_config_reload_timestamp_seconds",
            "最近一次成功 SIGHUP 重载的 Unix 时间戳（秒）",
            registry=registry,
        )
        self.storage_operation_total = Counter(
            "fping_monitor_storage_operation_total",
            "按操作/结果分类的 outbox 操作次数",
            labelnames=("operation", "result"),
            registry=registry,
        )

        # 预先以 0 初始化 Counter，避免在第一次 inc 之前看不到对应标签
        for value in _ROUND_RESULT_VALUES:
            self.probe_rounds_total.labels(result=value)
        for value in _FPING_RESULT_VALUES:
            self.fping_process_total.labels(result=value)
        for value in _PROBE_RESULT_VALUES:
            self.probe_results_total.labels(result=value)
        for value in _NOTIFICATION_RESULT_VALUES:
            self.notification_attempts_total.labels(result=value)
        for value in ("success", "failed"):
            self.target_config_reload_total.labels(result=value)
        for op in ("read", "write"):
            for value in ("success", "error"):
                self.storage_operation_total.labels(operation=op, result=value)

        fping_version = _detect_fping_version(fping_binary)
        self.build_info.labels(
            version=version,
            python_version=platform.python_version(),
            fping_version=fping_version,
        ).set(1)
        self.start_time_seconds.set(time.time())
        self.probe_round_in_progress.set(0)
        self.fping_last_exit_code.set(0)
        self.targets_loaded.set(0)
        self.mass_failure_protection_active.set(0)
        self.notification_queue_size.set(0)
        self.notification_dead_letters.set(0)
        self.notification_oldest_pending_age_seconds.set(0.0)
        self.storage_healthy.set(1)
        self.last_successful_config_reload_timestamp_seconds.set(0)

    # ------------------------------------------------------------------
    # 主机级更新
    # ------------------------------------------------------------------
    def update_probe_results(
        self, results: dict[str, ProbeResult], probe: str = "icmp"
    ) -> None:
        for target_id, result in results.items():
            self.probe_success.labels(target=target_id, probe=probe).set(
                1 if result.success else 0
            )
            if result.latency_seconds is not None:
                self.probe_latency_seconds.labels(
                    target=target_id, probe=probe
                ).set(result.latency_seconds)
            self.probe_packet_loss_ratio.labels(
                target=target_id, probe=probe
            ).set(result.packet_loss_ratio)
            self.probe_results_total.labels(result=_classify_probe(result)).inc()

    def update_host_state(self, states: Iterable[HostState]) -> None:
        for state in states:
            value = 1 if state.status == HostStatus.UP else 0
            self.host_up.labels(target=state.target.id).set(value)

    def record_state_change(self, change: StateChange) -> None:
        self.host_state_changes_total.labels(
            target=change.target.id, state=change.current.value
        ).inc()

    # ------------------------------------------------------------------
    # 程序自身指标更新
    # ------------------------------------------------------------------
    def set_targets_loaded(self, count: int) -> None:
        self.targets_loaded.set(count)

    def mark_probe_round_start(self) -> None:
        self.probe_round_in_progress.set(1)
        self.last_probe_start_timestamp_seconds.set(time.time())

    def mark_probe_round_complete(
        self, duration_seconds: float, result: str
    ) -> None:
        if result not in _ROUND_RESULT_VALUES:
            raise ValueError(
                f"probe round result 必须是 {_ROUND_RESULT_VALUES} 之一"
            )
        self.probe_round_in_progress.set(0)
        self.last_probe_completion_timestamp_seconds.set(time.time())
        self.probe_rounds_total.labels(result=result).inc()
        self.probe_round_duration_seconds.observe(duration_seconds)

    def record_fping_process(self, result: str, exit_code: Optional[int]) -> None:
        if result not in _FPING_RESULT_VALUES:
            raise ValueError(
                f"fping result 必须是 {_FPING_RESULT_VALUES} 之一"
            )
        self.fping_process_total.labels(result=result).inc()
        if exit_code is not None:
            self.fping_last_exit_code.set(exit_code)

    # ------------------------------------------------------------------
    # 大面积故障保护
    # ------------------------------------------------------------------
    def set_mass_failure(self, active: bool) -> None:
        self.mass_failure_protection_active.set(1 if active else 0)
        if active:
            self.mass_failure_events_total.inc()

    # ------------------------------------------------------------------
    # outbox 健康
    # ------------------------------------------------------------------
    def set_storage_healthy(self, healthy: bool) -> None:
        self.storage_healthy.set(1 if healthy else 0)

    def set_outbox_stats(
        self, *, pending: int, dead: int, oldest_pending_age: Optional[float]
    ) -> None:
        self.notification_queue_size.set(pending)
        self.notification_dead_letters.set(dead)
        self.notification_oldest_pending_age_seconds.set(
            oldest_pending_age if oldest_pending_age is not None else 0.0
        )

    def record_config_reload(self, success: bool, ts: Optional[float] = None) -> None:
        result = "success" if success else "failed"
        self.target_config_reload_total.labels(result=result).inc()
        if success:
            self.last_successful_config_reload_timestamp_seconds.set(
                ts if ts is not None else time.time()
            )

    def record_storage_operation(self, operation: str, success: bool) -> None:
        self.storage_operation_total.labels(
            operation=operation, result=("success" if success else "error")
        ).inc()


def _classify_probe(result: ProbeResult) -> str:
    if result.error in ("timeout", "resolve_error", "process_error"):
        return result.error
    if result.success:
        return "up"
    return "timeout"


def _detect_fping_version(binary: str) -> str:
    """读取 `fping -v` 的第一行；二进制不可用时返回 'unknown'。"""
    try:
        proc = subprocess.run(
            [binary, "-v"],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return "unknown"
    text = (proc.stdout or proc.stderr or "").strip()
    return text.splitlines()[0] if text else "unknown"


_DEFAULT: Optional[MetricsStore] = None


def get_default_store() -> MetricsStore:
    """获取进程级 MetricsStore 单例，首次调用时创建。"""
    global _DEFAULT
    if _DEFAULT is None:
        # 延迟导入避免在模块加载阶段就要求 prometheus_client 已初始化
        from prometheus_client import REGISTRY

        from . import __version__

        _DEFAULT = MetricsStore(
            REGISTRY,
            version=__version__,
            fping_binary="/usr/sbin/fping",
        )
    return _DEFAULT
