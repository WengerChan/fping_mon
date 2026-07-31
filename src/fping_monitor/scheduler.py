"""探测调度器。

调度器运行一个 async 循环：定期探测所有目标，把结果灌入
:class:`StateManager`，并把状态变化事件转发给 :class:`Notifier`。
接口设计成 `await probe_once()` 看起来像同步调用，方便在不 sleep
的情况下在测试里反复调用。
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from .config import AppConfig
from .metrics import MetricsStore
from .models import AlertEvent, ProbeResult, Target
from .notifications import Notifier
from .probes import probe_targets
from .state import HostState, HostStatus, StateChange, StateManager


_LOG = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return uuid.uuid4().hex


def _classify_round(results: dict[str, ProbeResult]) -> str:
    """把一批结果归类为 success / partial / error 之一。"""
    if not results:
        return "error"
    success = sum(1 for r in results.values() if r.success)
    if success == len(results):
        return "success"
    # 即便全部失败，也仍然算 partial —— 说明 fping 至少跑起来了
    return "partial"


@dataclass
class Scheduler:
    config: AppConfig
    state_manager: StateManager
    notifier: Optional[Notifier]
    metrics: MetricsStore
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    _round_count: int = 0
    _last_round_completed_at: Optional[float] = None
    # 测试可注入的探针函数；为 None 时调用真正的 fping
    probe_fn: Optional[Callable] = None
    # 大面积故障窗口起点；用于窗口结束后的 host_down 补发。
    _mass_failure_window_started_at: Optional[float] = None
    # 窗口内"应该发但被抑制了"的 host_down；窗口结束时为这些 host 补发
    _mass_failure_pending_down: set[str] = field(default_factory=set)

    @property
    def round_count(self) -> int:
        return self._round_count

    @property
    def last_round_completed_at(self) -> Optional[float]:
        return self._last_round_completed_at

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        """持续探测，直到 :meth:`request_stop` 被调用。

        循环节奏：用 `asyncio.wait_for(stop_event, timeout=interval)` 而不是
        `await asyncio.sleep(interval)`。两者都等 `interval` 秒，但前者
        在收到 stop 信号时会立即醒来；后者必须睡满 `interval` 才检查，
        关闭会有最长 1 个 interval 的延迟。
        """
        probe_cfg = self.config.probe
        while not self._stop_event.is_set():
            try:
                await self.probe_once()
            except Exception:  # noqa: BLE001 - 顶层兜底，保证循环继续
                _LOG.exception("probe_once 抛出异常，继续下一轮")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=probe_cfg.interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def probe_once(self) -> int:
        """执行一轮完整探测，返回本轮分批数。"""
        probe_cfg = self.config.probe
        targets = [s.target for s in self.state_manager.all_states()]
        if not targets:
            self.metrics.set_targets_loaded(0)
            self._record_round_complete("error", 0.0)
            return 0

        # 整批超时：单包超时的 N 倍再加 1s 兜底
        overall_timeout = max(1.0, probe_cfg.timeout_ms * probe_cfg.packets / 1000.0 * 2.0 + 1.0)

        self.metrics.mark_probe_round_start()
        start = time.monotonic()
        batches = 0
        all_results: list[ProbeResult] = []

        probe_callable = self.probe_fn if self.probe_fn is not None else probe_targets
        for offset in range(0, len(targets), probe_cfg.batch_size):
            batch = targets[offset : offset + probe_cfg.batch_size]
            results = await asyncio.to_thread(
                probe_callable,
                batch,
                probe_cfg.fping_binary,
                probe_cfg.packets,
                probe_cfg.timeout_ms,
                overall_timeout,
            )
            # 把探针返回的 dict 摊平成 list，方便状态机顺序处理
            for r in results.values():
                all_results.append(r)
            self.metrics.update_probe_results(results)
            self.metrics.record_fping_process(
                "error" if any(r.error == "process_error" for r in results.values())
                else "timeout" if all(not r.success for r in results.values())
                else "success",
                None,
            )
            batches += 1
            if probe_cfg.batch_jitter_ms > 0:
                # 错峰：在每批之间插入一段随机 sleep
                import random

                await asyncio.sleep(
                    random.uniform(0, probe_cfg.batch_jitter_ms) / 1000.0
                )

        duration = time.monotonic() - start

        # 1. 应用状态机结果；当整批同时失败（很可能是监控节点/出口
        #    出问题而非单台主机离线）时抑制逐台 DOWN 告警。
        #    仅对具备一定规模（>=5）的批次生效，避免单台主机离线被误判。
        changes = self.state_manager.apply(all_results)
        total = max(1, len(all_results))
        failed = sum(1 for r in all_results if not r.success)
        failed_ratio = failed / total
        mass_failure = total >= 5 and failed_ratio >= self.config.state.mass_failure_ratio
        prev_mass_failure_active = (
            self._mass_failure_window_started_at is not None
        )
        self.metrics.set_mass_failure(mass_failure)

        # 跟踪大面积故障窗口；用于窗口结束后的 host_down 补发。
        if mass_failure:
            if self._mass_failure_window_started_at is None:
                self._mass_failure_window_started_at = time.monotonic()
                self._mass_failure_pending_down.clear()
        else:
            self._mass_failure_window_started_at = None
            self._mass_failure_pending_down.clear()

        for change in changes:
            self.metrics.record_state_change(change)

        # 2. 用最新状态刷新 host_up 指标
        self.metrics.update_host_state(self.state_manager.all_states())

        # 3. 转发事件（mass_failure 期间跳过 DOWN，同时清掉 state 上分配的
        #    incident_id，以便窗口解除后能识别出"从未真正发出过"的主机）
        if self.notifier is not None and self.config.webhook.enabled:
            for change in changes:
                if change.event is None:
                    continue
                if mass_failure and change.event.event_type == "host_down":
                    change.host_state.incident_id = None
                    self._mass_failure_pending_down.add(change.target.id)
                    continue
                try:
                    await self.notifier.send(change.event)
                except Exception:  # noqa: BLE001 - 任何单事件失败都不能阻塞主循环
                    _LOG.exception(
                        "通知发送失败 (target=%s event_id=%s)",
                        change.target.id,
                        change.event.event_id,
                    )

        # 4. 大面积故障窗口结束后（或本次从激活转为未激活时），
        #    对仍处于 DOWN 但 incident_id 已存在的 host 不需要补发；
        #    对"被抑制了 host_down"且仍未真正发过 host_down 的 host，
        #    主动构造一次补发事件。
        if (
            prev_mass_failure_active
            and not mass_failure
            and self.notifier is not None
            and self.config.webhook.enabled
        ):
            await self._emit_mass_failure_recovery_down()

        self._record_round_complete(_classify_round({r.target_id: r for r in all_results}), duration)
        return batches

    async def _emit_mass_failure_recovery_down(self) -> None:
        """大面积故障窗口解除后，对窗口内被抑制的 host_down 补发一次。

        判定逻辑：以"当前状态仍为 DOWN 且 incident_id 为 None"为信号，
        说明这台主机在窗口内被抑制期间从未真正发出 host_down——状态机
        只产生了 incident_id=None 的恢复路径，但 DOWN 事件本身没发出去。
        """
        for state in self.state_manager.all_states():
            if state.status != HostStatus.DOWN:
                continue
            # incident_id 已经存在说明状态机此前已经为这台机分派过 incident
            # （无论是否真正发到 webhook），不会发重复。
            if state.incident_id is not None:
                continue
            event = AlertEvent(
                event_id=_new_id(),
                incident_id=_new_id(),
                event_type="host_down",
                target=state.target,
                occurred_at=_utcnow_iso(),
                confirmed_at=_utcnow_iso(),
                last_success_at=state.last_success_at,
                consecutive_failures=state.consecutive_failures,
                packet_loss_ratio=state.last_packet_loss_ratio,
                probe_type="icmp",
                monitor_instance=self.config.webhook.monitor_instance,
            )
            state.incident_id = event.incident_id
            try:
                await self.notifier.send(event)
                _LOG.info(
                    "mass-failure 补发 host_down (target=%s event_id=%s)",
                    state.target.id,
                    event.event_id,
                )
            except Exception:  # noqa: BLE001
                _LOG.exception(
                    "mass-failure 补发失败 (target=%s event_id=%s)",
                    state.target.id,
                    event.event_id,
                )

    def _record_round_complete(self, result: str, duration_seconds: float) -> None:
        self.metrics.mark_probe_round_complete(duration_seconds, result)
        self._round_count += 1
        self._last_round_completed_at = time.time()
        self.metrics.set_targets_loaded(len(self.state_manager.all_states()))
