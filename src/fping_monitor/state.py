"""主机状态机。

状态机刻意保持简单：跟踪每台目标连续成功/失败的次数，仅在双向
越过配置阈值时才发出 `状态变化` 事件。冷启动规则（首次确认 UP
不产生恢复事件）和 incident_id 的连续性在这里处理，而不是调度器。
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, Optional

from .models import AlertEvent, ProbeResult, Target


_LOG = logging.getLogger(__name__)


class HostStatus(str, Enum):
    UNKNOWN = "unknown"
    UP = "up"
    DOWN = "down"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return uuid.uuid4().hex


@dataclass
class HostState:
    target: Target
    status: HostStatus = HostStatus.UNKNOWN
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    last_probe_at: Optional[str] = None
    last_success_at: Optional[str] = None
    state_changed_at: Optional[str] = None
    last_latency_seconds: Optional[float] = None
    last_packet_loss_ratio: float = 1.0
    # 当存在活动 incident 时设置
    incident_id: Optional[str] = None


@dataclass
class StateChange:
    """状态机观察到的一次状态迁移。

    不需要发通知的迁移（例如冷启动后首次确认 UP）会把 `event`
    置为 None。
    """

    target: Target
    previous: HostStatus
    current: HostStatus
    host_state: HostState
    event: Optional[AlertEvent] = None


@dataclass
class StateManager:
    down_after_failures: int = 3
    up_after_successes: int = 3
    monitor_instance: str = "monitor-a"
    # 被移除但仍在保留窗口内的 HostState：id -> (HostState, 移除时间戳)
    _recently_removed: dict[str, tuple[HostState, float]] = field(default_factory=dict)
    # 移除后保留多久（秒）；超过则真正从内存里删除
    remove_grace_seconds: float = 300.0
    _states: dict[str, HostState] = field(default_factory=dict)

    def upsert_targets(self, targets: Iterable[Target]) -> list[str]:
        """确保每个已知目标都有一个 HostState。

        新增目标初始为 UNKNOWN；被移除的目标进入保留窗口（默认 5 分钟），
        在窗口内如果再次出现，复用原 HostState（保留 incident_id 与计数），
        以免刚发出去的 host_down 还没收到响应就被丢。

        返回真正被丢弃的 host id 列表（保留窗口已过期的）。
        """
        import time

        now = time.time()
        keep: set[str] = set()
        for t in targets:
            keep.add(t.id)
            if t.id in self._states:
                # 刷新地址和标签，使配置重载生效
                self._states[t.id].target = t
                continue
            # 不在主表里：先看保留窗口里有没有
            if t.id in self._recently_removed:
                old_state, _ = self._recently_removed.pop(t.id)
                old_state.target = t
                self._states[t.id] = old_state
                _LOG.info(
                    "host 重新出现，复用旧状态 (target=%s, status=%s)",
                    t.id,
                    old_state.status.value,
                )
            else:
                self._states[t.id] = HostState(target=t)

        # 不在 keep 集合里的：进保留窗口
        for tid in [tid for tid in self._states if tid not in keep]:
            state = self._states.pop(tid)
            self._recently_removed[tid] = (state, now)

        # 清理过期的保留项
        expired = [
            tid
            for tid, (_, ts) in self._recently_removed.items()
            if now - ts >= self.remove_grace_seconds
        ]
        for tid in expired:
            self._recently_removed.pop(tid, None)
        return expired

    def get(self, target_id: str) -> Optional[HostState]:
        return self._states.get(target_id)

    def all_states(self) -> list[HostState]:
        return list(self._states.values())

    def _is_success(self, result: ProbeResult) -> bool:
        return result.success and (result.error is None)

    def apply(self, results: list[ProbeResult]) -> list[StateChange]:
        """应用一批探测结果，返回所有发生的状态变化。

        按列表顺序依次处理，以便一次批量回填多轮结果时按序生效。
        """
        now = _utcnow_iso()
        changes: list[StateChange] = []
        for result in results:
            state = self._states.get(result.target_id)
            if state is None:
                continue
            change = self._apply_one(state, result, now)
            if change is not None:
                changes.append(change)
        return changes

    def _apply_one(
        self, state: HostState, result: ProbeResult, now: str
    ) -> Optional[StateChange]:
        state.last_probe_at = now
        state.last_packet_loss_ratio = result.packet_loss_ratio
        if result.latency_seconds is not None:
            state.last_latency_seconds = result.latency_seconds

        previous = state.status
        if self._is_success(result):
            state.consecutive_successes += 1
            state.consecutive_failures = 0
            state.last_success_at = now
        else:
            state.consecutive_failures += 1
            state.consecutive_successes = 0

        new_status = self._next_status(state)
        if new_status == previous:
            return None

        state.status = new_status
        state.state_changed_at = now
        event = self._maybe_build_event(state, previous, new_status, now)
        return StateChange(
            target=state.target,
            previous=previous,
            current=new_status,
            host_state=state,
            event=event,
        )

    def _next_status(self, state: HostState) -> HostStatus:
        if state.status == HostStatus.UNKNOWN:
            if state.consecutive_successes >= self.up_after_successes:
                return HostStatus.UP
            if state.consecutive_failures >= self.down_after_failures:
                return HostStatus.DOWN
            return HostStatus.UNKNOWN
        if state.status == HostStatus.UP:
            if state.consecutive_failures >= self.down_after_failures:
                return HostStatus.DOWN
            return HostStatus.UP
        # DOWN: 需要连续成功才能回到 UP
        if state.consecutive_successes >= self.up_after_successes:
            return HostStatus.UP
        return HostStatus.DOWN

    def _maybe_build_event(
        self,
        state: HostState,
        previous: HostStatus,
        current: HostStatus,
        now: str,
    ) -> Optional[AlertEvent]:
        # 冷启动期间不发送通知
        if previous == HostStatus.UNKNOWN and current == HostStatus.UP:
            state.incident_id = None
            return None

        if current == HostStatus.DOWN:
            state.incident_id = state.incident_id or _new_id()
            return AlertEvent(
                event_id=_new_id(),
                incident_id=state.incident_id,
                event_type="host_down",
                target=state.target,
                occurred_at=now,
                confirmed_at=now,
                last_success_at=state.last_success_at,
                consecutive_failures=state.consecutive_failures,
                packet_loss_ratio=state.last_packet_loss_ratio,
                probe_type="icmp",
                monitor_instance=self.monitor_instance,
            )

        if current == HostStatus.UP and previous == HostStatus.DOWN:
            # 如果此前从未发出过 host_down（threshold 没达到或被 mass-failure 抑制），
            # 就不应该单独发出 host_recovered：接收方没有对应的 incident 可关联。
            incident_id = state.incident_id
            if incident_id is None:
                _LOG.info(
                    "跳过 host_recovered：未产生过 host_down (target=%s)",
                    state.target.id,
                )
                state.incident_id = None
                return None
            state.incident_id = None  # incident 结束
            return AlertEvent(
                event_id=_new_id(),
                incident_id=incident_id,
                event_type="host_recovered",
                target=state.target,
                occurred_at=now,
                confirmed_at=now,
                last_success_at=state.last_success_at,
                consecutive_failures=0,
                packet_loss_ratio=state.last_packet_loss_ratio,
                probe_type="icmp",
                monitor_instance=self.monitor_instance,
            )
        return None
