"""主机状态机测试。"""

from fping_monitor.models import ProbeResult, Target
from fping_monitor.state import HostStatus, StateManager


def _ok(target_id: str) -> ProbeResult:
    return ProbeResult(
        target_id=target_id, success=True, latency_seconds=0.001, packet_loss_ratio=0.0
    )


def _fail(target_id: str) -> ProbeResult:
    return ProbeResult(
        target_id=target_id, success=False, error="timeout", packet_loss_ratio=1.0
    )


def _target(tid: str) -> Target:
    return Target(id=tid, address=f"10.0.0.{tid[-1]}")


def test_three_consecutive_failures_confirm_down():
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])

    sm.apply([_ok("a")])  # 1 次成功，仍为 UNKNOWN
    sm.apply([_fail("a")])
    sm.apply([_fail("a")])
    changes = sm.apply([_fail("a")])
    assert len(changes) == 1
    c = changes[0]
    assert c.previous == HostStatus.UNKNOWN
    assert c.current == HostStatus.DOWN
    assert c.event is not None
    assert c.event.event_type == "host_down"


def test_recovery_requires_three_successes():
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])
    sm.apply([_fail("a"), _fail("a"), _fail("a")])  # -> DOWN
    sm.apply([_ok("a")])  # DOWN 状态下第 1 次成功
    changes = sm.apply([_ok("a")])
    assert changes == []
    changes = sm.apply([_ok("a")])
    assert len(changes) == 1
    assert changes[0].current == HostStatus.UP
    assert changes[0].event is not None
    assert changes[0].event.event_type == "host_recovered"
    # down 和 recovered 事件共享同一个 incident_id
    assert changes[0].event.incident_id


def test_cold_start_recovery_emits_no_event():
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])
    sm.apply([_ok("a"), _ok("a"), _ok("a")])
    # 从未发生过 DOWN，因此首次确认 UP 不应产生通知
    assert sm.get("a").status == HostStatus.UP
    assert sm.get("a").incident_id is None


def test_single_failure_does_not_change_state():
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])
    # UNKNOWN -> UP 需要连续 3 次成功，先喂够
    sm.apply([_ok("a"), _ok("a"), _ok("a")])
    assert sm.get("a").status == HostStatus.UP
    changes = sm.apply([_fail("a")])
    assert changes == []
    assert sm.get("a").status == HostStatus.UP


def test_intermediate_success_resets_failure_counter():
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])
    sm.apply([_ok("a")])  # 1 次成功
    sm.apply([_fail("a"), _fail("a")])
    sm.apply([_ok("a")])  # 中间成功会重置连续失败计数
    changes = sm.apply([_fail("a"), _fail("a")])
    # 此时只有 2 次连续失败，仍为 UNKNOWN
    assert changes == []
    assert sm.get("a").status == HostStatus.UNKNOWN


def test_upsert_targets_removes_stale_entries():
    sm = StateManager()
    sm.upsert_targets([_target("a"), _target("b")])
    sm.upsert_targets([_target("a")])
    assert sm.get("a") is not None
    assert sm.get("b") is None


def test_upsert_targets_keeps_removed_state_in_grace_window():
    """被移除的 host 在保留窗口内仍能通过 get 拿到其 HostState。"""
    sm = StateManager(remove_grace_seconds=60.0)
    sm.upsert_targets([_target("a")])
    # 让它进入 DOWN 状态并产生 incident_id
    sm.apply([_fail("a"), _fail("a"), _fail("a")])
    incident_id = sm.get("a").incident_id
    assert incident_id is not None

    sm.upsert_targets([])  # 移除
    assert sm.get("a") is None  # 主表里没了
    # 但保留窗口里仍然有
    assert "a" in sm._recently_removed
    state, _ = sm._recently_removed["a"]
    assert state.incident_id == incident_id

    # 再次 upsert 应该复用同一个 HostState 与 incident_id
    sm.upsert_targets([_target("a")])
    assert sm.get("a") is not None
    assert sm.get("a").incident_id == incident_id
    assert "a" not in sm._recently_removed


def test_upsert_targets_clears_after_grace_window():
    import time as _time

    sm = StateManager(remove_grace_seconds=0.1)
    sm.upsert_targets([_target("a")])
    sm.upsert_targets([])
    assert "a" in sm._recently_removed
    _time.sleep(0.15)
    expired = sm.upsert_targets([])
    assert "a" not in sm._recently_removed
    assert expired == ["a"]


def test_no_recovery_without_prior_down():
    """曾经只 fail 不到阈值（未真正 host_down），恢复时不应产生 host_recovered。"""
    sm = StateManager(down_after_failures=3, up_after_successes=3)
    sm.upsert_targets([_target("a")])

    # 只 fail 1 次，不达 down 阈值；incident_id 仍为 None
    sm.apply([_fail("a")])
    assert sm.get("a").incident_id is None
    assert sm.get("a").status == HostStatus.UNKNOWN

    # 接着连续 3 次成功，进入 UP
    changes = sm.apply([_ok("a"), _ok("a"), _ok("a")])
    # 应该没有产生 host_recovered（因为从未发出 host_down）
    assert all(c.event is None for c in changes)
    assert sm.get("a").status == HostStatus.UP


def test_recovery_after_real_down():
    """真实产生 host_down 后，恢复才会发 host_recovered。"""
    sm = StateManager(down_after_failures=2, up_after_successes=2)
    sm.upsert_targets([_target("a")])
    down_changes = sm.apply([_fail("a"), _fail("a")])
    assert len(down_changes) == 1
    assert down_changes[0].event is not None
    incident_id = down_changes[0].event.incident_id

    rec_changes = sm.apply([_ok("a"), _ok("a")])
    assert len(rec_changes) == 1
    assert rec_changes[0].event is not None
    # 同一个 incident_id
    assert rec_changes[0].event.incident_id == incident_id
