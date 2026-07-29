"""调度器测试。

这些测试不真正执行 fping：调度器接受注入的 `probe_fn`，
返回我们合成的结果。
"""

import asyncio

import pytest
from prometheus_client import CollectorRegistry

from fping_monitor.config import (
    AppConfig,
    NotificationConfig,
    ProbeConfig,
    StateConfig,
)
from fping_monitor.metrics import MetricsStore
from fping_monitor.models import ProbeResult, Target
from fping_monitor.scheduler import Scheduler
from fping_monitor.state import HostStatus, StateManager


def _config(down: int = 3, up: int = 3) -> AppConfig:
    return AppConfig(
        probe=ProbeConfig(interval_seconds=1, timeout_ms=200, packets=1, batch_size=2),
        state=StateConfig(down_after_failures=down, up_after_successes=up),
        notification=NotificationConfig(enabled=True, url="http://localhost/hook"),
    )


def _ok(tid: str) -> ProbeResult:
    return ProbeResult(
        target_id=tid, success=True, latency_seconds=0.001, packet_loss_ratio=0.0
    )


def _fail(tid: str) -> ProbeResult:
    return ProbeResult(target_id=tid, success=False, error="timeout", packet_loss_ratio=1.0)


def _make_scheduler(cfg: AppConfig, *, notifier=None) -> Scheduler:
    sm = StateManager(
        down_after_failures=cfg.state.down_after_failures,
        up_after_successes=cfg.state.up_after_successes,
    )
    store = MetricsStore(
        registry=CollectorRegistry(), version="test", fping_binary="/usr/sbin/fping"
    )
    return Scheduler(config=cfg, state_manager=sm, notifier=notifier, metrics=store)


@pytest.mark.asyncio
async def test_probe_once_calls_injected_probe_fn():
    cfg = _config()
    scheduler = _make_scheduler(cfg)
    scheduler.state_manager.upsert_targets(
        [Target(id="a", address="10.0.0.1"), Target(id="b", address="10.0.0.2")]
    )

    def fake_probe(targets, binary, packets, timeout_ms, overall_timeout_seconds):
        return {t.id: (_ok(t.id) if t.id == "a" else _fail(t.id)) for t in targets}

    scheduler.probe_fn = fake_probe
    batches = await scheduler.probe_once()
    assert batches == 1
    assert scheduler.round_count == 1


@pytest.mark.asyncio
async def test_state_change_triggers_notifier():
    cfg = _config(down=1, up=1)
    scheduler = _make_scheduler(cfg)
    scheduler.state_manager.upsert_targets([Target(id="a", address="10.0.0.1")])

    sent: list = []

    class FakeNotifier:
        async def send(self, event):
            sent.append(event)

    scheduler.notifier = FakeNotifier()
    scheduler.probe_fn = lambda targets, *a, **kw: {t.id: _fail(t.id) for t in targets}
    await scheduler.probe_once()
    assert len(sent) == 1
    assert sent[0].event_type == "host_down"

    sent.clear()
    scheduler.probe_fn = lambda targets, *a, **kw: {t.id: _ok(t.id) for t in targets}
    await scheduler.probe_once()
    assert len(sent) == 1
    assert sent[0].event_type == "host_recovered"


@pytest.mark.asyncio
async def test_run_forever_respects_stop():
    cfg = _config()
    scheduler = _make_scheduler(cfg)
    scheduler.state_manager.upsert_targets([Target(id="a", address="10.0.0.1")])
    scheduler.probe_fn = lambda targets, *a, **kw: {t.id: _ok(t.id) for t in targets}
    task = asyncio.create_task(scheduler.run_forever())
    await asyncio.sleep(0.05)
    scheduler.request_stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert scheduler.round_count >= 1


@pytest.mark.asyncio
async def test_mass_failure_recovery_emits_pending_downs():
    """大面积故障期间抑制的 host_down，在窗口解除后应被补发一次。"""
    cfg = AppConfig(
        probe=ProbeConfig(interval_seconds=1, timeout_ms=200, packets=1, batch_size=10),
        state=StateConfig(down_after_failures=1, up_after_successes=1, mass_failure_ratio=0.5),
        notification=NotificationConfig(enabled=True, url="http://localhost/hook"),
    )
    scheduler = _make_scheduler(cfg)
    targets = [Target(id=f"h{i}", address=f"10.0.0.{i}") for i in range(6)]
    scheduler.state_manager.upsert_targets(targets)
    # 先全部成功
    scheduler.probe_fn = lambda ts, *a, **kw: {t.id: _ok(t.id) for t in ts}
    await scheduler.probe_once()

    sent: list = []

    class N:
        async def send(self, event):
            sent.append(event)

    scheduler.notifier = N()
    # 第一轮：整批失败 → 触发 mass_failure（抑制 host_down）
    scheduler.probe_fn = lambda ts, *a, **kw: {t.id: _fail(t.id) for t in ts}
    await scheduler.probe_once()
    assert sent == []
    # 第二轮：仍维持 mass_failure（继续全失败）
    await scheduler.probe_once()
    assert sent == []
    # 第三轮：网络恢复，但其中 2 台仍然 DOWN（incident_id 应仍为 None）
    def mixed(ts, *a, **kw):
        out = {t.id: _ok(t.id) for t in ts}
        out["h0"] = _fail("h0")
        out["h1"] = _fail("h1")
        return out

    scheduler.probe_fn = mixed
    await scheduler.probe_once()
    # 至少应当有补发的 host_down（incident_id 来自 scheduler 而非状态机）
    down_events = [e for e in sent if e.event_type == "host_down"]
    assert down_events, "expected at least one mass-failure recovery host_down"
    # 这些补发的事件的 incident_id 与状态机内部 incident_id 已绑定，避免重复
    for state in scheduler.state_manager.all_states():
        if state.status == HostStatus.DOWN:
            assert state.incident_id is not None
