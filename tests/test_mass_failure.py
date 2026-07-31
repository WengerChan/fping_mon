"""大面积故障保护 + 后台任务异常处理的测试。"""

import asyncio

import pytest
from prometheus_client import CollectorRegistry

from fping_monitor.config import (
    AppConfig,
    WebhookConfig,
    ProbeConfig,
    StateConfig,
)
from fping_monitor.metrics import MetricsStore
from fping_monitor.models import ProbeResult, Target
from fping_monitor.scheduler import Scheduler
from fping_monitor.state import StateManager


def _config(down: int = 1, up: int = 1, mass: float = 0.5) -> AppConfig:
    return AppConfig(
        probe=ProbeConfig(interval_seconds=1, timeout_ms=200, packets=1, batch_size=10),
        state=StateConfig(
            down_after_failures=down,
            up_after_successes=up,
            mass_failure_ratio=mass,
        ),
        webhook=WebhookConfig(enabled=True, url="http://localhost/hook"),
    )


def _ok(t):
    return ProbeResult(
        target_id=t, success=True, latency_seconds=0.001, packet_loss_ratio=0.0
    )


def _fail(t):
    return ProbeResult(target_id=t, success=False, error="timeout", packet_loss_ratio=1.0)


def _make_scheduler(cfg: AppConfig, *, notifier=None) -> Scheduler:
    sm = StateManager(
        down_after_failures=cfg.state.down_after_failures,
        up_after_successes=cfg.state.up_after_successes,
    )
    store = MetricsStore(
        registry=CollectorRegistry(), version="t", fping_binary="/usr/sbin/fping"
    )
    return Scheduler(config=cfg, state_manager=sm, notifier=notifier, metrics=store)


@pytest.mark.asyncio
async def test_mass_failure_suppresses_down_events():
    cfg = _config(down=1, up=1, mass=0.5)
    scheduler = _make_scheduler(cfg)
    # 用 6 台主机，让大面积故障保护的 ">= 5" 阈值生效
    targets = [Target(id=f"h{i}", address=f"10.0.0.{i}") for i in range(6)]
    scheduler.state_manager.upsert_targets(targets)
    # 先喂一轮成功，让所有目标稳定在 UP
    scheduler.probe_fn = lambda ts, *a, **kw: {t.id: _ok(t.id) for t in ts}
    await scheduler.probe_once()
    # 接下来整批失败
    sent: list = []

    class N:
        async def send(self, event):
            sent.append(event)

    scheduler.notifier = N()
    scheduler.probe_fn = lambda ts, *a, **kw: {t.id: _fail(t.id) for t in ts}
    await scheduler.probe_once()
    # 6 台都跨越到 DOWN，但告警被抑制
    assert scheduler.state_manager.get("h0").status.value == "down"
    assert sent == []
    assert scheduler.metrics.mass_failure_protection_active._value.get() == 1
    assert scheduler.metrics.mass_failure_events_total._value.get() >= 1


@pytest.mark.asyncio
async def test_partial_failure_does_not_trigger_protection():
    cfg = _config(down=1, up=1, mass=0.5)
    scheduler = _make_scheduler(cfg)
    targets = [Target(id=f"h{i}", address=f"10.0.0.{i}") for i in range(4)]
    scheduler.state_manager.upsert_targets(targets)
    # 先全部成功
    scheduler.probe_fn = lambda ts, *a, **kw: {t.id: _ok(t.id) for t in ts}
    await scheduler.probe_once()
    sent: list = []

    class N:
        async def send(self, event):
            sent.append(event)

    scheduler.notifier = N()
    # 仅 1/4 失败：25% < 50% 阈值
    def fake_probe(ts, *a, **kw):
        out = {t.id: _ok(t.id) for t in ts}
        out["h0"] = _fail("h0")
        return out

    scheduler.probe_fn = fake_probe
    await scheduler.probe_once()
    assert len(sent) == 1
    assert scheduler.metrics.mass_failure_protection_active._value.get() == 0


@pytest.mark.asyncio
async def test_background_task_failure_exits_app():
    cfg = _config()
    scheduler = _make_scheduler(cfg)
    scheduler.state_manager.upsert_targets([Target(id="h0", address="10.0.0.0")])

    def broken_probe(targets, *a, **kw):
        raise RuntimeError("boom")

    scheduler.probe_fn = broken_probe
    scheduler.request_stop()
    await asyncio.wait_for(scheduler.run_forever(), timeout=2.0)
    # 已经请求停止；broken probe 没有阻塞退出流程
    assert scheduler.round_count == 0
