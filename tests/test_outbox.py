"""基于 outbox 的通知 worker 测试。"""

from pathlib import Path

import httpx
import pytest

from fping_monitor.models import AlertEvent, Target
from fping_monitor.notifications import WebhookNotifier
from fping_monitor.outbox import OutboxNotifier, OutboxWorker
from fping_monitor.storage import Outbox


def _event(eid: str) -> AlertEvent:
    return AlertEvent(
        event_id=eid,
        incident_id="i1",
        event_type="host_down",
        target=Target(id="host-a", address="10.0.0.1"),
        occurred_at="2026-07-28T00:00:00Z",
        confirmed_at="2026-07-28T00:00:10Z",
        last_success_at="2026-07-28T00:00:05Z",
        consecutive_failures=3,
        packet_loss_ratio=1.0,
    )


# 测试用单 channel 配置
_TEST_CHANNELS = [("webhook", 10)]


def _mock_webhook(handler) -> WebhookNotifier:
    """构造一个返回由 ``handler`` 决定的 mock WebhookNotifier。"""
    transport = WebhookNotifier(
        url="https://example.invalid/hook",
        max_attempts=1,
        max_backoff_seconds=1,
    )
    transport._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        timeout=transport._client.timeout,
    )
    return transport


@pytest.mark.asyncio
async def test_enqueue_then_drain_succeeds(tmp_path: Path):
    """enqueue → drain_once → delivered。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        notifier = OutboxNotifier(outbox, channels=_TEST_CHANNELS)
        await notifier.send(_event("e1"))
        await notifier.send(_event("e2"))
        assert outbox.count("pending") == 2

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        transport = _mock_webhook(handler)
        worker = OutboxWorker(
            outbox,
            {"webhook": transport},
            poll_interval_seconds=0.05,
            batch_size=8,
        )
        n = await worker.drain_once()
        assert n == 2
        assert outbox.count("delivered") == 2
        await transport.close()
    finally:
        outbox.close()


@pytest.mark.asyncio
async def test_transport_error_reschedules(tmp_path: Path):
    """transport 持续 5xx 时，worker 用 backoff 重新调度；子表 attempts+1。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        notifier = OutboxNotifier(outbox, channels=_TEST_CHANNELS)
        await notifier.send(_event("e1"))
        assert outbox.count("pending") == 1

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        transport = _mock_webhook(handler)
        worker = OutboxWorker(
            outbox,
            {"webhook": transport},
            poll_interval_seconds=0.05,
            batch_size=8,
            max_backoff_seconds=1,
        )
        await worker.drain_once()
        # 行依然处于 pending，但 attempts +1，且下次执行被推到未来，
        # 所以下一次 claim_due_for_channel 不会立刻拿到它
        assert outbox.count("pending") == 1
        assert outbox.claim_due_for_channel("webhook") == []
        with outbox._lock:  # noqa: SLF001
            row = outbox._conn.execute(
                "SELECT attempts FROM outbox_delivery "
                "WHERE outbox_id=(SELECT id FROM outbox WHERE event_id='e1')"
            ).fetchone()
        assert row["attempts"] == 1
        await transport.close()
    finally:
        outbox.close()


def test_outbox_max_event_attempts_default():
    outbox = Outbox("/tmp/_default_outbox.db")
    try:
        assert outbox.max_event_attempts == 32
    finally:
        outbox.close()


def test_outbox_is_exhausted_boundary():
    # 已做 attempts 次；下一次成功后 attempts+1
    # 当 attempts == max 时不能再尝试
    assert Outbox.is_exhausted(attempts=2, max_event_attempts=2) is True
    assert Outbox.is_exhausted(attempts=1, max_event_attempts=2) is False
    assert Outbox.is_exhausted(attempts=0, max_event_attempts=2) is False


def test_outbox_rejects_invalid_max():
    with pytest.raises(ValueError):
        Outbox("/tmp/_x.db", max_event_attempts=0)


@pytest.mark.asyncio
async def test_worker_marks_dead_when_exhausted(tmp_path: Path):
    """子表 attempts 已达 channel 自己的 max_event_attempts 时，worker 直接 mark_dead。"""
    outbox = Outbox(tmp_path / "outbox.db", max_event_attempts=3)
    # channel max_event_attempts=3，与 outbox 默认一致；enqueue 时固化为 3
    channels = [("webhook", 3)]
    try:
        notifier = OutboxNotifier(outbox, channels=channels)
        await notifier.send(_event("e1"))

        # 强制让 delivery 行 attempts 直接到上限（>= max_event_attempts）
        with outbox._lock:  # noqa: SLF001
            outbox._conn.execute(
                "UPDATE outbox_delivery SET attempts=?", (3,)
            )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        transport = _mock_webhook(handler)
        worker = OutboxWorker(
            outbox,
            {"webhook": transport},
            poll_interval_seconds=0.05,
            batch_size=8,
        )
        await worker.drain_once()
        # 即便 transport 抛 RetriesExhausted，worker 在尝试前已判断耗尽，
        # 直接 mark_dead，不会再调 transport
        assert outbox.count("dead") == 1
        assert outbox.count("pending") == 0
        await transport.close()
    finally:
        outbox.close()


@pytest.mark.asyncio
async def test_worker_records_notification_attempts_metric(tmp_path: Path):
    """metrics 计数按 channel + result 二维 label 增长。"""
    from prometheus_client import CollectorRegistry

    from fping_monitor.metrics import MetricsStore

    outbox = Outbox(tmp_path / "outbox.db", max_event_attempts=3)
    registry = CollectorRegistry()
    store = MetricsStore(registry=registry, version="t", fping_binary="/usr/sbin/fping")
    try:
        notifier = OutboxNotifier(outbox, channels=_TEST_CHANNELS)
        await notifier.send(_event("e1"))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        transport = _mock_webhook(handler)
        worker = OutboxWorker(
            outbox,
            {"webhook": transport},
            poll_interval_seconds=0.05,
            batch_size=8,
            metrics=store,
        )
        await worker.drain_once()
        # success 计数 +1，且按 channel="webhook" label 区分
        assert (
            store.notification_attempts_total.labels(
                channel="webhook", result="success"
            )._value.get()
            == 1
        )
        await transport.close()
    finally:
        outbox.close()


@pytest.mark.asyncio
async def test_multi_channel_independent_delivery(tmp_path: Path):
    """同一事件在两个 channel 上独立投递，任一失败不影响另一个。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        channels = [("webhook", 10), ("cuckoo.receivemap", 3)]
        notifier = OutboxNotifier(outbox, channels=channels)
        await notifier.send(_event("e1"))
        assert outbox.count("pending") == 2

        # webhook 500，cuckoo.receivemap 200
        def webhook_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        from fping_monitor.notifications import CuckooNotifier
        from fping_monitor.cuckoo import Cuckoo
        from fping_monitor.config import AppConfig, CuckooConfig

        app_cfg = AppConfig(
            cuckoo=CuckooConfig(
                enabled=True,
                app_key="k",
                url={"receivemap": "https://cuckoo.example/r", "forward": ""},
            )
        )
        cuckoo = Cuckoo(app_cfg)

        def cuckoo_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        wh = _mock_webhook(webhook_handler)
        cn = CuckooNotifier(
            cuckoo,
            endpoint="receivemap",
            max_attempts=1,
            max_backoff_seconds=1,
        )
        cn._client = httpx.AsyncClient(
            transport=httpx.MockTransport(cuckoo_handler),
            timeout=cn._client.timeout,
        )

        worker = OutboxWorker(
            outbox,
            {"webhook": wh, "cuckoo.receivemap": cn},
            poll_interval_seconds=0.05,
            batch_size=8,
            max_backoff_seconds=1,
        )
        await worker.drain_once()
        # cuckoo.receivemap 投递成功；webhook 失败但还在 pending（attempts+1）
        assert outbox.count("delivered") == 1
        assert outbox.count("pending") == 1
        await wh.close()
        await cn.close()
    finally:
        outbox.close()