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


@pytest.mark.asyncio
async def test_enqueue_then_drain_succeeds(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        notifier = OutboxNotifier(outbox)
        await notifier.send(_event("e1"))
        await notifier.send(_event("e2"))
        assert outbox.count("pending") == 2

        # 用 mock transport 模拟总是成功的 webhook
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        transport = WebhookNotifier(
            url="https://example.invalid/hook",
            timeout_seconds=1.0,
            max_attempts=1,
        )
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=transport._client.timeout,
        )
        worker = OutboxWorker(outbox, transport, poll_interval_seconds=0.05, batch_size=8)
        n = await worker.drain_once()
        assert n == 2
        assert outbox.count("delivered") == 2
        await transport.close()
    finally:
        outbox.close()


@pytest.mark.asyncio
async def test_transport_error_reschedules(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        notifier = OutboxNotifier(outbox)
        await notifier.send(_event("e1"))
        assert outbox.count("pending") == 1

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        transport = WebhookNotifier(
            url="https://example.invalid/hook",
            timeout_seconds=1.0,
            max_attempts=1,
        )
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=transport._client.timeout,
        )
        worker = OutboxWorker(
            outbox,
            transport,
            poll_interval_seconds=0.05,
            batch_size=8,
            max_backoff_seconds=1,
        )
        await worker.drain_once()
        # 行依然处于 pending，但 attempts 计数 +1，且下次执行被推到
        # 未来，所以下一次 claim_due 不会立刻拿到它
        assert outbox.count("pending") == 1
        assert outbox.claim_due() == []
        with outbox._lock:  # noqa: SLF001
            row = outbox._conn.execute(
                "SELECT attempts FROM outbox WHERE event_id='e1'"
            ).fetchone()
        assert row["attempts"] == 1
        await transport.close()
    finally:
        outbox.close()


def test_outbox_max_delivery_attempts_default():
    outbox = Outbox("/tmp/_default_outbox.db")
    try:
        assert outbox.max_delivery_attempts == 32
    finally:
        outbox.close()


def test_outbox_is_exhausted_boundary():
    # 已做 attempts 次；下一次成功后 attempts+1
    # 当 attempts == max 时不能再尝试
    assert Outbox.is_exhausted(attempts=2, max_delivery_attempts=2) is True
    assert Outbox.is_exhausted(attempts=1, max_delivery_attempts=2) is False
    assert Outbox.is_exhausted(attempts=0, max_delivery_attempts=2) is False


def test_outbox_rejects_invalid_max():
    import pytest

    with pytest.raises(ValueError):
        Outbox("/tmp/_x.db", max_delivery_attempts=0)


@pytest.mark.asyncio
async def test_worker_marks_dead_when_exhausted(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db", max_delivery_attempts=3)
    try:
        notifier = OutboxNotifier(outbox)
        await notifier.send(_event("e1"))

        # 强制让 attempts 直接到上限（2 次已做，下一次会超）
        with outbox._lock:  # noqa: SLF001
            outbox._conn.execute(
                "UPDATE outbox SET attempts=?", (3,)
            )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        transport = WebhookNotifier(
            url="https://example.invalid/hook",
            timeout_seconds=1.0,
            max_attempts=1,
        )
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=transport._client.timeout,
        )
        worker = OutboxWorker(
            outbox, transport, poll_interval_seconds=0.05, batch_size=8
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
    from prometheus_client import CollectorRegistry

    from fping_monitor.metrics import MetricsStore

    outbox = Outbox(tmp_path / "outbox.db", max_delivery_attempts=3)
    registry = CollectorRegistry()
    store = MetricsStore(registry=registry, version="t", fping_binary="/usr/sbin/fping")
    try:
        notifier = OutboxNotifier(outbox)
        await notifier.send(_event("e1"))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        transport = WebhookNotifier(
            url="https://example.invalid/hook",
            timeout_seconds=1.0,
            max_attempts=1,
        )
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=transport._client.timeout,
        )
        worker = OutboxWorker(
            outbox, transport, poll_interval_seconds=0.05, batch_size=8, metrics=store
        )
        await worker.drain_once()
        # success 计数 +1
        assert (
            store.notification_attempts_total.labels(result="success")._value.get()
            == 1
        )
        await transport.close()
    finally:
        outbox.close()
