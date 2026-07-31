"""Webhook 通知模块的测试。"""

import httpx
import pytest

from fping_monitor.models import AlertEvent, Target
from fping_monitor.notifications import (
    NotificationFailed,
    RetriesExhausted,
    WebhookNotifier,
    compute_backoff,
    should_retry,
)


def _event(event_id: str = "e1", incident_id: str = "i1") -> AlertEvent:
    return AlertEvent(
        event_id=event_id,
        incident_id=incident_id,
        event_type="host_down",
        target=Target(id="host-a", address="10.0.0.1"),
        occurred_at="2026-07-28T00:00:00Z",
        confirmed_at="2026-07-28T00:00:10Z",
        last_success_at="2026-07-28T00:00:05Z",
        consecutive_failures=3,
        packet_loss_ratio=1.0,
    )


def test_should_retry_logic():
    assert should_retry(None) is True
    assert should_retry(429) is True
    assert should_retry(503) is True
    assert should_retry(400) is False
    assert should_retry(404) is False
    assert should_retry(200) is False


def test_compute_backoff_grows_and_caps():
    # jitter_ratio=0 关闭抖动，断言精确值；生产路径默认有 ±10% 抖动
    assert compute_backoff(1, 60, 5, jitter_ratio=0.0) == 1.0
    assert compute_backoff(2, 60, 5, jitter_ratio=0.0) == 2.0
    assert compute_backoff(3, 60, 5, jitter_ratio=0.0) == 4.0
    assert compute_backoff(4, 60, 5, jitter_ratio=0.0) == 8.0
    # attempt == max_attempts 表示已到上限；-1 是停止信号
    assert compute_backoff(5, 60, 5, jitter_ratio=0.0) == -1.0


def test_compute_backoff_within_jitter_bounds():
    """默认 ±10% 抖动：返回值在 [base * 0.9, base * 1.1] 之间。"""
    for n in (1, 2, 3, 4):
        base = min(2 ** (n - 1), 60)
        for _ in range(20):
            d = compute_backoff(n, 60, 10)  # 不传 jitter_ratio，用默认 0.1
            assert base * 0.9 - 1e-9 <= d <= base * 1.1 + 1e-9


@pytest.mark.asyncio
async def test_webhook_success_2xx():
    received: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        received.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    notifier = WebhookNotifier(
        url="https://example.invalid/webhook",
        timeout_seconds=1.0,
        max_attempts=3,
        max_backoff_seconds=1,
    )
    notifier._client = httpx.AsyncClient(
        transport=transport, timeout=notifier._client.timeout
    )
    try:
        await notifier.send(_event())
    finally:
        await notifier.close()
    assert len(received) == 1
    assert received[0]["event_id"] == "e1"
    assert received[0]["host_id"] == "host-a"


@pytest.mark.asyncio
async def test_webhook_retries_on_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 2:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    notifier = WebhookNotifier(
        url="https://example.invalid/webhook",
        timeout_seconds=1.0,
        max_attempts=3,
        max_backoff_seconds=1,
    )
    notifier._client = httpx.AsyncClient(
        transport=transport, timeout=notifier._client.timeout
    )
    try:
        await notifier.send(_event())
    finally:
        await notifier.close()
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_webhook_does_not_retry_on_4xx():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, text="bad")

    transport = httpx.MockTransport(handler)
    notifier = WebhookNotifier(
        url="https://example.invalid/webhook",
        timeout_seconds=1.0,
        max_attempts=3,
        max_backoff_seconds=1,
    )
    notifier._client = httpx.AsyncClient(
        transport=transport, timeout=notifier._client.timeout
    )
    try:
        with pytest.raises(NotificationFailed) as excinfo:
            await notifier.send(_event())
    finally:
        await notifier.close()
    assert calls["n"] == 1
    assert excinfo.value.status_code == 400


@pytest.mark.asyncio
async def test_webhook_exhausts_retries():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, text="boom")

    transport = httpx.MockTransport(handler)
    notifier = WebhookNotifier(
        url="https://example.invalid/webhook",
        timeout_seconds=1.0,
        max_attempts=2,
        max_backoff_seconds=1,
    )
    notifier._client = httpx.AsyncClient(
        transport=transport, timeout=notifier._client.timeout
    )
    try:
        with pytest.raises(RetriesExhausted) as excinfo:
            await notifier.send(_event())
    finally:
        await notifier.close()
    assert calls["n"] == 2
    assert excinfo.value.attempts == 2


def test_webhook_requires_url():
    with pytest.raises(ValueError):
        WebhookNotifier(url="", timeout_seconds=1.0)


def test_placeholder_token_detected():
    from fping_monitor.notifications import _is_placeholder_token

    assert _is_placeholder_token("replace-me") is True
    assert _is_placeholder_token("CHANGEME") is True
    assert _is_placeholder_token("") is True
    assert _is_placeholder_token("short") is True
    assert _is_placeholder_token("a" * 32) is False


@pytest.mark.asyncio
async def test_placeholder_token_triggers_failed():
    import os

    from fping_monitor.notifications import NotificationFailedPlaceholder

    os.environ["PLACEHOLDER_TEST_TOKEN"] = "replace-me"
    try:
        notifier = WebhookNotifier(
            url="https://example.invalid/hook",
            timeout_seconds=1.0,
            max_attempts=1,
            token_env="PLACEHOLDER_TEST_TOKEN",
        )
        with pytest.raises(NotificationFailedPlaceholder):
            await notifier.send(_event())
    finally:
        os.environ.pop("PLACEHOLDER_TEST_TOKEN", None)
