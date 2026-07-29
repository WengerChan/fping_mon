"""通知接口与传输层。

MVP 阶段只提供 :class:`WebhookNotifier`：把 :class:`AlertEvent` 以
JSON 形式 POST 到一个 HTTP 接口。传输细节（认证、重试、超时）都
封装在 WebhookNotifier 内部，调度器只需把事件交给它即可。

后续任务会在此基础上增加 SQLite Outbox；目前阶段事件在内存中
排队，进程退出即丢失。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional, Protocol

import httpx

from .models import AlertEvent


_LOG = logging.getLogger(__name__)


class Notifier(Protocol):
    """任何可以把 :class:`AlertEvent` 投递到外部的实现。"""

    async def send(self, event: AlertEvent) -> None:
        ...


@dataclass
class _Attempt:
    event: AlertEvent
    next_delay_seconds: float = 0.0
    attempts: int = 0
    last_status_code: Optional[int] = None
    last_error: Optional[str] = None


def _should_retry(status_code: Optional[int]) -> bool:
    """429 / 5xx 状态码或传输错误（status_code 为 None）可重试。"""
    if status_code is None:
        return True
    if status_code == 429 or status_code == 408:
        return True
    if 500 <= status_code < 600:
        return True
    return False


def _compute_backoff(
    attempts: int, max_backoff_seconds: int, max_attempts: int
) -> float:
    """指数退避，封顶 max_backoff_seconds。attempts 为 1-based。

    返回 -1 表示已达最大重试次数，调用方应停止重试。
    """
    if attempts >= max_attempts:
        return -1.0
    base = min(2 ** (attempts - 1), max_backoff_seconds)
    return float(base)


def _event_to_payload(event: AlertEvent) -> dict:
    return {
        "event_id": event.event_id,
        "incident_id": event.incident_id,
        "event_type": event.event_type,
        "host_id": event.target.id,
        "address": event.target.address,
        "occurred_at": event.occurred_at,
        "confirmed_at": event.confirmed_at,
        "last_success_at": event.last_success_at,
        "consecutive_failures": event.consecutive_failures,
        "packet_loss_ratio": event.packet_loss_ratio,
        "probe_type": event.probe_type,
        "monitor_instance": event.monitor_instance,
        "labels": dict(event.target.labels),
    }


class WebhookNotifier:
    """把事件 POST 到 HTTP 接口，支持简单重试。

    内部持有一个共享的 :class:`httpx.AsyncClient`；关闭时应
    `await close()`。
    """

    def __init__(
        self,
        url: str,
        timeout_seconds: float = 5.0,
        max_attempts: int = 8,
        max_backoff_seconds: int = 60,
        token_env: str = "ALERT_API_TOKEN",
    ) -> None:
        if not url:
            raise ValueError("必须提供 webhook url")
        self._url = url
        self._timeout = timeout_seconds
        self._max_attempts = max_attempts
        self._max_backoff_seconds = max_backoff_seconds
        self._token_env = token_env
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    @property
    def url(self) -> str:
        return self._url

    async def close(self) -> None:
        await self._client.aclose()

    def _build_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "User-Agent": "fping-monitor/0.1"}
        token = os.environ.get(self._token_env)
        if token:
            if _is_placeholder_token(token):
                # 占位 token 不发请求；抛 NotificationFailed 让 outbox 走
                # dead 路径并打 ERROR，便于运维发现配置问题。
                raise NotificationFailedPlaceholder(
                    f"环境变量 {self._token_env} 是占位值 ({token!r})，"
                    "请设置真实 token",
                )
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def send(self, event: AlertEvent) -> None:
        """发送单个事件，失败时按规则重试。

        重试耗尽或遇到不可重试的状态码时抛出 :class:`NotificationFailed`。
        """
        payload = _event_to_payload(event)
        body = json.dumps(payload, sort_keys=True)
        headers = self._build_headers()

        last_status: Optional[int] = None
        last_error: Optional[str] = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                resp = await self._client.post(
                    self._url, content=body, headers=headers
                )
            except httpx.HTTPError as exc:
                last_status = None
                last_error = repr(exc)
                _LOG.warning(
                    "webhook 传输错误 (第 %d/%d 次, event_id=%s): %s",
                    attempt,
                    self._max_attempts,
                    event.event_id,
                    exc,
                )
            else:
                last_status = resp.status_code
                last_error = None
                if 200 <= resp.status_code < 300:
                    _LOG.info(
                        "webhook 发送成功 event_id=%s status=%d",
                        event.event_id,
                        resp.status_code,
                    )
                    return
                _LOG.warning(
                    "webhook 非 2xx 响应 (第 %d/%d 次, event_id=%s, status=%d)",
                    attempt,
                    self._max_attempts,
                    event.event_id,
                    resp.status_code,
                )

            # 不可重试的状态码（例如 4xx）直接抛错，不浪费后续退避
            if not _should_retry(last_status):
                raise NotificationFailed(
                    event=event,
                    status_code=last_status,
                    last_error=last_error,
                    attempts=attempt,
                )

            delay = _compute_backoff(attempt, self._max_backoff_seconds, self._max_attempts)
            if delay < 0:
                break
            import asyncio

            await asyncio.sleep(delay)

        # 走完所有尝试：再判断最后一次状态，决定抛哪种异常
        if last_status is not None and not _should_retry(last_status):
            raise NotificationFailed(
                event=event,
                status_code=last_status,
                last_error=last_error,
                attempts=attempt,
            )
        raise RetriesExhausted(
            event=event,
            status_code=last_status,
            last_error=last_error,
            attempts=self._max_attempts,
        )


class NotificationFailed(Exception):
    """Webhook 不再可重试时抛出。"""

    def __init__(
        self,
        event: AlertEvent,
        status_code: Optional[int],
        last_error: Optional[str],
        attempts: int,
    ) -> None:
        super().__init__(
            f"notification 失败，已尝试 {attempts} 次 "
            f"(event_id={event.event_id}, status={status_code}, error={last_error})"
        )
        self.event = event
        self.status_code = status_code
        self.last_error = last_error
        self.attempts = attempts


class RetriesExhausted(NotificationFailed):
    """Webhook 在可重试错误上耗尽了所有重试次数。

    outbox worker 应当用 backoff 重新调度这条事件，而不是直接放弃。
    """


class NotificationFailedPlaceholder(Exception):
    """检测到占位 token（配置没改）导致的失败，应直接 dead 不再重试。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.event = None
        self.status_code: Optional[int] = None
        self.last_error = message
        self.attempts = 1


_PLACEHOLDER_TOKENS = {"replace-me", "changeme", "your-token", "todo", "xxxxxx"}


def _is_placeholder_token(token: str) -> bool:
    if not token:
        return True
    if token.strip().lower() in _PLACEHOLDER_TOKENS:
        return True
    # 长度过短也视为占位
    if len(token.strip()) < 16:
        return True
    return False


def should_retry(status_code: Optional[int]) -> bool:  # 测试使用的公开别名
    return _should_retry(status_code)


def compute_backoff(
    attempts: int, max_backoff_seconds: int, max_attempts: int
) -> float:
    return _compute_backoff(attempts, max_backoff_seconds, max_attempts)
