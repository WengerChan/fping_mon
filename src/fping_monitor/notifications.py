"""通知接口与传输层。

每个 ``Notifier`` 实现只负责"调一次 transport.send"，内部**不**做
HTTP 重试——网络抖动由 transport 内的退避循环吸收，持续故障则交给
:class:`OutboxWorker` 多次 tick 调度。退避采用 k8s pod 重启风格
的指数退避 + 抖动，避免多个 worker 在同一时刻打挂下游。

模块结构：

* :class:`Notifier`：所有传输层实现的协议。
* :class:`WebhookNotifier`：HTTP webhook。
* :class:`CuckooNotifier`：布谷鸟接入/转发接口。
* 共享工具函数 :func:`_compute_backoff`、:func:`_should_retry`、状态码判定。
"""

from __future__ import annotations

import json
import logging
import os
import random
from dataclasses import dataclass
from typing import Optional, Protocol

import httpx

from .cuckoo import Cuckoo, CuckooForwardBody, CuckooReceiveMapBody
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
    """429 / 408 / 5xx 或传输错误（status_code 为 None）可重试。"""
    if status_code is None:
        return True
    if status_code == 429 or status_code == 408:
        return True
    if 500 <= status_code < 600:
        return True
    return False


def _compute_backoff(
    attempts: int,
    max_backoff_seconds: int,
    jitter_ratio: float = 0.1,
) -> float:
    """k8s 风格的指数退避：``min(cap, base * 2^(n-1))`` 叠加 ±10% 抖动。

    ``attempts`` 是 1-based；调用前已经失败过 ``attempts`` 次，
    这是下一次发送前要等的秒数。返回 ``-1.0`` 表示已达 max_attempts，
    调用方应停止重试。
    """
    if attempts < 1:
        return 0.0
    base = min(2 ** (attempts - 1), max_backoff_seconds)
    if base <= 0:
        return -1.0
    jitter = base * jitter_ratio * (random.random() * 2 - 1)
    return float(max(0.0, base + jitter))


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
    """把事件 POST 到 HTTP 接口；transport 内 HTTP 重试 + 退避。

    抗网络抖动：单次 ``send`` 调用内部最多发起 ``max_attempts`` 次
    HTTP 请求，遇 5xx/429/网络错误按指数退避重试。仍失败的最终异常
    由 :class:`OutboxWorker` 决定是否换一轮 tick 重发。
    """

    def __init__(
        self,
        url: str,
        timeout_seconds: float = 5.0,
        max_attempts: int = 3,
        max_backoff_seconds: int = 60,
        token_env: str = "ALERT_API_TOKEN",
    ) -> None:
        if not url:
            raise ValueError("必须提供 webhook url")
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
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
        """单次 worker tick 的投递：内部最多 max_attempts 次 HTTP 重试。

        退避采用 k8s 风格（指数 + 抖动）。仍失败抛 :class:`RetriesExhausted`
        让 worker 决定是否换一轮 tick；遇不可重试状态码抛
        :class:`NotificationFailed` 让 worker 直接标记 dead。
        """
        import asyncio

        payload = _event_to_payload(event)
        body = json.dumps(payload, sort_keys=True)
        headers = self._build_headers()

        last_status: Optional[int] = None
        last_error: Optional[str] = None
        attempt = 0
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

            delay = _compute_backoff(attempt, self._max_backoff_seconds)
            if delay < 0:
                break
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


class CuckooNotifier:
    """布谷鸟告警发送器（transport 层）。

    把 :class:`AlertEvent` 转成 :class:`CuckooReceiveMapBody` 并通过
    :class:`Cuckoo` 同步发送。transport 内做 HTTP 重试 + 退避。
    """

    def __init__(
        self,
        cuckoo: Cuckoo,
        *,
        endpoint: str,
        max_attempts: int = 3,
        max_backoff_seconds: int = 60,
        timeout_seconds: float = 5.0,
        monitor_instance: str = "monitor-a",
    ) -> None:
        if endpoint not in ("receivemap", "forward"):
            raise ValueError(f"endpoint 必须是 receivemap 或 forward，得到 {endpoint!r}")
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        self._cuckoo = cuckoo
        self._endpoint = endpoint
        self._max_attempts = max_attempts
        self._max_backoff_seconds = max_backoff_seconds
        self._timeout = timeout_seconds
        self._monitor_instance = monitor_instance
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def close(self) -> None:
        await self._client.aclose()

    def _build_body(self, event: AlertEvent):
        if self._endpoint == "receivemap":
            return CuckooReceiveMapBody(
                alrmaName=f"fping-monitor: {event.event_type}",
                alarmContent=(
                    f"host={event.target.id} address={event.target.address} "
                    f"failures={event.consecutive_failures} "
                    f"loss={event.packet_loss_ratio:.2f} "
                    f"event_id={event.event_id}"
                ),
                application=self._monitor_instance,
                entityId=event.event_id,
                lastOccurrence=event.confirmed_at,
                originalEventId=event.event_id,
                sourceName="fping-monitor",
                priority=3 if event.event_type == "host_recovered" else 4,
            )
        # forward
        return CuckooForwardBody(
            sn=event.event_id,
            sys=self._monitor_instance,
            alrmaName=f"fping-monitor: {event.event_type}",
            alarmContent=(
                f"host={event.target.id} address={event.target.address} "
                f"event_id={event.event_id}"
            ),
            mode=1,
        )

    async def _post_once(self, body) -> int:
        """发一次 HTTP 请求，返回状态码；网络异常时返回 None。"""
        import asyncio

        payload = self._cuckoo.payload(body)
        url = self._cuckoo.config.cuckoo.url.get(self._endpoint, "")
        if not url:
            # 与构造时的契约：URL 必须存在；这里再校验一次防漂移
            raise NotificationFailed(
                event=None,  # type: ignore[arg-type]
                status_code=None,
                last_error=f"布谷鸟 {self._endpoint} URL 未配置",
                attempts=1,
            )
        try:
            resp = await self._client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            _LOG.warning("cuckoo %s 传输错误: %s", self._endpoint, exc)
            raise
        if 200 <= resp.status_code < 300:
            _LOG.info("cuckoo %s 发送成功 status=%d", self._endpoint, resp.status_code)
            return resp.status_code
        _LOG.warning(
            "cuckoo %s 非 2xx 响应 status=%d body=%s",
            self._endpoint,
            resp.status_code,
            resp.text[:200],
        )
        return resp.status_code

    async def send(self, event: AlertEvent) -> None:
        import asyncio

        body = self._build_body(event)
        last_status: Optional[int] = None
        last_error: Optional[str] = None
        attempt = 0
        for attempt in range(1, self._max_attempts + 1):
            try:
                last_status = await self._post_once(body)
                last_error = None
            except httpx.HTTPError as exc:
                last_status = None
                last_error = repr(exc)
            if last_status is not None and 200 <= last_status < 300:
                return
            if last_status is not None and not _should_retry(last_status):
                raise NotificationFailed(
                    event=event,
                    status_code=last_status,
                    last_error=last_error,
                    attempts=attempt,
                )
            delay = _compute_backoff(attempt, self._max_backoff_seconds)
            if delay < 0:
                break
            await asyncio.sleep(delay)

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
            f"(event_id={getattr(event, 'event_id', None)}, status={status_code}, "
            f"error={last_error})"
        )
        self.event = event
        self.status_code = status_code
        self.last_error = last_error
        self.attempts = attempts


class RetriesExhausted(NotificationFailed):
    """transport 内 HTTP 重试耗尽，让 OutboxWorker 决定是否换一轮 tick。"""


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
    """兼容旧 API：传入 max_attempts 仅用于判断上界（>=时返回 -1）。"""
    if attempts >= max_attempts:
        return -1.0
    return _compute_backoff(attempts, max_backoff_seconds)