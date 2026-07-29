"""基于 Outbox 的通知发送器。

这里包含两个部分：

* :class:`OutboxNotifier`：先写盘再投递，保证进程崩溃时事件不丢。
* :class:`OutboxWorker`：后台协程，轮询 outbox 并把到期行交给
  真正的传输层（例如 :class:`WebhookNotifier`）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .metrics import MetricsStore
from .models import AlertEvent
from .notifications import (
    NotificationFailed,
    NotificationFailedPlaceholder,
    RetriesExhausted,
    WebhookNotifier,
)
from .storage import Outbox, StoredEvent


_LOG = logging.getLogger(__name__)


class OutboxNotifier:
    """把事件先持久化到 outbox，再返回。

    `send` 在行被持久化后立即返回；真正的 HTTP 调用由 worker 处理。
    """

    def __init__(self, outbox: Outbox) -> None:
        self._outbox = outbox

    @property
    def outbox(self) -> Outbox:
        return self._outbox

    async def send(self, event: AlertEvent) -> None:
        # outbox 是同步 API，包装成 async 保持调用方接口一致
        await asyncio.to_thread(self._outbox.enqueue, event)


class OutboxWorker:
    """后台协程：定期消费 outbox 中的待发送行。

    每轮取一批到期的行，调用底层传输；成功则标记 `delivered`，
    可重试失败则用 backoff 重新调度，不可重试失败或累计 attempts
    超过 `max_delivery_attempts` 则标记 `dead`，让队列能继续往前走。
    """

    def __init__(
        self,
        outbox: Outbox,
        transport: WebhookNotifier,
        *,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 16,
        max_backoff_seconds: int = 60,
        metrics: Optional[MetricsStore] = None,
    ) -> None:
        self._outbox = outbox
        self._transport = transport
        self._poll_interval = poll_interval_seconds
        self._batch_size = batch_size
        self._max_backoff = max_backoff_seconds
        self._metrics = metrics
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.drain_once()
            except Exception:  # noqa: BLE001
                _LOG.exception("outbox worker tick failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self.run_forever(), name="outbox-worker")

    async def stop(self) -> None:
        self.request_stop()
        if self._task is not None:
            await self._task
            self._task = None

    async def drain_once(self) -> int:
        rows = await asyncio.to_thread(self._outbox.claim_due, None, self._batch_size)
        for row in rows:
            await self._deliver(row)
        return len(rows)

    def _record_attempt(self, result: str) -> None:
        if self._metrics is not None:
            try:
                self._metrics.notification_attempts_total.labels(result=result).inc()
            except Exception:  # noqa: BLE001
                # 指标更新失败不应影响通知主流程
                _LOG.debug("metrics notification_attempts_total update failed")

    async def _deliver(self, row: StoredEvent) -> None:
        # 先判断是否已经超过累计最大尝试次数。
        # 这一步必须在调用 transport 之前：
        # 1. 避免再做一次注定失败的 HTTP 调用（节省资源 + 不打扰下游）；
        # 2. 在 webhook 接收端持续 500 时，避免 attempts 无限累加
        #    （transport.send 内部已经做过 max_attempts 次重试，
        #     累计到 max_delivery_attempts 就直接 dead）。
        if Outbox.is_exhausted(row.attempts, self._outbox.max_delivery_attempts):
            await asyncio.to_thread(
                self._outbox.mark_dead, row.id,
                f"exhausted: attempts={row.attempts} >= "
                f"max={self._outbox.max_delivery_attempts}",
            )
            self._record_attempt("dead")
            _LOG.error(
                "通知放弃，已达最大尝试次数 (event_id=%s attempts=%d)",
                row.event.event_id,
                row.attempts,
            )
            return

        try:
            await self._transport.send(row.event)
        except RetriesExhausted as exc:
            # 5xx / 传输错误已经在线重试耗尽：用更长的 backoff 重新调度
            delay = self._backoff_seconds(row.attempts + 1)
            await asyncio.to_thread(
                self._outbox.mark_retry, row.id, delay, str(exc)
            )
            self._record_attempt("retry")
            _LOG.warning(
                "重试耗尽，%.1fs 后再次调度 (event_id=%s, status=%s)",
                delay,
                row.event.event_id,
                exc.status_code,
            )
        except (NotificationFailed, NotificationFailedPlaceholder) as exc:
            # 4xx 类不可重试失败 / 占位 token：直接放弃，标记 dead
            await asyncio.to_thread(
                self._outbox.mark_dead, row.id, str(exc)
            )
            self._record_attempt("dead")
            _LOG.error(
                "通知放弃 (event_id=%s status=%s): %s",
                row.event.event_id,
                getattr(exc, "status_code", None),
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            delay = self._backoff_seconds(row.attempts + 1)
            await asyncio.to_thread(
                self._outbox.mark_retry, row.id, delay, repr(exc)
            )
            self._record_attempt("retry")
            _LOG.warning(
                "通知将在 %.1fs 后重试 (event_id=%s, 第 %d 次): %s",
                delay,
                row.event.event_id,
                row.attempts + 1,
                exc,
            )
        else:
            await asyncio.to_thread(self._outbox.mark_delivered, row.id)
            self._record_attempt("success")
            _LOG.info("通知已发送 (event_id=%s)", row.event.event_id)

    def _backoff_seconds(self, attempts: int) -> float:
        return float(min(2 ** max(0, attempts - 1), self._max_backoff))
