"""基于 Outbox 的通知发送器。

这里包含两个部分：

* :class:`OutboxNotifier`：先写盘再投递，保证进程崩溃时事件不丢。
* :class:`OutboxWorker`：后台协程，轮询 outbox 并把到期行交给
  对应 channel 的 :class:`Notifier`。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .config import CuckooConfig, NotificationConfig
from .metrics import MetricsStore
from .models import AlertEvent
from .notifications import (
    NotificationFailed,
    NotificationFailedPlaceholder,
    RetriesExhausted,
)
from .storage import DeliveryRow, Outbox, StoredEvent


_LOG = logging.getLogger(__name__)


class OutboxNotifier:
    """把事件先持久化到 outbox，再返回。

    `send` 在行被持久化后立即返回；真正的 HTTP 调用由 worker 处理。
    """

    def __init__(
        self,
        outbox: Outbox,
        *,
        channels: list[tuple[str, int]],
    ) -> None:
        if not channels:
            raise ValueError("OutboxNotifier 至少需要一个 channel")
        # 同一 channel 不能重复，避免 enqueue 时插重复行
        seen: set[str] = set()
        for name, _ in channels:
            if name in seen:
                raise ValueError(f"channel 重复: {name!r}")
            seen.add(name)
        self._outbox = outbox
        self._channels = list(channels)

    @property
    def outbox(self) -> Outbox:
        return self._outbox

    @property
    def channels(self) -> list[tuple[str, int]]:
        return list(self._channels)

    async def send(self, event: AlertEvent) -> None:
        # outbox 是同步 API，包装成 async 保持调用方接口一致
        await asyncio.to_thread(self._outbox.enqueue, event, self._channels)


class OutboxWorker:
    """后台协程：定期消费 outbox 中的待发送行。

    每轮按 channel 拉取到期的 delivery 行交给对应 ``Notifier``：

    * ``NotificationFailedPlaceholder`` / ``NotificationFailed``（4xx
      等不可重试）→ ``mark_dead``，不再调度；
    * ``RetriesExhausted``（transport 内 HTTP 重试耗尽）→ ``mark_retry``，
      backoff 后下一轮 tick；
    * 其它异常 → ``mark_retry``，同上下一次重试；
    * 成功 → ``mark_delivered``。

    每个 channel 的 ``attempts`` 独立累加，达到 channel 自己的
    ``max_event_attempts`` 时 ``mark_dead``。
    """

    def __init__(
        self,
        outbox: Outbox,
        transports: dict[str, "object"],
        *,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 16,
        max_backoff_seconds: int = 60,
        metrics: Optional[MetricsStore] = None,
    ) -> None:
        if not transports:
            raise ValueError("OutboxWorker 至少需要一个 channel 的 transport")
        self._outbox = outbox
        self._transports = dict(transports)
        self._poll_interval = poll_interval_seconds
        self._batch_size = batch_size
        self._max_backoff = max_backoff_seconds
        self._metrics = metrics
        self._stop_event: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None

    def request_stop(self) -> None:
        if self._stop_event is not None:
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
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        else:
            self._stop_event.clear()
        self._task = asyncio.create_task(self.run_forever(), name="outbox-worker")

    async def stop(self) -> None:
        self.request_stop()
        if self._task is not None:
            await self._task
            self._task = None

    async def drain_once(self) -> int:
        """对每个 channel 各扫一轮；返回本轮总投递条数（含 dead）。"""
        total = 0
        for channel in self._transports:
            rows = await asyncio.to_thread(
                self._outbox.claim_due_for_channel, channel, None, self._batch_size
            )
            for delivery, stored in rows:
                await self._deliver(channel, delivery, stored)
                total += 1
        return total

    def _record_attempt(self, channel: str, result: str) -> None:
        if self._metrics is None:
            return
        try:
            self._metrics.notification_attempts_total.labels(
                channel=channel, result=result
            ).inc()
        except Exception:  # noqa: BLE001
            _LOG.debug("metrics notification_attempts_total update failed")

    async def _deliver(
        self,
        channel: str,
        delivery: DeliveryRow,
        stored: StoredEvent,
    ) -> None:
        transport = self._transports.get(channel)
        if transport is None:
            _LOG.error("channel %s 未注册 transport，跳过 delivery=%d", channel, delivery.id)
            await asyncio.to_thread(
                self._outbox.mark_dead, delivery.id, f"channel {channel!r} has no transport"
            )
            self._record_attempt(channel, "dead")
            return

        if Outbox.is_exhausted(delivery.attempts, delivery.max_event_attempts):
            await asyncio.to_thread(
                self._outbox.mark_dead, delivery.id,
                f"exhausted: attempts={delivery.attempts} >= "
                f"max={delivery.max_event_attempts}",
            )
            self._record_attempt(channel, "dead")
            _LOG.error(
                "通知放弃，已达最大尝试次数 (channel=%s delivery=%d event_id=%s attempts=%d)",
                channel, delivery.id, stored.event.event_id, delivery.attempts,
            )
            return

        try:
            await transport.send(stored.event)
        except RetriesExhausted as exc:
            delay = self._backoff_seconds(delivery.attempts + 1)
            await asyncio.to_thread(
                self._outbox.mark_retry, delivery.id, delay, str(exc)
            )
            self._record_attempt(channel, "retry")
            _LOG.warning(
                "transport 重试耗尽，%.1fs 后再次调度 (channel=%s delivery=%d event_id=%s status=%s)",
                delay, channel, delivery.id, stored.event.event_id, exc.status_code,
            )
        except (NotificationFailed, NotificationFailedPlaceholder) as exc:
            await asyncio.to_thread(
                self._outbox.mark_dead, delivery.id, str(exc)
            )
            self._record_attempt(channel, "dead")
            _LOG.error(
                "通知放弃 (channel=%s delivery=%d event_id=%s status=%s): %s",
                channel, delivery.id, stored.event.event_id,
                getattr(exc, "status_code", None), exc,
            )
        except Exception as exc:  # noqa: BLE001
            delay = self._backoff_seconds(delivery.attempts + 1)
            await asyncio.to_thread(
                self._outbox.mark_retry, delivery.id, delay, repr(exc)
            )
            self._record_attempt(channel, "retry")
            _LOG.warning(
                "通知将在 %.1fs 后重试 (channel=%s delivery=%d event_id=%s, 第 %d 次): %s",
                delay, channel, delivery.id, stored.event.event_id,
                delivery.attempts + 1, exc,
            )
        else:
            await asyncio.to_thread(self._outbox.mark_delivered, delivery.id)
            self._record_attempt(channel, "success")
            _LOG.info(
                "通知已发送 (channel=%s delivery=%d event_id=%s)",
                channel, delivery.id, stored.event.event_id,
            )

    def _backoff_seconds(self, attempts: int) -> float:
        return float(min(2 ** max(0, attempts - 1), self._max_backoff))


def build_channels(
    notification: NotificationConfig,
    cuckoo: CuckooConfig,
) -> list[tuple[str, int]]:
    """根据配置返回所有启用 channel 及其 max_event_attempts。

    顺序：webhook → cuckoo.receivemap → cuckoo.forward。仅返回 URL
    非空（或 enabled=True）的 channel。
    """
    channels: list[tuple[str, int]] = []
    if notification.enabled and notification.url:
        channels.append(("webhook", notification.max_event_attempts))
    if cuckoo.enabled or cuckoo.url.get("receivemap"):
        if cuckoo.url.get("receivemap"):
            channels.append(("cuckoo.receivemap", cuckoo.max_event_attempts))
    if cuckoo.enabled or cuckoo.url.get("forward"):
        if cuckoo.url.get("forward"):
            channels.append(("cuckoo.forward", cuckoo.max_event_attempts))
    return channels