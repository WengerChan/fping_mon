"""应用装配。

`bootstrap` 是唯一公开的入口：读取配置、构造指标存储、状态机、
按 channel 启用的 transport、outbox、调度器和小型 HTTP 服务，全部
作为 :class:`Application` 字段返回。测试可以直接读取任意字段；
生产代码调用 :meth:`Application.run` 和 :meth:`Application.shutdown`。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from prometheus_client import REGISTRY

from . import __version__
from .api import MetricsHTTPServer
from .config import AppConfig, load_config
from .cuckoo import Cuckoo
from .metrics import MetricsStore
from .notifications import CuckooNotifier, Notifier, WebhookNotifier
from .outbox import OutboxNotifier, OutboxWorker, _NullOutboxNotifier, build_channels
from .scheduler import Scheduler
from .state import StateManager
from .storage import Outbox


_LOG = logging.getLogger(__name__)


def _build_transports(config: AppConfig) -> dict[str, Notifier]:
    """根据配置构造每个 channel 的 transport；未启用的 channel 不出现。"""
    transports: dict[str, Notifier] = {}

    if config.notification.enabled and config.notification.url:
        try:
            transports["webhook"] = WebhookNotifier(
                url=config.notification.url,
                max_attempts=config.notification.max_attempts,
                max_backoff_seconds=config.notification.max_backoff_seconds,
                token_env=config.notification.token_env,
            )
        except ValueError as exc:
            _LOG.error("构造 webhook transport 失败: %s", exc)

    cuckoo_obj = Cuckoo(config)
    for endpoint in ("receivemap", "forward"):
        url = config.cuckoo.url.get(endpoint, "")
        if not url:
            continue
        if not (config.cuckoo.enabled or url):
            continue
        channel = f"cuckoo.{endpoint}"
        try:
            transports[channel] = CuckooNotifier(
                cuckoo_obj,
                endpoint=endpoint,
                max_attempts=config.cuckoo.max_attempts,
                max_backoff_seconds=config.cuckoo.max_backoff_seconds,
                monitor_instance=config.notification.monitor_instance,
            )
        except ValueError as exc:
            _LOG.error("构造 cuckoo %s transport 失败: %s", endpoint, exc)

    return transports


def _build_ready_check(scheduler: Scheduler, interval_seconds: int):
    deadline = max(interval_seconds * 2, 60)

    def ready_check() -> tuple[bool, str]:
        if scheduler.round_count == 0:
            return False, "尚未完成任何探测轮次"
        last = scheduler.last_round_completed_at
        if last is None:
            return False, "尚未完成任何探测轮次"
        age = time.time() - last
        if age > deadline:
            return False, f"最近一轮探测距今 {age:.1f}s（deadline={deadline}s）"
        return True, "ok"

    return ready_check


@dataclass
class Application:
    config: AppConfig
    config_path: Path
    metrics: MetricsStore
    state_manager: StateManager
    transports: dict[str, Notifier]
    outbox: Outbox
    outbox_notifier: "OutboxNotifier | _NullOutboxNotifier"
    outbox_worker: "OutboxWorker | _NullOutboxWorker"
    scheduler: Scheduler
    http_server: MetricsHTTPServer
    _outbox_stats_task: Optional[asyncio.Task] = None
    _stop_event: Optional[asyncio.Event] = None

    async def run(self) -> None:
        self._stop_event = asyncio.Event()
        self.http_server.start()
        self.metrics.set_storage_healthy(True)
        await self.outbox_worker.start()
        self._outbox_stats_task = asyncio.create_task(
            self._publish_outbox_stats(), name="outbox-stats"
        )
        _LOG.info(
            "fping-monitor 启动完成: targets=%d interval=%ds channels=%s",
            len(self.config.targets),
            self.config.probe.interval_seconds,
            sorted(self.transports.keys()),
        )
        try:
            await self.scheduler.run_forever()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        # 关闭顺序很重要：
        # 1. 先让 scheduler 停：避免 probe_once 继续往 outbox 写入新事件；
        # 2. 再 cancel 统计 task；
        # 3. 再让 outbox worker 停：worker 仍要消费已入队的事件；
        # 4. 关闭 http_server / transports / outbox（数据库）。
        # 颠倒任一步都可能导致：未消费事件丢失 / 重复 / 卡住。
        self.scheduler.request_stop()
        if self._outbox_stats_task is not None:
            self._outbox_stats_task.cancel()
            try:
                await self._outbox_stats_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._outbox_stats_task = None
        await self.outbox_worker.stop()
        self.http_server.stop()
        for name, transport in self.transports.items():
            close = getattr(transport, "close", None)
            if close is None:
                continue
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # noqa: BLE001
                _LOG.exception("关闭 transport %s 失败", name)
        self.outbox.close()
        self.metrics.set_storage_healthy(False)
        _LOG.info("fping-monitor 已关闭")

    async def reload_config(self) -> None:
        """SIGHUP 重载：仅刷新 targets 列表。

        其它字段（probe/state/notification/storage/cuckoo/server）需要
        重启程序才生效。
        """
        import time as _time

        new_config = load_config(self.config_path)
        old = self.config
        reload_section_changed = False
        for section in ("probe", "state", "notification", "storage", "cuckoo", "server"):
            old_obj = getattr(old, section, None)
            new_obj = getattr(new_config, section, None)
            if old_obj is not None and new_obj is not None and old_obj != new_obj:
                reload_section_changed = True
                _LOG.warning(
                    "%s 配置已变更，重载不会生效，需要重启程序 (old=%s, new=%s)",
                    section,
                    old_obj,
                    new_obj,
                )
        old_targets = set(t.id for t in old.targets)
        new_targets = set(t.id for t in new_config.targets)
        try:
            self.state_manager.upsert_targets(new_config.targets)
        except Exception:
            self.metrics.record_config_reload(success=False)
            raise
        added = new_targets - old_targets
        removed = old_targets - new_targets
        self.config = new_config
        self.metrics.set_targets_loaded(len(new_config.targets))
        self.metrics.record_config_reload(success=True, ts=_time.time())
        _LOG.info(
            "配置重载完成: targets=%d (added=%d, removed=%d)",
            len(new_config.targets),
            len(added),
            len(removed),
        )

    async def _publish_outbox_stats(self) -> None:
        try:
            while True:
                try:
                    pending = self.outbox.count("pending")
                    dead = self.outbox.count("dead")
                    age = self.outbox.oldest_pending_age_seconds()
                    self.metrics.set_outbox_stats(
                        pending=pending, dead=dead, oldest_pending_age=age
                    )
                    self.metrics.record_storage_operation("read", success=True)
                    self.metrics.set_storage_healthy(True)
                except Exception:  # noqa: BLE001
                    self.metrics.set_storage_healthy(False)
                    self.metrics.record_storage_operation("read", success=False)
                    _LOG.exception("更新 outbox 统计指标失败")
                await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            return


def bootstrap(
    config_path: str | Path,
    *,
    registry: Optional[object] = None,
    outbox_path: Optional[str] = None,
) -> Application:
    """从配置文件路径构造一个完整可用的 :class:`Application`。

    `registry` 默认使用 prometheus_client 全局 registry，测试可以
    传入独立的 CollectorRegistry 以保持隔离。`outbox_path` 用于在
    测试中覆盖 storage 路径。
    """
    path = Path(config_path)
    config = load_config(path)
    reg = registry if registry is not None else REGISTRY
    metrics = MetricsStore(
        registry=reg,
        version=__version__,
        fping_binary=config.probe.fping_binary,
    )
    state_manager = StateManager(
        down_after_failures=config.state.down_after_failures,
        up_after_successes=config.state.up_after_successes,
        monitor_instance=config.notification.monitor_instance,
    )
    state_manager.upsert_targets(config.targets)

    channels = build_channels(config.notification, config.cuckoo)
    if not channels:
        _LOG.warning("未启用任何告警 channel，outbox 不会启动 worker")

    outbox = Outbox(
        outbox_path or config.storage.path,
        max_event_attempts=max(
            (max_a for _, max_a in channels), default=10
        ),
    )
    transports = _build_transports(config)
    if transports:
        outbox_notifier: "OutboxNotifier | _NullOutboxNotifier" = OutboxNotifier(
            outbox, channels=channels
        )
        outbox_worker: "OutboxWorker | _NullOutboxWorker" = OutboxWorker(
            outbox,
            transports,
            poll_interval_seconds=1.0,
            max_backoff_seconds=config.notification.max_backoff_seconds,
            metrics=metrics,
        )
    else:
        # 未启用任何 channel：worker 不启动，notifier 是 no-op，
        # scheduler 仍按原路径调用 notifier.send（不会真投递）。
        outbox_notifier = _NullOutboxNotifier()
        outbox_worker = _NullOutboxWorker()

    scheduler = Scheduler(
        config=config,
        state_manager=state_manager,
        notifier=outbox_notifier,
        metrics=metrics,
    )

    ready_check = _build_ready_check(scheduler, config.probe.interval_seconds)

    http_server = MetricsHTTPServer(
        host=config.server.listen,
        port=config.server.port,
        registry=reg,
        metrics=metrics,
        ready_check=ready_check,
    )

    metrics.set_targets_loaded(len(config.targets))
    return Application(
        config=config,
        config_path=path,
        metrics=metrics,
        state_manager=state_manager,
        transports=transports,
        outbox=outbox,
        outbox_notifier=outbox_notifier,
        outbox_worker=outbox_worker,
        scheduler=scheduler,
        http_server=http_server,
    )


class _NullOutboxWorker:
    """通知关闭时的占位 worker：不实际消费 outbox。"""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def request_stop(self) -> None:
        return None