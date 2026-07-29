"""SQLite 通知 Outbox。

Notifier 本身可能短暂不可用：如果进程崩溃，队列中尚未投递的事件
必须能够存活到下次启动。我们用单表 SQLite (WAL 模式) 持久化事件，
由一个独立 worker 轮询并把到期的行交给 :class:`WebhookNotifier`。

行的 `status` 取值：
- `pending`：等待发送
- `in_flight`：worker 正在处理
- `delivered`：已成功送达
- `dead`：超过最大重试次数，放弃

进程内只有一个实例，因此 `in_flight` 在取出时设置；启动时把上次
进程遗留的 `in_flight` 全部回收为 `pending`。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

from .models import AlertEvent, Target


_LOG = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT NOT NULL UNIQUE,
    incident_id   TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    target_id     TEXT NOT NULL,
    address       TEXT NOT NULL,
    labels_json   TEXT NOT NULL,
    occurred_at   TEXT NOT NULL,
    confirmed_at  TEXT NOT NULL,
    last_success_at TEXT,
    consecutive_failures INTEGER NOT NULL,
    packet_loss_ratio REAL NOT NULL,
    probe_type    TEXT NOT NULL,
    monitor_instance TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    status        TEXT NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status_next ON outbox(status, next_attempt_at);
"""


@dataclass
class StoredEvent:
    id: int
    event: AlertEvent
    attempts: int
    payload: dict


class Outbox:
    """线程安全的 SQLite Outbox。

    所有方法都是同步的；调用方如果需要异步行为请包一层
    :func:`asyncio.to_thread`。
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_delivery_attempts: int = 32,
    ) -> None:
        if max_delivery_attempts < 1:
            raise ValueError("max_delivery_attempts 必须 >= 1")
        self._path = str(path)
        self._max_delivery_attempts = max_delivery_attempts
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
        self.reclaim_in_flight()

    @property
    def path(self) -> str:
        return self._path

    @property
    def max_delivery_attempts(self) -> int:
        """单条事件允许的最大累计尝试次数。超过即标记 dead。"""
        return self._max_delivery_attempts

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def enqueue(self, event: AlertEvent) -> int:
        payload = self._event_to_payload(event)
        body = json.dumps(payload, sort_keys=True)
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT OR IGNORE INTO outbox(
                    event_id, incident_id, event_type, target_id, address,
                    labels_json, occurred_at, confirmed_at, last_success_at,
                    consecutive_failures, packet_loss_ratio, probe_type,
                    monitor_instance, payload_json, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    event.event_id,
                    event.incident_id,
                    event.event_type,
                    event.target.id,
                    event.target.address,
                    json.dumps(dict(event.target.labels), sort_keys=True),
                    event.occurred_at,
                    event.confirmed_at,
                    event.last_success_at,
                    event.consecutive_failures,
                    event.packet_loss_ratio,
                    event.probe_type,
                    event.monitor_instance,
                    body,
                    time.time(),
                ),
            )
            return cur.lastrowid or 0

    def mark_delivered(self, row_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET status='delivered' WHERE id=?", (row_id,)
            )

    def mark_retry(self, row_id: int, delay_seconds: float, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET status='pending', attempts=attempts+1, "
                "next_attempt_at=?, last_error=? WHERE id=?",
                (time.time() + delay_seconds, error, row_id),
            )

    def mark_dead(self, row_id: int, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET status='dead', attempts=attempts+1, last_error=? "
                "WHERE id=?",
                (error, row_id),
            )

    @staticmethod
    def is_exhausted(attempts: int, max_delivery_attempts: int) -> bool:
        """返回 True 表示下一次再尝试就会超过最大尝试次数。"""
        # attempts 表示已经做过几次；下一次成功后 attempts+1；
        # 当 attempts 已经等于或超过 max_delivery_attempts 时不能再尝试。
        return attempts >= max_delivery_attempts

    def reclaim_in_flight(self) -> int:
        """把上次进程未完成、仍处于 `in_flight` 的行重置为 `pending`。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE outbox SET status='pending' WHERE status='in_flight' RETURNING id"
            )
            return len(cur.fetchall())

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------
    def claim_due(self, now: Optional[float] = None, limit: int = 32) -> list[StoredEvent]:
        """把最多 `limit` 条已到期 pending 行置为 in_flight 并返回。"""
        ts = now if now is not None else time.time()
        out: list[StoredEvent] = []
        with self._lock:
            cur = self._conn.execute(
                "SELECT id FROM outbox WHERE status='pending' AND next_attempt_at<=? "
                "ORDER BY next_attempt_at LIMIT ?",
                (ts, limit),
            )
            ids = [r["id"] for r in cur.fetchall()]
            for row_id in ids:
                self._conn.execute(
                    "UPDATE outbox SET status='in_flight' WHERE id=?", (row_id,)
                )
            if not ids:
                return out
            cur = self._conn.execute(
                "SELECT * FROM outbox WHERE id IN ({})".format(",".join("?" * len(ids))),
                ids,
            )
            for r in cur.fetchall():
                payload = json.loads(r["payload_json"])
                event = self._payload_to_event(payload)
                out.append(StoredEvent(id=r["id"], event=event, attempts=r["attempts"], payload=payload))
        return out

    def count(self, status: Optional[str] = None) -> int:
        with self._lock:
            if status is None:
                cur = self._conn.execute("SELECT COUNT(*) AS n FROM outbox")
            else:
                cur = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM outbox WHERE status=?", (status,)
                )
            return cur.fetchone()["n"]

    def oldest_pending_age_seconds(self) -> Optional[float]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT MIN(created_at) AS ts FROM outbox WHERE status='pending'"
            )
            row = cur.fetchone()
        if row is None or row["ts"] is None:
            return None
        return max(0.0, time.time() - row["ts"])

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    @staticmethod
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

    @staticmethod
    def _payload_to_event(payload: dict) -> AlertEvent:
        target = Target(
            id=payload["host_id"],
            address=payload["address"],
            labels=dict(payload.get("labels") or {}),
        )
        return AlertEvent(
            event_id=payload["event_id"],
            incident_id=payload["incident_id"],
            event_type=payload["event_type"],
            target=target,
            occurred_at=payload["occurred_at"],
            confirmed_at=payload["confirmed_at"],
            last_success_at=payload.get("last_success_at"),
            consecutive_failures=payload.get("consecutive_failures", 0),
            packet_loss_ratio=payload.get("packet_loss_ratio", 1.0),
            probe_type=payload.get("probe_type", "icmp"),
            monitor_instance=payload.get("monitor_instance", "monitor-a"),
        )
