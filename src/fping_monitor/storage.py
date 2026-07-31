"""SQLite 通知 Outbox。

Notifier 本身可能短暂不可用：如果进程崩溃，队列中尚未投递的事件
必须能够存活到下次启动。我们用 SQLite (WAL 模式) 持久化事件，
由一个独立 worker 轮询并把到期的行交给对应的 :class:`Notifier`。

数据分两张表：

* ``outbox`` 只存原始 :class:`AlertEvent`，一份事件一行。
* ``outbox_delivery`` 是投递账本：每条事件被每个启用的 channel
  生成一行 ``pending`` 记录。worker 按 channel 独立 claim/mark。

行的 ``status``（仅出现在 ``outbox_delivery``）取值：

- ``pending``：等待发送
- ``in_flight``：worker 正在处理
- ``delivered``：已成功送达（终态）
- ``dead``：超过最大重试次数，放弃（终态）

进程内只有一个实例，因此 ``in_flight`` 在取出时设置；启动时把上次
进程遗留的 ``in_flight`` 全部回收为 ``pending``。
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
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_delivery (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id           INTEGER NOT NULL REFERENCES outbox(id),
    channel             TEXT NOT NULL,
    status              TEXT NOT NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    max_event_attempts  INTEGER NOT NULL,
    next_attempt_at     REAL NOT NULL DEFAULT 0,
    last_error          TEXT,
    created_at          REAL NOT NULL,
    delivered_at        REAL,
    dead_at             REAL,
    UNIQUE(outbox_id, channel)
);
CREATE INDEX IF NOT EXISTS idx_outbox_delivery_status_next
    ON outbox_delivery(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_outbox_delivery_channel
    ON outbox_delivery(channel, status);
"""


@dataclass
class StoredEvent:
    """主表行，仅承载原始事件。"""
    id: int
    event: AlertEvent
    payload: dict


@dataclass
class DeliveryRow:
    """子表行：一条事件在一个 channel 上的投递状态。"""
    id: int
    outbox_id: int
    channel: str
    status: str
    attempts: int
    max_event_attempts: int
    next_attempt_at: float
    last_error: Optional[str]
    created_at: float
    delivered_at: Optional[float]
    dead_at: Optional[float]


class Outbox:
    """线程安全的 SQLite Outbox。

    所有方法都是同步的；调用方如果需要异步行为请包一层
    :func:`asyncio.to_thread`。
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_event_attempts: int = 32,
    ) -> None:
        if max_event_attempts < 1:
            raise ValueError("max_event_attempts 必须 >= 1")
        self._path = str(path)
        self._max_event_attempts = max_event_attempts
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
    def max_event_attempts(self) -> int:
        """每个 channel 默认的 worker tick 上限。delivery 行写入时会
        把当时的值固化（denormalize），允许以后单独调整某个 channel。
        """
        return self._max_event_attempts

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def enqueue(
        self,
        event: AlertEvent,
        channels: list[tuple[str, int]],
    ) -> int:
        """插入原始事件并为每个 channel 创建投递账本行。

        ``channels`` 是 ``[(channel_name, max_event_attempts), ...]``。
        必须在同一事务里完成，保证主表 + 子表原子写入。
        """
        if not channels:
            raise ValueError("enqueue 至少需要一个 channel")
        payload = self._event_to_payload(event)
        body = json.dumps(payload, sort_keys=True)
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT OR IGNORE INTO outbox(
                    event_id, incident_id, event_type, target_id, address,
                    labels_json, occurred_at, confirmed_at, last_success_at,
                    consecutive_failures, packet_loss_ratio, probe_type,
                    monitor_instance, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            outbox_id = cur.lastrowid
            if outbox_id == 0 or outbox_id is None:
                # event_id UNIQUE 冲突：去查已有的 id
                row = self._conn.execute(
                    "SELECT id FROM outbox WHERE event_id=?", (event.event_id,)
                ).fetchone()
                outbox_id = row["id"]
                return outbox_id
            now = time.time()
            self._conn.executemany(
                """
                INSERT INTO outbox_delivery(
                    outbox_id, channel, status, attempts, max_event_attempts,
                    next_attempt_at, created_at
                ) VALUES (?, ?, 'pending', 0, ?, ?, ?)
                """,
                [
                    (outbox_id, channel, max_attempts, now, now)
                    for channel, max_attempts in channels
                ],
            )
            return outbox_id

    def mark_delivered(self, delivery_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox_delivery SET status='delivered', "
                "delivered_at=?, last_error=NULL WHERE id=?",
                (time.time(), delivery_id),
            )

    def mark_retry(self, delivery_id: int, delay_seconds: float, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox_delivery SET status='pending', "
                "attempts=attempts+1, next_attempt_at=?, last_error=? WHERE id=?",
                (time.time() + delay_seconds, error, delivery_id),
            )

    def mark_dead(self, delivery_id: int, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox_delivery SET status='dead', attempts=attempts+1, "
                "dead_at=?, last_error=? WHERE id=?",
                (time.time(), error, delivery_id),
            )

    @staticmethod
    def is_exhausted(attempts: int, max_event_attempts: int) -> bool:
        """返回 True 表示下一次再尝试就会超过该 channel 的最大 tick 数。

        attempts 表示已经做过几次；下一次成功后 attempts+1；
        当 attempts 已经等于或超过 max_event_attempts 时不能再尝试。
        """
        return attempts >= max_event_attempts

    def reclaim_in_flight(self) -> int:
        """把上次进程未完成、仍处于 `in_flight` 的子表行重置为 `pending`。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE outbox_delivery SET status='pending' "
                "WHERE status='in_flight' RETURNING id"
            )
            return len(cur.fetchall())

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------
    def claim_due_for_channel(
        self,
        channel: Optional[str] = None,
        now: Optional[float] = None,
        limit: int = 32,
    ) -> list[tuple[DeliveryRow, StoredEvent]]:
        """把最多 `limit` 条已到期 pending 子表行置为 in_flight 并返回。

        ``channel`` 为 None 表示所有 channel；否则只 claim 指定 channel。
        返回 ``(delivery, stored_event)`` 对，便于 worker 一次拿全。
        """
        ts = now if now is not None else time.time()
        out: list[tuple[DeliveryRow, StoredEvent]] = []
        with self._lock:
            if channel is None:
                cur = self._conn.execute(
                    "SELECT id FROM outbox_delivery "
                    "WHERE status='pending' AND next_attempt_at<=? "
                    "ORDER BY next_attempt_at LIMIT ?",
                    (ts, limit),
                )
            else:
                cur = self._conn.execute(
                    "SELECT id FROM outbox_delivery "
                    "WHERE status='pending' AND next_attempt_at<=? AND channel=? "
                    "ORDER BY next_attempt_at LIMIT ?",
                    (ts, channel, limit),
                )
            ids = [r["id"] for r in cur.fetchall()]
            for row_id in ids:
                self._conn.execute(
                    "UPDATE outbox_delivery SET status='in_flight' WHERE id=?",
                    (row_id,),
                )
            if not ids:
                return out
            placeholders = ",".join("?" * len(ids))
            cur = self._conn.execute(
                f"""
                SELECT d.*, o.payload_json AS o_payload_json, o.event_id AS o_event_id,
                       o.incident_id AS o_incident_id, o.event_type AS o_event_type,
                       o.target_id AS o_target_id, o.address AS o_address,
                       o.labels_json AS o_labels_json, o.occurred_at AS o_occurred_at,
                       o.confirmed_at AS o_confirmed_at,
                       o.last_success_at AS o_last_success_at,
                       o.consecutive_failures AS o_consecutive_failures,
                       o.packet_loss_ratio AS o_packet_loss_ratio,
                       o.probe_type AS o_probe_type,
                       o.monitor_instance AS o_monitor_instance
                  FROM outbox_delivery d
                  JOIN outbox o ON o.id = d.outbox_id
                 WHERE d.id IN ({placeholders})
                """,
                ids,
            )
            for r in cur.fetchall():
                delivery = self._row_to_delivery(r)
                payload = json.loads(r["o_payload_json"])
                event = self._payload_to_event(payload)
                stored = StoredEvent(id=r["outbox_id"], event=event, payload=payload)
                out.append((delivery, stored))
        return out

    def count(self, status: Optional[str] = None) -> int:
        """统计子表中指定 status 的行数。

        兼容旧 API：``status='pending'``/``'dead'``/``'in_flight'``/``'delivered'``。
        """
        with self._lock:
            if status is None:
                cur = self._conn.execute("SELECT COUNT(*) AS n FROM outbox_delivery")
            else:
                cur = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM outbox_delivery WHERE status=?",
                    (status,),
                )
            return cur.fetchone()["n"]

    def oldest_pending_age_seconds(self) -> Optional[float]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT MIN(created_at) AS ts FROM outbox_delivery WHERE status='pending'"
            )
            row = cur.fetchone()
        if row is None or row["ts"] is None:
            return None
        return max(0.0, time.time() - row["ts"])

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_delivery(r: sqlite3.Row) -> DeliveryRow:
        return DeliveryRow(
            id=r["id"],
            outbox_id=r["outbox_id"],
            channel=r["channel"],
            status=r["status"],
            attempts=r["attempts"],
            max_event_attempts=r["max_event_attempts"],
            next_attempt_at=r["next_attempt_at"],
            last_error=r["last_error"],
            created_at=r["created_at"],
            delivered_at=r["delivered_at"],
            dead_at=r["dead_at"],
        )

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
