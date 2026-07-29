"""SQLite Outbox 测试。"""

from pathlib import Path

from fping_monitor.models import AlertEvent, Target
from fping_monitor.storage import Outbox


def _event(eid: str = "e1", iid: str = "i1") -> AlertEvent:
    return AlertEvent(
        event_id=eid,
        incident_id=iid,
        event_type="host_down",
        target=Target(id="host-a", address="10.0.0.1", labels={"site": "shanghai"}),
        occurred_at="2026-07-28T00:00:00Z",
        confirmed_at="2026-07-28T00:00:10Z",
        last_success_at="2026-07-28T00:00:05Z",
        consecutive_failures=3,
        packet_loss_ratio=1.0,
    )


def test_enqueue_is_idempotent(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"))
        outbox.enqueue(_event("e1"))  # 重复 event_id 会被忽略
        assert outbox.count() == 1
        assert outbox.count("pending") == 1
    finally:
        outbox.close()


def test_claim_due_returns_pending_rows(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"))
        outbox.enqueue(_event("e2", iid="i2"))
        rows = outbox.claim_due()
        assert len(rows) == 2
        assert {r.event.event_id for r in rows} == {"e1", "e2"}
        assert outbox.count("in_flight") == 2
    finally:
        outbox.close()


def test_claim_due_respects_next_attempt_at(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"))
        # 把唯一一行 pending 推到很远的未来
        with outbox._lock:  # noqa: SLF001 - 测试专用
            outbox._conn.execute(
                "UPDATE outbox SET next_attempt_at=? WHERE status='pending'",
                (10**12,),
            )
        assert outbox.claim_due() == []
    finally:
        outbox.close()


def test_mark_delivered_and_dead(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"))
        [row] = outbox.claim_due()
        outbox.mark_delivered(row.id)
        assert outbox.count("delivered") == 1

        outbox.enqueue(_event("e2", iid="i2"))
        [row] = outbox.claim_due()
        outbox.mark_dead(row.id, error="upstream 4xx")
        assert outbox.count("dead") == 1
    finally:
        outbox.close()


def test_reclaim_in_flight_simulates_crash(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"))
        outbox.claim_due()  # 转为 in_flight
        outbox.close()

        # 重新连接：必须把 in_flight 行回收
        outbox2 = Outbox(tmp_path / "outbox.db")
        try:
            assert outbox2.count("pending") == 1
            assert outbox2.count("in_flight") == 0
        finally:
            outbox2.close()
    finally:
        # 上一段已主动 close，这里仅做无害的"保险"
        if outbox is not None and not outbox._conn is None:  # type: ignore[truthy]
            pass


def test_oldest_pending_age(tmp_path: Path):
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        assert outbox.oldest_pending_age_seconds() is None
        outbox.enqueue(_event("e1"))
        age = outbox.oldest_pending_age_seconds()
        assert age is not None and age >= 0
    finally:
        outbox.close()
