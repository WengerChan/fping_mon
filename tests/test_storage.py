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


# 测试用 channel 列表：固定一个 channel，方便断言
_TEST_CHANNELS = [("webhook", 10)]


def test_enqueue_is_idempotent(tmp_path: Path):
    """重复 enqueue 同一 event_id 只插入一行；子表也只有一行 delivery。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)  # event_id UNIQUE 冲突被忽略
        assert outbox.count() == 1
        assert outbox.count("pending") == 1
    finally:
        outbox.close()


def test_claim_due_returns_pending_rows(tmp_path: Path):
    """claim_due_for_channel 把到期 pending 行置为 in_flight 并返回 (delivery, stored) 对。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        outbox.enqueue(_event("e2", iid="i2"), _TEST_CHANNELS)
        rows = outbox.claim_due_for_channel("webhook")
        assert len(rows) == 2
        assert {stored.event.event_id for _, stored in rows} == {"e1", "e2"}
        assert outbox.count("in_flight") == 2
    finally:
        outbox.close()


def test_claim_due_respects_next_attempt_at(tmp_path: Path):
    """next_attempt_at 在未来时，claim 不返回任何行。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        with outbox._lock:  # noqa: SLF001 - 测试专用
            outbox._conn.execute(
                "UPDATE outbox_delivery SET next_attempt_at=? WHERE status='pending'",
                (10**12,),
            )
        assert outbox.claim_due_for_channel("webhook") == []
    finally:
        outbox.close()


def test_mark_delivered_and_dead(tmp_path: Path):
    """mark_delivered / mark_dead 把子表行转终态。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        [row] = outbox.claim_due_for_channel("webhook")
        delivery, _ = row
        outbox.mark_delivered(delivery.id)
        assert outbox.count("delivered") == 1

        outbox.enqueue(_event("e2", iid="i2"), _TEST_CHANNELS)
        [row] = outbox.claim_due_for_channel("webhook")
        delivery, _ = row
        outbox.mark_dead(delivery.id, error="upstream 4xx")
        assert outbox.count("dead") == 1
    finally:
        outbox.close()


def test_reclaim_in_flight_simulates_crash(tmp_path: Path):
    """上次进程未完成的 in_flight 子表行，启动时回收为 pending。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        outbox.claim_due_for_channel("webhook")  # 转为 in_flight
        outbox.close()

        outbox2 = Outbox(tmp_path / "outbox.db")
        try:
            assert outbox2.count("pending") == 1
            assert outbox2.count("in_flight") == 0
        finally:
            outbox2.close()
    finally:
        # 上一段已主动 close
        pass


def test_oldest_pending_age(tmp_path: Path):
    """空表返回 None；有 pending 行时返回非负秒数。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        assert outbox.oldest_pending_age_seconds() is None
        outbox.enqueue(_event("e1"), _TEST_CHANNELS)
        age = outbox.oldest_pending_age_seconds()
        assert age is not None and age >= 0
    finally:
        outbox.close()


def test_multi_channel_fan_out(tmp_path: Path):
    """一个事件 × N 个 channel = N 行 delivery，分别独立 claim。"""
    outbox = Outbox(tmp_path / "outbox.db")
    try:
        channels = [("webhook", 10), ("cuckoo.receivemap", 3), ("cuckoo.forward", 3)]
        outbox.enqueue(_event("e1"), channels)
        assert outbox.count("pending") == 3
        for ch in ("webhook", "cuckoo.receivemap", "cuckoo.forward"):
            rows = outbox.claim_due_for_channel(ch)
            assert len(rows) == 1
            delivery, stored = rows[0]
            assert delivery.channel == ch
            assert stored.event.event_id == "e1"
            outbox.mark_delivered(delivery.id)
        assert outbox.count("delivered") == 3
        assert outbox.count("pending") == 0
    finally:
        outbox.close()