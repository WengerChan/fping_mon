"""Prometheus 指标 Facade 的测试。"""

from prometheus_client import CollectorRegistry

from fping_monitor.metrics import MetricsStore
from fping_monitor.models import ProbeResult, Target
from fping_monitor.state import HostState, HostStatus, StateChange


def _new_store() -> MetricsStore:
    return MetricsStore(
        CollectorRegistry(),
        version="test",
        fping_binary="/usr/sbin/fping",
    )


def _gauge_value(store: MetricsStore, name: str, **labels) -> float:
    metric = getattr(store, name)
    if labels:
        return metric.labels(**labels)._value.get()
    # 内部 API：无标签的 Gauge 在这里保存唯一样本
    return metric._value.get()


def _counter_value(store: MetricsStore, name: str, **labels) -> float:
    metric = getattr(store, name)
    return metric.labels(**labels)._value.get()


def test_update_probe_results_writes_gauges():
    store = _new_store()
    results = {
        "host-a": ProbeResult(
            target_id="host-a",
            success=True,
            latency_seconds=0.012,
            packet_loss_ratio=0.0,
        ),
        "host-b": ProbeResult(
            target_id="host-b",
            success=False,
            error="timeout",
            packet_loss_ratio=1.0,
        ),
    }
    store.update_probe_results(results)
    assert _gauge_value(store, "probe_success", target="host-a", probe="icmp") == 1
    assert _gauge_value(store, "probe_success", target="host-b", probe="icmp") == 0
    assert _gauge_value(store, "probe_latency_seconds", target="host-a", probe="icmp") == 0.012
    # host-b 没有延迟：gauge 不会被写入，但 counter 仍会增加
    assert _counter_value(store, "probe_results_total", result="up") == 1
    assert _counter_value(store, "probe_results_total", result="timeout") == 1


def test_update_host_state_maps_status_to_up_value():
    store = _new_store()
    states = [
        HostState(target=Target(id="a", address="10.0.0.1"), status=HostStatus.UP),
        HostState(target=Target(id="b", address="10.0.0.2"), status=HostStatus.DOWN),
        HostState(target=Target(id="c", address="10.0.0.3"), status=HostStatus.UNKNOWN),
    ]
    store.update_host_state(states)
    assert _gauge_value(store, "host_up", target="a") == 1
    assert _gauge_value(store, "host_up", target="b") == 0
    assert _gauge_value(store, "host_up", target="c") == 0


def test_record_state_change_increments_counter():
    store = _new_store()
    change = StateChange(
        target=Target(id="a", address="10.0.0.1"),
        previous=HostStatus.UP,
        current=HostStatus.DOWN,
        host_state=HostState(target=Target(id="a", address="10.0.0.1")),
    )
    store.record_state_change(change)
    store.record_state_change(change)
    assert _counter_value(store, "host_state_changes_total", target="a", state="down") == 2


def test_probe_round_marks_track_lifecycle():
    store = _new_store()
    store.mark_probe_round_start()
    assert _gauge_value(store, "probe_round_in_progress") == 1
    store.mark_probe_round_complete(duration_seconds=0.42, result="success")
    assert _gauge_value(store, "probe_round_in_progress") == 0
    assert _counter_value(store, "probe_rounds_total", result="success") == 1
    # Histogram 应当记录这次的观测
    assert store.probe_round_duration_seconds._sum.get() == 0.42


def test_record_fping_process_records_exit_code():
    store = _new_store()
    store.record_fping_process("success", 0)
    store.record_fping_process("error", 2)
    assert _gauge_value(store, "fping_last_exit_code") == 2
    assert _counter_value(store, "fping_process_total", result="success") == 1
    assert _counter_value(store, "fping_process_total", result="error") == 1


def test_build_info_exposes_version_labels():
    store = _new_store()
    value = _gauge_value(
        store,
        "build_info",
        version="test",
        python_version=__import__("platform").python_version(),
        fping_version="unknown",
    )
    assert value == 1


def test_set_targets_loaded_updates_gauge():
    store = _new_store()
    store.set_targets_loaded(42)
    assert _gauge_value(store, "targets_loaded") == 42


def test_invalid_round_result_rejected():
    import pytest

    store = _new_store()
    with pytest.raises(ValueError):
        store.mark_probe_round_complete(0.1, result="nope")
