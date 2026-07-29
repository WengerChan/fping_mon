"""应用装配（bootstrap）的测试。"""

import asyncio
import socket
from pathlib import Path

from prometheus_client import CollectorRegistry

from fping_monitor.app import _build_ready_check, bootstrap
from fping_monitor.models import ProbeResult


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _write_config(path: Path, body: str) -> Path:
    p = path / "config.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def _ok(t):
    return ProbeResult(
        target_id=t, success=True, latency_seconds=0.001, packet_loss_ratio=0.0
    )


def test_bootstrap_with_targets_creates_components(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        f"""
server:
  listen: 127.0.0.1
  port: {_free_port()}

probe:
  interval_seconds: 1
  timeout_ms: 200
  packets: 1
  batch_size: 10

state:
  down_after_failures: 1
  up_after_successes: 1

notification:
  enabled: false

targets:
  - id: host-a
    address: 127.0.0.1
""",
    )
    app = bootstrap(
        cfg_path,
        registry=CollectorRegistry(),
        outbox_path=str(tmp_path / "state.db"),
    )
    assert app.state_manager.get("host-a") is not None
    assert app.scheduler is not None
    assert app.http_server.port > 0
    app.http_server.stop()


def test_ready_check_false_before_any_round(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        f"""
server:
  listen: 127.0.0.1
  port: {_free_port()}

probe:
  interval_seconds: 1
  timeout_ms: 200
  packets: 1

state:
  down_after_failures: 3
  up_after_successes: 3

notification:
  enabled: false

targets:
  - id: host-a
    address: 127.0.0.1
""",
    )
    app = bootstrap(
        cfg_path,
        registry=CollectorRegistry(),
        outbox_path=str(tmp_path / "state.db"),
    )
    check = _build_ready_check(app.scheduler, app.config.probe.interval_seconds)
    ready, reason = check()
    assert ready is False
    assert "尚未完成" in reason

    app.scheduler.probe_fn = lambda targets, *a, **kw: {t.id: _ok(t.id) for t in targets}
    asyncio.run(app.scheduler.probe_once())
    ready, reason = check()
    assert ready is True, reason
    app.http_server.stop()


def test_bootstrap_with_no_targets(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        f"""
server:
  listen: 127.0.0.1
  port: {_free_port()}

notification:
  enabled: false
""",
    )
    app = bootstrap(
        cfg_path,
        registry=CollectorRegistry(),
        outbox_path=str(tmp_path / "state.db"),
    )
    assert app.state_manager.all_states() == []
    app.http_server.stop()

