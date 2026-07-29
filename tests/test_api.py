"""HTTP 端点测试。"""

import time
from http.client import HTTPConnection

from prometheus_client import CollectorRegistry

from fping_monitor.api import MetricsHTTPServer
from fping_monitor.metrics import MetricsStore


def _free_port() -> int:
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_ready(port: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=0.5)
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            resp.read()
            if resp.status == 200:
                return
        except OSError:
            time.sleep(0.01)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    raise RuntimeError("server did not start")


def _http_get(port: int, path: str) -> tuple[int, bytes, str]:
    conn = HTTPConnection("127.0.0.1", port, timeout=2.0)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, body, resp.getheader("Content-Type", "")
    finally:
        conn.close()


def _make_server(ready: tuple[bool, str]) -> MetricsHTTPServer:
    registry = CollectorRegistry()
    metrics = MetricsStore(registry=registry, version="t", fping_binary="/usr/sbin/fping")
    metrics.set_targets_loaded(7)
    return MetricsHTTPServer(
        host="127.0.0.1",
        port=_free_port(),
        registry=registry,
        metrics=metrics,
        ready_check=lambda: ready,
    )


def test_metrics_endpoint_exposes_targets():
    server = _make_server(ready=(True, "ok"))
    server.start()
    try:
        _wait_ready(server.port)
        status, body, ctype = _http_get(server.port, "/metrics")
        assert status == 200
        assert "text/plain" in ctype
        text = body.decode()
        assert "fping_monitor_targets 7.0" in text
    finally:
        server.stop()


def test_healthz_always_200():
    server = _make_server(ready=(False, "not ready"))
    server.start()
    try:
        _wait_ready(server.port)
        status, body, _ = _http_get(server.port, "/healthz")
        assert status == 200
        assert b'"status":"ok"' in body
    finally:
        server.stop()


def test_readyz_returns_503_when_not_ready():
    server = _make_server(ready=(False, "no targets"))
    server.start()
    try:
        _wait_ready(server.port)
        status, body, _ = _http_get(server.port, "/readyz")
        assert status == 503
        assert b"no targets" in body
    finally:
        server.stop()


def test_unknown_path_returns_404():
    server = _make_server(ready=(True, "ok"))
    server.start()
    try:
        _wait_ready(server.port)
        status, _, _ = _http_get(server.port, "/nope")
        assert status == 404
    finally:
        server.stop()
