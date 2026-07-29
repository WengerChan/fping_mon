"""HTTP 端点：/metrics、/healthz、/readyz。

只使用标准库，避免引入 Web 框架。每个端点都是几行代码，直接基于
``BaseHTTPRequestHandler``。
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
)

from .metrics import MetricsStore


_LOG = logging.getLogger(__name__)


def make_handler(
    registry: CollectorRegistry,
    metrics: MetricsStore,
    ready_check: Callable[[], tuple[bool, str]],
):
    """根据给定的 registry 与就绪检查函数构造请求处理器类。

    `ready_check` 返回 (is_ready, reason)。当 is_ready 为 False 时，
    /readyz 返回 503 并把 reason 写入响应体。
    """

    class Handler(BaseHTTPRequestHandler):
        # 屏蔽默认的 per-request 日志；只记录真正的错误
        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

        def _write(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - http.server 约定
            import json

            path = self.path.split("?", 1)[0]
            if path == "/metrics":
                # generate_latest() 是同步且 CPU-bound（序列化所有 metrics）；
                # 在每个请求里同步执行 = 直接阻塞线程池。
                # 1000 主机 / 17 个 series 的序列化约 5-20ms，可以接受；
                # 如果未来 series 增长到上万级别，可考虑放到 to_thread。
                output = generate_latest(registry)
                self._write(200, output, CONTENT_TYPE_LATEST)
                return
            if path == "/healthz":
                self._write(200, b'{"status":"ok"}\n', "application/json")
                return
            if path == "/readyz":
                # 200 / 503 由 ready_check 闭包决定；
                # 这里返回的 reason 字符串直接给运维看，需保持简短可读。
                ok, reason = ready_check()
                body = json.dumps({"ready": ok, "reason": reason}).encode() + b"\n"
                self._write(200 if ok else 503, body, "application/json")
                return
            self._write(404, b'{"error":"not found"}\n', "application/json")

        def log_error(self, format: str, *args) -> None:  # noqa: A002
            _LOG.warning("http error: " + format, *args)

    return Handler


class MetricsHTTPServer:
    """在后台线程里跑一个小型 HTTP 服务。

    `start` 非阻塞；`stop` 可以重复调用，幂等。
    """

    def __init__(
        self,
        host: str,
        port: int,
        registry: CollectorRegistry,
        metrics: MetricsStore,
        ready_check: Callable[[], tuple[bool, str]],
    ) -> None:
        self._host = host
        self._port = port
        handler = make_handler(registry, metrics, ready_check)
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread: Optional[threading.Thread] = None

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="metrics-http", daemon=True
        )
        self._thread.start()
        _LOG.info("metrics http 服务已启动: %s:%d", self._host, self._port)

    def stop(self) -> None:
        if self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None
        _LOG.info("metrics http 服务已停止")
