"""命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from pathlib import Path

from .app import bootstrap
from .logging import configure_logging


_LOG = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="fping-monitor",
        description="基于 fping 的主机在线监控程序",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="YAML 配置文件路径",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="日志级别（默认读取 FPING_MONITOR_LOG_LEVEL 环境变量，缺省 INFO）",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    configure_logging(args.log_level)

    app = bootstrap(args.config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 提醒：默认 monitor_instance 在多实例部署下需要唯一
    if app.config.webhook.monitor_instance == "monitor-a":
        _LOG.warning(
            "webhook.monitor_instance 仍是示例默认值 'monitor-a'，"
            "如果部署多个监控节点，请改成唯一标识（如 hostname）"
        )

    def _handle_signal(signum, frame):  # noqa: ANN001 - signal handler 签名
        if signum == signal.SIGHUP:
            _LOG.info("收到 SIGHUP，重新加载配置")
            loop.create_task(app.reload_config())
            return
        _LOG.info("收到信号 %d，准备关闭", signum)
        app.scheduler.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_signal)
    try:
        signal.signal(signal.SIGHUP, _handle_signal)
    except (AttributeError, ValueError):
        # 部分平台（如 Windows）没有 SIGHUP
        pass

    try:
        loop.run_until_complete(app.run())
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
