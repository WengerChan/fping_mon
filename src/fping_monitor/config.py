"""配置加载与校验。

单个 YAML 文件描述探测循环、状态机阈值、通知目标和存储位置。
解析保持简单：读取文件、构造 dataclass、对关键字段做手工校验，
配置错误直接 fail-fast。
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from pathlib import Path
import logging
from typing import Any, Optional

import yaml

from .models import Target


_LOG = logging.getLogger(__name__)


# 标签键白名单，未来可能作为指标标签使用
_ALLOWED_LABEL_KEYS = {"site", "group", "env", "role"}

# fping 接受主机名、IPv4、IPv6 地址。target id 不能以 "-" 开头
# 避免被 fping 当作命令行参数。
_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$")
_HOST_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class ConfigError(ValueError):
    """配置文件格式错误或字段非法。"""


@dataclass
class ServerConfig:
    listen: str = "0.0.0.0"
    port: int = 9100


@dataclass
class ProbeConfig:
    interval_seconds: int = 10
    timeout_ms: int = 1000
    packets: int = 3
    batch_size: int = 200
    batch_jitter_ms: int = 0
    # apt 包 fping 实际路径；macOS Homebrew 路径是 /usr/local/sbin/fping
    fping_binary: str = "/usr/bin/fping"


@dataclass
class StateConfig:
    down_after_failures: int = 3
    up_after_successes: int = 3
    mass_failure_ratio: float = 0.5


@dataclass
class NotificationConfig:
    enabled: bool = True
    url: str = ""
    timeout_seconds: float = 5.0
    max_attempts: int = 8
    max_backoff_seconds: int = 60
    token_env: str = "ALERT_API_TOKEN"
    monitor_instance: str = "monitor-a"


@dataclass
class StorageConfig:
    path: str = "/var/lib/fping-monitor/state.db"


@dataclass
class CuckooConfig:
    """布谷鸟告警接口配置。"""
    app_key: str = ""
    url: dict[str, str] = field(
        default_factory=lambda: {"receivemap": "", "forward": ""}
    )


@dataclass
class AppConfig:
    server: ServerConfig = field(default_factory=ServerConfig)
    probe: ProbeConfig = field(default_factory=ProbeConfig)
    state: StateConfig = field(default_factory=StateConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    targets: list[Target] = field(default_factory=list)
    cuckoo: CuckooConfig = field(default_factory=CuckooConfig)


def _require(d: dict, key: str, where: str) -> Any:
    if key not in d:
        raise ConfigError(f"缺少必填字段 '{key}'（位于 {where}）")
    return d[key]


def _parse_server(raw: Optional[dict]) -> ServerConfig:
    if raw is None:
        return ServerConfig()
    listen = str(raw.get("listen", "0.0.0.0"))
    port = int(raw.get("port", 9100))
    if not (0 < port < 65536):
        raise ConfigError(f"server.port 超出合法范围: {port}")
    return ServerConfig(listen=listen, port=port)


def _parse_probe(raw: Optional[dict]) -> ProbeConfig:
    if raw is None:
        return ProbeConfig()
    cfg = ProbeConfig(
        interval_seconds=int(raw.get("interval_seconds", 10)),
        timeout_ms=int(raw.get("timeout_ms", 1000)),
        packets=int(raw.get("packets", 3)),
        batch_size=int(raw.get("batch_size", 200)),
        batch_jitter_ms=int(raw.get("batch_jitter_ms", 0)),
        fping_binary=str(raw.get("fping_binary", "/usr/sbin/fping")),
    )
    if cfg.interval_seconds < 1:
        raise ConfigError("probe.interval_seconds 必须 >= 1")
    if cfg.timeout_ms < 1:
        raise ConfigError("probe.timeout_ms 必须 >= 1")
    if cfg.packets < 1:
        raise ConfigError("probe.packets 必须 >= 1")
    if cfg.batch_size < 1:
        raise ConfigError("probe.batch_size 必须 >= 1")
    if cfg.batch_jitter_ms < 0:
        raise ConfigError("probe.batch_jitter_ms 必须 >= 0")
    return cfg


def _parse_state(raw: Optional[dict]) -> StateConfig:
    if raw is None:
        return StateConfig()
    cfg = StateConfig(
        down_after_failures=int(raw.get("down_after_failures", 3)),
        up_after_successes=int(raw.get("up_after_successes", 3)),
        mass_failure_ratio=float(raw.get("mass_failure_ratio", 0.5)),
    )
    if cfg.down_after_failures < 1:
        raise ConfigError("state.down_after_failures 必须 >= 1")
    if cfg.up_after_successes < 1:
        raise ConfigError("state.up_after_successes 必须 >= 1")
    if not (0.0 < cfg.mass_failure_ratio <= 1.0):
        raise ConfigError("state.mass_failure_ratio 必须位于 (0, 1] 之间")
    return cfg


def _parse_notification(raw: Optional[dict]) -> NotificationConfig:
    if raw is None:
        return NotificationConfig()
    cfg = NotificationConfig(
        enabled=bool(raw.get("enabled", True)),
        url=str(raw.get("url", "")),
        timeout_seconds=float(raw.get("timeout_seconds", 5.0)),
        max_attempts=int(raw.get("max_attempts", 8)),
        max_backoff_seconds=int(raw.get("max_backoff_seconds", 60)),
        token_env=str(raw.get("token_env", "ALERT_API_TOKEN")),
        monitor_instance=str(raw.get("monitor_instance", "monitor-a")),
    )
    if cfg.timeout_seconds <= 0:
        raise ConfigError("notification.timeout_seconds 必须 > 0")
    if cfg.max_attempts < 1:
        raise ConfigError("notification.max_attempts 必须 >= 1")
    if cfg.max_backoff_seconds < 1:
        raise ConfigError("notification.max_backoff_seconds 必须 >= 1")
    # URL 合法性：
    # 1. scheme 必须是 http/https（防止误填 file://、javascript: 等）；
    # 2. netloc 必须非空（防止用户填 "https://" 这种缺主机名的"占位 URL"，
    #    运行时 httpx 会构造出 "https:///path" 的畸形请求）。
    if cfg.url:
        from urllib.parse import urlparse

        parsed = urlparse(cfg.url)
        if parsed.scheme not in ("http", "https"):
            raise ConfigError(
                f"notification.url 必须是 http 或 https 协议，当前是 {parsed.scheme!r}"
            )
        if not parsed.netloc:
            raise ConfigError(f"notification.url 缺少主机名：{cfg.url!r}")
    return cfg


def _parse_storage(raw: Optional[dict]) -> StorageConfig:
    if raw is None:
        return StorageConfig()
    return StorageConfig(path=str(raw.get("path", "/var/lib/fping-monitor/state.db")))


def _validate_address(value: str, target_id: str) -> str:
    if not value:
        raise ConfigError(f"target {target_id!r}: address 不能为空")
    # 先尝试按字面 IP 解析（v4 / v6），否则按主机名校验
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        pass
    if not _HOST_RE.match(value) or value.startswith("-"):
        raise ConfigError(
            f"target {target_id!r}: address {value!r} 不是合法的 IP 或主机名"
        )
    return value


def _parse_targets(raw: Any) -> list[Target]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError("'targets' 必须是 list")
    seen: set[str] = set()
    out: list[Target] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ConfigError(f"targets[{index}] 必须是 mapping")
        target_id = str(_require(item, "id", f"targets[{index}]"))
        if not _ID_RE.match(target_id):
            raise ConfigError(
                f"targets[{index}].id {target_id!r} 不匹配正则 {_ID_RE.pattern}"
            )
        if target_id in seen:
            raise ConfigError(f"重复的 target id: {target_id!r}")
        seen.add(target_id)
        address = _validate_address(
            str(_require(item, "address", f"targets[{index}]")), target_id
        )
        labels_raw = item.get("labels", {}) or {}
        if not isinstance(labels_raw, dict):
            raise ConfigError(f"targets[{index}].labels 必须是 mapping")
        labels: dict[str, str] = {}
        for k, v in labels_raw.items():
            if k not in _ALLOWED_LABEL_KEYS:
                raise ConfigError(
                    f"target {target_id!r}: 标签键 {k!r} 不在白名单中"
                )
            labels[k] = str(v)
        out.append(Target(id=target_id, address=address, labels=labels))
    return out

def _parse_cuckoo(raw: Optional[dict]) -> CuckooConfig:
    """解析 cuckoo 段。未配置时退化为默认空配置，便于未启用布谷鸟的场景。"""
    if raw is None:
        return CuckooConfig()
    app_key = str(raw.get("app_key", ""))
    url_raw = raw.get("url")
    if url_raw is None:
        url_raw = {}
    if not isinstance(url_raw, dict):
        raise ConfigError("cuckoo.url 必须是 mapping")
    url: dict[str, str] = {}
    for key in ("receivemap", "forward"):
        value = url_raw.get(key, "")
        if not value:
            _LOG.warning("cuckoo.url.%s 未配置", key)
        url[key] = str(value) if value is not None else ""

    return CuckooConfig(app_key=app_key, url=url)
        

def load_config(path: str | Path) -> AppConfig:
    """读取并校验 YAML 配置文件。"""
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"配置文件不存在: {p}")
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML 解析失败: {exc}") from exc
    if raw is None:
        raise ConfigError("配置文件为空")
    if not isinstance(raw, dict):
        raise ConfigError("配置根节点必须是 mapping")

    return AppConfig(
        server=_parse_server(raw.get("server")),
        probe=_parse_probe(raw.get("probe")),
        state=_parse_state(raw.get("state")),
        notification=_parse_notification(raw.get("notification")),
        storage=_parse_storage(raw.get("storage")),
        targets=_parse_targets(raw.get("targets")),
        cuckoo=_parse_cuckoo(raw.get("cuckoo")),
    )
