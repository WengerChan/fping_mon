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
    """HTTP 服务配置（暴露 Prometheus 指标与就绪探针）。

    Attributes:
        listen: 监听地址；生产环境通常绑 0.0.0.0，sidecar 模式可改 127.0.0.1。
        port: 监听端口；范围 1~65535。
    """
    listen: str = "0.0.0.0"
    port: int = 9100


@dataclass
class ProbeConfig:
    """探测循环参数：控制 fping 调用频率、超时与批大小。

    Attributes:
        interval_seconds: 每轮探测的间隔；太小会打挂目标，太大发现故障慢。
        timeout_ms: 单包超时，传给 fping -t；覆盖 fping 默认 500ms。
        packets: 每轮探测包数；≥3 便于区分偶发抖动与持续故障。
        batch_size: 一次 fping 进程承载的目标数；过大会触发 OS fd/argv 上限。
        batch_jitter_ms: 批次间随机延迟；>0 时把同一 round 内的批次错开，避免 burst。
        fping_binary: fping 可执行文件路径；apt 包实际路径，
            macOS Homebrew 在 /usr/local/sbin/fping。
    """
    interval_seconds: int = 10
    timeout_ms: int = 1000
    packets: int = 3
    batch_size: int = 200
    batch_jitter_ms: int = 0
    fping_binary: str = "/usr/bin/fping"


@dataclass
class StateConfig:
    """状态机阈值：决定一次 host_down/host_recovered 何时被确认。

    Attributes:
        down_after_failures: 连续失败多少轮才判定 DOWN；避免偶发丢包误报。
        up_after_successes: 连续成功多少轮才判定 UP；避免恢复后短暂抖动反复告警。
        mass_failure_ratio: 单轮失败目标占比超过该值视为大面积故障，单独抑制告警风暴。
    """
    down_after_failures: int = 3
    up_after_successes: int = 3
    mass_failure_ratio: float = 0.5


@dataclass
class WebhookConfig:
    """Webhook 通知通道配置。

    Attributes:
        enabled: 总开关；false 时整个 webhook channel 跳过。
        url: 接收端 URL；为空时即使 enabled=true 也不投递。
        max_attempts: transport 内 HTTP 重试上限（抗网络抖动），单次 worker
            tick 内最多发起这么多次请求。
        max_event_attempts: worker tick 上限（抗持续故障），整条事件最多被
            outbox worker 调度这么多次。
        max_backoff_seconds: 单层退避封顶秒数（k8s 风格指数退避 + ±10% 抖动）。
        token_env: 持有 Bearer token 的环境变量名；未设置时不带 Authorization 头。
        monitor_instance: 上报的事件里携带的监控实例标识，方便接收端区分来源。
    """
    enabled: bool = True
    url: str = ""
    max_attempts: int = 3
    max_event_attempts: int = 10
    max_backoff_seconds: int = 60
    token_env: str = "ALERT_API_TOKEN"
    monitor_instance: str = "monitor-a"


@dataclass
class StorageConfig:
    """Outbox 存储位置。

    Attributes:
        path: SQLite 文件路径；进程启动时自动创建父目录。
    """
    path: str = "/var/lib/fping-monitor/state.db"


@dataclass
class CuckooConfig:
    """布谷鸟告警通道配置。

    Attributes:
        enabled: 总开关；false 时整个 cuckoo 跳过。
        app_key: 布谷鸟管理员分配的应用标识；为空时 payload 仍能构造但接收端会拒。
        url: 各 endpoint 的 URL；任一为空则对应 channel 跳过。
        max_attempts: transport 内 HTTP 重试上限（抗网络抖动）。
        max_event_attempts: worker tick 上限（抗持续故障）。
        max_backoff_seconds: 单层退避封顶秒数。
    """
    enabled: bool = False
    app_key: str = ""
    url: dict[str, str] = field(
        default_factory=lambda: {"receivemap": "", "forward": ""}
    )
    max_attempts: int = 3
    max_event_attempts: int = 10
    max_backoff_seconds: int = 60


@dataclass
class AppConfig:
    """完整应用配置：顶层聚合各段。

    Attributes:
        server: HTTP 服务。
        probe: 探测循环。
        state: 状态机阈值。
        webhook: Webhook 通道。
        storage: Outbox 存储。
        targets: 监控目标列表（唯一必填段）。
        cuckoo: 布谷鸟通道。
    """
    server: ServerConfig = field(default_factory=ServerConfig)
    probe: ProbeConfig = field(default_factory=ProbeConfig)
    state: StateConfig = field(default_factory=StateConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    targets: list[Target] = field(default_factory=list)
    cuckoo: CuckooConfig = field(default_factory=CuckooConfig)


def _require(d: dict, key: str, where: str) -> Any:
    """从 mapping 取必填字段；缺失抛 :class:`ConfigError`。"""
    if key not in d:
        raise ConfigError(f"缺少必填字段 '{key}'（位于 {where}）")
    return d[key]


def _parse_server(raw: Optional[dict]) -> ServerConfig:
    """解析 ``server`` 段。``port`` 越界抛 :class:`ConfigError`。"""
    if raw is None:
        return ServerConfig()
    listen = str(raw.get("listen", "0.0.0.0"))
    port = int(raw.get("port", 9100))
    if not (0 < port < 65536):
        raise ConfigError(f"server.port 超出合法范围: {port}")
    return ServerConfig(listen=listen, port=port)


def _parse_probe(raw: Optional[dict]) -> ProbeConfig:
    """解析 ``probe`` 段；阈值不满足下界时抛 :class:`ConfigError`。"""
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
    """解析 ``state`` 段；阈值越界时抛 :class:`ConfigError`。"""
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


def _parse_webhook(raw: Optional[dict]) -> WebhookConfig:
    """解析 ``webhook`` 段，并对 ``url`` 做 scheme/netloc 校验。"""
    if raw is None:
        return WebhookConfig()
    cfg = WebhookConfig(
        enabled=bool(raw.get("enabled", True)),
        url=str(raw.get("url", "")),
        max_attempts=int(raw.get("max_attempts", 3)),
        max_event_attempts=int(raw.get("max_event_attempts", 10)),
        max_backoff_seconds=int(raw.get("max_backoff_seconds", 60)),
        token_env=str(raw.get("token_env", "ALERT_API_TOKEN")),
        monitor_instance=str(raw.get("monitor_instance", "monitor-a")),
    )
    if cfg.max_attempts < 1:
        raise ConfigError("webhook.max_attempts 必须 >= 1")
    if cfg.max_event_attempts < 1:
        raise ConfigError("webhook.max_event_attempts 必须 >= 1")
    if cfg.max_backoff_seconds < 1:
        raise ConfigError("webhook.max_backoff_seconds 必须 >= 1")
    # URL 合法性：
    # 1. scheme 必须是 http/https（防止误填 file://、javascript: 等）；
    # 2. netloc 必须非空（防止用户填 "https://" 这种缺主机名的"占位 URL"，
    #    运行时 httpx 会构造出 "https:///path" 的畸形请求）。
    if cfg.url:
        from urllib.parse import urlparse

        parsed = urlparse(cfg.url)
        if parsed.scheme not in ("http", "https"):
            raise ConfigError(
                f"webhook.url 必须是 http 或 https 协议，当前是 {parsed.scheme!r}"
            )
        if not parsed.netloc:
            raise ConfigError(f"webhook.url 缺少主机名：{cfg.url!r}")
    return cfg


def _parse_storage(raw: Optional[dict]) -> StorageConfig:
    """解析 ``storage`` 段。"""
    if raw is None:
        return StorageConfig()
    return StorageConfig(path=str(raw.get("path", "/var/lib/fping-monitor/state.db")))


def _validate_address(value: str, target_id: str) -> str:
    """校验 target.address 是否合法（IP 或主机名）。"""
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
    """解析 ``targets`` 段，校验 id/address/labels。"""
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
    """解析 ``cuckoo`` 段；未配置时返回默认（enabled=False）。"""
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

    cfg = CuckooConfig(
        enabled=bool(raw.get("enabled", False)),
        app_key=app_key,
        url=url,
        max_attempts=int(raw.get("max_attempts", 3)),
        max_event_attempts=int(raw.get("max_event_attempts", 10)),
        max_backoff_seconds=int(raw.get("max_backoff_seconds", 60)),
    )
    if cfg.max_attempts < 1:
        raise ConfigError("cuckoo.max_attempts 必须 >= 1")
    if cfg.max_event_attempts < 1:
        raise ConfigError("cuckoo.max_event_attempts 必须 >= 1")
    if cfg.max_backoff_seconds < 1:
        raise ConfigError("cuckoo.max_backoff_seconds 必须 >= 1")
    return cfg


def load_config(path: str | Path) -> AppConfig:
    """读取并校验 YAML 配置文件，返回完整 :class:`AppConfig`；解析失败抛 :class:`ConfigError`。"""
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
        webhook=_parse_webhook(raw.get("webhook")),
        storage=_parse_storage(raw.get("storage")),
        targets=_parse_targets(raw.get("targets")),
        cuckoo=_parse_cuckoo(raw.get("cuckoo")),
    )
