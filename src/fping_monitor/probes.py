"""fping 批量探测与结果解析。

`fping -C N -q` 会对每个目标输出一行，格式形如：

    host : 0.12 0.15 0.10         (全部收到应答)
    host : 0.12 - 0.10            (部分丢包)
    host : - - -                  (全部超时)

fping 退出码：0 表示所有目标都至少收到一个包，1 表示部分目标无应答，
>=2 表示其它错误。我们以文本输出作为每个目标结果的最终依据，
退出码仅用于区分"正常"与"异常"两种情况。
"""

from __future__ import annotations

import logging
import re
import shlex
import subprocess
import time
from dataclasses import dataclass

from .models import ProbeResult, Target


_LOG = logging.getLogger(__name__)

# 单行格式："host : 1.24 0.95 -" 或 "host : - - -"，允许前后空白
_LINE_RE = re.compile(r"^\s*(\S+)\s*:\s*(.*?)\s*$")

# fping 输出中一个 token 的合法字符：数字、点、负号，部分版本带 "ms"
_TOKEN_RE = re.compile(r"^(-?\d+(?:\.\d+)?)\s*(ms)?$")


@dataclass
class ParsedFping:
    """一次 `fping -C N -q` 调用的解析结果。"""

    results: dict[str, ProbeResult]
    timed_out_lines: list[str]  # 在输出中完全没有出现行的目标（如 DNS 错误）


def _parse_token(raw: str) -> float | None:
    """把 fping 输出的一个 token 解析为秒；'-' 或无法解析返回 None。"""
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    m = _TOKEN_RE.match(raw)
    if not m:
        # 保守起见，无法识别的 token 视为无应答
        return None
    value = float(m.group(1))
    unit = m.group(2)
    if unit == "ms":
        return value / 1000.0
    # fping 默认单位是毫秒
    return value / 1000.0


def parse_fping_output(output: str, address_to_id: dict[str, str]) -> ParsedFping:
    """解析 `fping -C N -q` 的标准输出。

    `address_to_id` 把传给 fping 的地址映射到监控目标 id。
    fping 有时会先解析主机名再打印 IP，调用方应当把两种形式都
    传进来以兼容这种情况。
    """
    results: dict[str, ProbeResult] = {}
    seen_addresses: set[str] = set()

    for line in output.splitlines():
        m = _LINE_RE.match(line)
        if not m:
            continue
        token = m.group(1)
        body = m.group(2)
        target_id = address_to_id.get(token)
        if target_id is None:
            # 兜底：再走一遍 mapping 查找（处理 fping 打印解析后 IP 的情况）
            for addr, tid in address_to_id.items():
                if addr == token:
                    target_id = tid
                    break
        if target_id is None:
            continue
        seen_addresses.add(token)
        tokens = [t for t in body.split() if t]
        if not tokens:
            # fping 对某些错误会输出 "host : "，视为探测失败
            results[target_id] = ProbeResult(
                target_id=target_id, success=False, error="timeout"
            )
            continue
        latencies: list[float] = []
        for t in tokens:
            seconds = _parse_token(t)
            if seconds is not None:
                latencies.append(seconds)
        if not latencies:
            results[target_id] = ProbeResult(
                target_id=target_id, success=False, error="timeout"
            )
            continue
        loss = (len(tokens) - len(latencies)) / len(tokens)
        avg = sum(latencies) / len(latencies)
        results[target_id] = ProbeResult(
            target_id=target_id,
            success=True,
            latency_seconds=avg,
            packet_loss_ratio=loss,
        )

    timed_out = [addr for addr in address_to_id.keys() if addr not in seen_addresses]
    return ParsedFping(results=results, timed_out_lines=timed_out)


def build_fping_command(
    binary: str, addresses: list[str], packets: int, timeout_ms: int
) -> list[str]:
    """构造 fping 的 argv 列表。地址合法性由调用方保证。"""
    return [
        binary,
        "-C",
        str(packets),
        "-q",
        "-t",
        str(timeout_ms),
        "-p",
        "200",
        *addresses,
    ]


def _is_address_safe(value: str) -> bool:
    """拒绝任何可能被解释为 CLI 参数或 shell 元字符的字符串。

    fping 内部使用 libfping 相对安全，但我们不希望看到以 '-' 开头
    的目标被当作 flag 解析。
    """
    if not value or value.startswith("-"):
        return False
    for ch in value:
        if ch.isspace():
            return False
        if ch in {";", "&", "|", "$", "`", "(", ")", "<", ">", "\\", "'", '"'}:
            return False
    return True


def probe_targets(
    targets: list[Target],
    binary: str,
    packets: int,
    timeout_ms: int,
    overall_timeout_seconds: float,
) -> dict[str, ProbeResult]:
    """执行一次批量 fping 探测，返回每个目标的 ProbeResult。

    任何目标地址未通过安全检查时，直接返回 process_error 而不调用
    fping。
    """
    if not targets:
        return {}

    for t in targets:
        if not _is_address_safe(t.address):
            return {
                t.id: ProbeResult(
                    target_id=t.id, success=False, error="process_error"
                )
                for t in targets
            }

    cmd = build_fping_command(binary, [t.address for t in targets], packets, timeout_ms)
    address_to_id = {t.address: t.id for t in targets}

    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=overall_timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        _LOG.warning(
            "fping 批量探测超时 (%.1fs, %d 个目标)",
            overall_timeout_seconds,
            len(targets),
        )
        return {
            t.id: ProbeResult(target_id=t.id, success=False, error="timeout")
            for t in targets
        }
    except FileNotFoundError:
        _LOG.error("找不到 fping 可执行文件: %s", binary)
        return {
            t.id: ProbeResult(target_id=t.id, success=False, error="process_error")
            for t in targets
        }
    except OSError as exc:
        _LOG.error("启动 fping 失败: %s", exc)
        return {
            t.id: ProbeResult(target_id=t.id, success=False, error="process_error")
            for t in targets
        }
    elapsed = time.monotonic() - start

    parsed = parse_fping_output(proc.stdout, address_to_id)

    # 任何在 stdout 中没有出现行的目标，都视为探测失败（DNS 错误、
    # 被静默丢弃等）
    results: dict[str, ProbeResult] = {}
    for t in targets:
        if t.id in parsed.results:
            results[t.id] = parsed.results[t.id]
        else:
            results[t.id] = ProbeResult(
                target_id=t.id, success=False, error="timeout"
            )

    if proc.returncode not in (0, 1):
        # 异常退出码：把任何"未成功"的目标标记为 process_error，
        # 但仍保留 fping 已经成功解析出来的结果
        for rid, res in results.items():
            if res.success:
                continue
            res.error = "process_error"
        _LOG.warning(
            "fping 异常退出码=%d (耗时=%.2fs, 结果数=%d)",
            proc.returncode,
            elapsed,
            len(results),
        )

    return results


def format_cmd_for_log(cmd: list[str]) -> str:
    """把命令列表格式化为适合日志的字符串。"""
    return " ".join(shlex.quote(p) for p in cmd)
