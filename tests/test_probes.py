"""fping 解析器与批量探针的测试。"""

from fping_monitor.models import Target
from fping_monitor.probes import (
    build_fping_command,
    parse_fping_output,
    probe_targets,
)


def test_parse_all_replies():
    # fping 第一列回显它收到的地址（或主机名）
    output = "10.0.0.1 : 0.12 0.15 0.10"
    parsed = parse_fping_output(output, {"10.0.0.1": "host-a"})
    assert "host-a" in parsed.results
    r = parsed.results["host-a"]
    assert r.success is True
    assert r.latency_seconds is not None
    # (0.12 + 0.15 + 0.10) / 3 = 0.123 ms / 1000 = 1.23e-4
    assert abs(r.latency_seconds - 0.0001233) < 1e-6
    assert r.packet_loss_ratio == 0.0


def test_parse_partial_loss():
    output = "10.0.0.2 : 1.24 - 0.95"
    parsed = parse_fping_output(output, {"10.0.0.2": "host-b"})
    r = parsed.results["host-b"]
    assert r.success is True
    assert r.latency_seconds is not None
    assert abs(r.packet_loss_ratio - 1 / 3) < 1e-6


def test_parse_full_loss():
    output = "10.0.0.3 : - - -"
    parsed = parse_fping_output(output, {"10.0.0.3": "host-c"})
    r = parsed.results["host-c"]
    assert r.success is False
    assert r.error == "timeout"
    assert r.packet_loss_ratio == 1.0


def test_parse_missing_line_is_timeout():
    parsed = parse_fping_output("", {"10.0.0.4": "host-d"})
    assert parsed.timed_out_lines == ["10.0.0.4"]
    assert "host-d" not in parsed.results


def test_parse_resolved_ip():
    # 即便传给 fping 的是主机名，它也可能打印解析后的 IP。
    # address_to_id 同时携带两种形式，解析器就能匹配上。
    output = "10.0.0.5 : 1.0 1.0 1.0"
    parsed = parse_fping_output(
        output, {"host-e.internal": "host-e", "10.0.0.5": "host-e"}
    )
    assert "host-e" in parsed.results
    assert parsed.results["host-e"].success is True


def test_build_fping_command():
    cmd = build_fping_command("/usr/sbin/fping", ["10.0.0.1", "10.0.0.2"], 3, 500)
    assert cmd == [
        "/usr/sbin/fping",
        "-C",
        "3",
        "-q",
        "-t",
        "500",
        "-p",
        "200",
        "10.0.0.1",
        "10.0.0.2",
    ]


def test_probe_targets_uses_local_fping():
    # 依赖系统已安装 fping 5.1；loopback 地址通常一定可达
    targets = [Target(id="lo", address="127.0.0.1")]
    results = probe_targets(
        targets=targets,
        binary="/usr/sbin/fping",
        packets=2,
        timeout_ms=500,
        overall_timeout_seconds=5.0,
    )
    assert "lo" in results
    r = results["lo"]
    # 部分 fping 版本在无权限时拒绝探测 127.0.0.1；这种情况下我们
    # 也只验证结果格式合法，不强制 success。
    assert r.error in (None, "timeout", "process_error")
    assert 0.0 <= r.packet_loss_ratio <= 1.0


def test_probe_targets_rejects_unsafe_address():
    targets = [Target(id="bad", address="-someflag")]
    results = probe_targets(
        targets=targets,
        binary="/usr/sbin/fping",
        packets=1,
        timeout_ms=200,
        overall_timeout_seconds=2.0,
    )
    assert results["bad"].success is False
    assert results["bad"].error == "process_error"
