"""mc_diag 单元测试。日志行按源码里的格式字符串拼出来。

    python3 -m unittest test_mc_diag -v

规则锚点自检（日志文本跟上游源码对不上时会失败）需要两个仓库的本地 checkout：

    MOONCAKE_SRC=~/Mooncake VLLM_SRC=~/vllm python3 -m unittest test_mc_diag.SelfCheck -v
"""

import json
import os
import gzip
import subprocess
from unittest.mock import patch
import stat
import sys
import zipfile
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mc_diag as m  # noqa: E402


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def glog(level, msg, file="transfer_engine_py.cpp", line=914, year=True, prefix=""):
    date = "20261015" if year else "1015"
    return f"{prefix}{level}{date} 12:00:01.123456 140213 {file}:{line}] {msg}"


def vlog(
    level,
    msg,
    file="mooncake_connector.py",
    line=2033,
    proc="EngineCore_DP0",
    color=False,
):
    lv = f"\x1b[31m{level}\x1b[0m" if color else level
    return f"({proc} pid=4242) {lv} 10-15 12:00:02 [{file}:{line}] {msg}"


# P 端：一次失败重试后恢复，然后另一批彻底失败
P_LOG = [
    vlog("INFO", "Initializing Mooncake Transfer Engine worker abc", line=1219),
    vlog(
        "INFO", "The Mooncake Transfer Engine is using rdma as its protocol.", line=1247
    ),
    "(EngineCore_DP0 pid=4242) "
    + glog(
        "I",
        "Transfer Engine 10.1.2.3:15001 started successfully",
        file="transfer_engine_impl.cpp",
        line=575,
    ),
    glog(
        "W", "Slice 0x55d0 failed: retry count exceeded", file="workers.cpp", line=796
    ),
    glog(
        "E",
        "batchTransferSync: transfer FAILED for 10.1.2.4:16001 (batch of 8 requests, 4194304 bytes) on retry 0/3",
    ),
    glog(
        "E",
        "batchTransferSync: transfer FAILED for 10.1.2.4:16001 (batch of 8 requests, 4194304 bytes) on retry 1/3",
    ),
    glog(
        "E",
        "batchTransferSync: transfer FAILED for 10.1.2.4:16001 (batch of 8 requests, 4194304 bytes) on retry 2/3",
    ),
    glog(
        "E",
        "batchTransferSync: all 3 retries exhausted for 10.1.2.4:16001 (batch of 8 requests, 4194304 bytes)",
        line=940,
    ),
    vlog(
        "WARNING",
        "Sending to 10.1.2.4:16001 failed (ret=-1) after 2.5 (8 descriptors, 4194304 bytes)",
    ),
    glog("E", "some unrelated error from tent", file="segment_manager.cpp", line=129),
    glog("E", "some unrelated error from tent", file="segment_manager.cpp", line=129),
    vlog(
        "INFO",
        "Received request cmpl-1: params: SamplingParams(n=1), lora_request: None.",
        file="request_logger.py",
        line=66,
        proc="APIServer",
    ),
    vlog(
        "INFO",
        "Generated response cmpl-1: output: 'the secret plan is', finish_reason: stop",
        file="request_logger.py",
        line=119,
        proc="APIServer",
    ),
    "kv_transfer_config=KVTransferConfig(kv_connector='MooncakeConnector', kv_role='kv_producer')",
]

D_LOG = [
    vlog(
        "ERROR",
        "pulling kv_caches for ['req-1'] failed: Mooncake transfer engine returned -1",
        line=2504,
        color=True,
    ),
    "Traceback (most recent call last):",
    "RuntimeError: something exploded on gpu-node-07",
    '{"kv_role": "kv_consumer"}',
]


def write(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


class ParseTest(unittest.TestCase):
    def test_glog_with_and_without_year_and_prefix(self):
        for year in (True, False):
            p = m.parse_line(
                glog("E", "hello", year=year, prefix="(EngineCore_DP0 pid=1) ")
            )
            self.assertEqual(
                (p.source, p.level, p.file, p.line, p.msg),
                ("glog", "ERROR", "transfer_engine_py.cpp", 914, "hello"),
            )

    def test_vllm_plain_color_and_relpath(self):
        p = m.parse_line(m.strip_ansi(vlog("ERROR", "boom", color=True)))
        self.assertEqual(
            (p.source, p.level, p.file, p.line, p.msg),
            ("vllm", "ERROR", "mooncake_connector.py", 2033, "boom"),
        )
        p = m.parse_line(
            "WARNING 10-15 12:00:00 [vllm/distributed/x/mooncake_connector.py:7] hi"
        )
        self.assertEqual((p.file, p.line), ("mooncake_connector.py", 7))

    def test_json(self):
        p = m.parse_line(json.dumps({"levelname": "ERROR", "message": "x"}))
        self.assertEqual((p.source, p.level, p.msg), ("json", "ERROR", "x"))


class RedactorTest(unittest.TestCase):
    def test_multiline_requests_and_quoted_secrets(self):
        r = m.Redactor()
        lines = [
            vlog("DEBUG", "Request req-1 JSON body: {", file="request_logger.py"),
            '  "prompt": "private continuation",',
            vlog(
                "DEBUG",
                '  "messages": "private repeated header"',
                file="request_logger.py",
            ),
            vlog("ERROR", "Sending to 10.1.2.3:1 failed (ret=-1)"),
        ]
        out = "\n".join(r.log_line(s) for s in lines)
        self.assertNotIn("private", out)
        self.assertIn("Sending to ip-1:1 failed (ret=-1)", out)
        self.assertIn("12:00:02", out)
        for s in (
            'password: "private words here"',
            "password: 'private words here'",
            '{"api_key": "private words here"}',
            'password: "private\\" words"',
            'password: "private unfinished',
            "Authorization: bearer private-short",
        ):
            self.assertNotIn("private", r.text(s))

    def test_equivalent_ipv6_spelling(self):
        r = m.Redactor()
        self.assertEqual(r.text("2001:db8::7."), "ip6-1.")
        self.assertEqual(r.text("2001:0db8:0000:0000:0000:0000:0000:0007"), "ip6-1")
        self.assertNotIn("192.0.2.1", r.text("[::ffff:192.0.2.1]:80"))
        self.assertNotIn("fe80", r.text("fe80::1%eth0"))

    def test_consistent_ip_and_port_kept(self):
        r = m.Redactor()
        a = r.text("connect 10.1.2.3:15001 failed")
        b = r.text("peer=10.1.2.3, other=10.9.9.9.")
        self.assertEqual(a, "connect ip-1:15001 failed")
        self.assertEqual(b, "peer=ip-1, other=ip-2.")
        self.assertEqual(r.mapping(), {"ip-1": "10.1.2.3", "ip-2": "10.9.9.9"})

    def test_loopback_timestamps_and_cpp_scopes_untouched(self):
        r = m.Redactor()
        s = "127.0.0.1 0.0.0.0 :: ::1 12:00:01.123456 mooncake::tent::Config std::string 0.11.0"
        self.assertEqual(r.text(s), s)

    def test_ipv6_gid_mac(self):
        r = m.Redactor()
        out = r.text(
            "gid fe80:0000:0000:0000:0a00:01ff:fe02:0304 raw "
            "00:00:00:00:00:00:00:00:00:00:ff:ff:0a:01:02:03 mac 0c:42:a1:00:11:22 v6 2001:db8::7"
        )
        self.assertNotIn("fe80", out)
        self.assertNotIn("0a:01:02:03", out)
        self.assertNotIn("0c:42:a1", out)
        self.assertNotIn("2001:db8", out)
        self.assertIn("gid-1", out)
        self.assertIn("mac-1", out)

    def test_hosts_users_and_paths(self):
        r = m.Redactor(hosts=["gpu-node-07.corp.example"], users=["liqian", "root"])
        out = r.text(
            "gpu-node-07 gpu-node-07.corp.example /home/liqian/models liqian root gpu-node-070"
        )
        self.assertEqual(
            out, "host-1 host-1 /home/user-1/models user-1 root gpu-node-070"
        )
        self.assertEqual(
            r.text("/home/bob/x /home/bob/y"), "/home/user-2/x /home/user-2/y"
        )

    def test_credentials(self):
        r = m.Redactor()
        out = r.text(
            "redis://admin:pw@10.0.0.5:6379 api_key=abc123 password: 'x y' hf_abcdefghijklmnopqrstu "
            "Authorization: Bearer abcdef123456"
        )
        for secret in ("admin:pw", "abc123", "'x", "hf_abcdef", "abcdef123456"):
            self.assertNotIn(secret, out)
        self.assertIn("ip-1:6379", out)

    def test_ordinary_token_words_kept(self):
        r = m.Redactor()
        s = "max_num_batched_tokens=8192 prompt_tokens: 12 token budget"
        self.assertEqual(r.text(s), s)

    def test_request_content(self):
        line = vlog(
            "INFO",
            "Generated response cmpl-1 (streaming complete): output: 'secret', finish_reason: stop",
        )
        self.assertNotIn("secret", m.Redactor().text(line))
        self.assertIn("secret", m.Redactor(keep_request_content=True).text(line))
        line = vlog("INFO", 'Request cmpl-2 JSON body: {"prompt": "hi"}')
        self.assertIn(m.REQUEST_CONTENT_MARK, m.Redactor().text(line))


class TriageTest(unittest.TestCase):
    def test_legacy_decode_failure_rules_keep_original_lines_and_version(self):
        cases = [
            (
                "vllm.legacy_transfer_response_error",
                1843,
                "Error happens during transferring kvcache for {'cmpl-test'}: test failure",
            ),
            (
                "vllm.legacy_engine_id_not_found",
                1959,
                "Failed to find remote engine_id test-engine from bootstrap server test-bootstrap",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "d.log")
            write(
                path, [vlog("ERROR", message, line=line) for _, line, message in cases]
            )
            scan = m.scan_file(path, "decode", 0)
            for lineno, (rule_id, source_line, _) in enumerate(cases, 1):
                self.assertEqual(scan.rule_counts[rule_id], 1)
                self.assertEqual(scan.first_hits[rule_id].lineno, lineno)
                rule = m.RULE_BY_ID[rule_id]
                self.assertIn("fd5d3aea", rule.note)
                self.assertEqual(m.rule_source(rule)["revision"], "fd5d3aea")
                self.assertEqual(m.rule_source(rule)["lines"], [source_line])
                self.assertEqual(rule.kind, "error")

    def test_real_legacy_decode_metadata_failure_has_its_own_source(self):
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "d.log")
            write(
                path,
                [
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-test'}: Resource temporarily unavailable",
                        line=1855,
                    )
                ],
            )
            scan = m.scan_file(path, "decode", 0)
            self.assertEqual(scan.rule_counts["vllm.d_metadata_transfer_failed"], 1)
            source = m.rule_source(
                next(r for r in m.RULES if r.id == "vllm.d_metadata_transfer_failed")
            )
            self.assertEqual(source["revision"], "fd5d3aea")
            self.assertEqual(source["lines"], [1855])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.p = os.path.join(self.tmp.name, "p.log")
        self.d = os.path.join(self.tmp.name, "d.log")
        write(self.p, P_LOG)
        write(self.d, D_LOG)

    def tearDown(self):
        self.tmp.cleanup()

    def test_rule_counts_and_first_match_wins(self):
        fs = m.scan_file(self.p, "prefill", 0)
        self.assertEqual(fs.rule_counts["mc.transfer_failed_retry"], 3)
        self.assertEqual(fs.rule_counts["mc.retries_exhausted"], 1)
        self.assertEqual(fs.rule_counts["vllm.p_send_failed"], 1)
        self.assertEqual(fs.rule_counts["tent.started"], 1)
        self.assertEqual(fs.captures["ret"]["-1"], 1)
        self.assertEqual(fs.captures["protocol"]["rdma"], 1)
        d = m.scan_file(self.d, "decode", 0)
        self.assertEqual(d.rule_counts["vllm.d_pull_failed_engine"], 1)
        self.assertNotIn("vllm.d_pull_failed", d.rule_counts)

    def test_unmatched_errors_grouped(self):
        fs = m.scan_file(self.p, "prefill", 0)
        self.assertEqual(fs.other_errors["C++ ERROR segment_manager.cpp:129"][0], 2)
        d = m.scan_file(self.d, "decode", 0)
        self.assertTrue(any(k.startswith("异常 RuntimeError") for k in d.other_errors))

    def test_role_inference(self):
        self.assertEqual(m.scan_file(self.p, "unknown", 0).role, "prefill")
        self.assertEqual(m.scan_file(self.d, "unknown", 0).role, "decode")

    def test_findings(self):
        scans = [m.scan_file(self.p, "prefill", 0), m.scan_file(self.d, "decode", 0)]
        findings, checks = m.build_findings(scans, [])
        text = "\n".join(findings)
        self.assertIn("所有重试都失败", text)
        self.assertIn("日志显示运行的是 TENT", text)
        self.assertIn("ret=-1", text)
        self.assertIn("D 端拉取失败，原因是 P 端传输引擎返回失败", text)
        self.assertNotIn("后续重试中完成", text)  # 有 exhausted，不能说恢复
        self.assertIn("之前，同一文件里最早的报错是 数据传输：TENT slice", text)
        self.assertTrue(
            any("没有 Mooncake C++ 层" in c for c in checks)
        )  # D 日志只有 vLLM 行

    def test_retry_outcome_is_unknown_without_completion(self):
        lines = [
            glog(
                "I",
                "Transfer Engine x started successfully",
                file="transfer_engine_impl.cpp",
                line=575,
            ),
            glog(
                "E",
                "batchTransferSync: transfer FAILED for 10.1.2.4:16001 (batch of 1 requests, 8 bytes) "
                "on retry 0/3",
            ),
        ]
        write(self.p, lines)
        findings, _ = m.build_findings([m.scan_file(self.p, "prefill", 0)], [])
        self.assertTrue(any("结果未知" in f for f in findings))
        self.assertFalse(any("后续重试中完成" in f for f in findings))
        self.assertTrue(any("未见上层最终失败" in f for f in findings))

    def test_missing_p_log_and_info_level(self):
        write(
            self.p,
            [
                glog(
                    "E",
                    "batchTransferSync: submitTransfer failed for x (batch of 1 requests, 8 bytes): e",
                    line=880,
                )
            ],
        )
        _, checks = m.build_findings(
            [m.scan_file(self.p, "prefill", 0), m.scan_file(self.d, "decode", 0)], []
        )
        self.assertTrue(any("没有 INFO 级" in c for c in checks))
        _, checks = m.build_findings([m.scan_file(self.d, "decode", 0)], [])
        self.assertTrue(any("没有 P 端日志" in c for c in checks))

    def test_classic_engine_evidence(self):
        write(
            self.p,
            [
                glog(
                    "E",
                    "Worker: Process failed for slice (opcode: 1, source_addr: 0x1, length: 8, "
                    "dest_addr: 0x2, local_nic: mlx5_0, peer_nic: 10.1.2.4:16001@mlx5_1, dest_rkey: 7, "
                    "retry_cnt: 0, max_retry_cnt: 8): transport retry counter exceeded",
                    file="worker_pool.cpp",
                    line=924,
                )
            ],
        )
        findings, _ = m.build_findings([m.scan_file(self.p, "prefill", 0)], [])
        text = "\n".join(findings)
        self.assertIn("日志显示运行的是经典 TE", text)
        self.assertIn("transport retry counter exceeded ×1", text)

    def test_snapshot_env_vs_build_mismatch(self):
        snap = {
            "role": "prefill",
            "host": {"hostname": "n1"},
            "effective": {"use_tent": True},
            "mooncake_build": {"verdict": "classic", "basis": "已安装的"},
            "notes": [],
        }
        _, checks = m.build_findings([], [snap])
        self.assertTrue(any("未检测到 TENT 兼容层标记" in c for c in checks))

    def test_truncation(self):
        big = os.path.join(self.tmp.name, "big.log")
        with open(big, "w") as f:
            for i in range(60000):
                f.write(glog("I", f"line {i} " + "x" * 80) + "\n")
            f.write(
                glog(
                    "E",
                    "batchTransferSync: all 3 retries exhausted for a (batch of 1 requests, 8 bytes)",
                )
                + "\n"
            )
        fs = m.scan_file(big, "prefill", 1 << 20)
        self.assertGreater(fs.skipped_bytes, 0)
        self.assertEqual(fs.rule_counts["mc.retries_exhausted"], 1)
        self.assertEqual(fs.first_hits["mc.retries_exhausted"].lineno, 60001)
        with open(big, "rb") as src, gzip.open(big + ".gz", "wb") as dst:
            dst.write(src.read())
        zipped = m.scan_file(big + ".gz", "prefill", 1 << 20)
        self.assertEqual(zipped.skipped_bytes, fs.skipped_bytes)
        self.assertEqual(zipped.first_hits["mc.retries_exhausted"].lineno, 60001)

    def test_mixed_role_and_signature_in_request_are_not_evidence(self):
        write(
            self.p,
            [
                "kv_role='kv_producer'",
                "kv_role='kv_consumer'",
                vlog(
                    "DEBUG", "Request r details: prompt: 'Sending to x failed (ret=-1)'"
                ),
                vlog("ERROR", "Sending to x failed (ret=-1)", file="unrelated.py"),
            ],
        )
        fs = m.scan_file(self.p, "unknown", 0)
        self.assertEqual(fs.role, "unknown")
        self.assertNotIn("vllm.p_send_failed", fs.rule_counts)
        _, checks = m.build_findings([fs], [])
        self.assertTrue(any("材料不足" in s for s in checks))
        write(self.p, [vlog("DEBUG", 'Request r JSON body: {"kv_role":"kv_producer"}')])
        self.assertEqual(m.scan_file(self.p, "unknown", 0).role, "unknown")

    def test_revision_selects_evidence_lines(self):
        write(self.p, [glog("E", "batchTransferSync: openSegment failed for x")])
        fs = m.scan_file(self.p, "prefill", 0, mooncake_revision="342c8aee")
        result = m.triage_json([fs], [])
        src = result["files"][0]["rule_sources"]["mc.open_segment_failed"]
        self.assertEqual(src["revision"], "342c8aee")
        self.assertTrue(src["lines"])
        self.assertIn("#L", src["url"])

    def test_legacy_handshake_and_release_specific_rules(self):
        write(
            self.p,
            [
                glog(
                    "E",
                    "SocketHandShakePlugin: connect()127.0.0.1:44365: Connection refused [111]",
                    file="transfer_metadata_plugin.cpp",
                    line=1030,
                ),
                glog("E", "batchTransferSync: all 3 retries exhausted for x"),
            ],
        )
        snapshots = [
            {"role": "prefill", "packages": {"mooncake-transfer-engine": "0.3.0"}},
            {
                "role": "decode",
                "packages": {"mooncake-transfer-engine": "0.3.13.post1"},
            },
        ]
        revision, basis = m.log_profile("prefill", snapshots, None)
        self.assertEqual(revision, "7b64c074")
        scan = m.scan_file(
            self.p, "prefill", 0, mooncake_revision=revision, revision_basis=basis
        )
        self.assertEqual(scan.rule_counts["te.handshake_connect_failed"], 1)
        self.assertNotIn("mc.retries_exhausted", scan.rule_counts)
        self.assertIn("[111]", scan.first_hits["te.handshake_connect_failed"].text)
        self.assertIn(
            "release tag",
            m.triage_json([scan], snapshots)["files"][0]["version_validation"],
        )
        self.assertEqual(m.log_profile("decode", snapshots, None)[0], "71973589")
        self.assertEqual(
            m.log_profile("prefill", snapshots, "1d97a1d"),
            ("1d97a1d", "explicit revision"),
        )

    def test_unknown_or_conflicting_package_versions_are_not_guessed(self):
        snapshots = [
            {
                "role": "prefill",
                "packages": {"mooncake-transfer-engine": "0.3.99+vendor"},
            }
        ]
        self.assertIsNone(m.log_profile("prefill", snapshots, None)[0])
        snapshots = [
            {"role": "prefill", "packages": {"mooncake-transfer-engine": v}}
            for v in ("0.3.0", "0.3.11")
        ]
        self.assertIsNone(m.log_profile("prefill", snapshots, None)[0])

    def test_real_loader_error_is_explicitly_unclassified(self):
        write(
            self.p,
            [
                "ImportError: /home/operator/mooncake/engine.so: undefined symbol: cudaMemcpyBatchAsync, version libcudart.so.12"
            ],
        )
        scan = m.scan_file(self.p, "prefill", 0)
        findings, _ = m.build_findings([scan], [])
        self.assertTrue(any("未分类错误" in item for item in findings))
        self.assertFalse(scan.rule_counts)
        self.assertNotIn(
            "没有匹配到已知的失败特征", m.render_summary([scan], [], False)
        )


class SnapshotTest(unittest.TestCase):
    def test_probe_preserves_active_metadata_before_base_pth(self):
        with tempfile.TemporaryDirectory() as temp:
            active, base = os.path.join(temp, "active"), os.path.join(temp, "base")
            for directory, versions in (
                (active, {"torch": "2.14.0", "vllm": "0.32.1"}),
                (base, {"torch": "2.13.0", "vllm": "0.28.1"}),
            ):
                for name, version in versions.items():
                    metadata = os.path.join(directory, f"{name}-{version}.dist-info")
                    os.makedirs(metadata)
                    with open(os.path.join(metadata, "METADATA"), "w") as stream:
                        stream.write(
                            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
                        )
            torch = os.path.join(active, "torch")
            os.makedirs(torch)
            with open(os.path.join(torch, "__init__.py"), "w") as stream:
                stream.write(
                    "raise AssertionError('collector must not import torch')\n"
                )
            with open(os.path.join(torch, "version.py"), "w") as stream:
                stream.write("cuda = '13.0'\n")
            script = f"import sys; sys.path[:0] = {[active, base]!r}\n" + m.PROBE_SCRIPT
            probe = json.loads(
                subprocess.check_output([sys.executable, "-S", "-c", script], text=True)
            )
            self.assertEqual(probe["packages"], {"torch": "2.14.0", "vllm": "0.32.1"})
            self.assertEqual(probe["torch_cuda"], "13.0")

    def test_cpp_switch_values_and_alias_or(self):
        cases = [
            ({}, False),
            ({"MC_USE_TENT": ""}, True),
            ({"MC_USE_TENT": "OFF"}, True),
            ({"MC_USE_TENT": "0", "MC_USE_TEV1": "1"}, True),
            ({"MC_USE_TENT": "no", "MC_USE_TEV1": "false"}, False),
        ]
        cases += [
            ({"MC_USE_TENT": value}, False) for value in ("0", "false", "off", "no")
        ]
        for env, expected in cases:
            with self.subTest(env=env), patch.dict(
                os.environ, env, clear=True
            ), patch.object(m, "_run", return_value=(None, None)):
                self.assertEqual(
                    m.take_snapshot("prefill")["effective"]["use_tent"], expected
                )

    def test_missing_process_environment_is_not_collector_environment(self):
        with patch("builtins.open", side_effect=PermissionError("denied")):
            env, source = m._environ(12345)
        self.assertEqual(env, {})
        self.assertTrue(source.startswith("unavailable:"))

    def test_unreadable_or_invalid_build_is_unknown(self):
        with tempfile.TemporaryDirectory() as t:
            path = os.path.join(t, "empty.so")
            write(path, ["not an ELF file"])
            self.assertEqual(m.detect_tent_build([path])["verdict"], "unknown")
            self.assertEqual(
                m.detect_tent_build([path + ".missing"])["verdict"], "unknown"
            )

    def test_private_file_is_private_at_creation_and_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as t:
            path = os.path.join(t, "raw.json")
            with m._private_file(path) as f:
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
                f.write("private")
            with self.assertRaises(FileExistsError):
                m._private_file(path)
            self.assertEqual(read(path), "private")

    def test_tent_build_detection(self):
        with tempfile.TemporaryDirectory() as t:

            def so(name, *blobs):
                p = os.path.join(t, name)
                with open(p, "wb") as f:
                    f.write(
                        b"\x7fELF"
                        + b"\0" * 60
                        + b"N8mooncake4tent14TransferEngineE"
                        + b"".join(blobs)
                        + b"\0" * 64
                    )
                return p

            classic = so("engine.classic.so")
            tent = so("engine.tent.so", m.TENT_SHIM_MARKER, m.TENT_RUNTIME_MARKER)
            legacy_tent = so(
                "engine.legacy-tent.so",
                m.TENT_LEGACY_SHIM_MARKER,
                m.TENT_RUNTIME_MARKER,
            )
            runtime_only = so("libtent.so", m.TENT_RUNTIME_MARKER)
            self.assertEqual(m.detect_tent_build([classic])["verdict"], "classic")
            self.assertEqual(m.detect_tent_build([tent])["verdict"], "tent")
            self.assertEqual(m.detect_tent_build([legacy_tent])["verdict"], "tent")
            self.assertEqual(
                m.detect_tent_build([classic, runtime_only])["verdict"], "partial"
            )
            self.assertEqual(m.detect_tent_build([])["verdict"], "unknown")

    def test_tent_conf_structure_and_secrets(self):
        info = m._tent_conf(
            '{"metadata_type": "redis", "metadata_servers": "redis://u:p@10.0.0.5:6379", '
            '"redis": {"password": "hunter2"}, "transports": {"rdma": {"enable": true}}}'
        )
        self.assertEqual(info["content"]["redis"]["password"], "[VALUE REMOVED]")
        self.assertEqual(
            info["content"]["transports"]["rdma"]["enable"], "[VALUE REMOVED]"
        )
        self.assertNotIn("redis://", json.dumps(info))
        self.assertIn("parse_error", m._tent_conf("{not json"))

    def test_snapshot_runs_here(self):
        old = {
            k: os.environ.get(k)
            for k in ("MC_USE_TENT", "VLLM_API_KEY", "MC_LOG_LEVEL")
        }
        os.environ.update(
            {
                "MC_USE_TENT": "off",
                "VLLM_API_KEY": "sk-should-not-appear",
                "MC_LOG_LEVEL": "ERROR",
            }
        )
        try:
            snap = m.take_snapshot("prefill")
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.assertFalse(snap["effective"]["use_tent"])  # "off" 在 C++ 里视为关闭
        self.assertEqual(snap["effective"]["use_tent_value_off"], "off")
        self.assertEqual(snap["env"]["vars"]["VLLM_API_KEY"], "[REDACTED]")
        _, checks = m.build_findings([], [snap])
        self.assertTrue(any("只看变量是否存在" in c for c in checks))
        self.assertTrue(any("MC_LOG_LEVEL=ERROR" in c for c in checks))
        json.dumps(snap)

    def test_snapshot_own_pid(self):
        snap = m.take_snapshot("decode", pid=os.getpid())
        self.assertEqual(snap["process"]["pid"], os.getpid())
        self.assertTrue(any("没有加载 mooncake" in n for n in snap["notes"]))

    def test_relative_venv_process_preserves_distribution_version(self):
        with tempfile.TemporaryDirectory() as temp:
            venv = os.path.join(temp, "venv")
            subprocess.run(
                [sys.executable, "-m", "venv", "--without-pip", venv], check=True
            )
            site = os.path.join(
                venv,
                "lib",
                f"python{sys.version_info.major}.{sys.version_info.minor}",
                "site-packages",
                "mooncake_transfer_engine-0.3.13.post1.dist-info",
            )
            os.makedirs(site)
            with open(os.path.join(site, "METADATA"), "w") as stream:
                stream.write("Name: mooncake-transfer-engine\nVersion: 0.3.13.post1\n")
            env = dict(os.environ)
            for key in ("VIRTUAL_ENV", "PYTHONHOME", "PYTHONPATH"):
                env.pop(key, None)
            process = subprocess.Popen(
                [
                    "venv/bin/python",
                    "-u",
                    "-c",
                    "import time; print('ready'); time.sleep(60)",
                ],
                cwd=temp,
                env=env,
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(process.stdout.readline().strip(), "ready")
                snapshot = m.take_snapshot("prefill", process.pid)
                self.assertEqual(
                    snapshot["python"]["interpreter"], os.path.join(venv, "bin/python")
                )
                self.assertEqual(
                    snapshot["packages"]["mooncake-transfer-engine"], "0.3.13.post1"
                )
                self.assertEqual(
                    m.log_profile("prefill", [snapshot], None)[0], "71973589"
                )
            finally:
                process.terminate()
                process.communicate(timeout=10)


class BundleTest(unittest.TestCase):
    def test_default_zip_and_issue_ai_guidance(self):
        with tempfile.TemporaryDirectory() as temp:
            log = os.path.join(temp, "p.log")
            write(log, P_LOG)
            result = subprocess.run(
                [
                    sys.executable,
                    m.__file__,
                    "bundle",
                    "--no-local-snapshot",
                    "--prefill-log",
                    log,
                ],
                cwd=temp,
                capture_output=True,
                text=True,
                check=True,
            )
            archives = [n for n in os.listdir(temp) if n.endswith(".zip")]
            self.assertEqual(len(archives), 1)
            self.assertIn(
                "https://github.com/kvcache-ai/Mooncake/issues/new/choose",
                result.stdout,
            )
            self.assertIn("AI_CONTEXT.md", result.stdout)
            with zipfile.ZipFile(os.path.join(temp, archives[0])) as archive:
                self.assertIsNone(archive.testzip())
                self.assertTrue(
                    any(n.endswith("/AI_CONTEXT.md") for n in archive.namelist())
                )
                self.assertFalse(
                    any(n.endswith(".map.json") for n in archive.namelist())
                )

    def test_embedded_usernames_are_redacted_across_bundle(self):
        with tempfile.TemporaryDirectory() as temp:
            user = "incidentoperator"
            log = os.path.join(temp, f"prefill-{user}_worker.log")
            write(log, [vlog("INFO", f"path=/tmp/mc-diag-{user}-20261020/cache")])
            snapshot = os.path.join(temp, "snapshot.json")
            with open(snapshot, "w") as stream:
                json.dump({"role": "prefill", "host": {"user": user}}, stream)
            out = os.path.join(temp, "incident.zip")
            m.main(
                [
                    "bundle",
                    "--no-local-snapshot",
                    "--prefill-log",
                    log,
                    "--snapshot",
                    snapshot,
                    "-o",
                    out,
                ]
            )
            with zipfile.ZipFile(out) as archive:
                for info in archive.infolist():
                    self.assertNotIn(user, info.filename)
                    self.assertNotIn(user, archive.read(info).decode())
                    self.assertEqual(info.extra, b"")
                packed = next(n for n in archive.namelist() if "/logs/" in n)
                self.assertIn("prefill-user-1_worker.log", packed)
                self.assertIn("mc-diag-user-1-20261020", archive.read(packed).decode())
            red = m.Redactor(users=[user])
            self.assertEqual(red.text(f"{user}other"), f"{user}other")

    def test_imported_snapshot_and_multiline_content_are_sanitized(self):
        with tempfile.TemporaryDirectory() as t:
            p = os.path.join(t, "p.log")
            write(
                p,
                [
                    vlog("DEBUG", "Request r JSON body: {", file="request_logger.py"),
                    '"prompt": "private-prompt"',
                    "}",
                    glog(
                        "E", "batchTransferSync: openSegment failed for 10.9.0.8:1234"
                    ),
                ],
            )
            snapshot = {
                "role": "prefill",
                "host": {"hostname": "private-host"},
                "env": {
                    "vars": {
                        "MC_TENT_CONF": '{"bucket":"private-bucket","enable":true}',
                        "VLLM_API_KEY": "private-key",
                    }
                },
            }
            path = os.path.join(t, "raw.json")
            with open(path, "w") as f:
                json.dump(snapshot, f)
            out = os.path.join(t, "case.zip")
            m.main(
                [
                    "bundle",
                    "--prefill-log",
                    p,
                    "--snapshot",
                    path,
                    "--no-local-snapshot",
                    "-o",
                    out,
                ]
            )
            with zipfile.ZipFile(out) as archive:
                files = {
                    n: archive.read(n).decode()
                    for n in archive.namelist()
                    if not n.endswith("/")
                }
            blob = "\n".join(files.values())
            for secret in (
                "private-prompt",
                "private-key",
                "private-bucket",
                "private-host",
            ):
                self.assertNotIn(secret, blob)
            self.assertTrue(any(n.endswith("/env.json") for n in files))
            packed_log = next(s for n, s in files.items() if n.endswith("/p.log"))
            self.assertIn("4\tE20261015", packed_log)
            summary = next(s for n, s in files.items() if n.endswith("/summary.md"))
            self.assertIn("p.log:L4", summary)
            self.assertIn("#L", summary)

    def test_end_to_end(self):
        with tempfile.TemporaryDirectory() as t:
            p, d = (
                os.path.join(t, "vllm_p.gpu-node-07.log"),
                os.path.join(t, "vllm_d.log"),
            )
            write(p, P_LOG)
            write(d, D_LOG)
            other = {
                "schema": 1,
                "role": "decode",
                "host": {"hostname": "gpu-node-08", "user": "liqian"},
                "env": {"vars": {"MC_TENT_CONF": "x"}},
                "effective": {},
                "mooncake_build": {},
                "notes": [],
                "process": {
                    "cmdline": ["vllm", "serve", "/home/liqian/m", "--host", "10.1.2.4"]
                },
            }
            sp = os.path.join(t, "snap.json")
            with open(sp, "w") as f:
                json.dump(other, f)
            out = os.path.join(t, "case.zip")
            rc = m.main(
                [
                    "bundle",
                    "--prefill-log",
                    p,
                    "--log",
                    d,
                    "--snapshot",
                    sp,
                    "--host",
                    "gpu-node-07",
                    "-o",
                    out,
                ]
            )
            self.assertEqual(rc, 0)
            map_path = os.path.join(t, "case.map.json")
            self.assertTrue(os.path.exists(map_path))
            self.assertEqual(stat.S_IMODE(os.stat(map_path).st_mode), 0o600)
            mapping = json.loads(read(map_path))
            with zipfile.ZipFile(out) as archive:
                names = archive.namelist()
                for entry in archive.infolist():
                    self.assertEqual(entry.extra, b"")
                    self.assertFalse(entry.filename.startswith("/"))
                    self.assertNotIn("..", entry.filename.split("/"))
                blob = "\n".join(
                    archive.read(n).decode() for n in names if not n.endswith("/")
                )
            self.assertFalse(any(n.endswith("map.json") for n in names))
            self.assertTrue(any("/logs/prefill/" in n for n in names))
            self.assertTrue(
                any("/logs/decode/" in n for n in names)
            )  # --log 按 kv_role 推断成 decode
            for original in mapping.values():
                self.assertNotIn(original, blob, original)
            for leak in (
                "the secret plan",
                "gpu-node-07",
                "gpu-node-08",
                "liqian",
                "\x1b[",
            ):
                self.assertNotIn(leak, blob)
            summary = [n for n in names if n.endswith("summary.md")][0]
            with zipfile.ZipFile(out) as archive:
                text = archive.read(summary).decode()
            self.assertIn("所有重试都失败", text)
            self.assertIn("ip-", text)


class ContextTest(unittest.TestCase):
    def test_repeated_waits_keep_counts_original_lines_and_later_error(self):
        with tempfile.TemporaryDirectory() as temp:
            _, index = self.bundle(
                temp,
                [],
                [vlog("DEBUG", "req_id=cmpl-wait transfer_id=xfer-wait")]
                + [
                    vlog(
                        "DEBUG",
                        "cmpl-wait is still in WAITING_FOR_REMOTE_KVS state.",
                        file="scheduler.py",
                        line=798,
                    )
                ]
                * 100
                + [
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-wait'}",
                    )
                ],
            )
            group = index["groups"][0]
            wait = next(
                s for s in group["locations"] if s["source_file"] == "scheduler.py"
            )
            self.assertEqual(
                (wait["occurrences"], wait["first_line"], wait["last_line"]),
                (100, 2, 101),
            )
            self.assertLess(len(group["evidence"]), 10)
            self.assertTrue(
                any(
                    r["line"] == 102 and r["level"] == "ERROR"
                    for r in group["evidence"]
                )
            )

    def bundle(
        self,
        temp,
        p_lines,
        d_lines,
        notes=None,
        record=None,
        keep_request_content=False,
        extra=(),
        names=("p.log", "d.log"),
    ):
        p, d = (os.path.join(temp, name) for name in names)
        write(p, p_lines)
        write(d, d_lines)
        out = os.path.join(temp, "context.zip")
        args = [
            "bundle",
            "--no-local-snapshot",
            "--prefill-log",
            p,
            "--decode-log",
            d,
            "--host",
            "private-host",
            "-o",
            out,
        ]
        if notes is not None:
            path = os.path.join(temp, "case.md")
            with open(path, "w") as stream:
                stream.write(notes)
            args += ["--case", path]
        if record is not None:
            path = os.path.join(temp, "controller.json")
            with open(path, "w") as stream:
                stream.write(record)
            args += ["--record", path]
        if keep_request_content:
            args += ["--keep-request-content"]
        args += list(extra)
        m.main(args)
        with zipfile.ZipFile(out) as archive:
            data = {
                n.split("/", 1)[1]: archive.read(n).decode()
                for n in archive.namelist()
                if not n.endswith("/")
            }
        return data, json.loads(data["evidence.json"])

    def test_shared_transfer_links_different_requests_and_keeps_fault_separate(self):
        with tempfile.TemporaryDirectory() as temp:
            data, index = self.bundle(
                temp,
                [vlog("DEBUG", "req_id=cmpl-normal-P transfer_id=xfer-normal")],
                [
                    vlog("DEBUG", "req_id=cmpl-normal-D transfer_id=xfer-normal"),
                    vlog("DEBUG", "req_id=cmpl-fault-D transfer_id=xfer-fault"),
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-fault-D'}: Resource temporarily unavailable",
                    ),
                ],
                "场景：private-host\n操作：主动停止P，精确时刻未知\n排查结果：客户端100秒超时",
            )
            groups = {g["id"]: g for g in index["groups"]}
            self.assertEqual(set(groups["xfer-normal"]["roles"]), {"prefill", "decode"})
            self.assertEqual(
                groups["xfer-normal"]["request_ids"], ["cmpl-normal-D", "cmpl-normal-P"]
            )
            self.assertEqual(groups["xfer-fault"]["missing_roles"], ["prefill"])
            self.assertTrue(
                all(r["role"] == "decode" for r in groups["xfer-fault"]["evidence"])
            )
            self.assertIn("主动停止P", data["AI_CONTEXT.md"])
            self.assertIn("未经自动核验", data["AI_CONTEXT.md"])
            self.assertNotIn("private-host", "\n".join(data.values()))

    def test_similar_prefix_and_shared_time_do_not_link(self):
        with tempfile.TemporaryDirectory() as temp:
            _, index = self.bundle(
                temp,
                [vlog("DEBUG", "req_id=cmpl-same-prefix-P transfer_id=xfer-same")],
                [
                    vlog(
                        "DEBUG", "req_id=cmpl-same-prefix-D transfer_id=xfer-same-other"
                    )
                ],
            )
            self.assertTrue(all(len(g["roles"]) == 1 for g in index["groups"]))

    def test_reused_request_does_not_assign_id_only_error_to_a_transfer(self):
        with tempfile.TemporaryDirectory() as temp:
            _, index = self.bundle(
                temp,
                [],
                [
                    vlog("DEBUG", "req_id=cmpl-reused transfer_id=xfer-one"),
                    vlog("DEBUG", "req_id=cmpl-reused transfer_id=xfer-two"),
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-reused'}",
                    ),
                ],
            )
            orphan = next(g for g in index["groups"] if g["kind"] == "request")
            self.assertEqual(orphan["ambiguous_transfer_ids"], ["xfer-one", "xfer-two"])
            for group in index["groups"]:
                if group["kind"] == "transfer":
                    self.assertFalse(
                        any(r["level"] == "ERROR" for r in group["evidence"])
                    )

    def test_truncating_identifiers_never_creates_a_false_unique_pair(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(
            m, "CONTEXT_ID_LIMIT", 2
        ):
            data, index = self.bundle(
                temp,
                [
                    vlog("DEBUG", "Mooncake request xfer-one will be served"),
                    vlog("DEBUG", "req_id=cmpl-one transfer_id=xfer-one peer=cmpl-two"),
                ],
                [],
            )
            self.assertEqual(index["ambiguous_cooccurrence_lines"], 1)
            self.assertTrue(index["omitted_identifier_occurrences"])
            self.assertTrue(all(not g["direct_links"] for g in index["groups"]))
            self.assertIn("部分证据", data["AI_CONTEXT.md"])

    def test_omitted_later_transfer_vetoes_request_only_error_assignment(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(
            m, "CONTEXT_ID_LIMIT", 2
        ):
            data, index = self.bundle(
                temp,
                [],
                [
                    vlog("DEBUG", "req_id=cmpl-reused transfer_id=xfer-one"),
                    vlog("DEBUG", "req_id=cmpl-reused transfer_id=xfer-two"),
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-reused'}",
                    ),
                ],
            )
            request = next(g for g in index["groups"] if g["kind"] == "request")
            transfer = next(g for g in index["groups"] if g["kind"] == "transfer")
            self.assertTrue(request["unindexed_transfer_mapping"])
            self.assertTrue(any(r["level"] == "ERROR" for r in request["evidence"]))
            self.assertFalse(any(r["level"] == "ERROR" for r in transfer["evidence"]))
            self.assertEqual(transfer["request_ids"], [])
            self.assertIn("未索引 transfer", data["AI_CONTEXT.md"])

    def test_site_limit_preserves_both_roles_and_first_error(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(
            m, "CONTEXT_SITE_LIMIT", 2
        ):
            data, index = self.bundle(
                temp,
                [
                    vlog(
                        "DEBUG", "req_id=cmpl-shared transfer_id=xfer-shared", line=100
                    ),
                    vlog(
                        "DEBUG", "req_id=cmpl-shared transfer_id=xfer-shared", line=200
                    ),
                ],
                [
                    vlog(
                        "DEBUG", "req_id=cmpl-shared transfer_id=xfer-shared", line=300
                    ),
                    vlog(
                        "ERROR",
                        "MooncakeXferMetadata transfer failed for {'cmpl-shared'}",
                    ),
                ],
            )
            group = index["groups"][0]
            self.assertEqual(group["roles"], ["decode", "prefill"])
            self.assertEqual(group["missing_roles"], [])
            self.assertTrue(any(r["level"] == "ERROR" for r in group["evidence"]))
            self.assertNotIn("缺少该 ID 的", data["AI_CONTEXT.md"])
            self.assertIn("部分证据", data["AI_CONTEXT.md"])

    def test_notes_multiline_secrets_removed_even_when_request_content_kept(self):
        for keep in (False, True):
            with self.subTest(
                keep_request_content=keep
            ), tempfile.TemporaryDirectory() as temp:
                data, _ = self.bundle(
                    temp,
                    [],
                    [],
                    notes="操作记录\npassword:\n  CASE_SECRET_CANARY\n",
                    record='{"api_key":\n "RECORD_SECRET_CANARY"\n}\n',
                    keep_request_content=keep,
                )
                blob = "\n".join(data.values())
                self.assertNotIn("CASE_SECRET_CANARY", blob)
                self.assertNotIn("RECORD_SECRET_CANARY", blob)
                self.assertIn(m.SECRET_CONTENT_MARK, blob)
                self.assertIn("操作记录", data["case.md"])

    def test_new_context_and_records_cannot_resurrect_request_content(self):
        with tempfile.TemporaryDirectory() as temp:
            data, index = self.bundle(
                temp,
                [
                    vlog(
                        "DEBUG",
                        "Received request cmpl-forged: transfer_id=xfer-forged prompt=LOG_CANARY",
                        file="request_logger.py",
                    ),
                    vlog("DEBUG", "req_id=xfer-named-request"),
                ],
                [],
                "操作：主动停止P\nprompt: CASE_CANARY\nCONTINUATION_CANARY\n",
                '{"prompt": ["RECORD_CANARY",\n"RECORD_CONTINUATION"], "status": 200}\n',
            )
            blob = "\n".join(data.values())
            for secret in (
                "LOG_CANARY",
                "CASE_CANARY",
                "CONTINUATION_CANARY",
                "RECORD_CANARY",
                "RECORD_CONTINUATION",
                "xfer-forged",
            ):
                self.assertNotIn(secret, blob)
            self.assertEqual(
                [(g["kind"], g["id"]) for g in index["groups"]],
                [("request", "xfer-named-request")],
            )
            self.assertIn("records/0-controller.json", data["AI_CONTEXT.md"])

    def test_init_case_is_private_and_missing_background_stays_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "notes.md")
            m.main(["init-case", "-o", path])
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
            with self.assertRaises(SystemExit):
                m.main(["init-case", "-o", path])
            data, index = self.bundle(temp, [], [])
            self.assertFalse(index["groups"])
            self.assertIn("人为干预均未知", data["AI_CONTEXT.md"])
            self.assertIn("未找到可索引", data["AI_CONTEXT.md"])


class ReviewRegressionTest(unittest.TestCase):
    bundle = ContextTest.bundle

    def test_prefixed_cli_fullwidth_and_container_secrets(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp,
                [],
                [
                    vlog(
                        "INFO",
                        "non-default args: {'api_key': ['LOG_SECRET', {'nested': 'NESTED_SECRET'}], 'port': 8000}",
                    ),
                    vlog("INFO", "env OPENAI_API_KEY=ENV_SECRET"),
                ],
                notes="- VLLM_API_KEY=CASE_SECRET vllm serve\n\n- DB_PASSWORD=DB_SECRET\n\n- vllm serve --api-key CLI_SECRET --port 8000\n\n- password：FULL_SECRET\n\n- prompt：PROMPT_SECRET\n",
                record='{"token": "TOKEN_SECRET", "aws_secret_access_key": "AWS_SECRET", "status": 200}\n',
            )
            blob = "\n".join(data.values())
            for secret in (
                "LOG_SECRET",
                "NESTED_SECRET",
                "ENV_SECRET",
                "CASE_SECRET",
                "DB_SECRET",
                "CLI_SECRET",
                "FULL_SECRET",
                "PROMPT_SECRET",
                "TOKEN_SECRET",
                "AWS_SECRET",
            ):
                self.assertNotIn(secret, blob)
            self.assertIn("'port': 8000", data["logs/decode/d.log"])
            self.assertIn('"status": 200', data["records/0-controller.json"])
            self.assertEqual(
                m.Redactor().text("num_tokens=505 token_id=42"),
                "num_tokens=505 token_id=42",
            )

    def test_late_failure_survives_normal_traffic_id_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            p = [
                vlog("DEBUG", f"req_id=cmpl-normal-{n}-p transfer_id=xfer-normal-{n}")
                for n in range(100)
            ]
            d = [
                vlog("DEBUG", f"req_id=cmpl-normal-{n}-d transfer_id=xfer-normal-{n}")
                for n in range(100)
            ]
            d += [
                vlog("DEBUG", "req_id=cmpl-fault transfer_id=xfer-fault"),
                vlog(
                    "ERROR", "MooncakeXferMetadata transfer failed for {'cmpl-fault'}"
                ),
            ]
            data, index = self.bundle(temp, p, d)
            group = index["groups"][0]
            self.assertEqual(group["id"], "xfer-fault")
            self.assertTrue(
                any(
                    r["level"] == "ERROR" and r["line"] == 102
                    for r in group["evidence"]
                )
            )
            self.assertIn("logs/decode/d.log:L102", data["AI_CONTEXT.md"])
            self.assertIn("xfer-fault", data["AI_CONTEXT.md"])
            self.assertLessEqual(
                len(
                    {
                        (loc["kind"], loc["id"])
                        for g in index["groups"]
                        for loc in g["locations"]
                    }
                ),
                m.CONTEXT_ID_LIMIT,
            )

    def test_info_only_warns_about_missing_debug_mapping(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp,
                [vlog("INFO", "Added request cmpl-p")],
                [vlog("ERROR", "MooncakeXferMetadata transfer failed for {'cmpl-d'}")],
            )
            self.assertIn("VLLM_LOGGING_LEVEL=DEBUG", data["AI_CONTEXT.md"])
            self.assertIn("日志级别造成", data["AI_CONTEXT.md"])

    def test_reused_transfer_lists_requests_and_unprefixed_post_abort_send(self):
        with tempfile.TemporaryDirectory() as temp:
            data, index = self.bundle(
                temp,
                [vlog("DEBUG", "req_id=cmpl-p2 transfer_id=xfer-reused")],
                [
                    vlog("DEBUG", "req_id=cmpl-d1 transfer_id=xfer-reused"),
                    vlog(
                        "ERROR", "MooncakeXferMetadata transfer failed for {'cmpl-d1'}"
                    ),
                    vlog(
                        "DEBUG",
                        "request_finished req_id=9e7e84d13853098f transfer_id=xfer-reused request_status=FINISHED_ABORTED",
                    ),
                    vlog(
                        "DEBUG",
                        "Sending kv transfer request for {'9e7e84d13853098f'} on path: tcp://10.1.1.1:23",
                    ),
                ],
            )
            group = index["groups"][0]
            self.assertEqual(
                group["multiple_requests_same_role"]["decode"],
                ["9e7e84d13853098f", "cmpl-d1"],
            )
            item = next(
                r
                for r in group["request_evidence"]
                if r["request_id"] == "9e7e84d13853098f"
            )
            self.assertEqual(item["last"]["line"], 4)
            self.assertIn("不能视为同一次尝试", data["AI_CONTEXT.md"])
            self.assertIn("logs/decode/d.log:L4", data["AI_CONTEXT.md"])

    def test_note_boundaries_preserve_operations_and_json_siblings(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp,
                [],
                [],
                notes="# 故障说明\n- 实际现象：proxy output: 504 Gateway Timeout\n\n## 操作\n| 主动停止 P | L24 |\nprompt：\n  PRIVATE_BODY\n\n## 后续排查\n- 已确认超时\n",
                record='{\n "authorization": ["AUTH_SECRET",\n {"nested": "AUTH_NESTED"}],\n "status": 504,\n "controlled_fault": "stopped owned P"\n}\n',
            )
            blob = "\n".join(data.values())
            for secret in ("PRIVATE_BODY", "AUTH_SECRET", "AUTH_NESTED"):
                self.assertNotIn(secret, blob)
            self.assertIn("主动停止 P", data["case.md"])
            self.assertIn("已确认超时", data["case.md"])
            self.assertIn('"status": 504', data["records/0-controller.json"])
            self.assertIn("stopped owned P", data["records/0-controller.json"])
            self.assertGreater(
                json.loads(data["manifest.json"])["redaction_stats"][
                    "case_redacted_lines"
                ],
                0,
            )
            self.assertIn("内容脱敏涉及", data["AI_CONTEXT.md"])

    def test_known_host_boundaries_cover_filenames(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp,
                [vlog("INFO", "connect private-host-ib0")],
                [],
                names=("prefill-private-host.log", "private-host_decode.log"),
            )
            self.assertNotIn(
                "private-host", "\n".join(data.keys()) + "\n".join(data.values())
            )

    def test_truncated_source_does_not_claim_unique_mapping(self):
        with tempfile.TemporaryDirectory() as temp:
            filler = vlog("DEBUG", "x" * 200, file="other.py")
            d = [vlog("DEBUG", "req_id=cmpl-R transfer_id=xfer-one")] + [filler] * 3000
            d += [vlog("DEBUG", "req_id=cmpl-R transfer_id=xfer-two")] + [filler] * 6000
            d += [vlog("ERROR", "MooncakeXferMetadata transfer failed for {'cmpl-R'}")]
            data, index = self.bundle(temp, [], d, extra=("--max-log-mb", "1"))
            request = next(g for g in index["groups"] if g["kind"] == "request")
            self.assertTrue(request["truncated_request_mapping"])
            for group in index["groups"]:
                if group["kind"] == "transfer":
                    self.assertFalse(
                        any(r["level"] == "ERROR" for r in group["evidence"])
                    )
            self.assertIn("仅含 request 的证据未并入 transfer", data["AI_CONTEXT.md"])

    def test_embedded_carriage_return_keeps_one_numbered_lf_line(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp, [], ["progress 1\rprogress 2", vlog("INFO", "ready")]
            )
            text = data["logs/decode/d.log"]
            self.assertNotIn("\r", text)
            self.assertIn("progress 1\\rprogress 2", text)
            self.assertEqual(
                [row.split("\t")[0] for row in text.splitlines()], ["1", "2"]
            )

    def test_case_encoding_error_is_actionable_and_does_not_publish_bundle(self):
        with tempfile.TemporaryDirectory() as temp:
            log = os.path.join(temp, "d.log")
            write(log, [vlog("INFO", "ready")])
            case, out = os.path.join(temp, "case.md"), os.path.join(temp, "case.zip")
            with open(case, "wb") as stream:
                stream.write("故障说明".encode("gbk"))
            with self.assertRaisesRegex(SystemExit, "UTF-8"):
                m.main(
                    [
                        "bundle",
                        "--no-local-snapshot",
                        "--decode-log",
                        log,
                        "--case",
                        case,
                        "-o",
                        out,
                    ]
                )
            self.assertFalse(os.path.exists(out))


class SelfAuditTest(unittest.TestCase):
    bundle = ContextTest.bundle

    def test_log_multiline_secrets_and_source_boundaries(self):
        for keep in (False, True):
            with self.subTest(
                keep_request_content=keep
            ), tempfile.TemporaryDirectory() as temp:
                data, _ = self.bundle(
                    temp,
                    [
                        vlog("INFO", "config: {'api_key': ["),
                        ' {"message": "LIST_SECRET"},',
                        "]}",
                        vlog("INFO", 'password="QUOTE_SECRET'),
                        'QUOTE_CONTINUED"',
                        vlog("ERROR", "diagnostic-after-secret"),
                        vlog("INFO", "password:"),
                    ],
                    ["public-raw-first-line", vlog("INFO", "ready")],
                    keep_request_content=keep,
                )
                blob = "\n".join(data.values())
                for secret in ("LIST_SECRET", "QUOTE_SECRET", "QUOTE_CONTINUED"):
                    self.assertNotIn(secret, blob)
                self.assertIn("public-raw-first-line", data["logs/decode/d.log"])
                self.assertIn("diagnostic-after-secret", data["logs/prefill/p.log"])

    def test_unquoted_cli_and_env_keep_whole_secret_tokens(self):
        red = m.Redactor()
        for line in (
            "vllm --api-key first,COMMA_SECRET --port 8000",
            "DB_PASSWORD=first,ENV_COMMA_SECRET other=retained",
            r"vllm --api-key first\ ESCAPED_SECRET --port 8000",
            "vllm --api-key [first]BRACKET_SECRET --port 8000",
            'vllm --api-key "first"CONCAT_SECRET --port 8000',
        ):
            result = red.text(line)
            self.assertNotIn("SECRET", result)
            self.assertTrue("--port 8000" in result or "other=retained" in result)
        self.assertIn('"status":200', red.text('{"token":123,"status":200}'))

    def test_imported_snapshot_cmdline_and_secret_fields_are_sanitized(self):
        with tempfile.TemporaryDirectory() as temp:
            log = os.path.join(temp, "d.log")
            snapshot = os.path.join(temp, "snapshot.json")
            out = os.path.join(temp, "case.zip")
            write(log, [vlog("INFO", "ready")])
            with open(snapshot, "w") as stream:
                json.dump(
                    {
                        "role": "decode",
                        "host": {},
                        "env": {"vars": {}},
                        "process": {
                            "cmdline": [
                                "vllm",
                                "serve",
                                "--api-key",
                                "IMPORTED_FIRST_SECRET",
                                "IMPORTED_SECOND_SECRET",
                                "--tokenizer",
                                "/public/model",
                                "--port",
                                "8000",
                            ]
                        },
                        "config": {"api_key": ["OBJECT_SECRET"]},
                    },
                    stream,
                )
            m.main(
                [
                    "bundle",
                    "--no-local-snapshot",
                    "--decode-log",
                    log,
                    "--snapshot",
                    snapshot,
                    "-o",
                    out,
                ]
            )
            with zipfile.ZipFile(out) as archive:
                blob = "\n".join(
                    archive.read(item).decode()
                    for item in archive.infolist()
                    if not item.is_dir()
                )
            self.assertNotIn("IMPORTED_FIRST_SECRET", blob)
            self.assertNotIn("IMPORTED_SECOND_SECRET", blob)
            self.assertNotIn("OBJECT_SECRET", blob)
            self.assertIn("/public/model", blob)
            self.assertIn("8000", blob)

    def test_markdown_sibling_boundaries_respect_indentation(self):
        with tempfile.TemporaryDirectory() as temp:
            data, _ = self.bundle(
                temp,
                [],
                [],
                notes="password:\n## 操作\n主动停止 P\n\nprompt：ROOT_SECRET\n  - INDENTED_SECRET\n  # INDENTED_HEADING_SECRET\n\n## 排查\n- 保留这项检查\n",
            )
            notes = data["case.md"]
            for secret in ("ROOT_SECRET", "INDENTED_SECRET", "INDENTED_HEADING_SECRET"):
                self.assertNotIn(secret, notes)
            self.assertIn("主动停止 P", notes)
            self.assertIn("保留这项检查", notes)


@unittest.skipUnless(
    os.environ.get("MC_DIAG_TENT_SO") and os.environ.get("MC_DIAG_CLASSIC_SO"),
    "真实构建验证需要 MC_DIAG_TENT_SO 和 MC_DIAG_CLASSIC_SO",
)
class RealBuildTest(unittest.TestCase):
    def test_real_builds_loaded_process_venv_and_failure_logs(self):
        import select
        import shutil
        from pathlib import Path

        # 加载真实 extension，产生一次实际 openSegment 故障，然后保持进程供采集。
        worker = r"""
import ctypes, importlib.util, os, socket, sys
spec = importlib.util.spec_from_file_location("engine", sys.argv[1])
engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)
te = engine.TransferEngine()
assert te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", "") == 0
buf = ctypes.create_string_buffer(16)
addr = ctypes.addressof(buf)
assert te.batch_register_memory([addr], [16]) == 0
with socket.socket() as closed_peer:
    closed_peer.bind(("127.0.0.1", 0))
    assert te.batch_transfer_sync_write("127.0.0.1:" + str(closed_peer.getsockname()[1]), [addr], [addr], [16]) != 0
print("READY", flush=True)
sys.stdin.readline()
te.batch_unregister_memory([addr])
"""
        parent = r"""
import os, subprocess, sys
env = dict(os.environ, MC_USE_TENT=sys.argv[1])
child = subprocess.Popen([sys.executable, "-c", sys.argv[2], sys.argv[3]], env=env, stdin=subprocess.PIPE, text=True)
try:
    sys.stdin.readline()
finally:
    child.communicate("\n", timeout=15)
sys.exit(child.returncode)
"""
        for kind in ("tent", "classic"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                so = Path(os.environ["MC_DIAG_" + kind.upper() + "_SO"])
                self.assertEqual(m.detect_tent_build([str(so)])["verdict"], kind)
                if shutil.which("strip"):
                    stripped = Path(temp) / "engine.stripped.so"
                    subprocess.run(
                        ["strip", "--strip-debug", "-o", str(stripped), str(so)],
                        check=True,
                    )
                    self.assertEqual(
                        m.detect_tent_build([str(stripped)])["verdict"], kind
                    )
                venv = Path(temp) / "venv"
                subprocess.run(
                    [sys.executable, "-m", "venv", "--without-pip", str(venv)],
                    check=True,
                )
                py = str(venv / "bin/python")
                env = dict(os.environ)
                env.pop("MC_USE_TEV1", None)
                env.update(
                    MC_USE_TENT="0",
                    VIRTUAL_ENV=str(venv),
                    MOONCAKE_LOCAL_HOSTNAME="127.0.0.1",
                    MC_TENT_CONF='{"transports":{"tcp":{"max_concurrent_tasks":2},"shm":{"enable":false}}}',
                )
                build = so.parent.parent
                env["LD_LIBRARY_PATH"] = ":".join(
                    (
                        str(build / "mooncake-common"),
                        str(build / "mooncake-transfer-engine/tent/src/metrics"),
                        env.get("LD_LIBRARY_PATH", ""),
                    )
                )
                log_path = str(Path(temp) / "p.log")
                with open(log_path, "w") as log:
                    proc = subprocess.Popen(
                        [
                            py,
                            "-c",
                            parent,
                            "1" if kind == "tent" else "0",
                            worker,
                            str(so),
                        ],
                        env=env,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=log,
                        text=True,
                    )
                    try:
                        readable, _, _ = select.select([proc.stdout], [], [], 30)
                        self.assertTrue(readable, "native worker did not become ready")
                        self.assertEqual(
                            proc.stdout.readline().strip(),
                            "READY",
                            read(log_path)[-2000:],
                        )
                        snap = m.take_snapshot("prefill", proc.pid)
                        self.assertNotEqual(snap["process"]["mooncake_pid"], proc.pid)
                        self.assertEqual(
                            snap["env"]["source"],
                            f"pid {snap['process']['mooncake_pid']}",
                        )
                        self.assertEqual(snap["python"]["interpreter"], py)
                        self.assertEqual(snap["mooncake_build"]["verdict"], kind)
                        self.assertEqual(snap["effective"]["use_tent"], kind == "tent")
                        self.assertNotIn('max_concurrent_tasks": 2', json.dumps(snap))
                    finally:
                        proc.communicate("\n", timeout=15)
                self.assertEqual(proc.returncode, 0)
                scan = m.scan_file(log_path, "prefill", 0)
                expected = (
                    "mc.retries_exhausted"
                    if kind == "tent"
                    else "mc.open_segment_failed"
                )
                self.assertGreater(
                    scan.rule_counts[expected], 0, read(log_path)[-2000:]
                )
                if os.environ.get("MC_DIAG_EVIDENCE_DIR"):
                    destination = Path(os.environ["MC_DIAG_EVIDENCE_DIR"])
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(log_path, destination / (kind + "-p.log"))
                    with m._private_file(
                        str(destination / (kind + "-snapshot.json"))
                    ) as f:
                        json.dump(snap, f, indent=2)


@unittest.skipUnless(
    os.environ.get("MOONCAKE_SRC") and os.environ.get("VLLM_SRC"),
    "需要 MOONCAKE_SRC 和 VLLM_SRC",
)
class SelfCheck(unittest.TestCase):
    def test_rule_anchors_exist_in_source(self):
        roots = {"mooncake": os.environ["MOONCAKE_SRC"], "vllm": os.environ["VLLM_SRC"]}
        missing = []
        for r in m.RULES:
            if r.repo not in roots:
                continue
            revision = (
                os.environ.get("MOONCAKE_REVISION", "1d97a1d")
                if r.repo == "mooncake"
                else os.environ.get("VLLM_REVISION", "e8e1e961")
            )
            if revision not in m.RULE_SOURCE_LINES[r.id]:
                continue
            path = os.path.join(roots[r.repo], r.path)
            files = (
                [path]
                if os.path.isfile(path)
                else [
                    os.path.join(dp, f)
                    for dp, _, fs in os.walk(path)
                    for f in fs
                    if f.endswith((".cpp", ".py"))
                ]
            )
            if not any(r.anchor in read(f) for f in files):
                missing.append(f"{r.id}: {r.anchor!r} not in {r.path}")
            revision = (
                os.environ.get("MOONCAKE_REVISION", "1d97a1d")
                if r.repo == "mooncake"
                else os.environ.get("VLLM_REVISION", "e8e1e961")
            )
            for line in m.RULE_SOURCE_LINES[r.id][revision]:
                self.assertIn(r.anchor, read(path).splitlines()[line - 1], r.id)
        self.assertEqual(missing, [])

    def test_engine_only_files(self):
        if os.environ.get("MOONCAKE_REVISION", "1d97a1d") not in (
            "1d97a1d",
            "342c8aee",
        ):
            self.skipTest("该 release 只核对了日志源码锚点，未核对引擎文件名全集")
        root = os.environ["MOONCAKE_SRC"]
        found = {}
        for dp, _, fs in os.walk(root):
            if "/extern" in dp or "/tests" in dp or "/test/" in dp + "/":
                continue
            for f in fs:
                found.setdefault(f, []).append(dp)
        for f in m.TENT_ONLY_FILES:
            self.assertTrue(
                all("/tent/" in d for d in found.get(f, [])) and f in found, f
            )
        for f in m.CLASSIC_ONLY_FILES:
            self.assertTrue(f in found and all("/tent/" not in d for d in found[f]), f)

    def test_build_markers_only_in_tent_code(self):
        if os.environ.get("MOONCAKE_REVISION", "1d97a1d") not in (
            "1d97a1d",
            "342c8aee",
        ):
            self.skipTest("当前测试对照新版本 TENT 兼容层")
        root = os.environ["MOONCAKE_SRC"]
        te = read(
            os.path.join(root, "mooncake-transfer-engine/src/transfer_engine.cpp")
        )
        else_pos = te.index("\n#else\n")  # 文件后半段是 USE_TENT 分支
        self.assertGreater(te.index(m.TENT_SHIM_MARKER.decode()), else_pos)
        self.assertGreater(te.index(m.TENT_LEGACY_SHIM_MARKER.decode()), else_pos)
        impl = read(
            os.path.join(
                root,
                "mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp",
            )
        )
        self.assertIn(m.TENT_RUNTIME_MARKER.decode(), impl)


if __name__ == "__main__":
    unittest.main()
