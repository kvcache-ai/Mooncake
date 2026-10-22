"""Offline snapshot, redaction, literal-evidence and ZIP lifecycle regressions."""

import gzip
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

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
        line = vlog("INFO", 'Request cmpl-2 JSON body: {"prompt": "hi"}')
        self.assertIn(m.REQUEST_CONTENT_MARK, m.Redactor().text(line))


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
            finally:
                process.terminate()
                process.communicate(timeout=10)


def bundle(
    temp, p=(), d=(), case=None, record=None, snapshot=None, output="incident.zip"
):
    args = ["bundle", "-o", str(Path(temp) / output)]
    for role, rows in (("prefill", p), ("decode", d)):
        if rows:
            path = Path(temp) / f"{role}.log"
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            args += [f"--{role}-log", str(path)]
    for option, value, name in (
        ("--case", case, "case.md"),
        ("--record", record, "record.txt"),
    ):
        if value is not None:
            path = Path(temp) / name
            path.write_text(value, encoding="utf-8")
            args += [option, str(path)]
    if snapshot is not None:
        path = Path(temp) / "snapshot.json"
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        args += ["--snapshot", str(path)]
    m.main(args)
    with zipfile.ZipFile(Path(temp) / output) as archive:
        return {
            name.split("/", 1)[1]: archive.read(name).decode()
            for name in archive.namelist()
        }


class IncidentTest(unittest.TestCase):
    def test_prior_secret_format_regressions_still_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            rows = [
                vlog(
                    "INFO",
                    "args: {'api_key': ['LOG_CANARY', {'nested': 'NESTED_CANARY'}], 'port': 8000}",
                ),
                vlog("INFO", "OPENAI_API_KEY=ENV_CANARY"),
            ]
            notes = "- VLLM_API_KEY=CASE_CANARY\n- DB_PASSWORD=DB_CANARY\n- vllm --api-key CLI_CANARY --port 8000\n- password：FULLWIDTH_CANARY\n- prompt：PROMPT_CANARY\n"
            record = '{\n "authorization": ["AUTH_CANARY", {"nested": "NESTED_AUTH_CANARY"}],\n "token": "TOKEN_CANARY", "aws_secret_access_key": "AWS_CANARY",\n "status": 504, "controlled_fault": "stopped owned P"\n}\n'
            data = bundle(temp, d=rows, case=notes, record=record)
            self.assertNotIn("CANARY", "\n".join(data.values()))
            self.assertIn("'port': 8000", data["logs/decode/0-decode.log"])
            self.assertIn('"status": 504', data["records/0-record.txt"])
            self.assertIn("stopped owned P", data["records/0-record.txt"])

    def test_unquoted_secret_token_boundaries(self):
        for text in (
            "vllm --api-key first,COMMA_CANARY --port 8000",
            "DB_PASSWORD=first,COMMA_CANARY other=retained",
            r"vllm --api-key first\ ESCAPED_CANARY --port 8000",
            "vllm --api-key [first]BRACKET_CANARY --port 8000",
            'vllm --api-key "first"CONCAT_CANARY --port 8000',
        ):
            result = m.Redactor().text(text)
            self.assertNotIn("CANARY", result)
            self.assertTrue("--port 8000" in result or "other=retained" in result)

    def test_multiline_log_secrets_do_not_erase_other_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            p = [
                vlog("INFO", "config: {'api_key': ["),
                ' {"message": "LIST_CANARY"},',
                "]}",
                vlog("INFO", 'password="QUOTE_CANARY'),
                'CONTINUED_CANARY"',
                vlog("ERROR", "ret=-1"),
                vlog("INFO", "password:"),
            ]
            data = bundle(temp, p, ["public-first-line", vlog("INFO", "ready")])
            self.assertNotIn("CANARY", "\n".join(data.values()))
            self.assertIn("public-first-line", data["logs/decode/1-decode.log"])
            self.assertIn("ret=-1", data["logs/prefill/0-prefill.log"])

    def test_invalid_case_encoding_does_not_publish(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "p.log"
            path.write_text("ready\n", encoding="utf-8")
            case, out = Path(temp) / "case.md", Path(temp) / "out.zip"
            case.write_bytes("故障说明".encode("gbk"))
            with self.assertRaisesRegex(SystemExit, "UTF-8"):
                m.main(
                    [
                        "bundle",
                        "--prefill-log",
                        str(path),
                        "--case",
                        str(case),
                        "-o",
                        str(out),
                    ]
                )
            self.assertFalse(out.exists())
            self.assertFalse(out.with_suffix(".map.json").exists())

    def test_id_first_error_is_not_replaced_by_later_success(self):
        with tempfile.TemporaryDirectory() as temp:
            rows = [
                vlog("DEBUG", "req_id=cmpl-reused ready"),
                vlog("ERROR", "req_id=cmpl-reused failed"),
            ]
            rows += [vlog("DEBUG", "req_id=cmpl-reused ready")] * 50
            data = bundle(temp, d=rows)
            group = data["AI_CONTEXT.md"].split("### request `cmpl-reused`", 1)[1]
            self.assertIn("logs/decode/0-decode.log:L2", group)
            self.assertIn("not necessarily one attempt", data["AI_CONTEXT.md"])

    def test_single_context_no_redundant_reports_and_valid_citations(self):
        with tempfile.TemporaryDirectory() as temp:
            data = bundle(
                temp,
                [vlog("DEBUG", "req_id=cmpl-P transfer_id=xfer-fault")],
                [
                    vlog("DEBUG", "req_id=cmpl-D transfer_id=xfer-fault"),
                    vlog("ERROR", "pulling kv_caches for {'cmpl-D'} failed"),
                ],
                case="## Checks\nP remained alive\n",
                record="Checked peer\n",
            )
            self.assertEqual(
                set(data),
                {
                    "AI_CONTEXT.md",
                    "case.md",
                    "env.json",
                    "manifest.json",
                    "logs/prefill/0-prefill.log",
                    "logs/decode/1-decode.log",
                    "records/0-record.txt",
                },
            )
            self.assertIn("xfer-fault", data["AI_CONTEXT.md"])
            self.assertIn("logs/decode/1-decode.log:L2", data["AI_CONTEXT.md"])
            self.assertIn("2\t", data["logs/decode/1-decode.log"])
            self.assertNotIn("root cause is", data["AI_CONTEXT.md"])

    def test_redis_and_rdma_capabilities_removed_everywhere(self):
        with tempfile.TemporaryDirectory() as temp:
            secrets = ["REDIS_CANARY", "AT_CANARY", "76543210", "87654321"]
            text = "redis://:REDIS_CANARY@10.1.2.3:6379 redis://user:pw@AT_CANARY@host/ rkey=76543210 lkey=87654321"
            data = bundle(
                temp,
                [vlog("ERROR", text)],
                case=text,
                record=text,
                snapshot={
                    "host": {},
                    "config": {"rkey": 76543210, "lkeys": [87654321], "url": text},
                },
            )
            blob = "\n".join(data.values())
            for value in secrets:
                self.assertNotIn(value, blob)
            self.assertIn("6379", blob)

    def test_multiline_prompt_blank_paragraph_and_structured_secrets(self):
        with tempfile.TemporaryDirectory() as temp:
            notes = (
                "prompt: FIRST_CANARY\n\nSECOND_CANARY\n\n## Checks\nP stayed alive\n"
            )
            record = 'password："QUOTE_CANARY\nCONTINUED_CANARY"\n## Result\nret=-1\n'
            data = bundle(temp, [vlog("INFO", "ready")], case=notes, record=record)
            blob = "\n".join(data.values())
            for value in (
                "FIRST_CANARY",
                "SECOND_CANARY",
                "QUOTE_CANARY",
                "CONTINUED_CANARY",
            ):
                self.assertNotIn(value, blob)
            self.assertIn("P stayed alive", data["case.md"])
            self.assertIn("ret=-1", data["records/0-record.txt"])

    def test_no_truncation_gap_can_expose_request_continuations(self):
        with tempfile.TemporaryDirectory() as temp:
            rows = [vlog("INFO", "ready")] * 200
            rows += [
                vlog("DEBUG", "Request cmpl-1 JSON body: {", file="request_logger.py")
            ]
            rows += ["PROMPT_CONTINUATION_CANARY"] * 400
            rows += [vlog("ERROR", "transport failed ret=-1")]
            data = bundle(temp, p=rows)
            log = data["logs/prefill/0-prefill.log"]
            self.assertEqual(len(log.splitlines()), len(rows))
            self.assertNotIn("PROMPT_CONTINUATION_CANARY", "\n".join(data.values()))
            self.assertIn(f"{len(rows)}\t", log)
            self.assertFalse(json.loads(data["manifest.json"])["input_truncated"])

    def test_failure_ids_precede_normal_traffic_without_false_merging(self):
        with tempfile.TemporaryDirectory() as temp:
            p = [
                vlog("DEBUG", f"req_id=cmpl-normal-{i} transfer_id=xfer-normal-{i}")
                for i in range(150)
            ]
            p += [vlog("DEBUG", "req_id=cmpl-healthy transfer_id=xfer-fault")]
            d = [
                vlog("DEBUG", "req_id=cmpl-failed transfer_id=xfer-fault"),
                vlog("ERROR", "pulling kv_caches for {'cmpl-failed'} failed"),
            ]
            data = bundle(temp, p, d)
            entry = data["AI_CONTEXT.md"]
            self.assertIn("### request `cmpl-failed`", entry)
            failed = entry.split("### request `cmpl-failed`", 1)[1].split("\n### ", 1)[
                0
            ]
            self.assertIn("No literal-ID evidence from: prefill", failed)
            self.assertNotIn("- prefill:", failed)
            self.assertIn("`cmpl-failed` -> `xfer-fault`", entry)
            self.assertIn("summary is limited", entry)

    def test_prefixes_and_non_connector_fields_do_not_correlate(self):
        with tempfile.TemporaryDirectory() as temp:
            data = bundle(
                temp,
                [vlog("DEBUG", "req_id=cmpl-one")],
                [
                    vlog("DEBUG", "req_id=cmpl-one-extra"),
                    vlog("ERROR", "req_id=cmpl-untrusted", file="application.py"),
                ],
            )
            entry = data["AI_CONTEXT.md"]
            one = entry.split("### request `cmpl-one`", 1)[1].split("\n### ", 1)[0]
            self.assertIn("No literal-ID evidence from: decode", one)
            self.assertNotIn("### request `cmpl-untrusted`", entry)

    def test_default_output_guidance_without_local_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "p.log"
            path.write_text(vlog("INFO", "ready") + "\n", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, m.__file__, "bundle", "--prefill-log", str(path)],
                cwd=temp,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("issues/new/choose", result.stdout)
            self.assertIn("AI_CONTEXT.md", result.stdout)
            outputs = list(Path(temp).glob("*.zip"))
            self.assertEqual(len(outputs), 1)
            with zipfile.ZipFile(outputs[0]) as archive:
                env = archive.read("mooncake-incident/env.json")
                self.assertEqual(json.loads(env), [])

    def test_existing_outputs_are_unchanged_and_failed_publish_rolls_back(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "ready.txt"
            source.write_text("ready", encoding="utf-8")
            output, mapping = (
                str(Path(temp) / "incident.zip"),
                str(Path(temp) / "incident.map.json"),
            )
            Path(output).write_bytes(b"OLD_ZIP")
            with self.assertRaises(FileExistsError):
                m.write_archive(temp, output, mapping, {})
            self.assertEqual(Path(output).read_bytes(), b"OLD_ZIP")
            self.assertFalse(Path(mapping).exists())
            Path(output).unlink()
            Path(mapping).write_text("OLD_MAP", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                m.write_archive(temp, output, mapping, {})
            self.assertFalse(Path(output).exists())
            self.assertEqual(Path(mapping).read_text(), "OLD_MAP")
            Path(mapping).unlink()
            with patch.object(
                m.zipfile.ZipFile, "write", side_effect=OSError("disk full")
            ):
                with self.assertRaises(OSError):
                    m.write_archive(temp, output, mapping, {})
            self.assertFalse(Path(output).exists())
            self.assertFalse(Path(mapping).exists())
            self.assertEqual(source.read_text(), "ready")

    def test_bundle_privacy_filenames_zip_metadata_and_repeated_identities(self):
        with tempfile.TemporaryDirectory() as temp:
            user = "incidentoperator"
            path = Path(temp) / f"prefill-{user}_worker.log"
            path.write_text(
                vlog("ERROR", f"path=/tmp/work-{user}-run peer=10.1.2.3") + "\n",
                encoding="utf-8",
            )
            snap = Path(temp) / "snapshot.json"
            snap.write_text(
                json.dumps(
                    {
                        "host": {"user": user, "hostname": "internal-host"},
                        "env": {"vars": {"MC_TENT_CONF": '{"bucket":"BUCKET_CANARY"}'}},
                        "process": {
                            "cmdline": [
                                "vllm",
                                "--api-key",
                                "CLI_CANARY",
                                "--port",
                                "8000",
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            out = Path(temp) / "incident.zip"
            m.main(
                [
                    "bundle",
                    "--prefill-log",
                    str(path),
                    "--snapshot",
                    str(snap),
                    "-o",
                    str(out),
                ]
            )
            with zipfile.ZipFile(out) as archive:
                self.assertIsNone(archive.testzip())
                blob = (
                    "\n".join(archive.namelist())
                    + "\n"
                    + "\n".join(archive.read(n).decode() for n in archive.namelist())
                )
                self.assertFalse(
                    any(n.endswith(".map.json") for n in archive.namelist())
                )
                for info in archive.infolist():
                    self.assertEqual(info.extra, b"")
                    self.assertNotIn("..", info.filename.split("/"))
                for secret in (
                    user,
                    "internal-host",
                    "10.1.2.3",
                    "CLI_CANARY",
                    "BUCKET_CANARY",
                ):
                    self.assertNotIn(secret, blob)
                self.assertIn("work-user-1-run", blob)
            self.assertEqual(stat.S_IMODE(out.stat().st_mode), 0o600)
            self.assertEqual(
                stat.S_IMODE(out.with_suffix(".map.json").stat().st_mode), 0o600
            )

    def test_gzip_carriage_returns_and_duplicate_names_keep_source_lines(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / "p.log.gz"
            with gzip.open(p, "wb") as stream:
                stream.write(b"progress1\rprogress2\nready\n")
            out = Path(temp) / "incident.zip"
            m.main(
                [
                    "bundle",
                    "--prefill-log",
                    str(p),
                    "--prefill-log",
                    str(p),
                    "-o",
                    str(out),
                ]
            )
            with zipfile.ZipFile(out) as archive:
                logs = [n for n in archive.namelist() if "/logs/" in n]
                self.assertEqual(len(logs), 2)
                for name in logs:
                    self.assertFalse(name.endswith(".gz"))
                    text = archive.read(name).decode()
                    self.assertNotIn("\r", text)
                    self.assertEqual(text, "1\tprogress1\\rprogress2\n2\tready\n")

    def test_directory_rejected_instead_of_collecting_private_map(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(SystemExit, "explicit log file"):
                m.main(
                    ["bundle", "--prefill-log", temp, "-o", str(Path(temp) / "out.zip")]
                )

    def test_note_fields_keep_checks_and_remove_nested_secret_forms(self):
        notes = """proxy output: 504
## Intervention
Stopped P at 09:00
"api_key": ["LIST_CANARY"]
VLLM_API_KEY=ENV_CANARY
password：FULLWIDTH_CANARY
## Checks
ret=-1
"""
        redacted = m.redact_case(notes, m.Redactor())
        self.assertNotIn("CANARY", redacted)
        self.assertIn("Stopped P", redacted)
        self.assertIn("ret=-1", redacted)


if __name__ == "__main__":
    unittest.main()
