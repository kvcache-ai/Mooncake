"""Real-etcd tests for the c-shared wrapper, including Python environment updates.

From mooncake-common/etcd, run:

    go build -buildmode=c-shared -o /tmp/libetcd_wrapper.so etcd_wrapper.go
    MOONCAKE_TEST_ETCD=/path/to/etcd \
        MOONCAKE_TEST_ETCD_WRAPPER=/tmp/libetcd_wrapper.so python test_etcd_tls.py -v

Each probe runs in a separate process because Go clients are process-global.
"""

import ctypes as C
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest


TLS_ENV = (
    "MOONCAKE_ETCD_CA_CERT",
    "MOONCAKE_ETCD_CLIENT_CERT",
    "MOONCAKE_ETCD_CLIENT_KEY",
    "MOONCAKE_ETCD_SERVER_NAME",
)
STR, INT, I64, PTR = C.c_char_p, C.c_int, C.c_int64, C.c_void_p


class Wrapper:
    def __init__(self, path):
        self.lib = C.CDLL(path)
        self.free = C.CDLL(None).free
        self.free.argtypes = [PTR]
        self.free.restype = None
        # Enter Go before changing Python's environment: this ensures that the
        # runtime has already taken its environment snapshot for this library.
        self.lib.EtcdCloseWrapper.argtypes = []
        self.lib.EtcdCloseWrapper.restype = None
        self.lib.EtcdCloseWrapper()

    def call(self, name, types, *args, expect=0):
        fn = getattr(self.lib, name)
        fn.argtypes = [*types, C.POINTER(PTR)]
        fn.restype = INT
        error = PTR()
        result = fn(*args, C.byref(error))
        message = C.string_at(error.value).decode() if error.value else ""
        if error.value:
            self.free(error)
        if expect is not None and result != expect:
            raise AssertionError(f"{name}: result={result}, error={message}")
        return result

    def read(self, name, key, sized):
        value, size, revision = PTR(), INT(), I64()
        if sized:
            self.call(
                name,
                [STR, INT, C.POINTER(PTR), C.POINTER(INT), C.POINTER(I64)],
                key,
                len(key),
                C.byref(value),
                C.byref(size),
                C.byref(revision),
            )
        else:
            self.call(name, [STR, C.POINTER(PTR)], key, C.byref(value))
        try:
            return C.string_at(value, size.value) if sized else C.string_at(value)
        finally:
            self.free(value)


def probe():
    if not __debug__:
        raise RuntimeError(
            "etcd TLS probes require assertions; disable PYTHONOPTIMIZE and -O"
        )
    settings = json.loads(os.environ["MOONCAKE_TEST_SETTINGS"])
    wrapper = Wrapper(os.environ["MOONCAKE_TEST_ETCD_WRAPPER"])
    # These values deliberately become visible only AFTER loading/entering Go.
    os.environ.update(settings["env"])
    endpoint = settings["endpoint"].encode()
    expected = settings.get("failure")
    if settings.get("recover_initialization"):
        os.environ[TLS_ENV[0]] = str(
            Path(settings["env"][TLS_ENV[0]]).with_name("missing.pem")
        )
        for name in (
            "NewEtcdClient",
            "NewStoreEtcdClient",
            "NewSnapshotEtcdClient",
        ):
            assert wrapper.call(name, [STR], endpoint, expect=None) < 0, name
        os.environ.update(settings["env"])
    if settings.get("recover_handshake"):
        os.environ[TLS_ENV[3]] = "wrong.test"
        wrapper.call("NewEtcdClient", [STR], endpoint)
        assert (
            wrapper.call(
                "EtcdPutWrapper", [STR, STR], b"/tls/recovery", b"value", expect=None
            )
            < 0
        )
        wrapper.lib.EtcdCloseWrapper()
        os.environ.update(settings["env"])
    if expected == "initialization":
        for name in (
            "NewEtcdClient",
            "NewStoreEtcdClient",
            "NewSnapshotEtcdClient",
            "EtcdStoreResetClientWrapper",
        ):
            assert wrapper.call(name, [STR], endpoint, expect=None) < 0, name
        return

    wrapper.call("NewEtcdClient", [STR], endpoint)
    key, value = b"/tls/plain-value", b"test value"
    if expected == "handshake":
        assert wrapper.call("EtcdPutWrapper", [STR, STR], key, value, expect=None) < 0
        return
    wrapper.call("EtcdPutWrapper", [STR, STR], key, value)
    assert wrapper.read("EtcdGetWrapper", key, False) == value
    wrapper.call("EtcdDeleteWrapper", [STR], key)

    wrapper.call("NewStoreEtcdClient", [STR], endpoint)
    wrapper.call("NewSnapshotEtcdClient", [STR], endpoint)
    key, value = b"/tls/binary-value", b"value\x00with binary data"
    for put, get in (
        ("EtcdStorePutWrapper", "EtcdStoreGetWrapper"),
        ("SnapshotStorePutWrapper", "SnapshotStoreGetWrapper"),
    ):
        wrapper.call(put, [STR, INT, STR, INT], key, len(key), value, len(value))
        assert wrapper.read(get, key, True) == value

    # A rejected reset must leave the existing, authenticated Store client usable.
    if settings["env"]:
        saved = os.environ.get(TLS_ENV[0], "")
        os.environ[TLS_ENV[0]] = "/nonexistent/mooncake-test-ca.pem"
        assert (
            wrapper.call("EtcdStoreResetClientWrapper", [STR], endpoint, expect=None)
            < 0
        )
        assert wrapper.read("EtcdStoreGetWrapper", key, True) == value
        os.environ[TLS_ENV[0]] = saved

    # Establish a real server-side prefix watch, then deliver a value over TLS.
    received = threading.Event()
    callback_type = C.CFUNCTYPE(None, PTR, PTR, C.c_size_t, PTR, C.c_size_t, INT, I64)

    @callback_type
    def callback(_ctx, _key, _key_size, data, data_size, _event, _revision):
        if data and C.string_at(data, data_size) == b"watch event":
            received.set()

    prefix = b"/tls/watch/"
    wrapper.call(
        "EtcdStoreWatchWithPrefixFromRevisionWrapper",
        [STR, INT, I64, PTR, PTR],
        prefix,
        len(prefix),
        0,
        None,
        C.cast(callback, PTR),
    )
    wrapper.call("EtcdPutWrapper", [STR, STR], prefix + b"key", b"watch event")
    assert received.wait(5), "prefix watch did not receive a value"
    wrapper.call(
        "EtcdStoreCancelWatchWithPrefixWrapper", [STR, INT], prefix, len(prefix)
    )
    wrapper.call(
        "EtcdStoreWaitWatchWithPrefixStoppedWrapper",
        [STR, INT, INT],
        prefix,
        len(prefix),
        5000,
    )

    # Exercise the dedicated keepalive connection and its independence from reset.
    lease = I64()
    wrapper.call("EtcdStoreGrantLeaseWrapper", [I64, C.POINTER(I64)], 3, C.byref(lease))
    keepalive_result = []
    thread = threading.Thread(
        target=lambda: keepalive_result.append(
            wrapper.call("EtcdStoreKeepAliveWrapper", [I64], lease.value, expect=None)
        ),
        daemon=True,
    )
    thread.start()
    wrapper.call("EtcdStoreWaitKeepAliveReadyWrapper", [I64, INT], lease.value, 5000)
    lease_key, revision = b"/tls/lease", I64()
    wrapper.call(
        "EtcdStoreCreateWithLeaseWrapper",
        [STR, INT, STR, INT, I64, C.POINTER(I64)],
        lease_key,
        len(lease_key),
        value,
        len(value),
        lease.value,
        C.byref(revision),
    )
    if settings.get("rotate_client_cert"):
        root = Path(settings["env"][TLS_ENV[1]]).parent
        os.environ[TLS_ENV[1]] = str(root / "expired-client.pem")
        os.environ[TLS_ENV[2]] = str(root / "expired-client.key")
        # Reset validates certificate files eagerly, but the TLS handshake is
        # lazy. An expired, parseable certificate must fail the first operation.
        wrapper.call("EtcdStoreResetClientWrapper", [STR], endpoint)
        rotation_key = b"/tls/rotation"
        assert (
            wrapper.call(
                "EtcdStorePutWrapper",
                [STR, INT, STR, INT],
                rotation_key,
                len(rotation_key),
                value,
                len(value),
                expect=None,
            )
            < 0
        )
        os.environ[TLS_ENV[1]] = str(root / "rotated-client.pem")
        os.environ[TLS_ENV[2]] = str(root / "rotated-client.key")
    wrapper.call("EtcdStoreResetClientWrapper", [STR], endpoint)
    time.sleep(5)  # Longer than the original lease TTL: requires successful renewal.
    assert wrapper.read("EtcdStoreGetWrapper", lease_key, True) == value
    wrapper.call("EtcdStoreCancelKeepAliveWrapper", [I64], lease.value)
    thread.join(5)
    assert not thread.is_alive() and keepalive_result == [-2], keepalive_result
    wrapper.call("EtcdStoreRevokeLeaseWrapper", [I64], lease.value)
    wrapper.call("SnapshotStoreDeleteWrapper", [STR, INT, INT], b"/tls/", 5, 1)
    wrapper.lib.EtcdCloseWrapper()


class EtcdTLSIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.etcd = os.environ.get("MOONCAKE_TEST_ETCD") or shutil.which("etcd")
        cls.library = os.environ.get("MOONCAKE_TEST_ETCD_WRAPPER")
        if not cls.etcd or not cls.library:
            raise unittest.SkipTest(
                "set MOONCAKE_TEST_ETCD and MOONCAKE_TEST_ETCD_WRAPPER"
            )
        if not shutil.which("openssl"):
            raise RuntimeError("openssl is required to generate test certificates")
        temporary = tempfile.TemporaryDirectory(prefix="mooncake-etcd-tls-")
        cls.addClassCleanup(temporary.cleanup)
        cls.root = Path(temporary.name)

        def openssl(*args):
            subprocess.run(
                ["openssl", *args],
                cwd=cls.root,
                check=True,
                capture_output=True,
                timeout=20,
            )

        for name in ("ca", "untrusted"):
            openssl(
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                f"/CN={name}",
                "-keyout",
                f"{name}.key",
                "-out",
                f"{name}.pem",
            )
        for name, usage, days in (
            ("server", "serverAuth,clientAuth", "1"),
            ("client", "clientAuth", "1"),
            ("expired-client", "clientAuth", "-1"),
            ("wrong-eku-client", "serverAuth", "1"),
            ("rotated-client", "clientAuth", "1"),
        ):
            openssl(
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                f"/CN={name}",
                "-keyout",
                f"{name}.key",
                "-out",
                f"{name}.csr",
            )
            san = "DNS:etcd.test" if name == "server" else "IP:127.0.0.1"
            (cls.root / f"{name}.ext").write_text(
                f"extendedKeyUsage={usage}\nsubjectAltName={san}\n"
            )
            openssl(
                "x509",
                "-req",
                "-in",
                f"{name}.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-days",
                days,
                "-extfile",
                f"{name}.ext",
                "-out",
                f"{name}.pem",
            )
        cls.endpoints = {
            mode: cls.start_etcd(mode) for mode in ("plaintext", "tls", "mtls")
        }

    @classmethod
    def start_etcd(cls, mode):
        def free_port():
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                return sock.getsockname()[1]

        port, peer_port = free_port(), free_port()
        scheme = "http" if mode == "plaintext" else "https"
        endpoint, peer = f"{scheme}://127.0.0.1:{port}", f"http://127.0.0.1:{peer_port}"
        command = [
            cls.etcd,
            "--name",
            mode,
            "--data-dir",
            str(cls.root / mode),
            "--listen-client-urls",
            endpoint,
            "--advertise-client-urls",
            endpoint,
            "--listen-peer-urls",
            peer,
            "--initial-advertise-peer-urls",
            peer,
            "--initial-cluster",
            f"{mode}={peer}",
        ]
        context = None
        if mode != "plaintext":
            command += [
                "--cert-file",
                str(cls.root / "server.pem"),
                "--key-file",
                str(cls.root / "server.key"),
            ]
            context = ssl.create_default_context(cafile=str(cls.root / "ca.pem"))
            # Readiness uses the IP address; wrapper probes independently verify
            # the DNS-only certificate using MOONCAKE_ETCD_SERVER_NAME.
            context.check_hostname = False
            if mode == "mtls":
                command += [
                    "--client-cert-auth",
                    "--trusted-ca-file",
                    str(cls.root / "ca.pem"),
                ]
                context.load_cert_chain(
                    cls.root / "client.pem", cls.root / "client.key"
                )
        log = (cls.root / f"{mode}.log").open("w")
        cls.addClassCleanup(log.close)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def stop():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)

        cls.addClassCleanup(stop)
        deadline = time.monotonic() + 20
        while process.poll() is None and time.monotonic() < deadline:
            connection = (
                http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                if context is None
                else http.client.HTTPSConnection(
                    "127.0.0.1", port, context=context, timeout=1
                )
            )
            try:
                connection.request("GET", "/health")
                if connection.getresponse().status == 200:
                    return endpoint
            except (OSError, http.client.HTTPException):
                time.sleep(0.1)
            finally:
                connection.close()
        raise RuntimeError((cls.root / f"{mode}.log").read_text()[-3000:])

    def run_probe(self, mode, failure=None, overrides=None, bare=False, options=None):
        env = {key: value for key, value in os.environ.items() if key not in TLS_ENV}
        config = {}
        if mode != "plaintext":
            config = {TLS_ENV[0]: str(self.root / "ca.pem"), TLS_ENV[3]: "etcd.test"}
            if mode == "mtls":
                config.update(
                    {
                        TLS_ENV[1]: str(self.root / "client.pem"),
                        TLS_ENV[2]: str(self.root / "client.key"),
                    }
                )
        config.update(overrides or {})
        endpoint = self.endpoints[mode]
        if bare:
            endpoint = endpoint.split("://", 1)[1]
        env["MOONCAKE_TEST_SETTINGS"] = json.dumps(
            {"env": config, "endpoint": endpoint, "failure": failure, **(options or {})}
        )
        env["MOONCAKE_TEST_ETCD_WRAPPER"] = self.library
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--probe"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_plaintext_baseline(self):
        self.run_probe("plaintext")

    def test_server_tls_ca_only(self):
        self.run_probe("tls")

    def test_mtls_after_library_load(self):
        self.run_probe("mtls")

    def test_mtls_bare_endpoint(self):
        self.run_probe("mtls", bare=True)

    def test_untrusted_ca(self):
        self.run_probe(
            "mtls", "handshake", {TLS_ENV[0]: str(self.root / "untrusted.pem")}
        )

    def test_hostname_mismatch(self):
        self.run_probe("mtls", "handshake", {TLS_ENV[3]: "wrong.test"})

    def test_missing_client_certificate(self):
        self.run_probe("mtls", "handshake", {TLS_ENV[1]: "", TLS_ENV[2]: ""})

    def test_expired_client_certificate(self):
        self.run_probe(
            "mtls",
            "handshake",
            {
                TLS_ENV[1]: str(self.root / "expired-client.pem"),
                TLS_ENV[2]: str(self.root / "expired-client.key"),
            },
        )

    def test_wrong_client_certificate_usage(self):
        self.run_probe(
            "mtls",
            "handshake",
            {
                TLS_ENV[1]: str(self.root / "wrong-eku-client.pem"),
                TLS_ENV[2]: str(self.root / "wrong-eku-client.key"),
            },
        )

    def test_initialization_failure_can_be_corrected(self):
        self.run_probe("mtls", options={"recover_initialization": True})

    def test_handshake_failure_can_be_corrected_after_close(self):
        self.run_probe("mtls", options={"recover_handshake": True})

    def test_reset_reloads_client_certificate_without_stopping_keepalive(self):
        self.run_probe("mtls", options={"rotate_client_cert": True})

    def test_http_cannot_disable_configured_tls(self):
        self.run_probe(
            "plaintext", "initialization", {TLS_ENV[0]: str(self.root / "ca.pem")}
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["--probe"]:
        probe()
    else:
        unittest.main()
