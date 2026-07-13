"""
Mooncake Store 基础环境启动脚本

启动 mooncake_master 和 store client，
为 EPD 分离推理提供分布式 KV Cache 存储。
"""

import os
import sys
import time
import subprocess
import signal
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MooncakeEnvironment:
    """
    Mooncake 基础环境管理

    负责启动和管理：
    1. etcd 元数据服务（可选）
    2. mooncake_master 主服务
    3. store client 节点
    """

    def __init__(
        self,
        master_port: int = 50051,
        metadata_port: int = 8080,
        protocol: str = "tcp",
        global_segment_size: str = "16gb",
        local_buffer_size: str = "4gb",
    ):
        self.master_port = master_port
        self.metadata_port = metadata_port
        self.protocol = protocol
        self.global_segment_size = global_segment_size
        self.local_buffer_size = local_buffer_size
        self._processes = []

    def start_master(
        self,
        enable_http_metadata: bool = True,
    ) -> Optional[subprocess.Popen]:
        """启动 mooncake_master"""
        cmd = [
            "mooncake_master",
            f"--rpc_port={self.master_port}",
            f"--enable_http_metadata_server={'true' if enable_http_metadata else 'false'}",
            f"--http_metadata_server_port={self.metadata_port}",
            "--http_metadata_server_host=0.0.0.0",
        ]

        logger.info(f"Starting mooncake_master: {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._processes.append(proc)
            time.sleep(2)
            if proc.poll() is None:
                logger.info(f"mooncake_master started (PID: {proc.pid})")
                return proc
            else:
                logger.error("mooncake_master failed to start")
                return None
        except FileNotFoundError:
            logger.warning(
                "mooncake_master not found in PATH. "
                "Install via 'pip install mooncake-transfer-engine' and "
                "run 'sudo make install' after building from source."
            )
            return None

    def start_store_client(
        self,
        local_hostname: str = "localhost",
        device: str = "cuda:0",
    ) -> Optional[subprocess.Popen]:
        """启动 store client 服务"""
        env = os.environ.copy()
        env["MOONCAKE_MASTER"] = f"127.0.0.1:{self.master_port}"
        env["MOONCAKE_TE_META_DATA_SERVER"] = (
            f"http://127.0.0.1:{self.metadata_port}/metadata"
        )
        env["MOONCAKE_PROTOCOL"] = self.protocol
        env["MOONCAKE_GLOBAL_SEGMENT_SIZE"] = self.global_segment_size
        env["MOONCAKE_LOCAL_BUFFER_SIZE"] = self.local_buffer_size
        env["MOONCAKE_LOCAL_HOSTNAME"] = local_hostname

        cmd = [
            sys.executable, "-m", "mooncake.mooncake_store_service",
        ]

        logger.info(f"Starting store client: {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(cmd, env=env)
            self._processes.append(proc)
            time.sleep(2)
            if proc.poll() is None:
                logger.info(f"Store client started (PID: {proc.pid})")
                return proc
            else:
                logger.error("Store client failed to start")
                return None
        except Exception as e:
            logger.warning(f"Store client start failed: {e}")
            return None

    def start_etcd(self) -> Optional[subprocess.Popen]:
        """启动 etcd 元数据服务"""
        cmd = [
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://localhost:2379",
        ]

        logger.info(f"Starting etcd: {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._processes.append(proc)
            time.sleep(2)
            if proc.poll() is None:
                logger.info(f"etcd started (PID: {proc.pid})")
                return proc
            else:
                logger.error("etcd failed to start")
                return None
        except FileNotFoundError:
            logger.warning("etcd not found. Install with: sudo apt install etcd")
            return None

    def start_all(
        self,
        with_etcd: bool = False,
        local_hostname: str = "localhost",
    ):
        """启动所有服务"""
        logger.info("Starting Mooncake environment...")

        if with_etcd:
            self.start_etcd()

        self.start_master()
        self.start_store_client(local_hostname=local_hostname)

        logger.info("Mooncake environment started")

    def stop_all(self):
        """停止所有服务"""
        for proc in reversed(self._processes):
            if proc.poll() is None:
                logger.info(f"Stopping process {proc.pid}")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self._processes.clear()
        logger.info("All services stopped")

    def health_check(self) -> dict:
        """检查服务健康状态"""
        import requests

        health = {"master": False, "metadata": False}

        try:
            r = requests.get(
                f"http://localhost:9003/metrics/summary", timeout=2
            )
            health["master"] = r.status_code == 200
        except Exception:
            pass

        try:
            r = requests.get(
                f"http://localhost:{self.metadata_port}/metadata", timeout=2
            )
            health["metadata"] = r.status_code == 200
        except Exception:
            pass

        return health

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop_all()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    env = MooncakeEnvironment(protocol="tcp")

    def signal_handler(sig, frame):
        env.stop_all()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    env.start_all()

    # 健康检查
    time.sleep(3)
    health = env.health_check()
    print(f"\nService health: {health}")

    if all(health.values()):
        print("Mooncake environment is ready!")
    else:
        print("Some services failed to start. Check logs above.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        env.stop_all()


if __name__ == "__main__":
    main()
