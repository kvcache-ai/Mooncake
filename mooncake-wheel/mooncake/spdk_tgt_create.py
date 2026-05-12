#!/usr/bin/env python3
# Tool to remotely create SPDK targets on multiple nodes
# Usage: python3 -m mooncake.spdk_tgt_create --spdk_target_info="ip:192.168.65.56 path:/home/zwt/spdk pci:0000:01:00.0,0000:02:00.0" --spdk_target_info="ip:192.168.65.57 path:/home/spdk pci:0000:01:00.0"

import argparse
import logging
import paramiko
import time
from typing import List, Dict, Any


class SPDKTgtCreator:
    """
    Remotely creates SPDK targets on multiple nodes via SSH.
    """

    def __init__(self, spdk_targets: List[str]):
        self.spdk_targets = spdk_targets
        self._setup_logging()
        self.target_configs = self._parse_spdk_targets()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _parse_spdk_targets(self) -> List[Dict[str, Any]]:
        """
        Parse SPDP target information from command line arguments.
        Format: "ip:<ip> path:<spdk_path> pci:<pci1> <pci2> ..."
        """
        target_configs = []

        for target_info in self.spdk_targets:
            target = {
                'ip': None,
                'path': None,
                'pci_devices': []
            }

            # Split the target info by spaces
            parts = target_info.split()

            # Simple state machine to parse the target info
            state = None  # Can be 'ip', 'path', or 'pci'

            for part in parts:
                if ':' in part:
                    # This is a key-value pair
                    key, value = part.split(':', 1)
                    key = key.strip()
                    value = value.strip()

                    if key == 'ip':
                        target['ip'] = value
                        state = 'ip'
                    elif key == 'path':
                        target['path'] = value
                        state = 'path'
                    elif key == 'pci':
                        # Parse PCI devices separated by commas
                        if value:
                            # Split by commas and strip whitespace
                            pci_list = [dev.strip() for dev in value.split(',')]
                            target['pci_devices'].extend(pci_list)
                        state = 'pci'
                else:
                    # This is a continuation of the current state
                    if state == 'path':
                        # Path might contain spaces (unlikely but possible)
                        target['path'] += ' ' + part
                    # Ignore other continuations

            # Validate required fields
            if not target['ip']:
                raise ValueError("Each spdk_target_info must contain 'ip' field")
            if not target['path']:
                raise ValueError("Each spdk_target_info must contain 'path' field")
            if not target['pci_devices']:
                raise ValueError("Each spdk_target_info must contain at least one 'pci' device")

            target_configs.append(target)
            self.logger.info(f"Parsed target: IP={target['ip']}, Path={target['path']}, PCI devices={target['pci_devices']}")

        return target_configs

    def _ssh_connect(self, ip: str, username: str = 'root', password: str = None, key_file: str = None) -> paramiko.SSHClient:
        """
        Establish an SSH connection to the target host.
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if key_file:
                self.logger.info(f"Connecting to {ip} using key file {key_file}")
                ssh.connect(ip, username=username, key_filename=key_file)
            else:
                self.logger.info(f"Connecting to {ip} using password authentication")
                ssh.connect(ip, username=username, password=password)
            return ssh
        except Exception as e:
            self.logger.error(f"Failed to connect to {ip}: {e}")
            raise

    def _execute_command(self, ssh: paramiko.SSHClient, command: str, working_dir: str = None, sudo: bool = False, log_errors: bool = True) -> tuple:
        """
        Execute a command on the remote host via SSH.
        """
        if working_dir:
            command = f"cd {working_dir} && {command}"

        if sudo:
            command = f"sudo {command}"

        self.logger.debug(f"Executing command: {command}")

        stdin, stdout, stderr = ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')

        if output:
            self.logger.debug(f"Command output: {output}")
        if error:
            self.logger.debug(f"Command error: {error}")
        if exit_status != 0:
            if log_errors:
                self.logger.error(f"Command failed with exit code {exit_status}: {command}")
                self.logger.error(f"Error output: {error}")
            raise RuntimeError(f"Command execution failed: {error}")

        return output, error

    def _start_spdk_tgt(self, ssh: paramiko.SSHClient, spdk_path: str) -> None:
        """
        Start the SPDK NVMF target service in the background.
        """
        # Check if tgt is already running
        try:
            self._execute_command(ssh, "pgrep -x nvmf_tgt", log_errors=False)
            self.logger.info("SPDK tgt service is already running, stopping it first")
            self._execute_command(ssh, "pkill -x nvmf_tgt")
            time.sleep(2)  # Give it time to stop
        except RuntimeError:
            self.logger.debug("SPDK tgt service is not running, will start it")

        # Start tgt in the background using absolute path
        self.logger.info("Starting SPDK tgt service")
        tgt_binary = f"{spdk_path}/build/bin/nvmf_tgt"
        log_file = f"{spdk_path}/tgt.log"
        self._execute_command(ssh, f"{tgt_binary} -m 0xff > {log_file} 2>&1 &")
        time.sleep(3)  # Give it time to start

    def _setup_spdk(self, ssh: paramiko.SSHClient, spdk_path: str, pci_devices: List[str]) -> None:
        """
        Setup SPDK with the specified PCI devices.
        """
        self.logger.info(f"Setting up SPDK with PCI devices: {', '.join(pci_devices)}")
        pci_allowed = ' '.join(pci_devices)
        setup_script = f"{spdk_path}/scripts/setup.sh"
        self._execute_command(ssh, f"PCI_ALLOWED='{pci_allowed}' {setup_script}", sudo=True)

    def _create_transport(self, ssh: paramiko.SSHClient, spdk_path: str) -> None:
        """
        Create RDMA transport for SPDK.
        """
        self.logger.info("Creating RDMA transport")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        self._execute_command(ssh, f"{rpc_script} nvmf_create_transport -t RDMA -q 128 -m 127 -c 4096 -i 131072 -u 131072 -a 128 -n 4096 -b 32")

    def _create_bdevs(self, ssh: paramiko.SSHClient, spdk_path: str, pci_devices: List[str]) -> List[str]:
        """
        Create block devices for the specified PCI devices.
        """
        bdevs = []
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        for i, pci in enumerate(pci_devices):
            bdev_name = f"Nvme{i}"
            self.logger.info(f"Creating bdev {bdev_name} for PCI {pci}")
            self._execute_command(ssh, f"{rpc_script} bdev_nvme_attach_controller -b {bdev_name} -t PCIe -a {pci}")
            bdevs.append(f"{bdev_name}n1")
        return bdevs

    def _create_subsystem(self, ssh: paramiko.SSHClient, spdk_path: str) -> str:
        """
        Create an NVMF subsystem.
        """
        subsystem_nqn = "nqn.2016-06.io.spdk:cnode1"
        self.logger.info(f"Creating subsystem {subsystem_nqn}")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        self._execute_command(ssh, f"{rpc_script} nvmf_create_subsystem {subsystem_nqn} -a -s SPDK00000000000001 -m 12")
        return subsystem_nqn

    def _add_namespaces(self, ssh: paramiko.SSHClient, spdk_path: str, subsystem_nqn: str, bdevs: List[str]) -> None:
        """
        Add namespaces to the subsystem.
        """
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        for bdev in bdevs:
            self.logger.info(f"Adding namespace {bdev} to subsystem {subsystem_nqn}")
            self._execute_command(ssh, f"{rpc_script} nvmf_subsystem_add_ns {subsystem_nqn} {bdev}")

    def _add_listener(self, ssh: paramiko.SSHClient, spdk_path: str, subsystem_nqn: str, ip: str) -> None:
        """
        Add RDMA listener to the subsystem.
        """
        self.logger.info(f"Adding RDMA listener on {ip}:4420")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        self._execute_command(ssh, f"{rpc_script} nvmf_subsystem_add_listener {subsystem_nqn} -t RDMA -a {ip} -s 4420")

    def deploy_target(self, target_config: Dict[str, Any]) -> bool:
        """
        Deploy SPDK target on a single node.
        """
        ip = target_config['ip']
        spdk_path = target_config['path']
        pci_devices = target_config['pci_devices']

        self.logger.info(f"Deploying SPDK target on {ip}")

        try:
            # Establish SSH connection
            ssh = self._ssh_connect(ip)

            try:
                # Setup SPDK
                self._setup_spdk(ssh, spdk_path, pci_devices)

                # Start tgt service
                self._start_spdk_tgt(ssh, spdk_path)

                # Create transport
                self._create_transport(ssh, spdk_path)

                # Create bdevs
                bdevs = self._create_bdevs(ssh, spdk_path, pci_devices)

                # Create subsystem
                subsystem_nqn = self._create_subsystem(ssh, spdk_path)

                # Add namespaces
                self._add_namespaces(ssh, spdk_path, subsystem_nqn, bdevs)

                # Add listener
                self._add_listener(ssh, spdk_path, subsystem_nqn, ip)

                self.logger.info(f"Successfully deployed SPDK target on {ip}")
                return True
            finally:
                ssh.close()
        except Exception as e:
            self.logger.error(f"Failed to deploy SPDK target on {ip}: {e}")
            return False

    def deploy_all_targets(self) -> bool:
        """
        Deploy SPDK targets on all specified nodes.
        """
        success_count = 0
        total_count = len(self.target_configs)

        for i, target_config in enumerate(self.target_configs):
            self.logger.info(f"=== Deploying target {i+1}/{total_count} ===")
            if self.deploy_target(target_config):
                success_count += 1

        self.logger.info("=== Deployment Summary ===")
        self.logger.info(f"Total targets: {total_count}")
        self.logger.info(f"Successfully deployed: {success_count}")
        self.logger.info(f"Failed: {total_count - success_count}")

        return success_count == total_count


def parse_arguments():
    parser = argparse.ArgumentParser(description='SPDK Target Creator Tool')
    parser.add_argument('--spdk_target_info', action='append',
                        help='SPDK target information (e.g., "ip:192.168.65.56 path:/home/zwt pci:0000:01:00.0 0000:02:00.0")',
                        required=True)
    parser.add_argument('--username', type=str, default='root',
                        help='SSH username for target nodes (default: root)')
    parser.add_argument('--password', type=str,
                        help='SSH password for target nodes')
    parser.add_argument('--key-file', type=str,
                        help='SSH private key file path')
    return parser.parse_args()


def main():
    args = parse_arguments()

    try:
        creator = SPDKTgtCreator(args.spdk_target_info)
        success = creator.deploy_all_targets()
        exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
