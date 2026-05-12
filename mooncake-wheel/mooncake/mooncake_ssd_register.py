#!/usr/bin/env python3
# python3 -m mooncake.mooncake_ssd_register --master_server_address=192.168.65.81:50051 --spdk_target_info="ip:192.168.65.56 path:/home/spdk" --spdk_target_info="ip:192.168.65.57 path:/root/spdk"

import argparse
import json
import logging
import time
import re
from typing import List, Dict, Any

import paramiko

from mooncake.store import MooncakeDistributedNoFRegister
from mooncake.mooncake_config import MooncakeConfig


class MooncakeNoFRegister:
    """
    Registers SSDs from remote SPDK targets to Mooncake master server.
    """

    def __init__(self, cli_config: dict = None, spdk_targets: List[str] = None):
        self.register = None
        self.config_list: List[Dict[str, Any]] = []
        self.cli_config = cli_config or {}
        self.spdk_targets = spdk_targets or []
        self._setup_logging()

        try:
            # Only support getting SSD info from remote SPDK targets
            if self.spdk_targets:
                # Get SSD info from remote SPDK targets
                master_server_address = self.cli_config.get('master_server_address')
                if not master_server_address:
                    raise ValueError("master_server_address is required when using spdk_target_info")
                self.config_list = self._get_remote_ssd_info(master_server_address)
            else:
                raise ValueError("spdk_target_info is required")

            # Apply CLI overrides to every config (if key exists)
            for config in self.config_list:
                for key, value in self.cli_config.items():
                    if key in config:
                        # Convert trsvcid/nsid to int if needed
                        if key in ("trsvcid", "nsid"):
                            try:
                                config[key] = int(value)
                            except ValueError:
                                logging.warning(f"Invalid integer for {key}: {value}")
                        else:
                            config[key] = value

            # Remove duplicate SSDs based on their unique identifiers
            self._remove_duplicate_ssds()

            logging.info("Loaded %d SSD configuration(s)", len(self.config_list))
        except Exception as e:
            logging.error("Configuration load failed: %s", e)
            raise



    def _remove_duplicate_ssds(self):
        """
        Remove duplicate SSD configurations from the config list
        """
        unique_configs = []
        seen_keys = set()

        for config in self.config_list:
            # Generate unique key directly without calling _get_ssd_unique_key
            key = (config['nqn'], config['nsid'], config['traddr'], config['trsvcid'])
            if key not in seen_keys:
                seen_keys.add(key)
                unique_configs.append(config)

        if len(unique_configs) < len(self.config_list):
            logging.info(f"Removed {len(self.config_list) - len(unique_configs)} duplicate SSD configuration(s)")

        self.config_list = unique_configs



    def _parse_spdk_target_info(self, target_info: str) -> Dict[str, str]:
        """
        Parse spdk target info string like "ip:192.168.65.56 path:/home"
        """
        result = {}
        parts = re.findall(r'(\w+):([^\s]+)', target_info)
        for key, value in parts:
            result[key] = value
        return result

    def _execute_ssh_command(self, ip: str, command: str, path: str) -> str:
        """
        Execute command on remote server via SSH
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # Connect with default key and user
            ssh.connect(ip, port=22, username='root', timeout=10)

            # Try multiple possible paths to find the RPC script
            possible_paths = [
                path,  # Direct path provided by user
                f"{path}/spdk"  # Common case: spdk is a subdirectory
            ]

            for test_path in possible_paths:
                full_command = f"cd {test_path} && ls -la scripts/rpc.py"
                stdin, stdout, stderr = ssh.exec_command(full_command, timeout=5)
                error = stderr.read().decode('utf-8')
                if not error:
                    # Found the script, execute the actual command
                    full_command = f"cd {test_path} && {command}"
                    stdin, stdout, stderr = ssh.exec_command(full_command, timeout=30)
                    output = stdout.read().decode('utf-8')
                    error = stderr.read().decode('utf-8')
                    if error:
                        logging.error(f"SSH command error on {ip}: {error}")
                        raise RuntimeError(f"SSH command failed: {error}")
                    return output

            # If we get here, none of the paths worked
            raise RuntimeError(f"Could not find scripts/rpc.py in any of the possible paths: {possible_paths}")
        finally:
            ssh.close()

    def _get_remote_ssd_info(self, master_server_address: str) -> List[Dict[str, Any]]:
        """
        Get SSD info from remote SPDK targets
        """
        ssd_configs = []

        for target_info in self.spdk_targets:
            target = self._parse_spdk_target_info(target_info)
            ip = target.get('ip')
            path = target.get('path')

            if not ip or not path:
                logging.error(f"Invalid target info: {target_info}")
                continue

            logging.info(f"Getting SSD info from target: {ip} (path: {path})")

            try:
                # Get subsystems info
                subsystems_cmd = "./scripts/rpc.py nvmf_get_subsystems"
                subsystems_output = self._execute_ssh_command(ip, subsystems_cmd, path)
                subsystems = json.loads(subsystems_output)

                # Process each subsystem
                for subsystem in subsystems:
                    if subsystem.get('subtype') != 'NVMe':
                        continue

                    nqn = subsystem.get('nqn')
                    listen_addresses = subsystem.get('listen_addresses', [])

                    if not nqn or not listen_addresses:
                        continue

                    # Get transport info from first listen address
                    traddr = listen_addresses[0].get('traddr')
                    trsvcid = listen_addresses[0].get('trsvcid')

                    if not traddr or not trsvcid:
                        continue

                    # Process each namespace
                    namespaces = subsystem.get('namespaces', [])
                    for namespace in namespaces:
                        nsid = namespace.get('nsid')
                        bdev_name = namespace.get('bdev_name')

                        if not nsid or not bdev_name:
                            continue

                        # Get bdev info to calculate size
                        bdev_cmd = f"./scripts/rpc.py bdev_get_bdevs -b {bdev_name}"
                        bdev_output = self._execute_ssh_command(ip, bdev_cmd, path)
                        bdevs = json.loads(bdev_output)

                        if not bdevs:
                            continue

                        bdev = bdevs[0]
                        block_size = bdev.get('block_size', 512)
                        num_blocks = bdev.get('num_blocks', 0)
                        size = block_size * num_blocks

                        # Create SSD config
                        ssd_config = {
                            'nqn': nqn,
                            'nsid': nsid,
                            'traddr': traddr,
                            'trsvcid': int(trsvcid),  # Ensure trsvcid is integer
                            'base': 0,
                            'size': size,
                            'master_server_address': master_server_address,
                            'metadata_server': ''
                        }

                        ssd_configs.append(ssd_config)
                        logging.info(f"Found SSD: nqn={nqn}, nsid={nsid}, traddr={traddr}, size={size}")

            except Exception as e:
                logging.error(f"Failed to get SSD info from {ip}: {e}")
                continue

        if not ssd_configs:
            raise RuntimeError("No SSD information found from remote targets")

        return ssd_configs

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def start_ssd_service(self):
        success_count = 0
        skipped_count = 0
        failed_count = 0
        total = len(self.config_list)

        for i, cfg in enumerate(self.config_list):
            try:
                logging.info("Registering SSD %d/%d: nqn=%s, traddr=%s", i + 1, total, cfg.get("nqn"), cfg.get("traddr"))

                # Create register instance and register SSD
                self.register = MooncakeDistributedNoFRegister()
                ret = self.register.real_register(
                    cfg["nqn"],
                    cfg["nsid"],
                    cfg["traddr"],
                    cfg["trsvcid"],
                    cfg["base"],
                    cfg["size"],
                    cfg["master_server_address"]
                )

                if ret != 0:
                    raise RuntimeError(f"Registration failed with code {ret}")

                logging.info("Register SSD %d/%d succeeded", i + 1, total)
                success_count += 1

            except Exception as e:
                # Check if the error is due to the segment already existing on the server
                if "SEGMENT_ALREADY_EXISTS" in str(e) or "segment already exists" in str(e):
                    logging.info("SSD %d/%d (nqn=%s, traddr=%s) already registered on server, skipping",
                                i + 1, total, cfg.get("nqn"), cfg.get("traddr"))
                    skipped_count += 1
                else:
                    logging.error("Failed to register SSD %d/%d: %s", i + 1, total, e)
                    failed_count += 1

        # Summary
        logging.info("SSD registration summary:")
        logging.info("- Total SSDs: %d", total)
        logging.info("- Successfully registered: %d", success_count)
        logging.info("- Already registered (skipped): %d", skipped_count)
        logging.info("- Failed: %d", failed_count)

        # Return success if all SSDs were either registered or already existed
        return failed_count == 0


def parse_arguments():
    parser = argparse.ArgumentParser(description='Mooncake SSD Register with REST API')
    parser.add_argument('--master_server_address', type=str,
                        help='Master server address (e.g., 192.168.65.81:50051)',
                        required=True)
    parser.add_argument('--spdk_target_info', action='append',
                        help='SPDK target information (e.g., "ip:192.168.65.56 path:/home")',
                        required=True)
    parser.add_argument('-D', '--define', action='append',
                        help='Override configuration fields globally (e.g., -Dtrsvcid=4420)',
                        default=[])
    return parser.parse_args()


def main():
    args = parse_arguments()

    cli_config = {}
    for item in args.define:
        if '=' in item:
            key, value = item.split('=', 1)
            cli_config[key] = value
        else:
            logging.warning(f"Ignoring invalid CLI config: {item}")

    # Add master_server_address to cli_config if provided
    if args.master_server_address:
        cli_config['master_server_address'] = args.master_server_address

    register = MooncakeNoFRegister(cli_config, args.spdk_target_info)
    success = register.start_ssd_service()
    if not success:
        exit(1)

if __name__ == "__main__":
    main()
