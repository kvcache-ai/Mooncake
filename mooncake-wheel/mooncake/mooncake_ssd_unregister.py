#!/usr/bin/env python3
# python3 -m mooncake.mooncake_ssd_unregister --master_server_address=192.168.65.81:50051 --spdk_target_info="ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1"

import argparse
import logging
import json
import re
import shlex
from typing import List, Dict, Any

import paramiko

from mooncake.store import MooncakeDistributedNoFRegister
from mooncake.mooncake_config import MooncakeConfig


class MooncakeNoFUnregister:
    """
    Unregisters SSDs from Mooncake master server by endpoint information.
    """

    def __init__(self, cli_config: dict = None, spdk_targets: List[str] = None):
        self.register = None
        self.config_list: List[Dict[str, Any]] = []
        self.cli_config = cli_config or {}
        self.spdk_targets = spdk_targets or []
        self._setup_logging()

        try:
            # Only support getting SSD info from command line
            if self.spdk_targets:
                # Get SSD info from command line parameters
                master_server_address = self.cli_config.get('master_server_address')
                if not master_server_address:
                    raise ValueError("master_server_address is required when using spdk_target_info")
                self.config_list = self._parse_spdk_targets(master_server_address)
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

            logging.info("Loaded %d SSD configuration(s) to unregister", len(self.config_list))
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
            # Generate unique key directly
            key = (config['nqn'], config['nsid'], config['traddr'], config['trsvcid'])
            if key not in seen_keys:
                seen_keys.add(key)
                unique_configs.append(config)

        if len(unique_configs) < len(self.config_list):
            logging.info(f"Removed {len(self.config_list) - len(unique_configs)} duplicate SSD configuration(s)")

        self.config_list = unique_configs

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
                # Use shlex.quote() to escape test_path
                full_command = f"cd {shlex.quote(test_path)} && ls -la scripts/rpc.py"
                stdin, stdout, stderr = ssh.exec_command(full_command, timeout=5)
                error = stderr.read().decode('utf-8')
                if not error:
                    # Found the script, execute the actual command
                    full_command = f"cd {shlex.quote(test_path)} && {command}"
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

    def _parse_spdk_targets(self, master_server_address: str) -> List[Dict[str, Any]]:
        """
        Parse spdk target info strings like "ip:192.168.65.56" or "ip:192.168.65.56 ns:2"
        or "ip:192.168.65.56 path:/home/spdk" to get actual namespaces from SPDK target
        """
        ssd_configs = []

        for target_info in self.spdk_targets:
            # Parse target info
            target = {}
            parts = target_info.split()
            for part in parts:
                if ':' in part:
                    key, value = part.split(':', 1)
                    target[key.strip()] = value.strip()

            # Validate required fields
            if 'ip' not in target:
                raise ValueError("spdk_target_info must contain 'ip' field")

            ip = target['ip']
            specified_ns = int(target['ns']) if 'ns' in target else None
            path = target.get('path')

            # We need to know the NQN to build the te_endpoint
            # For now, we'll use a default NQN pattern (this should be improved)
            default_nqn = "nqn.2016-06.io.spdk:cnode1"
            nqn = target.get('nqn', default_nqn)

            # Default transport parameters
            trsvcid = int(target.get('port', '4420'))
            trtype = target.get('trtype', 'RDMA')

            # Create SSD config for each namespace (or specified ns only)
            if specified_ns is not None:
                # Unregister specific namespace
                ssd_config = {
                    'nqn': nqn,
                    'nsid': specified_ns,
                    'traddr': ip,
                    'trsvcid': trsvcid,
                    'base': 0,
                    'size': 0,  # Size is not needed for unregister
                    'master_server_address': master_server_address,
                    'metadata_server': ''
                }
                ssd_configs.append(ssd_config)
                logging.info(f"Will unregister SSD: nqn={nqn}, nsid={specified_ns}, traddr={ip}")
            elif path is not None:
                # Get actual namespaces from SPDK target
                logging.info(f"Getting namespace info from target: {ip} (path: {path})")
                try:
                    # Get subsystems info
                    subsystems_cmd = "./scripts/rpc.py nvmf_get_subsystems"
                    subsystems_output = self._execute_ssh_command(ip, subsystems_cmd, path)
                    subsystems = json.loads(subsystems_output)

                    # Process each subsystem
                    for subsystem in subsystems:
                        if subsystem.get('subtype') != 'NVMe':
                            continue

                        subsystem_nqn = subsystem.get('nqn')
                        listen_addresses = subsystem.get('listen_addresses', [])

                        if not subsystem_nqn or not listen_addresses:
                            continue

                        # Get transport info from first listen address
                        traddr = listen_addresses[0].get('traddr')
                        target_trsvcid = listen_addresses[0].get('trsvcid')

                        if not traddr or not target_trsvcid:
                            continue

                        # Use the nqn from the subsystem if not specified
                        current_nqn = nqn if nqn != default_nqn else subsystem_nqn
                        current_trsvcid = int(target_trsvcid) if target_trsvcid else trsvcid

                        # Process each namespace
                        namespaces = subsystem.get('namespaces', [])
                        for namespace in namespaces:
                            nsid = namespace.get('nsid')

                            if not nsid:
                                continue

                            # Create SSD config
                            ssd_config = {
                                'nqn': current_nqn,
                                'nsid': nsid,
                                'traddr': traddr,
                                'trsvcid': current_trsvcid,
                                'base': 0,
                                'size': 0,  # Size is not needed for unregister
                                'master_server_address': master_server_address,
                                'metadata_server': ''
                            }
                            ssd_configs.append(ssd_config)
                            logging.info(f"Will unregister SSD: nqn={current_nqn}, nsid={nsid}, traddr={traddr}")

                except Exception as e:
                    logging.error(f"Failed to get namespace info from {ip}: {e}")
                    logging.warning("Falling back to unregistering default namespace (nsid=1)")
                    # Fall back to unregistering default namespace
                    ssd_config = {
                        'nqn': nqn,
                        'nsid': 1,
                        'traddr': ip,
                        'trsvcid': trsvcid,
                        'base': 0,
                        'size': 0,
                        'master_server_address': master_server_address,
                        'metadata_server': ''
                    }
                    ssd_configs.append(ssd_config)
                    logging.info(f"Will unregister SSD (fallback): nqn={nqn}, nsid=1, traddr={ip}")
            else:
                # No path provided, unregister default namespace only
                logging.warning("No 'path' provided in spdk_target_info, cannot query actual namespaces from SPDK target")
                logging.warning("Will unregister default namespace (nsid=1) only")
                ssd_config = {
                    'nqn': nqn,
                    'nsid': 1,
                    'traddr': ip,
                    'trsvcid': trsvcid,
                    'base': 0,
                    'size': 0,
                    'master_server_address': master_server_address,
                    'metadata_server': ''
                }
                ssd_configs.append(ssd_config)
                logging.info(f"Will unregister SSD: nqn={nqn}, nsid=1, traddr={ip}")

        if not ssd_configs:
            raise RuntimeError("No SSD configuration generated from target info")

        return ssd_configs

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def start_ssd_unregister_service(self):
        success_count = 0
        failed_count = 0
        total = len(self.config_list)

        for i, cfg in enumerate(self.config_list):
            try:
                logging.info("Unregistering SSD %d/%d: nqn=%s, traddr=%s, nsid=%d", i + 1, total, cfg.get("nqn"), cfg.get("traddr"), cfg.get("nsid"))

                # Create register instance and unregister SSD
                self.register = MooncakeDistributedNoFRegister()
                ret = self.register.real_unregister_by_endpoint(
                    cfg["nqn"],
                    cfg["nsid"],
                    cfg["traddr"],
                    cfg["trsvcid"],
                    cfg["master_server_address"]
                )

                if ret != 0:
                    raise RuntimeError(f"Unregistration failed with code {ret}")

                logging.info("Unregister SSD %d/%d succeeded", i + 1, total)
                success_count += 1

            except Exception as e:
                logging.error("Failed to unregister SSD %d/%d: %s", i + 1, total, e)
                failed_count += 1

        # Summary
        logging.info("SSD unregistration summary:")
        logging.info("- Total SSDs: %d", total)
        logging.info("- Successfully unregistered: %d", success_count)
        logging.info("- Failed: %d", failed_count)

        # Return success if all SSDs were unregistered successfully
        return failed_count == 0


def parse_arguments():
    parser = argparse.ArgumentParser(description='Mooncake SSD Unregister with REST API')
    parser.add_argument('--master_server_address', type=str,
                        help='Master server address (e.g., 192.168.65.81:50051)',
                        default='192.168.65.81:50051')
    parser.add_argument('--spdk_target_info', action='append',
                        help='SPDK target information (e.g., "ip:192.168.65.56 ns:2 nqn:nqn.2016-06.io.spdk:cnode1" or "ip:192.168.65.56 path:/home/spdk")',
                        default=None)
    parser.add_argument('-D', '--define', action='append',
                        help='Override configuration fields globally (e.g., -Dtrsvcid=4420)',
                        default=[])
    args = parser.parse_args()
    
    # If no spdk_target_info is provided, use default
    if args.spdk_target_info is None:
        args.spdk_target_info = ['ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1']
    
    return args


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

    unregister = MooncakeNoFUnregister(cli_config, args.spdk_target_info)
    success = unregister.start_ssd_unregister_service()
    if not success:
        exit(1)

if __name__ == "__main__":
    main()