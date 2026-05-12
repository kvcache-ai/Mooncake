#!/usr/bin/env python3
# python3 -m mooncake.mooncake_ssd_unregister --master_server_address=192.168.65.81:50051 --spdk_target_info="ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1"

import argparse
import logging
from typing import List, Dict, Any

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

    def _parse_spdk_targets(self, master_server_address: str) -> List[Dict[str, Any]]:
        """
        Parse spdk target info strings like "ip:192.168.65.56" or "ip:192.168.65.56 ns:2"
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
            else:
                # Unregister all namespaces on this target
                # Since we don't have a way to get all namespaces from master yet,
                # we'll unregister a range of possible nsids (1-10)
                # This is a temporary solution until we can query master for all segments
                for nsid in range(1, 11):
                    ssd_config = {
                        'nqn': nqn,
                        'nsid': nsid,
                        'traddr': ip,
                        'trsvcid': trsvcid,
                        'base': 0,
                        'size': 0,  # Size is not needed for unregister
                        'master_server_address': master_server_address,
                        'metadata_server': ''
                    }
                    ssd_configs.append(ssd_config)
                    logging.info(f"Will unregister SSD: nqn={nqn}, nsid={nsid}, traddr={ip}")

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
                        help='SPDK target information (e.g., "ip:192.168.65.56" or "ip:192.168.65.56 ns:2")',
                        default=['ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1'])
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

    unregister = MooncakeNoFUnregister(cli_config, args.spdk_target_info)
    success = unregister.start_ssd_unregister_service()
    if not success:
        exit(1)

if __name__ == "__main__":
    main()