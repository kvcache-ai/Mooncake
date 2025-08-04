import argparse
import sys, os

def parse_arguments():
    parser = argparse.ArgumentParser(description='Mooncake TransferEngine topology self-check tool')

    parser.add_argument('--enable-custom-topo',
                        action="store_true",
                        help="Enable detecting the custom topology", required=False)
    parser.add_argument('-F', '--filter-devices', type=str,
                        help='HCA(s) that want to be included, only valid when custom topology is not detected',
                        required=False)
    return parser.parse_args()

def main():
    args = parse_arguments()

    os.environ['MC_LOG_LEVEL'] = 'ERROR'
    from mooncake.engine import TransferEngine
    engine = TransferEngine()
    print('Local topology: ', engine.get_local_topology(not args.enable_custom_topo, args.filter_devices))

if __name__ == "__main__":
    sys.exit(main())