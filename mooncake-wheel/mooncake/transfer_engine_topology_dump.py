import argparse
import sys
import os

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Dump device topology",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--device-name", 
        type=str, 
        default="", 
        help="Filter topology by given device name"
    )
    return parser.parse_args()
    
def main():
    args = parse_args()
    os.environ['MC_LOG_LEVEL'] = 'ERROR'
    os.environ['MC_CUSTOM_TOPO_JSON'] = ''
    from mooncake.engine import TransferEngine
    engine = TransferEngine()
    print('Local topology: ', engine.get_local_topology(device_name=args.device_name))

if __name__ == "__main__":
    sys.exit(main())