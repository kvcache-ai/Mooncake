import sys
import os

def main():
    os.environ['MC_LOG_LEVEL'] = 'ERROR'
    os.environ['MC_CUSTOM_TOPO_JSON'] = ''
    from mooncake.engine import TransferEngine
    engine = TransferEngine()
    print('Local topology: ', engine.get_local_topology())

if __name__ == "__main__":
    sys.exit(main())