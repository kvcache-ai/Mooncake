import sys, os

def main():
    os.environ['MC_LOG_LEVEL'] = 'ERROR'
    from mooncake.engine import TransferEngine
    engine = TransferEngine()
    print('Local topology: ', engine.get_local_topology())

if __name__ == "__main__":
    sys.exit(main())