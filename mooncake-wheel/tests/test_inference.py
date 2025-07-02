from load_model import get_tensor_info
import yaml
import torch
from new_tensor import create_tensor, get_dtype
from mooncake_inference import MooncakeInference
import logging
import time

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')

def inference_recv(conf):
    models = get_tensor_info(conf["model_path"])
    total = len(models)

    mckInference = MooncakeInference(conf)

    now = 0
    while now < total:
        d = mckInference.recv_tensor()
        now += 1
    time.sleep(1)
    

if __name__ == "__main__":
    conf = yaml.load(open("torch.yaml"), Loader=yaml.FullLoader)
    logging.info(f"configuration: {conf}")
    inference_recv(conf)
    
