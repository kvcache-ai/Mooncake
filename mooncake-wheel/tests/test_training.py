from new_tensor import create_tensor
from load_model import get_tensor_info
from mooncake_training import MooncakeTraining
import yaml
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')

def training_send(conf):
    models = get_tensor_info(conf["model_path"])

    mckTrain = MooncakeTraining(conf)

    now_size = 0
    for model in models:
        name, shape, dtype, size = model[0], model[1], model[2], model[3] * 2
        tensor = create_tensor(shape, dtype, gpu_id=conf["training_gpu_id"])
        mckTrain.reg_tensor(name, tensor)
        now_size += size
    mckTrain.reg_tensor(None, None)
    
    print("total: ", now_size, mckTrain.report())

if __name__ == "__main__":
    conf = yaml.load(open("torch.yaml"), Loader=yaml.FullLoader)
    logging.info(f"Server configuration: {conf}")
    training_send(conf)
    
