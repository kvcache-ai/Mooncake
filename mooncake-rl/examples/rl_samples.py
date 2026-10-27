# This is a dummy RL training example for demonstrating the usage of Mooncake Store
# in transmission of data between rollout engines and training engines when distributed
import os
import random
import torch
from typing import List
from mooncake.store import MooncakeDistributedStore


class TrainActor:
    """
    Simulate a single training worker (GPU or process).
    
    Responsibilities:
    1. Initialize its own model and optimizer.
    2. Perform forward/backward passes on rollout data fetched from Mooncake store.
    """

    def __init__(self):
        self.model = torch.nn.Linear(10, 2)  # input_dim=10, output_dim=2 (dummy)
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)

    def init_model(self, args=None):
        """
        Initialize a simple linear model and its optimizer.
        In real slime, this would load checkpoints and move model to GPU.
        """
        # randomly generate model weights here
        torch.nn.init.xavier_uniform_(self.model.weight)
        torch.nn.init.zeros_(self.model.bias)
        print("[TrainActor] Model and optimizer initialized")

    def train(self, samples):
        """
        Perform one dummy training step on rollout samples.
        
        Each sample is expected to be a dict with fields:
            - "obs": list[int], representing observations
            - "action": int, action taken
            - "reward": float, scalar reward
        
        Training logic:
        1. Convert obs to tensor.
        2. Forward pass through model.
        3. Compute dummy loss = (predicted[action] - reward)^2.
        4. Backward + optimizer step.
        """
        self.model.train()
        obs = torch.tensor(samples["obs"], dtype=torch.float32).unsqueeze(0)  # shape [1, dim]
        action = samples["action"]
        reward = torch.tensor([samples["reward"]], dtype=torch.float32)

        # Forward pass
        logits = self.model(obs)  # shape [1, 2]
        pred = logits[0, action % logits.shape[1]]

        # Dummy MSE loss
        loss = (pred - reward).pow(2).mean()

        # Backward + optimize
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

        print(f"[TrainActor] Trained on sample (action={action}, reward={reward.item():.4f}), "
              f"loss={loss.item():.4f}")
        return loss.item()

    def save_model(self, rollout_id: int):
        """
        Save model to the specific path
        """
        torch.save(self.model.state_dict(), f"model_{rollout_id}.pth")
        print(f"[TrainActor] Model saved to model_{rollout_id}.pth")

class TrainGroup:
    """
    Simulate the group of training engines.
    
    Responsibilities:
    1. Initialize model state across multiple training actors.
    2. Connect to rollout manager for weight updates.
    3. Update weights after each rollout.
    4. Train on rollout data fetched from Mooncake store.
    5. Save checkpoints periodically.
    
    All functionality is mocked except the data flow through MooncakeStore.
    """

    def __init__(self, args):
        # number of training actors
        self.world_size = args.num_train_actor
        # init actor handlers
        self.actor_handlers = []
        for rank in range(self.world_size):
            self.actor_handlers.append(TrainActor())
        # init Mooncake store client
        self.training_client = MooncakeDistributedStore()
        # RDMA initialization
        self.training_client.setup("localhost:12345", 
                                   "http://localhost:8080/metadata", 
                                   512*1024*1024, 
                                   128*1024*1024, 
                                   "rdma", 
                                   "erdma_1", # or other NIC like mlx5_1
                                   "localhost:50051")


    def init_actors(self, args, role="actor"):
        """
        Initialize all training actors by creating models and optimizers.
        In real slime this would involve loading checkpoints and model weights.
        """
        for actor in self.actor_handlers:
            actor.init_model(args)
        print("[TrainGroup] Initialized with args")
        return [0]

    def init_weight_update_connections(self, rollout_manager):
        """
        Establish connection with rollout manager for weight synchronization.
        """
        print("[TrainGroup] Connected to rollout manager")

    def update_weights(self):
        """
        Update model weights.
        In real training, this would sync parameters from trainer to rollout engines.
        """
        print("[TrainGroup] Weights updated")

    def train(self, rollout_id: int, rollout_key: str):
        """
        Consume rollout data from MooncakeStore and compute a dummy loss.
        
        Steps:
        1. Fetch rollout samples from Mooncake store.
        2. Distribute samples across training actors.
        3. Each actor performs one training step.
        4. Print aggregated average loss.
        """
        samples = self.training_client.get_tensor(rollout_key)
        if samples is None:
            print(f"[TrainGroup] Rollout {rollout_id} not found in store")
            return

        losses = []
        for actor, sample in zip(self.actor_handlers, samples):
            loss = actor.train(sample)
            losses.append(loss)

        if losses:
            avg_loss = sum(losses) / len(losses)
            print(f"[TrainGroup] Rollout {rollout_id} average loss: {avg_loss:.4f}")

    def save_model(self, rollout_id: int):
        """
        Save checkpoint.
        In dummy mode, we only print a message.
        """
        for actor in self.actor_handlers:
            actor.save_model(rollout_id)

        print(f"[TrainGroup] Saved checkpoint at rollout {rollout_id}")


class RolloutEngine:
    """
    Simulate a single rollout engine (inference worker).
    
    Responsibilities:
    1. Generate rollout samples (obs, action, reward).
    2. Provide a dummy evaluation interface.
    
    In a real RL setup, this would:
    - Run inference on the policy model given an environment state.
    - Collect (obs, action, reward, next_obs) tuples.
    - Possibly handle batching, KV-cache, etc.
    
    Here, everything is mocked. We just produce random data.
    """
    def __init__(self, args):
        pass

    def generate(self, rollout_id: int):
        """
        Mock rollout generating
        """
        data = torch.randint(0, 100, (4,), dtype=torch.int32).tolist()
        action = random.randint(0, 9)
        reward = random.uniform(-1.0, 1.0)

        sample = {
            "rollout_id": rollout_id,
            "obs": data,
            "action": action,
            "reward": reward,
        }

        return sample

    def eval(self, rollout_id: int, samples: List[dict]):
        """
        Mock evaluation
        """
        if samples is None:
            print(f"[RolloutEngine] Rollout {rollout_id} not found in store")
            return
        # Fake "eval" = reward^2
        avg_reward = sum(s["reward"] for s in samples) / len(samples)
        eval_score = avg_reward ** 2
        print(f"[RolloutEngine] Evaluating rollout {rollout_id} "
              f"(action={samples[0]['action']}, reward={avg_reward:.4f}) "
              f"=> eval_score={eval_score:.4f}")


class RolloutController:
    """
    Simulate controller inside rollout manager.
    Handles dataset load/save commands across rollouts.
    """

    def __init__(self, args):
        # store config arguments
        self.args = args
        # init data source args
        self.epoch_id = 0
        self.sample_index = 0
        self.sample_offset = 0
        self.metadata = {}
        # init Mooncake store client
        self.rollout_client = MooncakeDistributedStore()
        # RDMA initialization
        self.rollout_client.setup("localhost:12346", 
                                  "http://localhost:8080/metadata", 
                                  512*1024*1024, 
                                  128*1024*1024, 
                                  "rdma", 
                                  "erdma_0", # or other NIC like mlx5_0 
                                  "localhost:50051")

    def load(self, rollout_id=None):
        """
        Load previous dataset.
        """
        path = os.path.join(self.args.model_path, f"rollout/global_dataset_state_dict_{rollout_id}.pt")
        if not os.path.exists(path):
            print(f"Checkpoint {path} does not exist.")
            return

        print(f"load metadata from {path}")
        print(f"load metadata: {self.metadata}")
        state_dict = torch.load(path)
        self.sample_offset = state_dict.get("sample_offset", 0)
        self.epoch_id = state_dict.get("epoch_id", 0)
        self.sample_index = state_dict.get("sample_index", 0)
        self.metadata = state_dict.get("metadata", {})

    def save(self, rollout_id: int):
        """
        Save dataset at given rollout.
        """
        state_dict = {
            "sample_offset": self.sample_offset,
            "epoch_id": self.epoch_id,
            "sample_index": self.sample_index,
            "metadata": self.metadata,
        }
        path = os.path.join(self.args.model_path, f"rollout/global_dataset_state_dict_{rollout_id}.pt")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        torch.save(state_dict, path)

class RolloutManager:
    """
    Simulate the rollout manager (inference engine + buffer).
    
    Responsibilities:
    1. Generate rollout samples and push them into MooncakeStore.
    2. Provide eval interface.
    3. Manage controller for dataset load/save.
    """

    def __init__(self, args):
        # init buffer controller
        self.controller = RolloutController(args)
        # init rollout engines
        self.rollout_engines = []
        for i in range(args.num_rollout_actor):
            self.rollout_engines.append(RolloutEngine(args))

    def generate(self, rollout_id: int) -> str:
        """
        Generate dummy rollout data:
        - obs: observation vector (4 ints)
        - action: random integer [0, 9]
        - reward: random float [-1, 1]
        
        Store the sample in MooncakeStore under key = str(rollout_id).
        """
        rollout_samples = []
        for engine in self.rollout_engines:
            sample = engine.generate(rollout_id)
            rollout_samples.append(sample)
        
        key = str(rollout_id)
        self.controller.rollout_client.put_tensor(key, rollout_samples)

        print(f"[RolloutManager] Generated rollout {rollout_id}: {rollout_samples}")
        return key

    def eval(self, rollout_id: int):
        """
        Perform dummy evaluation.
        """
        samples = self.controller.rollout_client.get_tensor(str(rollout_id))

        for engine in self.rollout_engines:
            engine.eval(rollout_id, samples)
        
        print(f"[RolloutManager] Evaluation at rollout {rollout_id}")

def create_actor_group(args):
    """
    Factory to create the training engine group.
    """
    return TrainGroup(args)


def create_rollout_manager(args):
    """
    Factory to create rollout manager.
    """
    return RolloutManager(args)

def train(args):
    """
    Dummy RL training loop adapted from THUDM/slime
    Specifically designed for distributed placement of training and rollout
    """
    # create training engine group
    actor_model = create_actor_group(args)

    # create the rollout manager, with engines inside.
    rollout_manager = create_rollout_manager(args)

    # sync the initialization (model initialization, load checkpoint, etc.)
    start_rollout_ids = actor_model.init_actors(args)
    
    assert len(set(start_rollout_ids)) == 1
    if args.start_rollout_id is None:
        args.start_rollout_id = start_rollout_ids[0]

    # load the previous rollout dataset
    rollout_manager.controller.load(args.start_rollout_id - 1)

    # initialize the connection for weight update during training
    actor_model.init_weight_update_connections(rollout_manager)

    # always update weight first so that sglang has the loaded weights from training.
    actor_model.update_weights()

    # warm up eval if needed
    if args.eval_interval is not None:
        warmup_id = args.start_rollout_id
        rollout_data_ref = rollout_manager.generate(warmup_id)
        actor_model.train(warmup_id, rollout_data_ref)
        rollout_manager.eval(warmup_id)

    # train loop.
    for rollout_id in range(args.start_rollout_id, args.num_rollout):
        if args.eval_interval is not None and rollout_id == 0:
            rollout_manager.eval(rollout_id)

        rollout_data_ref = rollout_manager.generate(rollout_id)

        actor_model.train(rollout_id, rollout_data_ref)

        if args.save_interval is not None and (
            (rollout_id + 1) % args.save_interval == 0
        ):
            actor_model.save_model(rollout_id)

        actor_model.update_weights()

        if args.eval_interval is not None and (
            (rollout_id + 1) % args.eval_interval == 0
        ):
            rollout_manager.eval(rollout_id)

import argparse

def parse_args():
    """
    Parse command-line arguments for dummy training.

    Arguments:
        --num_rollout: int
            The total number of rollouts to generate.
        --num_train_actor: int
            The number of GPUs allocated for training (dummy in this setup).
        --num_rollout_actor: int
            The number of rollout engines to simulate.
        --save_interval: int
            Interval (in rollouts) to save model checkpoints.
        --eval_interval: int
            Interval (in rollouts) to run evaluation.
        --model_path: str
            Path to save model checkpoints.
        --start_rollout_id: int
            Starting rollout ID (default 0).
        --num_epoch: int
            Number of epochs (default 1, affects num_rollout if unspecified).
        --rollout_global_dataset: bool
            Whether to simulate a global rollout dataset (default False).
        --rollout_shuffle: bool
            Whether to shuffle dataset on load (default False).

    Returns:
        argparse.Namespace with all attributes.
    """
    parser = argparse.ArgumentParser(description="Dummy training with MooncakeStore")

    parser.add_argument("--num_rollout", type=int, default=5,
                        help="Total number of rollouts to generate")
    parser.add_argument("--num_train_actor", type=int, default=1,
                        help="Number of GPUs for training (dummy)")
    parser.add_argument("--num_rollout_actor", type=int, default=1,
                        help="Number of rollout engines (dummy)")
    parser.add_argument("--save_interval", type=int, default=2,
                        help="Interval for saving model checkpoints")
    parser.add_argument("--eval_interval", type=int, default=2,
                        help="Interval for evaluating rollout data")
    parser.add_argument("--model_path", type=str, default="./checkpoints",
                        help="Path to save model checkpoints")
    parser.add_argument("--start_rollout_id", type=int, default=0,
                        help="Starting rollout ID")
    parser.add_argument("--num_epoch", type=int, default=1,
                        help="Number of epochs (only meaningful if num_rollout is None)")
    parser.add_argument("--rollout_global_dataset", action="store_true",
                        help="Enable global rollout dataset (dummy mode)")
    parser.add_argument("--rollout_shuffle", action="store_true",
                        help="Shuffle dataset on load (dummy mode)")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    train(args)
