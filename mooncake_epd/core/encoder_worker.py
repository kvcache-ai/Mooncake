"""
Encoder Worker - Vision Encoder 阶段

负责运行多模态模型的 Vision Encoder（如 ViT），
将输入图像编码为 Hidden States，并通过 Transfer Engine 传输到 Prefill 节点。
"""

import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

import torch
import torch.nn as nn

logger = logging.getLogger(__name__)


@dataclass
class EncoderOutput:
    """Vision Encoder 输出"""
    hidden_states: torch.Tensor  # [batch, num_patches, hidden_dim]
    metadata: Dict[str, Any]     # image_size, patch_size, num_patches 等
    encode_time_ms: float        # 编码耗时
    request_id: str              # 请求 ID


class EncoderWorker:
    """
    Vision Encoder Worker

    在多模态模型（如 Qwen-VL）中，Vision Encoder 将图像编码为视觉特征。
    EPD 分离后，Encoder Worker 独立运行在一个节点/GPU 上，
    输出的 Hidden States 通过 Mooncake Transfer Engine 传输到 Prefill 节点。
    """

    def __init__(
        self,
        model: nn.Module,
        device: torch.device,
        transfer_engine=None,
    ):
        self.model = model
        self.device = device
        self.transfer_engine = transfer_engine
        self._request_counter = 0

    @torch.no_grad()
    def encode_images(
        self,
        pixel_values: torch.Tensor,
        image_sizes: Optional[List[Tuple[int, int]]] = None,
    ) -> EncoderOutput:
        """
        运行 Vision Encoder 提取图像特征。

        Args:
            pixel_values: [batch, channels, height, width] 预处理后的图像张量
            image_sizes: 原始图像尺寸列表

        Returns:
            EncoderOutput: 包含 hidden states 和元信息
        """
        start_time = time.perf_counter()

        pixel_values = pixel_values.to(self.device)

        if hasattr(self.model, 'visual'):
            hidden_states = self.model.visual(pixel_values)
        elif hasattr(self.model, 'vision_model'):
            hidden_states = self.model.vision_model(pixel_values)
        else:
            hidden_states = self.model(pixel_values)

        encode_time_ms = (time.perf_counter() - start_time) * 1000

        self._request_counter += 1
        request_id = f"enc_{self._request_counter}_{int(time.time() * 1000)}"

        metadata = {
            "image_sizes": image_sizes,
            "pixel_shape": list(pixel_values.shape),
            "hidden_shape": list(hidden_states.shape),
            "dtype": str(hidden_states.dtype),
        }

        output = EncoderOutput(
            hidden_states=hidden_states,
            metadata=metadata,
            encode_time_ms=encode_time_ms,
            request_id=request_id,
        )

        logger.info(
            f"Encoded request {request_id}: shape={hidden_states.shape}, "
            f"time={encode_time_ms:.2f}ms"
        )

        return output

    def encode_and_transfer(
        self,
        pixel_values: torch.Tensor,
        image_sizes: Optional[List[Tuple[int, int]]] = None,
        target_device: Optional[torch.device] = None,
    ) -> EncoderOutput:
        """
        编码图像并通过 Transfer Engine 传输到 Prefill 节点。

        实现 EPD 中的 E→P 数据传输。
        """
        output = self.encode_images(pixel_values, image_sizes)

        if self.transfer_engine is not None and target_device is not None:
            transferred_states, transferred_meta = (
                self.transfer_engine.transfer_hidden_states(
                    output.hidden_states,
                    output.metadata,
                    target_device=target_device,
                )
            )
            output.hidden_states = transferred_states
            output.metadata.update(transferred_meta)
            transfer_stats = self.transfer_engine.get_stats()
            logger.info(
                f"Transferred hidden states: {transfer_stats.summary()}"
            )

        return output


class MockVisionEncoder(nn.Module):
    """
    模拟 Vision Encoder，用于测试 EPD 流水线。
    模拟 Qwen-VL 的 ViT 输出维度。
    """

    def __init__(
        self,
        hidden_dim: int = 1024,
        patch_size: int = 14,
        image_size: int = 448,
    ):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.patch_size = patch_size
        self.image_size = image_size
        self.num_patches = (image_size // patch_size) ** 2

        self.conv = nn.Conv2d(3, hidden_dim, kernel_size=patch_size, stride=patch_size)
        self.gelu = nn.GELU()
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, pixel_values: torch.Tensor) -> torch.Tensor:
        x = self.conv(pixel_values)
        x = self.gelu(x)
        x = x.flatten(2).transpose(1, 2)
        x = self.norm(x)
        return x


def create_encoder_worker(
    device: torch.device,
    transfer_engine=None,
    hidden_dim: int = 1024,
    model: Optional[nn.Module] = None,
) -> EncoderWorker:
    """创建 Encoder Worker，支持自定义模型或默认 MockVisionEncoder"""
    if model is None:
        model = MockVisionEncoder(hidden_dim=hidden_dim)
    model = model.to(device).eval()
    return EncoderWorker(model=model, device=device, transfer_engine=transfer_engine)
