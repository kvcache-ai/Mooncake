"""Explicit, version-probed vLLM integration for Mooncake EPD."""

from .adapter import (
    VLLMAdapterError,
    adapter_installation_report,
    install_vllm_epd_adapter,
)
from .capabilities import VLLMCapabilityReport, probe_vllm_epd_capabilities

__all__ = [
    "VLLMAdapterError",
    "VLLMCapabilityReport",
    "adapter_installation_report",
    "install_vllm_epd_adapter",
    "probe_vllm_epd_capabilities",
]
