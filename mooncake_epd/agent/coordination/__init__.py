"""Agent coordination layer.

Keep package imports light to avoid circular dependencies between the serving
control plane and the state/workflow stack. Heavy modules are loaded lazily via
``__getattr__``.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .scheduler import (
    AdmissionAction,
    AdmissionDecision,
    AgentRequest,
    AgentScheduler,
    AgentType,
    DegradeLevel,
    WorkerLoad,
)

__all__ = [
    "AdmissionAction",
    "AdmissionDecision",
    "AgentRequest",
    "AgentScheduler",
    "AgentType",
    "DegradeLevel",
    "ElasticEPDRatio",
    "OffloadManager",
    "ReusePipeline",
    "ReuseStats",
    "WorkerLoad",
    "Workflow",
    "WorkflowStep",
]


def __getattr__(name: str) -> Any:
    if name in {"Workflow", "WorkflowStep"}:
        mod = import_module(".workflow", __name__)
        return getattr(mod, name)
    if name == "OffloadManager":
        mod = import_module(".offload", __name__)
        return getattr(mod, name)
    if name in {"ReusePipeline", "ReuseStats"}:
        mod = import_module(".reuse_pipeline", __name__)
        return getattr(mod, name)
    if name == "ElasticEPDRatio":
        mod = import_module(".elastic_ratio", __name__)
        return getattr(mod, name)
    raise AttributeError(name)
