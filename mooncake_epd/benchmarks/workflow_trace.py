"""RFC §8.3 WorkflowTrace schema and synthetic dataset converter."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional

from mooncake_epd.tests.dataset import WorkflowExample, build_dataset, image_hash, make_image


def estimate_tokens(text: str) -> int:
    text = (text or "").strip()
    if not text:
        return 0
    return len(text.split())


@dataclass
class TraceImage:
    image_id: str
    path_or_url: str
    reuse_group: str


@dataclass
class TraceStep:
    step_id: int
    agent_role: str
    input_prompt: str
    input_token_len: int
    shared_prefix_token_len: int
    new_token_len: int
    expected_tool_call: Optional[str]
    expected_answer: Optional[str]
    parent_state_id: Optional[str]
    fork_group_id: Optional[str]


@dataclass
class TraceScoring:
    type: str


@dataclass
class WorkflowTrace:
    workflow_id: str
    source_dataset: str
    task_type: str
    priority_class: str
    modality: str = "multimodal"
    routing_path: str = "EPD"
    expected_prefetchable_images: List[str] = field(default_factory=list)
    expected_handoff: Optional[dict] = None
    images: List[TraceImage] = field(default_factory=list)
    steps: List[TraceStep] = field(default_factory=list)
    gold_answer: str = ""
    scoring: TraceScoring = field(default_factory=lambda: TraceScoring(type="llm_judge"))

    def to_dict(self) -> dict:
        return asdict(self)


def _task_type(example: WorkflowExample) -> str:
    return {
        "tool_use_vqa": "tool_use",
        "multi_turn": "multi_turn",
        "tree_of_thought": "fork",
        "a2a_handoff": "a2a",
    }.get(example.scenario, "single_turn")


def _priority_class(example: WorkflowExample) -> str:
    return {
        "tool_use_vqa": "INTERACTIVE",
        "multi_turn": "INTERACTIVE",
        "tree_of_thought": "THINKING",
        "a2a_handoff": "HYBRID",
    }.get(example.scenario, "INTERACTIVE")


def _agent_role(example: WorkflowExample, step_id: int) -> str:
    if example.scenario == "tree_of_thought":
        return "planner" if step_id == 0 else "solver"
    if example.scenario == "a2a_handoff":
        return ["planner", "tool_executor", "verifier"][min(step_id, 2)]
    if example.scenario == "tool_use_vqa":
        return "tool_executor" if step_id > 0 else "solver"
    return "solver"


def _scoring_type(example: WorkflowExample) -> str:
    if example.scenario == "tool_use_vqa":
        return "tool_success"
    if example.scenario == "multi_turn":
        return "llm_judge"
    if example.scenario == "tree_of_thought":
        return "llm_judge"
    if example.scenario == "a2a_handoff":
        return "llm_judge"
    return "exact_match"


def example_to_trace(example: WorkflowExample, index: int) -> WorkflowTrace:
    img = make_image(example.image_name)
    img_hash = image_hash(img)
    workflow_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{example.scenario}:{example.image_name}:{index}"))

    image_entry = TraceImage(
        image_id=img_hash,
        path_or_url=f"synthetic://{example.image_name}",
        reuse_group=f"{example.image_name}:{img_hash}",
    )

    shared_prefix = estimate_tokens(example.system_prompt)
    running_prefix = shared_prefix
    steps: List[TraceStep] = []
    for step_id, prompt in enumerate(example.steps):
        prompt_tokens = estimate_tokens(prompt)
        steps.append(
            TraceStep(
                step_id=step_id,
                agent_role=_agent_role(example, step_id),
                input_prompt=prompt,
                input_token_len=running_prefix + prompt_tokens,
                shared_prefix_token_len=running_prefix,
                new_token_len=prompt_tokens,
                expected_tool_call=None,
                expected_answer=example.tool_outputs[step_id] if step_id < len(example.tool_outputs) else None,
                parent_state_id=None if step_id == 0 else f"{workflow_id}:step:{step_id - 1}",
                fork_group_id=(f"{workflow_id}:fork" if example.scenario == "tree_of_thought" and step_id > 0 else None),
            )
        )
        running_prefix += prompt_tokens

    gold_answer = example.tool_outputs[-1] if example.tool_outputs else ""
    expected_handoff = None
    if example.scenario == "a2a_handoff":
        expected_handoff = {
            "from_role": "planner",
            "to_role": "tool_executor",
            "handoff_step_id": 1,
        }
    return WorkflowTrace(
        workflow_id=workflow_id,
        source_dataset="synthetic",
        task_type=_task_type(example),
        priority_class=_priority_class(example),
        modality="multimodal",
        routing_path="EPD",
        expected_prefetchable_images=[img_hash],
        expected_handoff=expected_handoff,
        images=[image_entry],
        steps=steps,
        gold_answer=gold_answer,
        scoring=TraceScoring(type=_scoring_type(example)),
    )


def build_workflow_traces(dataset: Optional[List[WorkflowExample]] = None) -> List[WorkflowTrace]:
    dataset = dataset or build_dataset()
    return [example_to_trace(example, idx) for idx, example in enumerate(dataset)]


def export_workflow_traces(output_path: str, dataset: Optional[List[WorkflowExample]] = None) -> Path:
    traces = build_workflow_traces(dataset)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [trace.to_dict() for trace in traces]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
