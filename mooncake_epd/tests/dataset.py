"""Agent-workflow test dataset: procedural images + multi-step prompts.

Generates deterministic synthetic images (no network, no heavy deps) and
pairs them with multi-step, multi-turn agent prompts covering the four
scenarios the RFC exercises:

1. **Tool-use visual QA** -- shared image + system prompt, multiple questions
   that branch into different reasoning paths (measures cross-step KV reuse
   and fork).
2. **Multi-turn image conversation** -- same image referenced across 4 turns
   (measures feature-bundle caching and turn-level prefix reuse).
3. **Tree-of-Thought reasoning** -- fork 3 parallel interpretations of a
   scene (measures O(1) clone + CoW).
4. **Multi-agent A2A handoff** -- describer -> analyst -> summarizer (measures
   state handoff + offload between tool calls).

Each scenario yields a list of ``WorkflowExample`` instances. Images are
generated on demand via PIL so the dataset is reproducible without any
external download.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from PIL import Image, ImageDraw, ImageFont


# ---------------------------------------------------------------------------
# Image generation
# ---------------------------------------------------------------------------
def _draw_gradient(img: Image.Image, c1, c2, direction: str = "vertical") -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    for i in range(h if direction == "vertical" else w):
        t = i / (h if direction == "vertical" else w)
        r = int(c1[0] * (1 - t) + c2[0] * t)
        g = int(c1[1] * (1 - t) + c2[1] * t)
        b = int(c1[2] * (1 - t) + c2[2] * t)
        if direction == "vertical":
            draw.line([(0, i), (w, i)], fill=(r, g, b))
        else:
            draw.line([(i, 0), (i, h)], fill=(r, g, b))


def _draw_bar_chart(img: Image.Image) -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    draw.rectangle([(0, 0), (w, h)], fill=(250, 250, 250))
    # Axes
    margin = 40
    draw.line([(margin, margin), (margin, h - margin)], fill=(0, 0, 0), width=2)
    draw.line([(margin, h - margin), (w - margin, h - margin)], fill=(0, 0, 0), width=2)
    # Bars
    values = [45, 72, 31, 88, 56, 64, 39]
    colors = [(230, 80, 80), (80, 180, 80), (80, 120, 220), (230, 180, 50),
              (180, 80, 200), (80, 200, 200), (200, 130, 80)]
    bar_w = (w - 2 * margin - 20) // len(values)
    max_v = max(values)
    for i, (v, c) in enumerate(zip(values, colors)):
        x0 = margin + 10 + i * bar_w
        x1 = x0 + bar_w - 6
        bar_h = int((h - 2 * margin - 10) * v / max_v)
        y0 = h - margin - bar_h
        draw.rectangle([(x0, y0), (x1, h - margin)], fill=c)
        draw.text((x0 + 4, y0 - 14), str(v), fill=(0, 0, 0))
    # Title
    draw.text((w // 2 - 60, 8), "Weekly Sales", fill=(0, 0, 0))


def _draw_room_scene(img: Image.Image) -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    # Sky / wall
    _draw_gradient(img, (200, 220, 240), (240, 230, 210))
    # Floor
    draw.rectangle([(0, int(h * 0.7)), (w, h)], fill=(120, 90, 60))
    # Window
    draw.rectangle([(int(w * 0.1), int(h * 0.15)), (int(w * 0.3), int(h * 0.5))],
                    outline=(40, 40, 40), width=4)
    draw.rectangle([(int(w * 0.1), int(h * 0.15)), (int(w * 0.3), int(h * 0.5))],
                    fill=(170, 210, 240))
    draw.line([(int(w * 0.2), int(h * 0.15)), (int(w * 0.2), int(h * 0.5))],
              fill=(40, 40, 40), width=3)
    # Table
    draw.rectangle([(int(w * 0.4), int(h * 0.55)), (int(w * 0.8), int(h * 0.7))],
                    fill=(160, 110, 60))
    # Vase on table
    draw.ellipse([(int(w * 0.55), int(h * 0.45)), (int(w * 0.65), int(h * 0.58))],
                  fill=(200, 50, 50))
    # Chair
    draw.rectangle([(int(w * 0.82), int(h * 0.5)), (int(w * 0.95), int(h * 0.7))],
                    fill=(80, 50, 30))


def _draw_document(img: Image.Image) -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    draw.rectangle([(0, 0), (w, h)], fill=(255, 255, 255))
    # Header
    draw.rectangle([(0, 0), (w, 40)], fill=(40, 60, 120))
    draw.text((10, 12), "QUARTERLY REPORT", fill=(255, 255, 255))
    # Body paragraphs (mock text with lines)
    y = 60
    for _ in range(8):
        line_w = w - 40 if _ % 3 else w - 80
        draw.rectangle([(20, y), (20 + line_w, y + 6)], fill=(60, 60, 60))
        y += 16
    # Image placeholder
    draw.rectangle([(20, y + 10), (w // 2 - 20, y + 80)], fill=(200, 200, 200))
    draw.text((30, y + 40), "Figure 1", fill=(100, 100, 100))
    # Footer
    draw.rectangle([(0, h - 24), (w, h)], fill=(240, 240, 240))
    draw.text((10, h - 18), "CONFIDENTIAL -- Page 3", fill=(80, 80, 80))


def _draw_map(img: Image.Image) -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    # Background
    draw.rectangle([(0, 0), (w, h)], fill=(230, 240, 220))
    # Roads
    draw.line([(0, h // 2), (w, h // 2)], fill=(180, 180, 180), width=8)
    draw.line([(w // 3, 0), (w // 3, h)], fill=(180, 180, 180), width=8)
    draw.line([(2 * w // 3, 0), (2 * w // 3, h)], fill=(180, 180, 180), width=6)
    # Parks (green)
    draw.ellipse([(40, 40), (w // 3 - 20, h // 2 - 20)], fill=(140, 200, 130))
    # Buildings
    for (x, y, bw, bh, c) in [
        (w // 3 + 10, 20, 60, 50, (200, 160, 140)),
        (w // 3 + 10, 80, 40, 30, (180, 180, 200)),
        (2 * w // 3 + 10, 40, 50, 60, (220, 190, 150)),
        (2 * w // 3 + 10, h // 2 + 10, 70, 40, (200, 200, 210)),
        (20, h // 2 + 10, 80, 60, (210, 170, 140)),
    ]:
        draw.rectangle([(x, y), (x + bw, y + bh)], fill=c, outline=(80, 80, 80))
    # Labels
    draw.text((w // 3 + 14, 24), "Library", fill=(0, 0, 0))
    draw.text((2 * w // 3 + 14, 44), "Museum", fill=(0, 0, 0))
    draw.text((24, h // 2 + 14), "Cafe", fill=(0, 0, 0))


def _draw_objects_scene(img: Image.Image) -> None:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    _draw_gradient(img, (240, 240, 220), (220, 220, 240), direction="horizontal")
    # Five distinct objects
    draw.ellipse([(40, 60), (120, 140)], fill=(230, 60, 60))          # red circle
    draw.rectangle([(150, 80), (230, 160)], fill=(60, 150, 60))        # green square
    draw.polygon([(320, 60), (280, 160), (360, 160)], fill=(60, 100, 220))  # blue triangle
    draw.ellipse([(380, 100), (440, 160)], fill=(230, 180, 40))        # yellow circle
    draw.rectangle([(80, 180), (200, 220)], fill=(180, 80, 200))       # purple rect


_GENERATORS: dict = {
    "gradient": (lambda: _draw_gradient(Image.new("RGB", (448, 448)), (0, 0, 0), (255, 255, 255))),
    "bar_chart": _draw_bar_chart,
    "room": _draw_room_scene,
    "document": _draw_document,
    "map": _draw_map,
    "objects": _draw_objects_scene,
}


def make_image(name: str, size: tuple = (448, 448)) -> Image.Image:
    """Return a deterministically-generated image by name."""
    if name == "gradient":
        img = Image.new("RGB", size)
        _draw_gradient(img, (0, 0, 0), (255, 255, 255))
        return img
    img = Image.new("RGB", size, (255, 255, 255))
    gen = _GENERATORS.get(name)
    if gen is None:
        raise ValueError(f"unknown image name: {name}")
    gen(img)
    return img


def image_hash(img: Image.Image) -> str:
    import numpy as np
    arr = np.asarray(img).tobytes()
    return hashlib.sha256(arr).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Workflow examples
# ---------------------------------------------------------------------------
@dataclass
class WorkflowExample:
    """One agent workflow example with an image and a sequence of steps."""

    scenario: str
    image_name: str
    system_prompt: str
    steps: List[str] = field(default_factory=list)  # user prompts per step
    tool_outputs: List[Optional[str]] = field(default_factory=list)
    expected_keywords: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
SYSTEM_ANALYST = (
    "You are a visual analysis assistant. When asked about an image, "
    "first describe what you see, then answer follow-up questions using "
    "only the information visible in the image."
)

SYSTEM_PLANNER = (
    "You are a planning agent. Break the task into steps, propose "
    "alternatives, and justify each choice based on the visual evidence."
)


def tool_use_vqa() -> List[WorkflowExample]:
    return [
        WorkflowExample(
            scenario="tool_use_vqa",
            image_name="objects",
            system_prompt=SYSTEM_ANALYST,
            steps=[
                "List every distinct object you can see in the image.",
                "Count how many of those objects are circular.",
                "What color is the triangle? Where is it relative to the red circle?",
                "If you had to pick one object to remove to make the scene more balanced, which would it be and why?",
            ],
            tool_outputs=[
                "Objects: red circle, green square, blue triangle, yellow circle, purple rectangle.",
                "Circular objects: 2 (red circle, yellow circle).",
                "Triangle is blue. It is to the right of the red circle.",
                "Remove the purple rectangle (bottom-left) since it breaks the vertical symmetry.",
            ],
            expected_keywords=["circle", "square", "triangle", "red", "blue"],
        ),
        WorkflowExample(
            scenario="tool_use_vqa",
            image_name="bar_chart",
            system_prompt=SYSTEM_ANALYST,
            steps=[
                "Describe the chart. What is its title and how many bars are there?",
                "Which day has the highest value and which has the lowest?",
                "Compute the difference between the highest and lowest values.",
                "Summarize the weekly trend in one sentence.",
            ],
            tool_outputs=[
                "Title: Weekly Sales. 7 bars.",
                "Highest: day 4 (88). Lowest: day 3 (31).",
                "Difference: 88 - 31 = 57.",
                "Sales rise mid-week and taper off at the weekend.",
            ],
            expected_keywords=["sales", "highest", "lowest", "trend"],
        ),
    ]


def multi_turn_conversation() -> List[WorkflowExample]:
    return [
        WorkflowExample(
            scenario="multi_turn",
            image_name="room",
            system_prompt=SYSTEM_ANALYST,
            steps=[
                "Describe the room in this image.",
                "What piece of furniture is closest to the window?",
                "Now look again: is there anything on the table? What color is it?",
                "If you were to rearrange, where would you move the chair and why?",
            ],
            expected_keywords=["room", "window", "table", "chair", "vase"],
        ),
        WorkflowExample(
            scenario="multi_turn",
            image_name="document",
            system_prompt=SYSTEM_ANALYST,
            steps=[
                "What kind of document is this?",
                "What text appears in the header?",
                "Is there a figure? Where is it placed on the page?",
                "What's in the footer?",
            ],
            expected_keywords=["report", "quarterly", "figure", "confidential"],
        ),
    ]


def tree_of_thought() -> List[WorkflowExample]:
    # Same image, 3 parallel reasoning branches from the same prefix.
    base_steps = [
        "Analyze this city map carefully. First, identify the main road layout.",
    ]
    branches = [
        ["Now find the shortest walking route from the Cafe to the Museum.",
         "Describe the path step by step."],
        ["Now evaluate where a new park would best serve the neighborhood.",
         "Justify the placement using the surrounding buildings."],
        ["Now argue why the Library is the most important building here.",
         "Compare it with the Museum and the Cafe."],
    ]
    examples = []
    for i, branch in enumerate(branches):
        examples.append(WorkflowExample(
            scenario="tree_of_thought",
            image_name="map",
            system_prompt=SYSTEM_PLANNER,
            steps=base_steps + branch,
            expected_keywords=["library", "museum", "cafe"],
        ))
    return examples


def multi_agent_handoff() -> List[WorkflowExample]:
    return [
        WorkflowExample(
            scenario="a2a_handoff",
            image_name="room",
            system_prompt=SYSTEM_ANALYST,
            steps=[
                # Agent A: describer
                "Produce a detailed inventory of every visible object in the room.",
                # Agent B: analyst (hands off to C after)
                "Group the objects by category (furniture, decor, architectural feature).",
                # Agent C: summarizer
                "Write a 2-sentence interior-design critique of the room.",
            ],
            expected_keywords=["inventory", "category", "critique"],
        ),
    ]


# ---------------------------------------------------------------------------
# Dataset loader
# ---------------------------------------------------------------------------
def build_dataset() -> List[WorkflowExample]:
    """Return the full set of workflow examples across all scenarios."""
    out: List[WorkflowExample] = []
    out.extend(tool_use_vqa())
    out.extend(multi_turn_conversation())
    out.extend(tree_of_thought())
    out.extend(multi_agent_handoff())
    return out


def summarize(dataset: List[WorkflowExample]) -> dict:
    from collections import Counter
    by_scenario = Counter(e.scenario for e in dataset)
    by_image = Counter(e.image_name for e in dataset)
    return {
        "total_examples": len(dataset),
        "by_scenario": dict(by_scenario),
        "by_image": dict(by_image),
        "unique_images": sorted({e.image_name for e in dataset}),
    }


if __name__ == "__main__":
    ds = build_dataset()
    print("Dataset summary:", summarize(ds))
    for ex in ds[:2]:
        img = make_image(ex.image_name)
        print(f"\n[{ex.scenario}] image={ex.image_name} ({img.size}) "
              f"hash={image_hash(img)}")
        print(f"   system: {ex.system_prompt[:60]}...")
        for i, s in enumerate(ex.steps):
            print(f"   step {i}: {s}")
        img.save(f"/tmp/dataset_{ex.image_name}.png")
        print(f"   saved /tmp/dataset_{ex.image_name}.png")
