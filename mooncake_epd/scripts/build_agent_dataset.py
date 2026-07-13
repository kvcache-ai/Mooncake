"""Export the procedural agent dataset to reproducible artifacts.

Outputs:
- PNG images under ``mooncake_epd/artifacts/agent_dataset/images``
- manifest JSON under ``mooncake_epd/artifacts/agent_dataset/manifest.json``
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.tests.dataset import build_dataset, image_hash, make_image, summarize


OUT_DIR = REPO_ROOT / "artifacts" / "agent_dataset"
IMAGE_DIR = OUT_DIR / "images"
MANIFEST_PATH = OUT_DIR / "manifest.json"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    dataset = build_dataset()
    manifest = {
        "summary": summarize(dataset),
        "examples": [],
    }

    for idx, ex in enumerate(dataset):
        img = make_image(ex.image_name)
        img_name = f"{idx:02d}_{ex.scenario}_{ex.image_name}.png"
        img_path = IMAGE_DIR / img_name
        img.save(img_path)
        manifest["examples"].append(
            {
                "index": idx,
                "scenario": ex.scenario,
                "image_name": ex.image_name,
                "image_file": str(img_path.relative_to(OUT_DIR)),
                "image_hash": image_hash(img),
                "system_prompt": ex.system_prompt,
                "steps": ex.steps,
                "tool_outputs": ex.tool_outputs,
                "expected_keywords": ex.expected_keywords,
            }
        )

    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"wrote {len(dataset)} examples to {OUT_DIR}")
    print(f"manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
