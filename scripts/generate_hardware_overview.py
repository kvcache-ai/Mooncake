# /// script
# dependencies = ["Pillow>=10.1"]
# ///
"""Regenerate the README hardware overview from the existing logo assets.

Run from any directory: uv run /path/to/scripts/generate_hardware_overview.py
Each logo gets the same cell, with whitespace trimmed and aspect ratio preserved.
A shared ink-area target balances visual weight within the same size limits.
"""

import math
from pathlib import Path

from PIL import Image, ImageChops

ROOT = Path(__file__).resolve().parents[1]
# Preserve the original README table order, left to right and top to bottom.
LOGOS = {
    "NVIDIA": "partners/nvidia_logo.png",
    "Huawei": "partners/huawei_logo.png",
    "AMD": "partners/amd_logo.png",
    "Cambricon": "hardwares/cambricon_logo.png",
    "Moore Threads": "partners/moore_thread_logo.jpg",
    "AWS": "partners/aws-logo.png",
    "MetaX": "hardwares/MetaX_logo.png",
    "T-Head": "hardwares/T-Head_logo.png",
    "Alibaba Cloud": "partners/aliyun_logo.png",
    "Sunrise": "partners/sunrise_logo.png",
    "Hygon": "partners/hygon_logo.png",
    "Biren Technology": "hardwares/biren_logo.png",
    "Intel": "hardwares/intel_logo.png",
    "Google": "hardwares/google_logo.png",
    "HPE": "hardwares/hpe_logo.png",
    "Iluvatar CoreX": "hardwares/iluvatar_corex_logo.png",
    "FlagOS": "hardwares/flagos_logo.png",
    "Enflame": "hardwares/enflame_logo.png",
}
OUTPUT = ROOT / "image/hardwares/supported_hardware.png"
COLUMNS = 6
CELL_WIDTH, CELL_HEIGHT = 300, 150
MARGIN = 28
LOGO_WIDTH, LOGO_HEIGHT = 236, 96
INK_AREA = 3600


def prepare_logo(path):
    with Image.open(path) as source:
        rgba = source.convert("RGBA")
    background = Image.new("RGBA", rgba.size, "white")
    background.alpha_composite(rgba)
    logo = background.convert("RGB")
    # Ignore near-white padding, including JPEG compression noise.
    ink = ImageChops.difference(logo, Image.new("RGB", logo.size, "white"))
    ink = ink.convert("L").point(lambda value: 255 if value > 12 else 0)
    bounds = ink.getbbox()
    if bounds is None:
        raise ValueError(f"Logo has no visible content: {path}")
    logo = logo.crop(bounds)
    ink_pixels = ink.histogram()[255]
    scale = min(
        math.sqrt(INK_AREA / ink_pixels),
        LOGO_WIDTH / logo.width,
        LOGO_HEIGHT / logo.height,
    )
    size = (max(1, round(logo.width * scale)), max(1, round(logo.height * scale)))
    return logo.resize(size, Image.Resampling.LANCZOS)


def main():
    entries = list(LOGOS.values())
    rows = math.ceil(len(entries) / COLUMNS)
    canvas = Image.new(
        "RGB",
        (COLUMNS * CELL_WIDTH + 2 * MARGIN, rows * CELL_HEIGHT + 2 * MARGIN),
        "white",
    )
    for index, path in enumerate(entries):
        row, column = divmod(index, COLUMNS)
        row_count = min(COLUMNS, len(entries) - row * COLUMNS)
        # Center a partial final row without changing its spacing or logo sizes.
        row_left = (canvas.width - row_count * CELL_WIDTH) // 2
        center_x = row_left + column * CELL_WIDTH + CELL_WIDTH // 2
        top = MARGIN + row * CELL_HEIGHT
        logo = prepare_logo(ROOT / "image" / path)
        canvas.paste(
            logo,
            (center_x - logo.width // 2, top + (CELL_HEIGHT - logo.height) // 2),
        )
    canvas.save(OUTPUT, optimize=True)
    print(f"Generated {OUTPUT.relative_to(ROOT)} ({len(entries)} logos)")


if __name__ == "__main__":
    main()
