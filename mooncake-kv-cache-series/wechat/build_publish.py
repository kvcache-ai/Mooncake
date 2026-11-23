#!/usr/bin/env python3
"""Build WeChat publish artifacts for the Mooncake KV Cache series.

The source articles live in this directory (``wechat/NN-*.md``). This script
does not rewrite prose: it only

  * renders each Mermaid block to a PNG (mermaid-cli + system Chrome),
  * replaces the Mermaid block with a Markdown image link,
  * normalises the caption marker so md2wx treats it as a caption,
  * rewrites relative links for the one-level-deeper ``publish/`` directory,
  * converts the result to inline-CSS HTML with md2wx,
  * refreshes a 11-entry manifest and index.

Usage:
    python3 build_publish.py --articles 1,2,3,4 \
        --md2wx /path/to/md2wx.py --chrome /usr/bin/google-chrome

Only 01-04 are built in this round; 05-11 stay source-only in the index.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

BASELINE_TAG = "v0.3.13.post1"
BASELINE_COMMIT = "719735896c86b56fabec6cf3e825fb2ea640597a"

MERMAID_RE = re.compile(r"^```mermaid[ \t]*\n(.*?)^```[ \t]*$", re.MULTILINE | re.DOTALL)
PLACEHOLDER_RE = re.compile(r"^\*配图(\d+)[：:]\s*(.*?)\*\s*$", re.MULTILINE)
CAPTION_RE = re.compile(r"^\*图注[：:]\s*(.*?)\*\s*$", re.MULTILINE)
H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
ABSTRACT_RE = re.compile(r"^\*\*摘要：\*\*\s*(.+?)\s*$", re.MULTILINE)
SOURCE_ANALYSIS_RE = re.compile(r"\[源码解析版\]\(([^)]+)\)")
IMAGE_LINK_RE = re.compile(r"^!\[(.*?)\]\((\S+?)\)\s*$", re.MULTILINE)
MD_LINK_RE = re.compile(r"\]\(([^)]+)\)")
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

# (source filename, source-analysis link target) for all 11 articles.
SERIES = [
    ("01-shared-kv-cache.md", "../00-architecture-overview.md"),
    ("02-objects-and-replicas.md", "../04-core-data-structures.md"),
    ("03-put-visibility.md", "../02-put-path.md"),
    ("04-get-and-lease.md", "../03-get-path.md"),
    ("05-source-reading.md", "../01-source-navigation.md"),
    ("06-transfer-lifecycle.md", "../05-transfer-engine-task-lifecycle.md"),
    ("07-rdma-data-path.md", "../06-rdma-data-path.md"),
    ("08-multi-nic-numa.md", "../07-multi-nic-numa.md"),
    ("09-cache-eviction.md", "../08-cache-lifecycle.md"),
    ("10-ssd-tiering.md", "../09-ssd-tiering.md"),
    ("11-kv-reuse-and-inference.md", "../10-vllm-integration.md"),
]

HARD_WARNINGS = (
    "检测到 mermaid 代码块",
    "图片后未紧跟图注",
    "出现多个 H1",
)


class BuildError(RuntimeError):
    pass


@dataclass
class Figure:
    number: int
    code: str
    png: Path
    render_status: str = "pending"
    width: int = 0
    height: int = 0
    sha256: str = ""
    error: str = ""


@dataclass
class ParsedArticle:
    order: int
    source: Path
    stem: str
    text: str
    title: str
    abstract: str
    source_analysis: str
    figures: list = field(default_factory=list)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return start.resolve()


def rel_to_repo(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path)


def parse_article(order: int, source: Path) -> ParsedArticle:
    text = source.read_text(encoding="utf-8")
    title_match = H1_RE.search(text)
    abstract_match = ABSTRACT_RE.search(text)
    if not title_match:
        raise BuildError(f"{source.name}: 找不到一级标题")
    if not abstract_match:
        raise BuildError(f"{source.name}: 找不到摘要行")
    analysis_match = SOURCE_ANALYSIS_RE.search(text)
    analysis = analysis_match.group(1) if analysis_match else ""

    parsed = ParsedArticle(
        order=order,
        source=source,
        stem=source.stem,
        text=text,
        title=title_match.group(1).strip(),
        abstract=abstract_match.group(1).strip(),
        source_analysis=analysis,
    )

    placeholders = PLACEHOLDER_RE.findall(text)
    blocks = MERMAID_RE.findall(text)
    captions = CAPTION_RE.findall(text)
    if len(blocks) != len(placeholders):
        raise BuildError(
            f"{source.name}: Mermaid 图 {len(blocks)} 个，配图占位 {len(placeholders)} 个，数量不一致"
        )
    if len(blocks) != len(captions):
        raise BuildError(
            f"{source.name}: Mermaid 图 {len(blocks)} 个，图注 {len(captions)} 个，数量不一致"
        )
    for index, (number, _) in enumerate(placeholders, start=1):
        if int(number) != index:
            raise BuildError(f"{source.name}: 配图编号不连续（期望 {index}，实际 {number}）")
    parsed.figures = [Figure(number=i, code=code, png=Path()) for i, code in enumerate(blocks, start=1)]
    return parsed


def parse_selection(raw: str) -> list:
    if raw.strip().lower() == "all":
        return list(range(1, len(SERIES) + 1))
    selected = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", token)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            if start > end:
                start, end = end, start
            selected.update(range(start, end + 1))
            continue
        if not token.isdigit():
            raise BuildError(f"无法解析 --articles 片段：{token}")
        selected.add(int(token))
    if not selected:
        raise BuildError("--articles 为空")
    invalid = sorted(n for n in selected if n < 1 or n > len(SERIES))
    if invalid:
        raise BuildError(f"--articles 超出范围 1-{len(SERIES)}：{invalid}")
    return sorted(selected)


def locate_md2wx(explicit: str) -> Path:
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("MD2WX")
    if env:
        candidates.append(Path(env))
    candidates.append(Path("/home/yangxx/.claude/skills/producing-tech-blog/scripts/md2wx.py"))
    candidates.append(Path.home() / ".claude/skills/producing-tech-blog/scripts/md2wx.py")
    candidates.append(Path(__file__).resolve().parent / "tools" / "md2wx.py")
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    tried = "\n  ".join(str(c) for c in candidates)
    raise BuildError(f"找不到 md2wx.py，已尝试：\n  {tried}")


def locate_chrome(explicit: str) -> Path:
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("CHROME_PATH")
    if env:
        candidates.append(Path(env))
    candidates.extend(
        Path(p)
        for p in (
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/snap/bin/chromium",
        )
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise BuildError("找不到 Chrome/Chromium，请用 --chrome 指定可执行文件")


def renderer_command(args) -> list:
    if args.renderer == "mmdc":
        if not shutil.which("mmdc"):
            raise BuildError("--renderer mmdc 但 PATH 中没有 mmdc")
        return ["mmdc"]
    if args.renderer == "npx":
        if not shutil.which("npx"):
            raise BuildError("--renderer npx 但 PATH 中没有 npx")
        return ["npx", "-y", args.mermaid_cli]
    if shutil.which("mmdc"):
        return ["mmdc"]
    if shutil.which("npx"):
        return ["npx", "-y", args.mermaid_cli]
    raise BuildError("找不到 mmdc 或 npx；可改用 --renderer none")


def write_tool_configs(work_dir: Path, chrome: Path, font_family: str) -> tuple:
    work_dir.mkdir(parents=True, exist_ok=True)
    puppeteer = work_dir / "puppeteer.json"
    puppeteer.write_text(
        json.dumps(
            {
                "executablePath": str(chrome),
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--font-render-hinting=none",
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    mermaid_config = work_dir / "mermaid-config.json"
    font_stack = f"{font_family}, 'Noto Sans CJK SC', 'WenQuanYi Zen Hei', 'Microsoft YaHei', sans-serif"
    mermaid_config.write_text(
        json.dumps(
            {
                "theme": "base",
                "themeVariables": {
                    "fontFamily": font_stack,
                    "fontSize": "16px",
                    "primaryColor": "#F2F7FF",
                    "primaryBorderColor": "#1677FF",
                    "primaryTextColor": "#0B1F3A",
                    "lineColor": "#5B6B82",
                    "tertiaryColor": "#F7F9FC",
                },
                "flowchart": {
                    "htmlLabels": True,
                    "curve": "linear",
                    "nodeSpacing": 36,
                    "rankSpacing": 48,
                    "useMaxWidth": True,
                },
                "sequence": {
                    "useMaxWidth": True,
                    "wrap": True,
                    "boxMargin": 8,
                    "noteMargin": 8,
                    "messageMargin": 30,
                    "mirrorActors": False,
                    "showSequenceNumbers": False,
                },
                "themeCSS": (
                    ".label,.nodeLabel,.messageText,.noteText,.actor,.loopText"
                    "{font-family:'Noto Sans CJK SC','WenQuanYi Zen Hei',sans-serif !important;}"
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return puppeteer, mermaid_config


def verify_png(path: Path) -> tuple:
    data = path.read_bytes()
    if not data:
        raise BuildError(f"{path.name}: 空文件")
    if data[:8] != PNG_MAGIC:
        raise BuildError(f"{path.name}: 不是 PNG 文件")
    try:
        from PIL import Image, ImageStat
    except ImportError:
        return 0, 0
    image = Image.open(path)
    if image.format != "PNG":
        raise BuildError(f"{path.name}: PIL 识别为 {image.format}，不是 PNG")
    width, height = image.size
    if width <= 0 or height <= 0:
        raise BuildError(f"{path.name}: 尺寸无效 {width}x{height}")
    if width > 4000:
        raise BuildError(f"{path.name}: 宽度 {width}px 过大")
    if height < 100:
        raise BuildError(f"{path.name}: 高度 {height}px 过小")
    stddev = ImageStat.Stat(image.convert("L")).stddev[0]
    if stddev <= 3:
        raise BuildError(f"{path.name}: 图像接近空白（stddev={stddev:.2f}）")
    return width, height


def render_figures(articles, args, work_dir: Path, stage_images: Path, chrome: Path) -> None:
    if args.renderer == "none":
        return
    command = renderer_command(args)
    puppeteer, mermaid_config = write_tool_configs(work_dir, chrome, args.font)
    stage_images.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PUPPETEER_SKIP_DOWNLOAD"] = "1"
    env["PUPPETEER_SKIP_CHROMIUM_DOWNLOAD"] = "true"
    env["PUPPETEER_EXECUTABLE_PATH"] = str(chrome)

    for article in articles:
        for figure in article.figures:
            staged = stage_images / f"{article.stem}-fig{figure.number}.png"
            figure.png = staged
            if staged.exists() and not args.force:
                width, height = verify_png(staged)
                figure.width, figure.height = width, height
                figure.sha256 = sha256_file(staged)
                figure.render_status = "ok"
                print(f"  复用 {staged.name}")
                continue
            mmd = work_dir / f"{article.stem}-fig{figure.number}.mmd"
            mmd.write_text(figure.code.rstrip() + "\n", encoding="utf-8")
            cmd = command + [
                "-i",
                str(mmd),
                "-o",
                str(staged),
                "-p",
                str(puppeteer),
                "-c",
                str(mermaid_config),
                "-b",
                "white",
                "-w",
                str(args.width),
                "-s",
                str(args.scale),
            ]
            print(f"  渲染 {article.stem}-fig{figure.number}.png")
            proc = subprocess.run(cmd, cwd=str(work_dir), env=env, capture_output=True, text=True)
            if proc.returncode != 0:
                message = (proc.stderr or proc.stdout or "").strip().splitlines()
                detail = message[-1] if message else f"exit={proc.returncode}"
                figure.render_status = "failed"
                figure.error = detail
                if not args.allow_missing_images:
                    raise BuildError(
                        f"{article.stem}-fig{figure.number}: Mermaid 渲染失败：{detail}"
                    )
                print(f"    警告：渲染失败，保留 Mermaid 源（{detail}）")
                continue
            try:
                width, height = verify_png(staged)
            except BuildError as exc:
                figure.render_status = "failed"
                figure.error = str(exc)
                if not args.allow_missing_images:
                    raise
                print(f"    警告：{exc}，保留 Mermaid 源")
                continue
            figure.width, figure.height = width, height
            figure.sha256 = sha256_file(staged)
            figure.render_status = "ok"


def rewrite_links(text: str, stems: set, built_stems: set) -> str:
    def replace(match):
        target = match.group(1)
        if target.startswith(("http://", "https://", "#")):
            return match.group(0)
        name = target.split("/")[-1]
        if Path(name).stem in stems:
            if Path(name).stem in built_stems:
                return f"]({name})"
            return f"](../{name})"
        if target == "README.md":
            return match.group(0)
        if target.startswith("../"):
            return f"](../../{target[3:]})"
        return match.group(0)

    return MD_LINK_RE.sub(replace, text)


def build_publish_markdown(article: ParsedArticle, stems: set, built_stems: set) -> str:
    text = article.text
    placeholders = PLACEHOLDER_RE.findall(text)
    text = PLACEHOLDER_RE.sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    counter = {"n": 0}

    def replace_block(match):
        counter["n"] += 1
        number = counter["n"]
        alt = ""
        if number <= len(placeholders):
            alt = placeholders[number - 1][1].strip()
            if alt.startswith("配图"):
                alt = alt.split("：", 1)[-1].split(":", 1)[-1].strip()
        figure = article.figures[number - 1] if number <= len(article.figures) else None
        if figure is not None and figure.render_status != "ok":
            # Degrade explicitly: keep the Mermaid source and say so.
            return (
                match.group(0)
                + "\n\n> 配图渲染失败，暂保留 Mermaid 源。\n"
            )
        if not alt:
            alt = f"配图{number}"
        return f"![配图{number}：{alt}](images/{article.stem}-fig{number}.png)"

    text = MERMAID_RE.sub(replace_block, text)
    text = CAPTION_RE.sub(lambda m: f"图注：{m.group(1).strip()}", text)
    text = rewrite_links(text, stems, built_stems)
    return text


def strip_first_h1(text: str) -> str:
    return re.sub(r"^#\s+.*\n+", "", text, count=1)


def run_md2wx(md2wx: Path, md_path: Path, html_path: Path, title: str, args) -> dict:
    html_src = md_path.with_name(md_path.stem + ".html-src.md")
    html_src.write_text(strip_first_h1(md_path.read_text(encoding="utf-8")), encoding="utf-8")
    if html_path.exists():
        html_path.unlink()
    report_path = html_path.with_suffix(".report.json")
    if report_path.exists():
        report_path.unlink()
    cmd = [
        sys.executable,
        str(md2wx),
        str(html_src),
        "-o",
        str(html_path),
        "--title",
        title,
        "--accent",
        args.accent,
        "--font-size",
        str(args.font_size),
        "--line-height",
        str(args.line_height),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    html_src.unlink(missing_ok=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise BuildError(f"md2wx 失败：{detail}")
    if not html_path.exists():
        raise BuildError("md2wx 未生成 HTML")
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
    return report


def check_publish_markdown(path: Path, expected_figures: int) -> list:
    problems = []
    text = path.read_text(encoding="utf-8")
    if "```mermaid" in text:
        problems.append("仍有 Mermaid 代码块")
    images = IMAGE_LINK_RE.findall(text)
    if len(images) != expected_figures:
        problems.append(f"图片链接 {len(images)} 个，期望 {expected_figures} 个")
    for alt, src in images:
        if not src.startswith("images/"):
            problems.append(f"图片路径不是 images/：{src}")
    if text.count("图注：") < expected_figures:
        problems.append(f"图注少于图片数（{text.count('图注：')} < {expected_figures}）")
    return problems


def check_html(path: Path, expected_images: int) -> list:
    problems = []
    text = path.read_text(encoding="utf-8")
    images = re.findall(r'<img src="(images/[^"]+)"', text)
    if len(images) != expected_images:
        problems.append(f"HTML 图片 {len(images)} 个，期望 {expected_images} 个")
    if "mermaid" in text.lower():
        problems.append("HTML 中仍出现 mermaid 字样")
    for bad in ("/builder_img", "/home/", "file://"):
        if bad in text:
            problems.append(f"HTML 中出现本机路径：{bad}")
    return problems


def resolve_link_targets(publish_dir: Path) -> list:
    problems = []
    for md in sorted(publish_dir.glob("*.md")):
        text = md.read_text(encoding="utf-8")
        for target in MD_LINK_RE.findall(text):
            if target.startswith(("http://", "https://", "#")):
                continue
            if not (md.parent / target).resolve().exists():
                problems.append(f"{md.name}: 链接无法解析 {target}")
    return problems


def toolchain_record(args, md2wx: Path, chrome: Path) -> dict:
    version = ""
    if args.renderer != "none":
        try:
            proc = subprocess.run(
                ["npx", "-y", args.mermaid_cli, "--version"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if proc.returncode == 0:
                output = (proc.stdout or proc.stderr).strip().splitlines()
                version = output[0] if output else ""
        except Exception:
            pass
    chrome_version = ""
    if chrome.exists():
        try:
            proc = subprocess.run([str(chrome), "--version"], capture_output=True, text=True, timeout=30)
            chrome_version = (proc.stdout or proc.stderr).strip()
        except Exception:
            pass
    return {
        "python": sys.version.split()[0],
        "md2wx": {"path": str(md2wx), "sha256": sha256_file(md2wx)},
        "mmdc": {"spec": args.mermaid_cli, "resolved_version": version, "via": args.renderer},
        "chrome": {"path": str(chrome), "version": chrome_version},
        "font_family": args.font,
        "scale": args.scale,
        "figure_width": args.width,
    }


def build_manifest(args, root: Path, articles: list, selected: list, built_stems: set,
                   parsed_all: dict, md2wx: Path, chrome: Path) -> dict:
    entries = []
    previous = {}
    if args.manifest.exists():
        try:
            previous = json.loads(args.manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = {}
    previous_by_slug = {a.get("slug"): a for a in previous.get("articles", [])}

    for article in articles:
        spec_source = article.source
        entry = {
            "order": article.order,
            "id": article.stem[:2],
            "slug": article.stem,
            "title": article.title,
            "abstract": article.abstract,
            "source": rel_to_repo(spec_source, root),
            "source_analysis": article.source_analysis,
            "source_sha256": sha256_file(spec_source),
            "figure_count": len(article.figures),
        }
        if article.order in selected:
            status = "image-pending" if any(
                figure.render_status != "ok" for figure in article.figures
            ) else "built"
            entry.update(
                {
                    "status": status,
                    "markdown": rel_to_repo(args.publish_dir / f"{article.stem}.md", root),
                    "html": rel_to_repo(args.publish_dir / f"{article.stem}.html", root),
                    "report": rel_to_repo(args.publish_dir / f"{article.stem}.report.json", root),
                    "figures": [
                        {
                            "n": figure.number,
                            "png": rel_to_repo(args.publish_dir / "images" / f"{article.stem}-fig{figure.number}.png", root),
                            "width": figure.width,
                            "height": figure.height,
                            "sha256": figure.sha256,
                            "render_status": figure.render_status,
                            "mermaid_sha256": sha256_bytes(figure.code.encode("utf-8")),
                            "error": figure.error or None,
                        }
                        for figure in article.figures
                    ],
                }
            )
            md_path = args.publish_dir / f"{article.stem}.md"
            report_path = args.publish_dir / f"{article.stem}.report.json"
            report = {}
            if report_path.exists():
                report = json.loads(report_path.read_text(encoding="utf-8"))
            entry["md2wx"] = {
                "warnings": report.get("warnings", []),
                "stats": report.get("stats", {}),
            }
        else:
            prior = previous_by_slug.get(article.stem)
            assets_ok = prior and prior.get("status") == "built"
            if assets_ok:
                paths = [args.publish_dir / f"{article.stem}.md", args.publish_dir / f"{article.stem}.html"]
                paths += [args.publish_dir / "images" / f"{article.stem}-fig{n}.png" for n in range(1, len(article.figures) + 1)]
                assets_ok = all(p.exists() for p in paths)
                assets_ok = assets_ok and prior.get("source_sha256") == entry["source_sha256"]
            if assets_ok:
                entry.update(
                    {
                        "status": "built",
                        "markdown": prior.get("markdown"),
                        "html": prior.get("html"),
                        "report": prior.get("report"),
                        "figures": prior.get("figures", []),
                        "md2wx": prior.get("md2wx", {"warnings": [], "stats": {}}),
                    }
                )
            else:
                entry.update(
                    {
                        "status": "source-only",
                        "markdown": None,
                        "html": None,
                        "report": None,
                        "figures": [],
                        "md2wx": {"warnings": [], "stats": {}},
                    }
                )
        entries.append(entry)

    built = [a["slug"] for a in entries if a["status"] == "built"]
    pending = [a["slug"] for a in entries if a["status"] != "built"]
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "baseline": {"tag": BASELINE_TAG, "commit": BASELINE_COMMIT},
        "series": {"total": len(SERIES), "built": built, "pending": pending},
        "toolchain": toolchain_record(args, md2wx, chrome),
        "verification": {
            "wechat_editor_preview": {"status": "not_performed", "reason": "non-GUI environment"},
            "mobile_legibility": {"status": "not_performed", "reason": "no GUI"},
            "wechat_asset_upload": {"status": "not_performed"},
            "github_permalinks": {"status": "not_checked", "reason": "constructed from local tag SHA"},
        },
        "articles": entries,
    }


def render_index(manifest: dict) -> str:
    lines = [
        "# Mooncake KV Cache 微信公众号发布稿",
        "",
        "本目录由 `wechat/build_publish.py` 生成。已生成的篇目包含 Markdown、内联样式 HTML 与配图 PNG；其余篇目保留源 Markdown。",
        "",
        f"基线：`{BASELINE_TAG}` / `{BASELINE_COMMIT[:7]}`。生成时间：{manifest['generated_at']}。",
        "",
        "## 系列目录（11 篇）",
        "",
        "| 篇次 | 文章 | 状态 | Markdown | HTML | 配图 |",
        "|---|---|---|---|---|---|",
    ]
    for entry in manifest["articles"]:
        if entry["status"] == "built":
            status = "已生成"
            md_link = f"[MD](./{entry['slug']}.md)"
            html_link = f"[HTML](./{entry['slug']}.html)"
        elif entry["status"] == "image-pending":
            status = "配图待生成"
            md_link = f"[MD](./{entry['slug']}.md)"
            html_link = f"[HTML](./{entry['slug']}.html)"
        else:
            status = "源稿"
            md_link = f"[源稿](../{entry['slug']}.md)"
            html_link = "—"
        figures = entry.get("figure_count", len(entry.get("figures", [])))
        lines.append(
            f"| {entry['order']} | {entry['title']} | {status} | {md_link} | {html_link} | {figures} |"
        )
    lines += [
        "",
        "## 构建与校验",
        "",
        "```bash",
        "cd mooncake-kv-cache-series/wechat",
        "python3 build_publish.py --articles 1,2,3,4 \\",
        "  --md2wx /home/yangxx/.claude/skills/producing-tech-blog/scripts/md2wx.py \\",
        "  --chrome /usr/bin/google-chrome --renderer npx",
        "```",
        "",
        "脚本只替换 Mermaid 图与相对链接，不改写正文。`manifest.json` 记录每篇的产物、配图尺寸与 md2wx 报告。",
        "",
        "## 未执行项",
        "",
        "- 未在公众号编辑器中粘贴预览（非 GUI 环境）。",
        "- 未做手机宽度下的人工可读性检查；配图宽高与是否空白由脚本校验。",
        "- 未上传图片到公众号素材库，也未发布。",
        "- GitHub 永久链接按本地 tag 的 commit 构造，未做 HTTP 可达性校验。",
        "",
    ]
    return "\n".join(lines)


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def build(args) -> int:
    script_dir = Path(__file__).resolve().parent
    source_dir = script_dir
    publish_dir = args.publish_dir.resolve()
    publish_dir.mkdir(parents=True, exist_ok=True)
    images_dir = publish_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    work_dir = publish_dir / ".work"
    stage_images = work_dir / "stage" / "images"
    if work_dir.exists() and args.force:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    root = repo_root(script_dir)
    md2wx = locate_md2wx(args.md2wx)
    chrome = locate_chrome(args.chrome) if args.renderer != "none" else Path(args.chrome or "none")
    selected = parse_selection(args.articles)

    parsed_all = {}
    for order, (name, _) in enumerate(SERIES, start=1):
        path = source_dir / name
        if not path.exists():
            raise BuildError(f"源稿不存在：{path}")
        parsed_all[order] = parse_article(order, path)

    all_stems = {spec.stem for spec in parsed_all.values()}
    articles = [parsed_all[n] for n in selected]
    built_stems = {article.stem for article in articles}

    print(f"基线 {BASELINE_TAG} ({BASELINE_COMMIT[:7]})，构建 {len(articles)} 篇")
    if args.renderer == "none":
        for article in articles:
            for figure in article.figures:
                png = images_dir / f"{article.stem}-fig{figure.number}.png"
                if not png.exists():
                    raise BuildError(f"{article.stem}-fig{figure.number}: --renderer none 但缺少 {png}")
                width, height = verify_png(png)
                figure.png = png
                figure.width, figure.height = width, height
                figure.sha256 = sha256_file(png)
                figure.render_status = "ok"
    else:
        render_figures(articles, args, work_dir, stage_images, chrome)
        # Publish only after every selected figure is either ok or explicitly degraded.
        for article in articles:
            for figure in article.figures:
                staged = stage_images / f"{article.stem}-fig{figure.number}.png"
                if figure.render_status == "ok" and staged.exists():
                    shutil.copy2(staged, images_dir / staged.name)

    degraded_stems = {
        article.stem for article in articles if any(f.render_status != "ok" for f in article.figures)
    }
    problems = []
    for article in articles:
        md_text = build_publish_markdown(article, all_stems, built_stems)
        md_path = publish_dir / f"{article.stem}.md"
        write_atomic(md_path, md_text)
        html_path = publish_dir / f"{article.stem}.html"
        report = run_md2wx(md2wx, md_path, html_path, article.title, args)
        report_warnings = report.get("warnings", [])
        for warning in report_warnings:
            if any(marker in warning for marker in HARD_WARNINGS):
                if article.stem in degraded_stems and "检测到 mermaid" in warning:
                    continue
                problems.append(f"{article.stem}: md2wx 硬警告：{warning}")
        md_problems = check_publish_markdown(md_path, len(article.figures))
        html_problems = check_html(html_path, len(article.figures))
        if article.stem in degraded_stems:
            md_problems = [p for p in md_problems if "Mermaid" not in p and "图片链接" not in p and "图注" not in p]
            html_problems = [p for p in html_problems if "mermaid" not in p.lower()]
        problems.extend(f"{article.stem}: {p}" for p in md_problems)
        problems.extend(f"{article.stem}: {p}" for p in html_problems)

    # 04 must still point at the existing 05 source.
    md04 = publish_dir / "04-get-and-lease.md"
    if md04.exists() and "](../05-source-reading.md)" not in md04.read_text(encoding="utf-8"):
        problems.append("04-get-and-lease.md: 篇尾未指向 ../05-source-reading.md")

    if not args.allow_missing_images:
        for article in articles:
            if len(article.figures) and any(f.render_status != "ok" for f in article.figures):
                problems.append(f"{article.stem}: 存在未成功渲染的配图")

    manifest = build_manifest(args, root, [parsed_all[n] for n in range(1, len(SERIES) + 1)], selected,
                              built_stems, parsed_all, md2wx, chrome)
    manifest_path = publish_dir / "manifest.json"
    write_atomic(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    write_atomic(publish_dir / "README.md", render_index(manifest))
    write_atomic(publish_dir / ".gitignore", ".work/\n")
    problems.extend(resolve_link_targets(publish_dir))

    if not args.keep_work and work_dir.exists():
        shutil.rmtree(work_dir, ignore_errors=True)

    print("")
    print(f"产物目录：{publish_dir}")
    for article in articles:
        figures = ", ".join(f"fig{f.number}={f.width}x{f.height}" for f in article.figures)
        print(f"  {article.stem}: {len(article.figures)} 图  {figures}")
    print(f"manifest：{manifest_path}")

    if problems:
        print("")
        print(f"校验失败 {len(problems)} 项：")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("校验通过：无 Mermaid 残留、图片与图注数量匹配、相对链接可解析。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 Mooncake KV Cache 公众号发布稿")
    parser.add_argument("--articles", default="all", help="1,2,3,4 / 01-04 / all")
    parser.add_argument("--md2wx", default="", help="md2wx.py 路径")
    parser.add_argument("--chrome", default="", help="Chrome/Chromium 可执行文件")
    parser.add_argument("--renderer", choices=["auto", "npx", "mmdc", "none"], default="auto")
    parser.add_argument("--mermaid-cli", default="@mermaid-js/mermaid-cli@11")
    parser.add_argument("--publish-dir", type=Path, default=Path(__file__).resolve().parent / "publish")
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--width", type=int, default=1200)
    parser.add_argument("--scale", type=int, default=2)
    parser.add_argument("--font", default="Noto Sans CJK SC")
    parser.add_argument("--accent", default="#1677FF")
    parser.add_argument("--font-size", type=int, default=16)
    parser.add_argument("--line-height", type=float, default=1.8)
    parser.add_argument("--force", action="store_true", help="忽略已有 PNG，强制重渲染")
    parser.add_argument("--allow-missing-images", action="store_true", help="渲染失败时保留 Mermaid 源")
    parser.add_argument("--keep-work", action="store_true", help="保留 .work 临时目录")
    args = parser.parse_args()
    args.publish_dir = args.publish_dir.resolve()
    if args.manifest is None:
        args.manifest = args.publish_dir / "manifest.json"
    args.manifest = args.manifest.resolve()
    try:
        return build(args)
    except BuildError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
