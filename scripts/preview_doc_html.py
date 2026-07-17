#!/usr/bin/env python3
"""Generate a standalone HTML preview for a Markdown document."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / ".dever/html_preview"


HTML_TEMPLATE = """<!doctype html>
<html lang="{html_lang}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <link rel="preconnect" href="https://cdn.jsdelivr.net" />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/github-markdown-css@5/github-markdown-light.css" />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11/styles/github.min.css" />
  <style>
    :root {{
      color-scheme: light;
      --page-bg: #f5f7fa;
      --panel-bg: #ffffff;
      --border: #d8dee4;
      --text-muted: #57606a;
      --accent: #0969da;
    }}

    body {{
      margin: 0;
      background: var(--page-bg);
      color: #24292f;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif;
    }}

    .topbar {{
      position: sticky;
      top: 0;
      z-index: 5;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 10px 18px;
      border-bottom: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.92);
      backdrop-filter: blur(10px);
    }}

    .topbar-title {{
      min-width: 0;
      font-size: 14px;
      font-weight: 600;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}

    .topbar-actions {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex: none;
    }}

    .status {{
      color: var(--text-muted);
      font-size: 13px;
      white-space: nowrap;
    }}

    button {{
      height: 32px;
      padding: 0 10px;
      border: 1px solid var(--border);
      border-radius: 6px;
      background: #ffffff;
      color: #24292f;
      font: inherit;
      cursor: pointer;
    }}

    button:hover {{
      border-color: var(--accent);
      color: var(--accent);
    }}

    .page {{
      max-width: 1040px;
      margin: 0 auto;
      padding: 28px 18px 64px;
    }}

    .markdown-body {{
      box-sizing: border-box;
      min-width: 200px;
      padding: 38px 48px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel-bg);
      box-shadow: 0 10px 30px rgba(31, 35, 40, 0.06);
    }}

    .markdown-body h1 {{
      margin-top: 0;
      padding-bottom: 0.4em;
      font-size: 2em;
    }}

    .markdown-body pre {{
      border: 1px solid #d0d7de;
    }}

    .mermaid-card {{
      position: relative;
      margin: 18px 0;
      padding: 16px;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      background: #ffffff;
      overflow: auto;
    }}

    .mermaid-card::before {{
      content: "Click diagram to fullscreen";
      position: absolute;
      top: 8px;
      right: 10px;
      font-size: 12px;
      color: var(--text-muted);
      pointer-events: none;
    }}

    .mermaid-card svg {{
      display: block;
      max-width: none;
      margin: 0 auto;
      cursor: zoom-in;
    }}

    .mermaid-error {{
      white-space: pre-wrap;
      color: #b42318;
      background: #fff1f0;
    }}

    .diagram-modal {{
      position: fixed;
      inset: 0;
      z-index: 1000;
      display: none;
      background: rgba(15, 23, 42, 0.86);
    }}

    .diagram-modal.open {{
      display: grid;
      grid-template-rows: auto 1fr;
    }}

    .diagram-toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      background: #ffffff;
      border-bottom: 1px solid var(--border);
    }}

    .diagram-toolbar-title {{
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #24292f;
      font-size: 14px;
      font-weight: 600;
    }}

    .diagram-toolbar-actions {{
      display: flex;
      gap: 8px;
      flex: none;
    }}

    .diagram-stage {{
      min-height: 0;
      background: #f6f8fa;
      overflow: hidden;
      cursor: grab;
    }}

    .diagram-stage:active {{
      cursor: grabbing;
    }}

    .diagram-stage svg {{
      width: 100%;
      height: 100%;
      background: #ffffff;
    }}

    .help {{
      color: var(--text-muted);
      font-size: 13px;
    }}

    @media (max-width: 720px) {{
      .markdown-body {{
        padding: 24px 18px;
      }}

      .topbar {{
        align-items: flex-start;
        flex-direction: column;
      }}

      .topbar-actions {{
        width: 100%;
        overflow-x: auto;
      }}
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-title" id="previewTitle">{title}</div>
    <div class="topbar-actions">
      <span class="status" id="renderStatus">Waiting</span>
      <button type="button" id="rerenderBtn">重新渲染 Mermaid</button>
      <button type="button" id="printBtn">打印 / PDF</button>
    </div>
  </header>

  <main class="page">
    <article id="content" class="markdown-body"></article>
  </main>

  <div id="diagramModal" class="diagram-modal" aria-hidden="true">
    <div class="diagram-toolbar">
      <div class="diagram-toolbar-title">Mermaid diagram preview</div>
      <div class="help">拖拽平移，滚轮缩放，Esc 关闭</div>
      <div class="diagram-toolbar-actions">
        <button type="button" id="zoomInBtn">放大</button>
        <button type="button" id="zoomOutBtn">缩小</button>
        <button type="button" id="resetZoomBtn">重置</button>
        <button type="button" id="closeModalBtn">关闭</button>
      </div>
    </div>
    <div id="diagramStage" class="diagram-stage"></div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/marked@12/marked.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/highlight.js@11/lib/common.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom@3/dist/svg-pan-zoom.min.js"></script>
  <script>
    const markdownSource = {markdown_json};
    let panZoom = null;
    let mermaidSources = [];

    marked.setOptions({{
      gfm: true,
      breaks: false,
      headerIds: true,
      mangle: false,
    }});

    mermaid.initialize({{
      startOnLoad: false,
      securityLevel: "loose",
      theme: "default",
      flowchart: {{ useMaxWidth: false, htmlLabels: true }},
      sequence: {{ useMaxWidth: false }},
    }});

    function renderMarkdown() {{
      const content = document.getElementById("content");
      const preparedMarkdown = extractMermaidBlocks(markdownSource);
      const unsafeHtml = marked.parse(preparedMarkdown);
      content.innerHTML = DOMPurify.sanitize(unsafeHtml, {{
        ADD_TAGS: ["foreignObject"],
        ADD_ATTR: ["target", "rel", "class", "style", "data-mermaid-index"],
      }});

      restoreMermaidSources();

      content.querySelectorAll("a[href]").forEach((link) => {{
        const href = link.getAttribute("href") || "";
        if (/^https?:\\/\\//.test(href)) {{
          link.target = "_blank";
          link.rel = "noopener noreferrer";
        }}
      }});

      content.querySelectorAll("pre code").forEach((block) => {{
        try {{
          const languageClass = Array.from(block.classList).find((name) => name.startsWith("language-"));
          const language = languageClass ? languageClass.slice("language-".length) : "";
          if (!["", "text", "txt", "plain", "plaintext"].includes(language)) {{
            hljs.highlightElement(block);
          }}
        }} catch (error) {{
          console.warn("highlight.js skipped a block:", error);
        }}
      }});

      renderMermaid();
    }}

    function extractMermaidBlocks(markdown) {{
      mermaidSources = [];
      return markdown.replace(/^```mermaid[^\\n]*\\n([\\s\\S]*?)\\n```\\s*$/gm, (_match, source) => {{
        const index = mermaidSources.push(source.trim()) - 1;
        return `\\n<div class="mermaid-card"><div class="mermaid" data-mermaid-index="${{index}}"></div></div>\\n`;
      }});
    }}

    function restoreMermaidSources() {{
      document.querySelectorAll(".mermaid[data-mermaid-index]").forEach((diagram) => {{
        const index = Number(diagram.dataset.mermaidIndex);
        diagram.textContent = mermaidSources[index] || "";
      }});
    }}

    async function renderMermaid() {{
      const diagrams = Array.from(document.querySelectorAll(".mermaid"));
      const status = document.getElementById("renderStatus");
      status.textContent = `Rendering ${{diagrams.length}} diagrams`;
      let rendered = 0;

      for (const [index, diagram] of diagrams.entries()) {{
        const sourceIndex = Number(diagram.dataset.mermaidIndex);
        const source = (mermaidSources[sourceIndex] || diagram.textContent || "").trim();
        diagram.classList.remove("mermaid-error");
        diagram.textContent = "";

        try {{
          const result = await mermaid.render(`doc-mermaid-${{Date.now()}}-${{index}}`, source);
          diagram.innerHTML = result.svg;
          if (typeof result.bindFunctions === "function") {{
            result.bindFunctions(diagram);
          }}
          const svg = diagram.querySelector("svg");
          if (svg) {{
            rendered += 1;
            svg.dataset.diagramTitle = "Diagram " + (index + 1);
            svg.addEventListener("click", () => openDiagram(svg));
          }}
        }} catch (error) {{
          diagram.classList.add("mermaid-error");
          diagram.textContent = "Mermaid render failed:\\n" + String(error) + "\\n\\n" + source;
          console.error("Mermaid render failed for diagram", index + 1, error);
        }}
      }}

      status.textContent = `Mermaid: ${{rendered}}/${{diagrams.length}} rendered`;
    }}

    function openDiagram(sourceSvg) {{
      const modal = document.getElementById("diagramModal");
      const stage = document.getElementById("diagramStage");
      stage.innerHTML = sourceSvg.outerHTML;
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      document.body.style.overflow = "hidden";

      const svg = stage.querySelector("svg");
      svg.removeAttribute("style");
      svg.removeAttribute("width");
      svg.removeAttribute("height");
      svg.setAttribute("width", "100%");
      svg.setAttribute("height", "100%");

      if (panZoom) {{
        panZoom.destroy();
      }}
      panZoom = svgPanZoom(svg, {{
        zoomEnabled: true,
        controlIconsEnabled: false,
        fit: true,
        center: true,
        minZoom: 0.1,
        maxZoom: 20,
        zoomScaleSensitivity: 0.35,
      }});
      window.setTimeout(() => {{
        panZoom.resize();
        panZoom.fit();
        panZoom.center();
      }}, 0);
    }}

    function closeDiagram() {{
      const modal = document.getElementById("diagramModal");
      const stage = document.getElementById("diagramStage");
      if (panZoom) {{
        panZoom.destroy();
        panZoom = null;
      }}
      stage.innerHTML = "";
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
      document.body.style.overflow = "";
    }}

    document.getElementById("closeModalBtn").addEventListener("click", closeDiagram);
    document.getElementById("diagramModal").addEventListener("click", (event) => {{
      if (event.target.id === "diagramModal") {{
        closeDiagram();
      }}
    }});
    document.getElementById("zoomInBtn").addEventListener("click", () => panZoom && panZoom.zoomIn());
    document.getElementById("zoomOutBtn").addEventListener("click", () => panZoom && panZoom.zoomOut());
    document.getElementById("resetZoomBtn").addEventListener("click", () => {{
      if (panZoom) {{
        panZoom.resetZoom();
        panZoom.fit();
        panZoom.center();
      }}
    }});
    document.getElementById("rerenderBtn").addEventListener("click", renderMarkdown);
    document.getElementById("printBtn").addEventListener("click", () => window.print());
    window.addEventListener("keydown", (event) => {{
      if (event.key === "Escape") {{
        closeDiagram();
      }}
    }});
    window.addEventListener("resize", () => {{
      if (panZoom) {{
        panZoom.resize();
        panZoom.fit();
        panZoom.center();
      }}
    }});

    renderMarkdown();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a standalone HTML preview for a Markdown document with Mermaid fullscreen zoom/pan support."
    )
    parser.add_argument(
        "markdown_path",
        type=Path,
        help="Markdown input path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=f"HTML output path. Default: {DEFAULT_OUTPUT_ROOT.relative_to(REPO_ROOT)}/<input-relative-path>.html",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the generated HTML file with the system browser.",
    )
    return parser.parse_args()


def default_output_path(input_path: Path) -> Path:
    try:
        relative_input = input_path.relative_to(REPO_ROOT)
    except ValueError:
        digest = hashlib.sha256(str(input_path).encode("utf-8")).hexdigest()[:12]
        relative_input = Path("external") / f"{input_path.stem}-{digest}{input_path.suffix}"
    return (DEFAULT_OUTPUT_ROOT / relative_input).with_suffix(".html")


def infer_html_lang(input_path: Path, markdown: str) -> str:
    parts = set(input_path.parts)
    if "fluxon_doc_cn" in parts or input_path.name == "README_CN.md":
        return "zh-CN"
    if "fluxon_doc_en" in parts or input_path.name == "README.md":
        return "en"
    return "zh-CN" if re.search(r"[\u4e00-\u9fff]", markdown) else "en"


def first_markdown_heading(markdown: str, fallback: str) -> str:
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip() or fallback
    return fallback


def open_in_browser(path: Path) -> None:
    if sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
    elif os.name == "nt":
        os.startfile(str(path))  # type: ignore[attr-defined]
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def main() -> int:
    args = parse_args()
    input_path = args.markdown_path.resolve()

    if not input_path.exists():
        print(f"Input markdown does not exist: {input_path}", file=sys.stderr)
        return 1
    if not input_path.is_file():
        print(f"Input path is not a file: {input_path}", file=sys.stderr)
        return 1

    output_path = args.output.resolve() if args.output else default_output_path(input_path)

    markdown = input_path.read_text(encoding="utf-8")
    title = first_markdown_heading(markdown, input_path.stem)
    html = HTML_TEMPLATE.format(
        title=title,
        html_lang=infer_html_lang(input_path, markdown),
        markdown_json=json.dumps(markdown, ensure_ascii=False),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print(f"Generated HTML preview: {output_path}")
    print("Open it in a browser. Mermaid fullscreen zoom/pan uses CDN assets, so internet access is required.")

    if args.open:
        open_in_browser(output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
