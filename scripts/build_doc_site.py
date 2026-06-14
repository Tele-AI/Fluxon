#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import shlex
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import SimpleHTTPRequestHandler
from pathlib import Path
from textwrap import dedent


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOC_ROOT = REPO_ROOT / "fluxon_doc_linked" / "fluxon_doc"
FALLBACK_DOC_ROOT = REPO_ROOT / "fluxon_doc"
OUTPUT_ROOT = REPO_ROOT / "fluxon_release" / "doc_site"
CACHE_ROOT = REPO_ROOT / ".cached" / "fluxon_doc_site"
PROJECT_ROOT = (
    Path(tempfile.gettempdir())
    / f"fluxon_doc_site_{hashlib.sha256(str(REPO_ROOT).encode('utf-8')).hexdigest()[:12]}"
)
STAGE_DOCS_ROOT = PROJECT_ROOT / "content"
HOMEPAGE_MARKDOWN_SOURCE = REPO_ROOT / "README_CN.md"
HOMEPAGE_ROOT_PICS_DIR = REPO_ROOT / "pics"
HOMEPAGE_SUPPORT_FILE_PATHS = (
    REPO_ROOT / "LICENSE",
    REPO_ROOT / "fluxon_rs" / "rust-toolchain.toml",
)
# Quartz is treated as ephemeral build tooling under .cached rather than a repo module.
# We intentionally do not route it through rather_no_git_submodule.py.
TOOLCHAIN_ROOT = CACHE_ROOT / "toolchain" / "quartz"
NPM_CACHE_ROOT = CACHE_ROOT / "npm-cache"
RUNTIME_CONFIG_PATH = TOOLCHAIN_ROOT / "quartz.config.yaml"
RUNTIME_LOCKFILE_PATH = TOOLCHAIN_ROOT / "quartz.lock.json"
NPM_STAMP_PATH = TOOLCHAIN_ROOT / ".fluxon-npm-stamp"
PLUGIN_STAMP_PATH = TOOLCHAIN_ROOT / ".fluxon-plugin-stamp"
DEFAULT_SERVE_ADDR = "127.0.0.1:18081"
DEFAULT_TRACK_POLL_SECONDS = 1.0
EXPLORER_FORCE_EXPANDED_CSS = dedent(
    """\
    /* Fluxon doc-site override: keep the left explorer fully expanded. */
    .explorer .folder-outer,
    .explorer .folder-outer.open {
      visibility: visible !important;
      grid-template-rows: 1fr !important;
    }

    .explorer li:has(> .folder-outer:not(.open)) > .folder-container > svg {
      transform: none !important;
    }
    """
)
EXPLORER_ADD_HOME_LINK_JS = dedent(
    """\
    ;(() => {
      function insertFluxonExplorerHomeLink() {
        const homeHref = document.querySelector(".left.sidebar .page-title a")?.getAttribute("href")
        if (!homeHref) return

        document.querySelectorAll(".explorer-ul").forEach((list) => {
          list.querySelector("li.fluxon-home-link")?.remove()

          const item = document.createElement("li")
          item.className = "fluxon-home-link"

          const link = document.createElement("a")
          link.href = homeHref
          link.className = "nav-file-title tree-item-self"
          link.textContent = "首页"

          if ((document.body?.dataset?.slug || "") === "index") {
            link.classList.add("active", "is-active")
          }

          item.appendChild(link)
          const overflowEnd = list.querySelector("li.overflow-end")
          list.insertBefore(item, overflowEnd || list.firstChild)

          const roadmapItem = Array.from(list.children).find((child) => {
            const firstElement = child.firstElementChild
            if (!(firstElement instanceof HTMLAnchorElement)) return false
            const href = firstElement.getAttribute("href") || ""
            return href === "/roadmap" || href === "./roadmap" || href === "roadmap" || href.endsWith("/roadmap")
          })
          if (roadmapItem && roadmapItem !== item.nextSibling) {
            list.insertBefore(roadmapItem, item.nextSibling || overflowEnd || null)
          }
        })
      }

      function scheduleFluxonExplorerHomeLink(attempt = 0) {
        const delayMs = attempt === 0 ? 0 : 120
        window.setTimeout(() => {
          insertFluxonExplorerHomeLink()
          const needsRetry = Array.from(document.querySelectorAll(".explorer-ul")).some(
            (list) => list.children.length <= 1,
          )
          if (needsRetry && attempt < 8) {
            scheduleFluxonExplorerHomeLink(attempt + 1)
          }
        }, delayMs)
      }

      document.addEventListener("DOMContentLoaded", () => scheduleFluxonExplorerHomeLink())
      document.addEventListener("render", () => scheduleFluxonExplorerHomeLink())
      document.addEventListener("nav", () => scheduleFluxonExplorerHomeLink())
      scheduleFluxonExplorerHomeLink()
    })();
    """
)
QUARTZ_REPO_URL = "https://github.com/jackyzha0/quartz.git"
QUARTZ_REF = "v5.0.0"
QUARTZ_COMMIT = "ab346fa66a895e12d63a308e70ce330ba795822a"
SPARSE_CHECKOUT_PATHS = (
    ".npmrc",
    "globals.d.ts",
    "index.d.ts",
    "package-lock.json",
    "package.json",
    "quartz",
    "quartz.ts",
    "tsconfig.json",
)
MARKDOWN_LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^)]+)(\))")
SKIP_DIR_NAMES = {"states"}
PLUGIN_COMMITS = {
    "created-modified-date": "c003199fb842969d43ee9e0f54120a85e588260e",
    "syntax-highlighting": "5bfdc2c3f42d3d0326c4e777eb575f3fb68d51fb",
    "obsidian-flavored-markdown": "07eaca7b31a537c7c4a0fd2848b1f00014c940af",
    "github-flavored-markdown": "3eabbaa252ce175665ab3f62e1af25948a83e8b6",
    "table-of-contents": "6984305e5dae0830c025450e160f12610406f7a4",
    "crawl-links": "43edc6d5182e79bf1b63fed7eb3ba0c7624a1526",
    "description": "56dc546614d905ad07dd0da8dd5820e25e5ea97b",
    "alias-redirects": "73a98dda7e4f55239310833299d91daf8611349f",
    "content-index": "c3d4f5c85311712c3355cd71da46b28e2d8eba71",
    "favicon": "85842d5c15f937a3d1a02c45accee27118146d73",
    "og-image": "31343c612d02c5fd22ff27a1e6035b2486be75f5",
    "content-page": "d22fae357ae74a3e97a2f450862f23f5227842c4",
    "folder-page": "93304d22e1d7f09f93a33658ec273f7cb8d17793",
    "explorer": "a2dfd1373abe58ace461ebea0b4e94cb287f894e",
    "search": "0f4c1a233cd03a0f562e13636b89b7708f8e2698",
    "backlinks": "7490f921b7bd974c3f2f985ad3744b06160827d6",
    "article-title": "e608ca815e137e22b598094f735bcd8a481dafaa",
    "content-meta": "dd6e94b5ca1cb195104a2b5e624a43ee6aa0a324",
    "page-title": "a1c1fe0a9c6a5ce1acf6efa01d473a7d9850e2a3",
    "darkmode": "c6484f72ebc6ea89339be7cf86ad14b40c47dcc7",
    "breadcrumbs": "cf2e161425165e1ac713f1feb7250b07fe0250ae",
    "footer": "6ed61928d3c0178d7cef972ebcbca6a206a2f065",
}


class OutputHTTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class OutputHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(OUTPUT_ROOT), **kwargs)

    def send_head(self):
        original_path = self.path
        self.path = self.resolve_output_path(original_path)
        try:
            return super().send_head()
        finally:
            self.path = original_path

    @staticmethod
    def resolve_output_path(raw_path: str) -> str:
        split = urllib.parse.urlsplit(raw_path)
        request_path = urllib.parse.unquote(split.path) or "/"
        if request_path.endswith("/") or Path(request_path).suffix:
            return raw_path

        html_rel_path = request_path.lstrip("/") + ".html"
        if not (OUTPUT_ROOT / html_rel_path).is_file():
            return raw_path

        resolved_path = "/" + urllib.parse.quote(html_rel_path, safe="/")
        if split.query:
            resolved_path += f"?{split.query}"
        return resolved_path


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("bootstrap")
    subparsers.add_parser("build")
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--addr", default=DEFAULT_SERVE_ADDR)
    track_parser = subparsers.add_parser("track")
    track_parser.add_argument("--addr", default=DEFAULT_SERVE_ADDR)
    track_parser.add_argument("--poll-seconds", type=float, default=DEFAULT_TRACK_POLL_SECONDS)

    args = parser.parse_args()
    command = args.command or "build"

    if command == "bootstrap":
        return bootstrap_toolchain()
    if command == "build":
        return build_site()
    if command == "serve":
        return serve_site(args.addr)
    if command == "track":
        return track_site(args.addr, args.poll_seconds)

    print(f"ERROR: unsupported command: {command}", file=sys.stderr)
    return 2


def bootstrap_toolchain() -> int:
    ensure_dir(CACHE_ROOT)
    ensure_dir(PROJECT_ROOT)
    ensure_dir(NPM_CACHE_ROOT)
    require_binary("git")
    require_supported_node_runtime()

    ensure_quartz_runtime_checkout()
    write_runtime_quartz_config()
    write_runtime_quartz_lockfile()
    ensure_node_modules()
    ensure_quartz_plugins()
    return 0


def build_site() -> int:
    bootstrap_toolchain()
    reset_staged_docs()
    stage_source_docs()
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    ensure_dir(OUTPUT_ROOT)
    run_quartz_build()
    return 0


def serve_site(addr: str) -> int:
    build_site()
    serve_output_root(addr)
    return 0


def track_site(addr: str, poll_seconds: float) -> int:
    if poll_seconds <= 0:
        print("ERROR: --poll-seconds must be > 0.", file=sys.stderr)
        return 2

    build_site()
    source_state = compute_source_state()
    httpd, server_thread = start_output_http_server(addr)

    try:
        while True:
            time.sleep(poll_seconds)
            next_state = compute_source_state()
            if next_state == source_state:
                continue

            print("doc_site track: source change detected, rebuilding output site...", flush=True)
            build_site()
            source_state = next_state
    except KeyboardInterrupt:
        print("doc_site track: stopping HTTP server.", flush=True)
    finally:
        stop_output_http_server(httpd, server_thread)
    return 0


def ensure_quartz_runtime_checkout() -> None:
    if quartz_runtime_is_ready():
        return

    if TOOLCHAIN_ROOT.exists():
        shutil.rmtree(TOOLCHAIN_ROOT)
    ensure_dir(TOOLCHAIN_ROOT.parent)

    run_cmd(
        [
            "git",
            "clone",
            "--branch",
            QUARTZ_REF,
            "--depth",
            "1",
            "--filter=blob:none",
            "--sparse",
            QUARTZ_REPO_URL,
            str(TOOLCHAIN_ROOT),
        ],
        cwd=REPO_ROOT,
    )
    run_cmd(
        [
            "git",
            "-C",
            str(TOOLCHAIN_ROOT),
            "sparse-checkout",
            "set",
            "--skip-checks",
            *SPARSE_CHECKOUT_PATHS,
        ],
        cwd=REPO_ROOT,
    )

    current_commit = git_capture(["rev-parse", "HEAD"], cwd=TOOLCHAIN_ROOT).strip()
    if current_commit != QUARTZ_COMMIT:
        raise SystemExit(
            "ERROR: unexpected Quartz checkout commit after clone: "
            f"expected={QUARTZ_COMMIT} actual={current_commit}"
        )


def quartz_runtime_is_ready() -> bool:
    if not (TOOLCHAIN_ROOT / ".git").exists():
        return False
    if not (TOOLCHAIN_ROOT / "package.json").is_file():
        return False
    if not (TOOLCHAIN_ROOT / "quartz" / "bootstrap-cli.mjs").is_file():
        return False

    try:
        remote_url = git_capture(["remote", "get-url", "origin"], cwd=TOOLCHAIN_ROOT).strip()
        current_commit = git_capture(["rev-parse", "HEAD"], cwd=TOOLCHAIN_ROOT).strip()
    except RuntimeError:
        return False

    if remote_url != QUARTZ_REPO_URL:
        return False
    return current_commit == QUARTZ_COMMIT


def write_runtime_quartz_config() -> None:
    ensure_dir(TOOLCHAIN_ROOT)
    write_text_if_changed(RUNTIME_CONFIG_PATH, build_quartz_config_text())


def write_runtime_quartz_lockfile() -> None:
    ensure_dir(TOOLCHAIN_ROOT)
    write_text_if_changed(RUNTIME_LOCKFILE_PATH, build_quartz_lockfile_text())


def build_quartz_config_text() -> str:
    base_url = resolve_site_base_url()
    return dedent(
        f"""\
        # yaml-language-server: $schema=./quartz/plugins/quartz-plugins.schema.json
        configuration:
          pageTitle: Fluxon Docs
          pageTitleSuffix: ""
          enableSPA: true
          enablePopovers: true
          analytics: null
          locale: zh-CN
          baseUrl: {base_url}
          ignorePatterns:
            - private
            - templates
            - .obsidian
          theme:
            fontOrigin: local
            cdnCaching: true
            typography:
              header: Noto Sans SC
              body: Noto Sans SC
              code: JetBrains Mono
            colors:
              lightMode:
                light: "#f7f4ee"
                lightgray: "#e2dbcf"
                gray: "#b2aa9f"
                darkgray: "#5b564f"
                dark: "#1e1c19"
                secondary: "#35633b"
                tertiary: "#8b6f47"
                highlight: rgba(101, 130, 101, 0.14)
                textHighlight: "#fff23688"
              darkMode:
                light: "#171613"
                lightgray: "#35322d"
                gray: "#70695f"
                darkgray: "#ddd5c9"
                dark: "#f6efe4"
                secondary: "#8ec792"
                tertiary: "#d3ad79"
                highlight: rgba(140, 174, 146, 0.12)
                textHighlight: "#b3aa0288"

        plugins:
          - source: "{plugin_source('created-modified-date')}"
            enabled: true
            options:
              defaultDateType: modified
              priority:
                - filesystem
            order: 10
          - source: "{plugin_source('syntax-highlighting')}"
            enabled: true
            options:
              theme:
                light: github-light
                dark: github-dark
              keepBackground: false
            order: 20
          - source: "{plugin_source('obsidian-flavored-markdown')}"
            enabled: true
            options:
              enableInHtmlEmbed: false
              enableCheckbox: true
            order: 30
          - source: "{plugin_source('github-flavored-markdown')}"
            enabled: true
            order: 40
          - source: "{plugin_source('table-of-contents')}"
            enabled: true
            order: 50
            layout:
              position: right
              priority: 20
          - source: "{plugin_source('crawl-links')}"
            enabled: true
            options:
              markdownLinkResolution: shortest
            order: 60
          - source: "{plugin_source('description')}"
            enabled: true
            order: 70
          - source: "{plugin_source('alias-redirects')}"
            enabled: true
          - source: "{plugin_source('content-index')}"
            enabled: true
            options:
              enableSiteMap: true
              enableRSS: false
          - source: "{plugin_source('favicon')}"
            enabled: true
          - source: "{plugin_source('og-image')}"
            enabled: false
          - source: "{plugin_source('content-page')}"
            enabled: true
          - source: "{plugin_source('folder-page')}"
            enabled: true
          - source: "{plugin_source('explorer')}"
            enabled: true
            options:
              folderDefaultState: open
              folderClickBehavior: link
              useSavedState: false
            layout:
              position: left
              priority: 40
          - source: "{plugin_source('search')}"
            enabled: true
            layout:
              position: left
              priority: 20
          - source: "{plugin_source('backlinks')}"
            enabled: true
            layout:
              position: right
              priority: 40
          - source: "{plugin_source('article-title')}"
            enabled: true
            layout:
              position: beforeBody
              priority: 10
          - source: "{plugin_source('content-meta')}"
            enabled: true
            layout:
              position: beforeBody
              priority: 20
          - source: "{plugin_source('page-title')}"
            enabled: true
            layout:
              position: left
              priority: 10
          - source: "{plugin_source('darkmode')}"
            enabled: true
            layout:
              position: left
              priority: 30
          - source: "{plugin_source('breadcrumbs')}"
            enabled: true
            layout:
              position: beforeBody
              priority: 5
              condition: not-index
          - source: "{plugin_source('footer')}"
            enabled: true
            options:
              links: {{}}
        """
    )


def plugin_source(name: str) -> str:
    return f"github:quartz-community/{name}"


def build_quartz_lockfile_text() -> str:
    plugins: dict[str, dict[str, str]] = {}
    for name, commit in sorted(PLUGIN_COMMITS.items()):
        source = plugin_source(name)
        plugins[name] = {
            "source": source,
            "resolved": f"https://github.com/quartz-community/{name}.git",
            "commit": commit,
        }

    return json.dumps({"version": "1.0.0", "plugins": plugins}, indent=2) + "\n"


def ensure_node_modules() -> None:
    package_lock_path = TOOLCHAIN_ROOT / "package-lock.json"
    if not package_lock_path.is_file():
        raise SystemExit(f"ERROR: missing Quartz package-lock.json: {package_lock_path}")

    expected_stamp = hash_text(QUARTZ_COMMIT + "\n" + package_lock_path.read_text(encoding="utf-8"))
    if NPM_STAMP_PATH.is_file() and NPM_STAMP_PATH.read_text(encoding="utf-8") == expected_stamp:
        if (TOOLCHAIN_ROOT / "node_modules").is_dir():
            return

    run_cmd(
        [
            require_binary("npm"),
            "--cache",
            str(NPM_CACHE_ROOT),
            "ci",
            "--no-fund",
            "--no-audit",
        ],
        cwd=TOOLCHAIN_ROOT,
    )
    NPM_STAMP_PATH.write_text(expected_stamp, encoding="utf-8")


def ensure_quartz_plugins() -> None:
    config_text = RUNTIME_CONFIG_PATH.read_text(encoding="utf-8")
    lockfile_text = RUNTIME_LOCKFILE_PATH.read_text(encoding="utf-8")
    expected_stamp = hash_text(QUARTZ_COMMIT + "\n" + config_text + "\n" + lockfile_text)
    plugins_root = TOOLCHAIN_ROOT / ".quartz" / "plugins"
    if (
        PLUGIN_STAMP_PATH.is_file()
        and PLUGIN_STAMP_PATH.read_text(encoding="utf-8") == expected_stamp
        and plugins_root.is_dir()
    ):
        return

    run_cmd(
        [
            require_binary("node"),
            "quartz/bootstrap-cli.mjs",
            "plugin",
            "install",
        ],
        cwd=TOOLCHAIN_ROOT,
    )
    PLUGIN_STAMP_PATH.write_text(expected_stamp, encoding="utf-8")


def reset_staged_docs() -> None:
    if PROJECT_ROOT.exists():
        shutil.rmtree(PROJECT_ROOT)
    ensure_dir(STAGE_DOCS_ROOT)


def stage_source_docs() -> None:
    doc_root = resolve_doc_root()
    if not doc_root.is_dir():
        raise SystemExit(f"ERROR: doc root not found: {doc_root}")

    for source_path in sorted(doc_root.rglob("*")):
        rel = source_path.relative_to(doc_root)
        if should_skip_rel_path(rel):
            continue

        if source_path.is_dir():
            ensure_dir(STAGE_DOCS_ROOT / rel)
            continue

        if source_path.suffix == ".md":
            write_staged_markdown(doc_root, source_path, rel)
            continue

        dst_path = STAGE_DOCS_ROOT / rel
        ensure_dir(dst_path.parent)
        shutil.copy2(source_path, dst_path)

    stage_readme_cn_homepage()


def should_skip_rel_path(rel: Path) -> bool:
    for part in rel.parts:
        if part.startswith("."):
            return True
        if part in SKIP_DIR_NAMES:
            return True
    rel_str = rel.as_posix()
    return rel_str.endswith(".canvas") or rel_str.endswith(".canvas.ext")


def write_staged_markdown(doc_root: Path, source_path: Path, rel: Path) -> None:
    if is_nested_doc_readme(rel):
        return

    dst_rel = rel.with_name("index.md") if rel.name == "README.md" else rel
    dst_path = STAGE_DOCS_ROOT / dst_rel
    ensure_dir(dst_path.parent)
    raw_md = source_path.read_text(encoding="utf-8")
    staged_md = rewrite_markdown_links(raw_md)
    dst_path.write_text(staged_md, encoding="utf-8")


def stage_readme_cn_homepage() -> None:
    if not HOMEPAGE_MARKDOWN_SOURCE.is_file():
        return

    raw_md = HOMEPAGE_MARKDOWN_SOURCE.read_text(encoding="utf-8")
    staged_md = rewrite_markdown_links(raw_md, target_rewriter=rewrite_homepage_target_path)
    write_text_if_changed(STAGE_DOCS_ROOT / "index.md", staged_md)
    stage_repo_asset_tree(HOMEPAGE_ROOT_PICS_DIR)
    for source_path in HOMEPAGE_SUPPORT_FILE_PATHS:
        stage_repo_file(source_path)


def stage_repo_asset_tree(source_root: Path) -> None:
    if not source_root.is_dir():
        return

    for source_path in sorted(source_root.rglob("*")):
        rel = source_path.relative_to(REPO_ROOT)
        dst_path = STAGE_DOCS_ROOT / rel
        if source_path.is_dir():
            ensure_dir(dst_path)
            continue
        ensure_dir(dst_path.parent)
        shutil.copy2(source_path, dst_path)


def stage_repo_file(source_path: Path) -> None:
    if not source_path.is_file():
        return

    rel = source_path.relative_to(REPO_ROOT)
    dst_path = STAGE_DOCS_ROOT / rel
    ensure_dir(dst_path.parent)
    shutil.copy2(source_path, dst_path)


def rewrite_markdown_links(raw_md: str, *, target_rewriter=None) -> str:
    if target_rewriter is None:
        target_rewriter = rewrite_target_path

    lines = raw_md.splitlines(keepends=True)
    out_lines: list[str] = []
    in_fence = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            out_lines.append(line)
            continue
        if in_fence:
            out_lines.append(line)
            continue
        out_lines.append(
            MARKDOWN_LINK_RE.sub(
                lambda match: rewrite_markdown_match(match, target_rewriter),
                line,
            )
        )
    return "".join(out_lines)


def rewrite_markdown_match(match: re.Match[str], target_rewriter) -> str:
    prefix = match.group(1)
    raw_target = match.group(2).strip()
    suffix = match.group(3)
    return f"{prefix}{target_rewriter(raw_target)}{suffix}"


def rewrite_target_path(raw_target: str) -> str:
    unescaped_target = decode_markdown_target(raw_target)
    if (
        not unescaped_target
        or unescaped_target.startswith("#")
        or unescaped_target.startswith("http://")
        or unescaped_target.startswith("https://")
        or unescaped_target.startswith("mailto:")
        or unescaped_target.startswith("tel:")
        or unescaped_target.startswith("data:")
    ):
        return raw_target

    split = urllib.parse.urlsplit(unescaped_target)
    path_part = urllib.parse.unquote(split.path)
    normalized_path = normalize_readme_target_path(path_part)
    if normalized_path is None:
        return raw_target

    rebuilt = urllib.parse.quote(normalized_path, safe="/")
    if split.query:
        rebuilt += f"?{split.query}"
    if split.fragment:
        rebuilt += f"#{split.fragment}"
    return rebuilt


def rewrite_homepage_target_path(raw_target: str) -> str:
    unescaped_target = decode_markdown_target(raw_target)
    if (
        not unescaped_target
        or unescaped_target.startswith("#")
        or unescaped_target.startswith("http://")
        or unescaped_target.startswith("https://")
        or unescaped_target.startswith("mailto:")
        or unescaped_target.startswith("tel:")
        or unescaped_target.startswith("data:")
    ):
        return raw_target

    split = urllib.parse.urlsplit(unescaped_target)
    path_part = urllib.parse.unquote(split.path)
    mapped_path = remap_homepage_repo_path(path_part)
    rebuilt = urllib.parse.quote(mapped_path, safe="/.")
    if split.query:
        rebuilt += f"?{split.query}"
    if split.fragment:
        rebuilt += f"#{split.fragment}"
    return rewrite_target_path(rebuilt)


def remap_homepage_repo_path(path_part: str) -> str:
    if path_part in {"./README_CN.md", "README_CN.md"}:
        return "./"
    if path_part.startswith("./fluxon_doc/"):
        return "./" + path_part[len("./fluxon_doc/") :]
    if path_part.startswith("fluxon_doc/"):
        return path_part[len("fluxon_doc/") :]
    return path_part


def is_nested_doc_readme(rel: Path) -> bool:
    return rel.name == "README.md" and rel.parent != Path(".")


def normalize_readme_target_path(path_part: str) -> str | None:
    for readme_name in ("README_CN.md", "README.md"):
        if not path_part.endswith(readme_name):
            continue

        directory_path = path_part[: -len(readme_name)]
        if directory_path in {"", "."}:
            return "./"
        if directory_path.endswith("/"):
            return directory_path
        return directory_path + "/"
    return None


def decode_markdown_target(raw_target: str) -> str:
    unescaped = html.unescape(raw_target).strip()
    if unescaped.startswith("<") and unescaped.endswith(">"):
        return unescaped[1:-1].strip()
    return unescaped


def run_quartz_build() -> None:
    run_cmd(
        [
            require_binary("node"),
            "quartz/bootstrap-cli.mjs",
            "build",
            "-d",
            str(STAGE_DOCS_ROOT),
            "-o",
            str(OUTPUT_ROOT),
        ],
        cwd=TOOLCHAIN_ROOT,
    )
    apply_output_overrides()


def apply_output_overrides() -> None:
    force_expand_explorer()
    add_explorer_home_link()


def force_expand_explorer() -> None:
    index_css_path = OUTPUT_ROOT / "index.css"
    if not index_css_path.is_file():
        raise SystemExit(f"ERROR: missing built Quartz stylesheet: {index_css_path}")

    css_text = index_css_path.read_text(encoding="utf-8")
    if EXPLORER_FORCE_EXPANDED_CSS in css_text:
        return
    index_css_path.write_text(css_text + "\n" + EXPLORER_FORCE_EXPANDED_CSS, encoding="utf-8")


def add_explorer_home_link() -> None:
    postscript_path = OUTPUT_ROOT / "postscript.js"
    if not postscript_path.is_file():
        raise SystemExit(f"ERROR: missing built Quartz script bundle: {postscript_path}")

    script_text = postscript_path.read_text(encoding="utf-8")
    if EXPLORER_ADD_HOME_LINK_JS in script_text:
        return
    postscript_path.write_text(script_text + "\n" + EXPLORER_ADD_HOME_LINK_JS, encoding="utf-8")


def resolve_doc_root() -> Path:
    raw_doc_root = os.environ.get("FLUXON_DOC_ROOT")
    if raw_doc_root and raw_doc_root.strip():
        doc_root = Path(raw_doc_root.strip())
        if not doc_root.is_absolute():
            doc_root = REPO_ROOT / doc_root
        return doc_root
    if DEFAULT_DOC_ROOT.is_dir():
        return DEFAULT_DOC_ROOT
    return FALLBACK_DOC_ROOT


def resolve_site_base_url() -> str:
    raw_base = os.environ.get("FLUXON_DOC_SITE_BASE_URL")
    if raw_base is None or not raw_base.strip():
        return "example.com"

    base = raw_base.strip()
    if base.startswith("http://") or base.startswith("https://"):
        split = urllib.parse.urlsplit(base)
        if not split.netloc:
            raise SystemExit(
                f"ERROR: FLUXON_DOC_SITE_BASE_URL must include a hostname when using a scheme: {raw_base!r}"
            )
        base = split.netloc + split.path

    base = base.strip("/")
    if not base:
        raise SystemExit("ERROR: FLUXON_DOC_SITE_BASE_URL must not be empty")
    if base.startswith("/"):
        raise SystemExit(
            "ERROR: FLUXON_DOC_SITE_BASE_URL must be host[/path] without a leading slash: "
            f"{raw_base!r}"
        )
    return base


def compute_source_state() -> tuple[tuple[str, int, int], ...]:
    doc_root = resolve_doc_root()
    rows: list[tuple[str, int, int]] = []
    for path in sorted(doc_root.rglob("*")):
        rel = path.relative_to(doc_root)
        if should_skip_rel_path(rel) or not path.is_file():
            continue
        stat = path.stat()
        rows.append((f"doc:{rel.as_posix()}", stat.st_mtime_ns, stat.st_size))

    for path in (
        Path(__file__),
        REPO_ROOT / ".github" / "workflows" / "docs-pages.yml",
        HOMEPAGE_MARKDOWN_SOURCE,
        *HOMEPAGE_SUPPORT_FILE_PATHS,
    ):
        if not path.is_file():
            continue
        stat = path.stat()
        rows.append((f"meta:{path.relative_to(REPO_ROOT).as_posix()}", stat.st_mtime_ns, stat.st_size))

    if HOMEPAGE_ROOT_PICS_DIR.is_dir():
        for path in sorted(HOMEPAGE_ROOT_PICS_DIR.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            rows.append((f"meta:{path.relative_to(REPO_ROOT).as_posix()}", stat.st_mtime_ns, stat.st_size))
    return tuple(rows)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_text_if_changed(path: Path, content: str) -> None:
    if path.is_file() and path.read_text(encoding="utf-8") == content:
        return
    path.write_text(content, encoding="utf-8")


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def require_binary(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise SystemExit(f"ERROR: `{name}` not found in PATH.")
    return path


def require_supported_node_runtime() -> None:
    node_path = require_binary("node")
    npm_path = require_binary("npm")

    node_major = int(
        subprocess.run(
            [node_path, "-p", "process.versions.node.split('.')[0]"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    npm_version_text = subprocess.run(
        [npm_path, "--version"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    npm_version = tuple(int(part) for part in npm_version_text.split(".") if part.isdigit())

    if node_major < 22:
        raise SystemExit(
            "ERROR: Quartz requires Node.js >= 22. "
            f"Found node={subprocess.run([node_path, '--version'], check=False, stdout=subprocess.PIPE, text=True).stdout.strip()} "
            f"npm={npm_version_text}"
        )
    if npm_version < (10, 9, 2):
        raise SystemExit(
            "ERROR: Quartz requires npm >= 10.9.2. "
            f"Found npm={npm_version_text}"
        )


def run_cmd(cmd: list[str], *, cwd: Path) -> None:
    print("+ " + " ".join(shlex.quote(v) for v in cmd), flush=True)
    rc = subprocess.run(cmd, cwd=str(cwd), check=False).returncode
    if rc != 0:
        raise SystemExit(rc)


def git_capture(args: list[str], *, cwd: Path) -> str:
    cmd = ["git", "-C", str(cwd), *args]
    completed = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if completed.returncode != 0:
        output = completed.stdout or ""
        raise RuntimeError(
            f"command failed (rc={completed.returncode}): {shlex.join(cmd)}\n{output}"
        )
    return completed.stdout or ""


def serve_output_root(addr: str) -> None:
    httpd, server_thread = start_output_http_server(addr)
    try:
        server_thread.join()
    except KeyboardInterrupt:
        print("doc_site serve: stopping HTTP server.", flush=True)
    finally:
        stop_output_http_server(httpd, server_thread)


def start_output_http_server(addr: str) -> tuple[OutputHTTPServer, threading.Thread]:
    host, port = parse_serve_addr(addr)
    httpd = OutputHTTPServer((host, port), OutputHTTPRequestHandler)
    httpd.daemon_threads = True
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=False)
    server_thread.start()
    print(f"doc_site serve: serving {OUTPUT_ROOT} on http://{addr}/", flush=True)
    return httpd, server_thread


def stop_output_http_server(
    httpd: OutputHTTPServer,
    server_thread: threading.Thread,
) -> None:
    httpd.shutdown()
    httpd.server_close()
    server_thread.join()


def parse_serve_addr(addr: str) -> tuple[str, int]:
    host, sep, port_text = addr.rpartition(":")
    if not sep or not host or not port_text:
        raise SystemExit(f"ERROR: invalid --addr: {addr}. Expected <host>:<port>.")
    if not port_text.isdigit():
        raise SystemExit(f"ERROR: invalid --addr port: {addr}")
    port = int(port_text)
    if port <= 0 or port > 65535:
        raise SystemExit(f"ERROR: invalid --addr port: {addr}")
    return host, port


if __name__ == "__main__":
    raise SystemExit(main())
