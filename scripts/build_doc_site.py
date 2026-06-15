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
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler
from pathlib import Path
from textwrap import dedent


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOC_EN_ROOT = REPO_ROOT / "fluxon_doc_linked" / "fluxon_doc_en"
FALLBACK_DOC_EN_ROOT = REPO_ROOT / "fluxon_doc_en"
DEFAULT_DOC_CN_ROOT = REPO_ROOT / "fluxon_doc_linked" / "fluxon_doc_cn"
FALLBACK_DOC_CN_ROOT = REPO_ROOT / "fluxon_doc_cn"
OUTPUT_ROOT = REPO_ROOT / "fluxon_release" / "doc_site"
CACHE_ROOT = REPO_ROOT / ".cached" / "fluxon_doc_site"
PROJECT_ROOT = (
    Path(tempfile.gettempdir())
    / f"fluxon_doc_site_{hashlib.sha256(str(REPO_ROOT).encode('utf-8')).hexdigest()[:12]}"
)
STAGE_DOCS_ROOT = PROJECT_ROOT / "content"
# Publish the repo-root README as the doc-site homepage.
HOMEPAGE_MARKDOWN_SOURCE = REPO_ROOT / "README.md"
HOMEPAGE_CN_MARKDOWN_SOURCE = REPO_ROOT / "README_CN.md"
HOMEPAGE_ROOT_PICS_DIR = REPO_ROOT / "pics"
HOMEPAGE_SUPPORT_FILE_PATHS = (
    REPO_ROOT / "LICENSE",
    REPO_ROOT / "fluxon_rs" / "rust-toolchain.toml",
)
# Keep Quartz as cached build tooling instead of a repo module.
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

    .fluxon-lang-switcher {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin: 0.75rem 0 1rem;
    }

    .fluxon-lang-switcher a {
      border: 1px solid var(--lightgray);
      border-radius: 999px;
      color: var(--dark);
      font-size: 0.85rem;
      padding: 0.25rem 0.7rem;
      text-decoration: none;
    }

    .fluxon-lang-switcher a.active {
      background: var(--secondary);
      border-color: var(--secondary);
      color: var(--light);
    }

    /* Keep the inline GitHub repository icon aligned with text in the homepage link row. */
    a[href="https://github.com/Tele-AI/fluxon"] {
      display: inline-flex;
      align-items: center;
      vertical-align: middle;
    }

    a[href="https://github.com/Tele-AI/fluxon"] > img[alt="GitHub repository"] {
      display: inline-block;
      margin: 0 !important;
      border-radius: 0;
      vertical-align: middle;
    }

    a[href="https://github.com/Tele-AI/fluxon"] > .external-icon {
      display: none;
    }
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
HTML_URL_ATTR_RE = re.compile(r"""(?P<prefix>\b(?:href|src)=["'])(?P<url>[^"']+)(?P<suffix>["'])""")
HTML_FETCH_URL_RE = re.compile(
    r"""(?P<prefix>\bfetch\((?P<quote>["']))(?P<url>[^"']+)(?P<suffix>(?P=quote)\))"""
)
LEADING_FRONTMATTER_RE = re.compile(r"\A---\s*\n.*?\n---\s*(?:\n|$)", re.DOTALL)
LEADING_H1_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$")
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


@dataclass(frozen=True)
class DocVariant:
    language: str
    doc_root_env: str
    default_doc_root: Path
    fallback_doc_root: Path
    output_prefix: Path
    homepage_source: Path


DOC_VARIANTS: tuple[DocVariant, ...] = (
    DocVariant(
        language="en",
        doc_root_env="FLUXON_DOC_EN_ROOT",
        default_doc_root=DEFAULT_DOC_EN_ROOT,
        fallback_doc_root=FALLBACK_DOC_EN_ROOT,
        output_prefix=Path("."),
        homepage_source=HOMEPAGE_MARKDOWN_SOURCE,
    ),
    DocVariant(
        language="cn",
        doc_root_env="FLUXON_DOC_CN_ROOT",
        default_doc_root=DEFAULT_DOC_CN_ROOT,
        fallback_doc_root=FALLBACK_DOC_CN_ROOT,
        output_prefix=Path("cn"),
        homepage_source=HOMEPAGE_CN_MARKDOWN_SOURCE,
    ),
)

LANGUAGE_ROUTE_PAIRS = (
    {"en": "/", "cn": "/cn"},
    {"en": "/roadmap", "cn": "/cn/roadmap"},
    {"en": "/user_doc", "cn": "/cn/user_doc"},
    {"en": "/user_doc/User---0---Installation", "cn": "/cn/user_doc/用户---0---安装"},
    {"en": "/user_doc/User---1---Architecture-and-Concepts", "cn": "/cn/user_doc/用户---1---架构和概念"},
    {"en": "/user_doc/User---2---Service-Plane", "cn": "/cn/user_doc/用户---2---服务平面"},
    {"en": "/user_doc/User---3---KV-and-RPC-Interface", "cn": "/cn/user_doc/用户---3---KV-RPC接口"},
    {"en": "/user_doc/User---4---MQ-Interface", "cn": "/cn/user_doc/用户---4---MQ接口"},
    {"en": "/user_doc/User---5---FS-Interface", "cn": "/cn/user_doc/用户---5---FS接口"},
    {"en": "/dev_doc", "cn": "/cn/dev_doc"},
    {
        "en": "/dev_doc/Developer---1---Package-Core-Install-Artifacts",
        "cn": "/cn/dev_doc/开发者---1---打包核心安装包",
    },
    {
        "en": "/dev_doc/Developer---2---Package-Middleware-and-Images",
        "cn": "/cn/dev_doc/开发者---2---打包中间件和镜像",
    },
)


def build_language_counterpart_routes() -> dict[str, str]:
    routes: dict[str, str] = {}
    for pair in LANGUAGE_ROUTE_PAIRS:
        en_route = pair["en"]
        cn_route = pair["cn"]
        routes[en_route] = cn_route
        routes[cn_route] = en_route
    return routes


LANGUAGE_COUNTERPART_ROUTES = build_language_counterpart_routes()
DOC_SITE_POSTSCRIPT_JS = dedent(
    f"""\
    ;(() => {{
      const FLUXON_LANGUAGE_COUNTERPART_ROUTES = {json.dumps(LANGUAGE_COUNTERPART_ROUTES, ensure_ascii=False, sort_keys=True)}

      function decodeFluxonPathname(pathname) {{
        return (pathname || "/")
          .split("/")
          .map((segment, index) => {{
            if (index === 0) return ""
            try {{
              return decodeURIComponent(segment)
            }} catch {{
              return segment
            }}
          }})
          .join("/")
      }}

      function normalizeFluxonPathname(pathname) {{
        let normalizedPath = decodeFluxonPathname(pathname)
        normalizedPath = normalizedPath.replace(/\/index\.html$/, "/")
        normalizedPath = normalizedPath.replace(/\.html$/, "")
        normalizedPath = normalizedPath.replace(/\/index$/, "/")
        normalizedPath = normalizedPath.replace(/\/+$/, "")
        if (!normalizedPath.startsWith("/")) {{
          normalizedPath = "/" + normalizedPath
        }}
        return normalizedPath === "" ? "/" : normalizedPath || "/"
      }}

      function routeFromFluxonSlug(slug) {{
        const normalizedSlug = (slug || "").replace(/^\/+|\/+$/g, "")
        if (!normalizedSlug) return null

        let route = "/" + normalizedSlug
        route = route.replace(/\/index$/, "")
        return route === "" ? "/" : route || "/"
      }}

      function resolveFluxonCurrentRouteFromSlug() {{
        return routeFromFluxonSlug(document.body?.dataset?.slug || "")
      }}

      // Return the project-site base path, for example "/Fluxon".
      function resolveFluxonSiteBasePath() {{
        const route = resolveFluxonCurrentRouteFromSlug()
        if (!route) return ""

        const normalizedPath = normalizeFluxonPathname(window.location.pathname)
        if (route === "/") {{
          return normalizedPath === "/" ? "" : normalizedPath
        }}
        if (normalizedPath === route) {{
          return ""
        }}
        if (!normalizedPath.endsWith(route)) {{
          return ""
        }}

        try {{
          const siteBasePath = normalizedPath.slice(0, normalizedPath.length - route.length)
          return siteBasePath.replace(/\/$/, "")
        }} catch {{
          return ""
        }}
      }}

      function normalizeFluxonRoute(pathname, siteBasePath) {{
        let route = normalizeFluxonPathname(pathname)
        if (siteBasePath && route === siteBasePath) {{
          return "/"
        }}
        if (siteBasePath && route.startsWith(siteBasePath + "/")) {{
          route = route.slice(siteBasePath.length) || "/"
        }}
        return route === "" ? "/" : route || "/"
      }}

      function currentFluxonRoute() {{
        return resolveFluxonCurrentRouteFromSlug() || normalizeFluxonRoute(window.location.pathname, resolveFluxonSiteBasePath())
      }}

      function buildFluxonSiteHref(route, siteBasePath) {{
        const normalizedBase = siteBasePath || ""
        const normalizedRoute = route === "/" ? "/" : route + "/"
        return normalizedBase + normalizedRoute
      }}

      function currentFluxonLanguage(route) {{
        return route === "/cn" || route.startsWith("/cn/") ? "cn" : "en"
      }}

      // Rewrite root-absolute internal links so they stay inside the project site.
      function rewriteFluxonRootInternalLinks() {{
        const siteBasePath = resolveFluxonSiteBasePath()
        if (!siteBasePath) return

        document.querySelectorAll("a[href^='/']").forEach((link) => {{
          const href = link.getAttribute("href") || ""
          const rewrittenHref = rewriteFluxonRootInternalHref(href, siteBasePath)
          if (rewrittenHref && rewrittenHref !== href) {{
            link.setAttribute("href", rewrittenHref)
          }}
        }})
      }}

      // Add the project-site base path to one href when needed.
      function rewriteFluxonRootInternalHref(href, siteBasePath) {{
        if (!href || !siteBasePath) return href
        if (href.startsWith("//")) return href
        if (href === siteBasePath || href.startsWith(siteBasePath + "/")) return href
        if (!href.startsWith("/")) return href
        return siteBasePath + href
      }}

      function isFluxonElement(node, tagName = null) {{
        if (!node || node.nodeType !== 1) return false
        if (!tagName) return true
        return node.tagName === tagName
      }}

      function insertFluxonLanguageSwitcher() {{
        const sidebar = document.querySelector(".left.sidebar")
        if (!isFluxonElement(sidebar, "DIV")) return
        sidebar.querySelector(".fluxon-lang-switcher")?.remove()

        const siteBasePath = resolveFluxonSiteBasePath()
        const route = currentFluxonRoute()
        const language = currentFluxonLanguage(route)
        const counterpart = FLUXON_LANGUAGE_COUNTERPART_ROUTES[route]
        const englishRoute = language === "en" ? route : counterpart || "/"
        const chineseRoute = language === "cn" ? route : counterpart || "/cn"

        const switcher = document.createElement("div")
        switcher.className = "fluxon-lang-switcher"

        const englishLink = document.createElement("a")
        englishLink.href = buildFluxonSiteHref(englishRoute, siteBasePath)
        englishLink.textContent = "English"
        if (language === "en") {{
          englishLink.classList.add("active")
        }}
        switcher.appendChild(englishLink)

        const chineseLink = document.createElement("a")
        chineseLink.href = buildFluxonSiteHref(chineseRoute, siteBasePath)
        chineseLink.textContent = "中文"
        if (language === "cn") {{
          chineseLink.classList.add("active")
        }}
        switcher.appendChild(chineseLink)

        const title = sidebar.querySelector(".page-title")
        sidebar.insertBefore(switcher, title?.nextSibling || sidebar.firstChild)
      }}

      function makeFluxonExplorerLink(route, text, siteBasePath, active, kind) {{
        const item = document.createElement("li")
        item.className = kind === "home" ? "fluxon-home-link" : "fluxon-roadmap-link"

        const link = document.createElement("a")
        link.href = buildFluxonSiteHref(route, siteBasePath)
        link.className = "nav-file-title tree-item-self"
        link.textContent = text
        if (active) {{
          link.classList.add("active", "is-active")
        }}

        item.appendChild(link)
        return item
      }}

      function matchesFluxonRoute(href, route, siteBasePath) {{
        if (!href) return false
        try {{
          const pathname = new URL(href, window.location.href).pathname
          return normalizeFluxonRoute(pathname, siteBasePath) === route
        }} catch {{
          return false
        }}
      }}

      function fluxonExplorerFolderPath(item) {{
        const folderContainer = item.querySelector(":scope > .folder-container")
        if (!isFluxonElement(folderContainer, "DIV")) return ""
        return folderContainer.dataset.folderpath || ""
      }}

      function fluxonExplorerDirectHref(item) {{
        const directLink = item.querySelector(":scope > a[href]")
        if (!isFluxonElement(directLink, "A")) return ""
        return directLink.getAttribute("href") || ""
      }}

      function isFluxonExplorerCustomItem(item) {{
        return (
          item.classList.contains("fluxon-home-link") ||
          item.classList.contains("fluxon-roadmap-link") ||
          item.classList.contains("overflow-end")
        )
      }}

      function isFluxonChineseExplorerItem(item, siteBasePath) {{
        const folderPath = fluxonExplorerFolderPath(item)
        if (folderPath) {{
          return folderPath === "cn/index" || folderPath.startsWith("cn/")
        }}
        const href = fluxonExplorerDirectHref(item)
        if (!href) return false
        return matchesFluxonRoute(href, "/cn", siteBasePath) || matchesFluxonRoute(href, "/cn/roadmap", siteBasePath)
      }}

      function explorerItemMatchesRoute(item, route, siteBasePath) {{
        const href = fluxonExplorerDirectHref(item)
        return !!href && matchesFluxonRoute(href, route, siteBasePath)
      }}

      function explorerItemsEquivalent(leftItem, rightItem, siteBasePath) {{
        const leftFolderPath = fluxonExplorerFolderPath(leftItem)
        const rightFolderPath = fluxonExplorerFolderPath(rightItem)
        if (leftFolderPath || rightFolderPath) {{
          return leftFolderPath !== "" && leftFolderPath === rightFolderPath
        }}

        const leftHref = fluxonExplorerDirectHref(leftItem)
        const rightHref = fluxonExplorerDirectHref(rightItem)
        if (!leftHref || !rightHref) return false
        return normalizeFluxonRoute(leftHref, siteBasePath) === normalizeFluxonRoute(rightHref, siteBasePath)
      }}

      function filterFluxonExplorerTreeForLanguage() {{
        const siteBasePath = resolveFluxonSiteBasePath()
        const route = currentFluxonRoute()
        const language = currentFluxonLanguage(route)

        document.querySelectorAll(".explorer-ul").forEach((list) => {{
          if (!isFluxonElement(list, "UL")) return

          Array.from(list.children).forEach((child) => {{
            if (!isFluxonElement(child, "LI")) return
            if (isFluxonExplorerCustomItem(child)) return

            if (language === "en") {{
              if (isFluxonChineseExplorerItem(child, siteBasePath)) {{
                child.remove()
              }}
              return
            }}

            const folderPath = fluxonExplorerFolderPath(child)
            if (folderPath && !(folderPath === "cn/index" || folderPath.startsWith("cn/"))) {{
              child.remove()
              return
            }}

            const href = fluxonExplorerDirectHref(child)
            if (href && !matchesFluxonRoute(href, "/cn", siteBasePath) && !normalizeFluxonRoute(href, siteBasePath).startsWith("/cn/")) {{
              child.remove()
            }}
          }})

          if (language !== "cn") return

          const cnRootFolder = Array.from(list.children).find((child) => {{
            if (!isFluxonElement(child, "LI")) return false
            return fluxonExplorerFolderPath(child) === "cn/index"
          }})
          if (!isFluxonElement(cnRootFolder, "LI")) return

          const nestedList = cnRootFolder.querySelector(":scope > .folder-outer > ul")
          if (!isFluxonElement(nestedList, "UL")) {{
            cnRootFolder.remove()
            return
          }}

          const overflowEnd = list.querySelector("li.overflow-end")
          Array.from(nestedList.children).forEach((nestedChild) => {{
            if (!isFluxonElement(nestedChild, "LI")) return
            if (explorerItemMatchesRoute(nestedChild, "/cn/roadmap", siteBasePath)) return

            const alreadyPresent = Array.from(list.children).some((rootChild) => {{
              if (!isFluxonElement(rootChild, "LI")) return false
              if (rootChild === cnRootFolder) return false
              return explorerItemsEquivalent(rootChild, nestedChild, siteBasePath)
            }})
            if (!alreadyPresent) {{
              list.insertBefore(nestedChild, overflowEnd || null)
            }}
          }})

          cnRootFolder.remove()
        }})
      }}

      function explorerNeedsFluxonLanguageFilter(siteBasePath, language) {{
        return Array.from(document.querySelectorAll(".explorer-ul")).some((list) => {{
          if (!isFluxonElement(list, "UL")) return false

          if (language === "en") {{
            return Array.from(list.children).some((child) => {{
              if (!isFluxonElement(child, "LI")) return false
              if (isFluxonExplorerCustomItem(child)) return false
              return isFluxonChineseExplorerItem(child, siteBasePath)
            }})
          }}

          const hasChineseRootFolder = Array.from(list.children).some((child) => {{
            if (!isFluxonElement(child, "LI")) return false
            return fluxonExplorerFolderPath(child) === "cn/index"
          }})
          if (hasChineseRootFolder) return true

          return Array.from(list.children).some((child) => {{
            if (!isFluxonElement(child, "LI")) return false
            if (isFluxonExplorerCustomItem(child)) return false
            const folderPath = fluxonExplorerFolderPath(child)
            if (folderPath) {{
              return !(folderPath === "cn/index" || folderPath.startsWith("cn/"))
            }}
            const href = fluxonExplorerDirectHref(child)
            if (!href) return false
            const normalizedRoute = normalizeFluxonRoute(href, siteBasePath)
            return normalizedRoute !== "/cn" && !normalizedRoute.startsWith("/cn/")
          }})
        }})
      }}

      // Insert stable localized home and roadmap entries at the top of the explorer tree.
      function insertFluxonExplorerHomeLink() {{
        const siteBasePath = resolveFluxonSiteBasePath()
        const route = currentFluxonRoute()
        const language = currentFluxonLanguage(route)
        const homeLabel = language === "cn" ? "首页" : "Home"
        const homeRoute = language === "cn" ? "/cn" : "/"
        const roadmapRoute = language === "cn" ? "/cn/roadmap" : "/roadmap"

        document.querySelectorAll(".explorer-ul").forEach((list) => {{
          list.querySelector("li.fluxon-home-link")?.remove()
          list.querySelector("li.fluxon-roadmap-link")?.remove()

          const overflowEnd = list.querySelector("li.overflow-end")
          const homeItem = makeFluxonExplorerLink(
            homeRoute,
            homeLabel,
            siteBasePath,
            route === homeRoute,
            "home",
          )
          list.insertBefore(homeItem, overflowEnd || list.firstChild)

          let roadmapItem = null
          if (language === "en") {{
            roadmapItem = Array.from(list.children).find((child) => {{
              if (!isFluxonElement(child, "LI")) return false
              if (child === homeItem) return false
              const firstElement = child.firstElementChild
              if (!isFluxonElement(firstElement, "A")) return false
              return matchesFluxonRoute(firstElement.href, roadmapRoute, siteBasePath)
            }})
          }}
          if (roadmapItem) {{
            const roadmapLink = roadmapItem.firstElementChild
            if (isFluxonElement(roadmapLink, "A")) {{
              roadmapLink.href = buildFluxonSiteHref(roadmapRoute, siteBasePath)
            }}
            if (roadmapItem !== homeItem.nextSibling) {{
              list.insertBefore(roadmapItem, homeItem.nextSibling || overflowEnd || null)
            }}
          }} else {{
            const customRoadmap = makeFluxonExplorerLink(
              roadmapRoute,
              "roadmap",
              siteBasePath,
              route === roadmapRoute,
              "roadmap",
            )
            list.insertBefore(customRoadmap, homeItem.nextSibling || overflowEnd || null)
          }}
        }})

        filterFluxonExplorerTreeForLanguage()
      }}

      // Patch a link right before navigation in case Quartz rendered it late.
      function installFluxonRootInternalClickGuard() {{
        if (window.__fluxonRootInternalClickGuardInstalled) return
        window.__fluxonRootInternalClickGuardInstalled = true

        document.addEventListener(
          "click",
          (event) => {{
            const target = event.target
            if (!isFluxonElement(target)) return
            const link = target.closest("a[href]")
            if (!isFluxonElement(link, "A")) return

            const siteBasePath = resolveFluxonSiteBasePath()
            if (!siteBasePath) return

            const href = link.getAttribute("href") || ""
            const rewrittenHref = rewriteFluxonRootInternalHref(href, siteBasePath)
            if (rewrittenHref && rewrittenHref !== href) {{
              link.setAttribute("href", rewrittenHref)
            }}
          }},
          true,
        )
      }}

      // Re-run link rewriting after Quartz mutates the page tree.
      function installFluxonRootInternalMutationObserver() {{
        if (window.__fluxonRootInternalMutationObserverInstalled) return
        const target = document.body
        if (!target) return

        const observer = new MutationObserver(() => {{
          rewriteFluxonRootInternalLinks()
          const needsLanguageSwitcher = !document.querySelector(".fluxon-lang-switcher")
          const siteBasePath = resolveFluxonSiteBasePath()
          const language = currentFluxonLanguage(currentFluxonRoute())
          const needsExplorerPatch = Array.from(document.querySelectorAll(".explorer-ul")).some(
            (list) => !list.querySelector("li.fluxon-home-link"),
          )
          const needsLanguageFilter = explorerNeedsFluxonLanguageFilter(siteBasePath, language)
          if (needsLanguageSwitcher || needsExplorerPatch || needsLanguageFilter) {{
            window.requestAnimationFrame(() => {{
              if (needsLanguageSwitcher) {{
                insertFluxonLanguageSwitcher()
              }}
              if (needsExplorerPatch || needsLanguageFilter) {{
                insertFluxonExplorerHomeLink()
              }}
            }})
          }}
        }})
        observer.observe(target, {{
          subtree: true,
          childList: true,
          attributes: true,
          attributeFilter: ["href"],
        }})
        window.__fluxonRootInternalMutationObserverInstalled = true
      }}

      // Retry explorer patching because Quartz fills the tree asynchronously.
      function scheduleFluxonExplorerHomeLink(attempt = 0) {{
        const delayMs = attempt === 0 ? 0 : 120
        window.setTimeout(() => {{
          rewriteFluxonRootInternalLinks()
          insertFluxonLanguageSwitcher()
          insertFluxonExplorerHomeLink()
          const siteBasePath = resolveFluxonSiteBasePath()
          const language = currentFluxonLanguage(currentFluxonRoute())
          const needsRetry =
            !document.querySelector(".fluxon-lang-switcher") ||
            Array.from(document.querySelectorAll(".explorer-ul")).some(
              (list) => list.children.length <= 1 || !list.querySelector("li.fluxon-home-link"),
            )
            || explorerNeedsFluxonLanguageFilter(siteBasePath, language)
          if (needsRetry && attempt < 8) {{
            scheduleFluxonExplorerHomeLink(attempt + 1)
          }}
        }}, delayMs)
      }}

      document.addEventListener("DOMContentLoaded", () => scheduleFluxonExplorerHomeLink())
      document.addEventListener("render", () => scheduleFluxonExplorerHomeLink())
      document.addEventListener("nav", () => scheduleFluxonExplorerHomeLink())
      installFluxonRootInternalClickGuard()
      installFluxonRootInternalMutationObserver()
      scheduleFluxonExplorerHomeLink()
    }})();
    """
)


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
          locale: en-US
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
          - source: "{plugin_source('note-properties')}"
            enabled: true
            options:
              includeAll: false
              includedProperties: []
              excludedProperties: []
              hidePropertiesView: true
              delimiters: "---"
              language: yaml
            order: 5
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

    for name, entry in sorted(read_existing_runtime_lock_plugins().items()):
        if name in plugins:
            continue
        source = entry.get("source")
        resolved = entry.get("resolved")
        commit = entry.get("commit")
        if not all(isinstance(value, str) for value in (source, resolved, commit)):
            continue
        plugins[name] = {
            "source": source,
            "resolved": resolved,
            "commit": commit,
        }

    return json.dumps({"version": "1.0.0", "plugins": plugins}, indent=2) + "\n"


def read_existing_runtime_lock_plugins() -> dict[str, dict[str, object]]:
    if not RUNTIME_LOCKFILE_PATH.is_file():
        return {}

    try:
        raw_data = json.loads(RUNTIME_LOCKFILE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    raw_plugins = raw_data.get("plugins")
    if not isinstance(raw_plugins, dict):
        return {}

    plugins: dict[str, dict[str, object]] = {}
    for name, entry in raw_plugins.items():
        if isinstance(name, str) and isinstance(entry, dict):
            plugins[name] = entry
    return plugins


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
            "resolve",
        ],
        cwd=TOOLCHAIN_ROOT,
    )
    lockfile_text = RUNTIME_LOCKFILE_PATH.read_text(encoding="utf-8")
    expected_stamp = hash_text(QUARTZ_COMMIT + "\n" + config_text + "\n" + lockfile_text)

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
    for variant in DOC_VARIANTS:
        stage_doc_variant(variant)

    stage_homepage(variant=resolve_doc_variant("en"))
    stage_homepage(variant=resolve_doc_variant("cn"))
    stage_repo_asset_tree(HOMEPAGE_ROOT_PICS_DIR)
    for source_path in HOMEPAGE_SUPPORT_FILE_PATHS:
        stage_repo_file(source_path)


def stage_doc_variant(variant: DocVariant) -> None:
    doc_root = resolve_doc_root(variant.language)
    if not doc_root.is_dir():
        raise SystemExit(f"ERROR: doc root not found: {doc_root}")

    for source_path in sorted(doc_root.rglob("*")):
        rel = source_path.relative_to(doc_root)
        if should_skip_rel_path(rel):
            continue

        dst_rel = variant.output_prefix / rel
        if source_path.is_dir():
            ensure_dir(STAGE_DOCS_ROOT / dst_rel)
            continue

        if source_path.suffix == ".md":
            write_staged_markdown(source_path, rel, dst_rel, language=variant.language)
            continue

        dst_path = STAGE_DOCS_ROOT / dst_rel
        ensure_dir(dst_path.parent)
        shutil.copy2(source_path, dst_path)


def should_skip_rel_path(rel: Path) -> bool:
    for part in rel.parts:
        if part.startswith("."):
            return True
        if part in SKIP_DIR_NAMES:
            return True
    rel_str = rel.as_posix()
    return rel_str.endswith(".canvas") or rel_str.endswith(".canvas.ext")


def write_staged_markdown(source_path: Path, source_rel: Path, dst_rel: Path, *, language: str) -> None:
    if is_nested_doc_readme(source_rel):
        return

    dst_rel = dst_rel.with_name("index.md") if source_rel.name == "README.md" else dst_rel
    dst_path = STAGE_DOCS_ROOT / dst_rel
    ensure_dir(dst_path.parent)
    raw_md = source_path.read_text(encoding="utf-8")
    staged_md = build_staged_markdown(
        raw_md,
        title_fallback=source_path.stem,
        target_rewriter=lambda raw_target: rewrite_variant_target_path(raw_target, language=language),
    )
    dst_path.write_text(staged_md, encoding="utf-8")


def stage_homepage(*, variant: DocVariant) -> None:
    if not variant.homepage_source.is_file():
        return

    raw_md = variant.homepage_source.read_text(encoding="utf-8")
    staged_md = build_staged_markdown(
        raw_md,
        title_fallback="Fluxon",
        target_rewriter=lambda raw_target: rewrite_homepage_target_path(raw_target, language=variant.language),
    )
    dst_path = STAGE_DOCS_ROOT / variant.output_prefix / "index.md"
    ensure_dir(dst_path.parent)
    write_text_if_changed(dst_path, staged_md)


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


def build_staged_markdown(raw_md: str, *, title_fallback: str | None, target_rewriter) -> str:
    if has_yaml_frontmatter(raw_md):
        return rewrite_markdown_links(raw_md, target_rewriter=target_rewriter)

    title, body_md = extract_leading_h1_title(raw_md)
    staged_body_md = rewrite_markdown_links(body_md, target_rewriter=target_rewriter)
    resolved_title = title or title_fallback
    if not resolved_title:
        return staged_body_md

    return (
        f"---\n"
        f"title: {json.dumps(resolved_title, ensure_ascii=False)}\n"
        f"---\n\n"
        f"{staged_body_md.lstrip()}"
    )


def has_yaml_frontmatter(raw_md: str) -> bool:
    return LEADING_FRONTMATTER_RE.match(raw_md) is not None


def extract_leading_h1_title(raw_md: str) -> tuple[str | None, str]:
    lines = raw_md.splitlines(keepends=True)
    idx = 0
    while idx < len(lines) and not lines[idx].strip():
        idx += 1

    if idx >= len(lines):
        return None, raw_md

    heading_line = lines[idx].rstrip("\r\n")
    match = LEADING_H1_RE.match(heading_line)
    if match is None:
        return None, raw_md

    title = match.group("title").strip()
    body_start = idx + 1
    while body_start < len(lines) and not lines[body_start].strip():
        body_start += 1
    body_md = "".join(lines[:idx] + lines[body_start:])
    return title, body_md


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


def rewrite_variant_target_path(raw_target: str, *, language: str) -> str:
    if language != "cn":
        return rewrite_target_path(raw_target)

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
    if path_part.endswith("README_CN.md"):
        normalized_path = normalize_readme_target_path(path_part)
        if normalized_path is not None:
            rebuilt = urllib.parse.quote(append_relative_path_segment(normalized_path, "cn"), safe="/.")
            if split.query:
                rebuilt += f"?{split.query}"
            if split.fragment:
                rebuilt += f"#{split.fragment}"
            return rebuilt

    return rewrite_target_path(raw_target)


def rewrite_homepage_target_path(raw_target: str, *, language: str) -> str:
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
    mapped_path = remap_homepage_repo_path(path_part, language=language)
    rebuilt = urllib.parse.quote(mapped_path, safe="/.")
    if split.query:
        rebuilt += f"?{split.query}"
    if split.fragment:
        rebuilt += f"#{split.fragment}"
    return rewrite_target_path(rebuilt)


def remap_homepage_repo_path(path_part: str, *, language: str) -> str:
    if language == "en":
        if path_part in {"./README.md", "README.md"}:
            return "./"
        if path_part in {"./README_CN.md", "README_CN.md"}:
            return "./cn/"
        if path_part.startswith("./fluxon_doc_en/"):
            return "./" + path_part[len("./fluxon_doc_en/") :]
        if path_part.startswith("fluxon_doc_en/"):
            return path_part[len("fluxon_doc_en/") :]
        if path_part.startswith("./fluxon_doc_cn/"):
            return "./cn/" + path_part[len("./fluxon_doc_cn/") :]
        if path_part.startswith("fluxon_doc_cn/"):
            return "cn/" + path_part[len("fluxon_doc_cn/") :]
        return path_part

    if language == "cn":
        if path_part in {"./README_CN.md", "README_CN.md"}:
            return "../cn/"
        if path_part in {"./README.md", "README.md"}:
            return "../"
        if path_part.startswith("./fluxon_doc_cn/"):
            return "../cn/" + path_part[len("./fluxon_doc_cn/") :]
        if path_part.startswith("fluxon_doc_cn/"):
            return "../cn/" + path_part[len("fluxon_doc_cn/") :]
        if path_part.startswith("./fluxon_doc_en/"):
            return "../" + path_part[len("./fluxon_doc_en/") :]
        if path_part.startswith("fluxon_doc_en/"):
            return "../" + path_part[len("fluxon_doc_en/") :]
        if path_part.startswith("./"):
            return "../" + path_part[len("./") :]
        return path_part

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


def append_relative_path_segment(base_path: str, segment: str) -> str:
    if base_path in {"", "."}:
        return f"{segment}/"
    if not base_path.endswith("/"):
        base_path += "/"
    return f"{base_path}{segment}/"


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
    # Apply post-build fixes for GitHub Pages routing and Quartz navigation.
    create_pretty_route_indexes()
    force_expand_explorer()
    add_explorer_home_link()


def create_pretty_route_indexes() -> None:
    # Duplicate foo.html to foo/index.html so GitHub Pages can serve /foo/.
    for html_path in sorted(OUTPUT_ROOT.rglob("*.html")):
        if html_path.name in {"index.html", "404.html"}:
            continue

        pretty_dir = html_path.with_suffix("")
        pretty_index_path = pretty_dir / "index.html"
        ensure_dir(pretty_dir)
        html_text = html_path.read_text(encoding="utf-8")
        pretty_index_path.write_text(rewrite_pretty_route_html(html_text), encoding="utf-8")


def rewrite_pretty_route_html(html_text: str) -> str:
    # Moving foo.html to foo/index.html adds one path segment to every relative URL.
    rewritten_html = HTML_URL_ATTR_RE.sub(rewrite_pretty_route_html_match, html_text)
    return HTML_FETCH_URL_RE.sub(rewrite_pretty_route_html_match, rewritten_html)


def rewrite_pretty_route_html_match(match: re.Match[str]) -> str:
    relative_url = match.group("url")
    return f"{match.group('prefix')}{rewrite_pretty_route_relative_url(relative_url)}{match.group('suffix')}"


def rewrite_pretty_route_relative_url(relative_url: str) -> str:
    parsed_url = urllib.parse.urlsplit(relative_url)
    if parsed_url.scheme or parsed_url.netloc:
        return relative_url
    if relative_url.startswith(("/", "#", "?")):
        return relative_url
    return "../" + relative_url


def force_expand_explorer() -> None:
    # Append CSS that keeps the left explorer fully expanded.
    index_css_path = OUTPUT_ROOT / "index.css"
    if not index_css_path.is_file():
        raise SystemExit(f"ERROR: missing built Quartz stylesheet: {index_css_path}")

    css_text = index_css_path.read_text(encoding="utf-8")
    if EXPLORER_FORCE_EXPANDED_CSS in css_text:
        return
    index_css_path.write_text(css_text + "\n" + EXPLORER_FORCE_EXPANDED_CSS, encoding="utf-8")


def add_explorer_home_link() -> None:
    # Append JavaScript that adds the explorer home link and fixes internal links.
    postscript_path = OUTPUT_ROOT / "postscript.js"
    if not postscript_path.is_file():
        raise SystemExit(f"ERROR: missing built Quartz script bundle: {postscript_path}")

    script_text = postscript_path.read_text(encoding="utf-8")
    if DOC_SITE_POSTSCRIPT_JS in script_text:
        return
    postscript_path.write_text(script_text + "\n" + DOC_SITE_POSTSCRIPT_JS, encoding="utf-8")


def resolve_doc_variant(language: str) -> DocVariant:
    for variant in DOC_VARIANTS:
        if variant.language == language:
            return variant
    raise KeyError(f"unsupported doc language: {language}")


def resolve_doc_root(language: str) -> Path:
    variant = resolve_doc_variant(language)
    raw_doc_root = os.environ.get(variant.doc_root_env)
    if raw_doc_root and raw_doc_root.strip():
        doc_root = Path(raw_doc_root.strip())
        if not doc_root.is_absolute():
            doc_root = REPO_ROOT / doc_root
        return doc_root

    if language == "en":
        legacy_doc_root = os.environ.get("FLUXON_DOC_ROOT")
        if legacy_doc_root and legacy_doc_root.strip():
            doc_root = Path(legacy_doc_root.strip())
            if not doc_root.is_absolute():
                doc_root = REPO_ROOT / doc_root
            return doc_root

    if variant.default_doc_root.is_dir():
        return variant.default_doc_root
    return variant.fallback_doc_root


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
    rows: list[tuple[str, int, int]] = []
    for variant in DOC_VARIANTS:
        doc_root = resolve_doc_root(variant.language)
        for path in sorted(doc_root.rglob("*")):
            rel = path.relative_to(doc_root)
            if should_skip_rel_path(rel) or not path.is_file():
                continue
            stat = path.stat()
            rows.append((f"doc:{variant.language}:{rel.as_posix()}", stat.st_mtime_ns, stat.st_size))

    for path in (
        Path(__file__),
        REPO_ROOT / ".github" / "workflows" / "docs-pages.yml",
        HOMEPAGE_MARKDOWN_SOURCE,
        HOMEPAGE_CN_MARKDOWN_SOURCE,
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
