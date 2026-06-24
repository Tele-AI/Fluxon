Keep this document concise.
- Core user, developer, and design docs are in-repo under fluxon_doc_cn/ and fluxon_doc_en/
- Detailed bilingual doc writing rules are indexed at `fluxon_doc_en/dev_doc/Developer - 3 - Documentation Writing Rules.md` and `fluxon_doc_cn/dev_doc/开发者 - 3 - 文档写作规约.md`
- teststack has two steps: start testbed and testrunner
- teststack has UI support; testrunner should own the UI authority and API surface, and the UI should run as a long-lived service that reuses the ops interfaces underneath
- All Python code in this project must be compatible with Python >=3.10
- YAML files in this project are examples by default. Do not edit them directly; create a YAML file for your specific development environment
- Start long-running commands in `tmux`. Do not run long-lived services directly in the foreground.
- Git operations are limited to basic `stage`, `unstage`, `commit`, and `push`. Do not use other Git operations.
- Prefer contraction over compatibility by default. Do not add compatibility layers, deprecated paths, or aliases unless the task explicitly requires them.
- Prefer one canonical name for one concept. Avoid synonym parameters, duplicated entrypoints, and parallel config surfaces.
- For test entrypoints, match the real execution model directly. If a test is a standalone script/process test, invoke it as a script/process; do not wrap it in `pytest` just for uniformity.
- Do not forward pytest-style flags (`-k`, `-q`, node selectors, etc.) through direct-process test wrappers unless the wrapper explicitly implements and documents that selector surface.
- For new integration or process-lifecycle tests, prefer direct process startup with explicit arguments and explicit exit-code checks over adding new pytest-only wrappers.
- Control branching deliberately. Prefer a small, explicit, enumerated set of supported branches in the style of a Rust enum over open-ended proliferation of near-duplicate cases.
- When extending a surface, prefer folding the new case into an existing finite branch set. If a new branch is unavoidable, make it explicit, bounded, and easy to list exhaustively.
- Names for testbed-scoped concepts should say `testbed` explicitly. Avoid generic names for testbed-only modes, ports, roots, workdirs, and other testbed-scoped settings.
- Keep `AGENTS.md` and `AGENTS_CN.md` aligned. Update both promptly when changing repo-level agent rules unless the task explicitly says otherwise.

## Doc Site
- Use Quartz for the doc site. Treat Quartz as cached build tooling under `.cached`; do not vendor it as a git submodule.
- Publish the repo-root `README.md` as the doc-site homepage.
- Do not add index `README.md` files under `fluxon_doc_cn/**` or `fluxon_doc_en/**`; use real content pages and generated navigation instead.
- GitHub Pages output must work under a project subpath such as `/Fluxon/`; avoid root-only internal links.
- In `README*.md`, relative hyperlinks that point to published `.md` doc pages should use GitHub Pages absolute URLs by default so clicks from GitHub land on the published site. Exception: keep the top language switch links between `README.md` and `README_CN.md` as repo-relative links.
- In the doc explorer, keep the left tree expanded, include `首页`, and place `roadmap` immediately after `首页`.
- In docs, lead with the stable conclusion, then expand. Follow progressive disclosure.
- When updating README, user docs, developer docs, or roadmap pages, keep Chinese and English versions in sync by default. Design docs may stay Chinese-only unless the task explicitly requires an English counterpart.
- Prefer natural engineering terms; avoid template language like “根对象”, “第一层分支”, or “authority object”.
- For cross-language boundaries, ownership/lifetime rules, or async dataflow, add a diagram or table by default.
- Separate public contracts, current implementation, and specialized fast paths explicitly.
- Keep repo-level doc rules reusable and technology-agnostic. Put case-specific lessons in examples or review notes, not in the rule itself.
- For behavior, ownership, or performance claims, define the scope, abstraction level, preconditions, and exclusions explicitly.
- Do not generalize from a local fact to a whole-system claim without tracing the full path at the same abstraction level.
- In docs, avoid `不是……而是……` by default. Use it only when the surrounding section has already established both sides of the contrast and the contrast materially helps the reader at that exact location.

## Code Comments
- Write code comments in English.
- Prefer short comments that explain what a function or block does.
- Keep comments easy to scan; use bullets only when structure materially helps.
- Use concise, structured causal-chain explanations when they materially help explain non-obvious logic, but avoid long causal essays in comments.

## Public API Contract
- Public APIs must use strong contracts. Do not expose "maybe this type, maybe that type" behavior.
- User-facing examples, quick starts, READMEs, and user docs must call the stable public contract directly.
- Do not use duck-typing, `getattr(...)`, `callable(...)`, or implementation probing in public-facing code paths.
- If compatibility logic is required, keep it inside a dedicated adapter layer, not in examples or docs.
- Type signatures, docs, and runtime behavior must match. If an API says it returns `MemHolder`, it must return `MemHolder`.
- For internal invariants, fail fast or assert. Do not silently probe and fallback as if the contract were unclear.
- For one semantic operation, keep one primary path. Do not mix `foo_blocking()` with `foo().wait()` in the same public pattern unless that distinction is itself part of the contract.
