Keep this document concise.
- Core user, developer, and design docs are in /mnt/ceph/zyc/fluxon_for_doc/fluxon_doc
- teststack has two steps: start testbed and testrunner
- teststack has UI support; testrunner should own the UI, and the UI should reuse the ops interfaces underneath
- All Python code in this project must be compatible with Python >=3.10
- YAML files in this project are examples by default. Do not edit them directly; create a YAML file for your specific development environment
- Start long-running commands in `tmux`. Do not run long-lived services directly in the foreground.
- Git operations are limited to basic `stage`, `unstage`, `commit`, and `push`. Do not use other Git operations.

## Doc Site
- Use Quartz for the doc site. Treat Quartz as cached build tooling under `.cached`; do not vendor it as a git submodule.
- Publish the repo-root `README.md` as the doc-site homepage.
- Do not add index `README.md` files under `fluxon_doc_cn/**` or `fluxon_doc_en/**`; use real content pages and generated navigation instead.
- GitHub Pages output must work under a project subpath such as `/Fluxon/`; avoid root-only internal links.
- In the doc explorer, keep the left tree expanded, include `首页`, and place `roadmap` immediately after `首页`.

## Code Comments
- Write code comments in English.
- Prefer short comments that explain what a function or block does.
- Keep comments easy to scan; use bullets only when structure materially helps.
- Avoid long causal essays in comments unless the logic would otherwise be hard to follow.

## Public API Contract
- Public APIs must use strong contracts. Do not expose "maybe this type, maybe that type" behavior.
- User-facing examples, quick starts, READMEs, and user docs must call the stable public contract directly.
- Do not use duck-typing, `getattr(...)`, `callable(...)`, or implementation probing in public-facing code paths.
- If compatibility logic is required, keep it inside a dedicated adapter layer, not in examples or docs.
- Type signatures, docs, and runtime behavior must match. If an API says it returns `MemHolder`, it must return `MemHolder`.
- For internal invariants, fail fast or assert. Do not silently probe and fallback as if the contract were unclear.
- For one semantic operation, keep one primary path. Do not mix `foo_blocking()` with `foo().wait()` in the same public pattern unless that distinction is itself part of the contract.
