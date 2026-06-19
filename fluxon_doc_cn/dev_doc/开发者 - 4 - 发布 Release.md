# 开发者 - 4 - 发布 Release

本文说明当前仓库如何发布一个对外 release。当前稳定流程是：先更新公开版本号和文案，然后本地提交、打 `v<version>` tag 并 push；GitHub Actions 会自动构建产物并创建 GitHub Release。如果本次改动包含 README 或文档，还要单独发布 GitHub Pages 文档站。

## 边界

| 本文覆盖 | 不覆盖 | 说明 |
|---|---|---|
| GitHub Releases 上的安装产物发布 | 远端机器上的 release dispatch | 远端部署属于 `deployment/manual_dispatch_release.py` 的范围 |
| `publish_release` 的构建入口 | 本地打 wheel 的实现细节 | 本地打包细节见“开发者 - 1 / 2” |
| GitHub Pages 文档站发布入口 | testbed / testrunner 的测试流程 | 测试栈另有独立流程 |

## 1. 先更新版本号和公开文案

当前仓库没有单一的“全局版本号文件”。发布前至少要核对这些公开入口：

| 对外对象 | 主要文件 | 说明 |
|---|---|---|
| Python 包版本 | `fluxon_py/__init__.py` | 根目录 `setup.py` 会从这里读取版本号 |
| Rust crate 版本 | `fluxon_rs/Cargo.toml`、`fluxon_rs/*/Cargo.toml`、`fluxon_rs/setup.py` | 对外 crate / wheel 版本应保持一致 |
| Quick Start 镜像标签 | `examples/fluxon_quick_start/build_image.py`、`examples/fluxon_quick_start/README.md` | 镜像名当前是 `fluxon_quick_start:<version>` |
| Release workflow 产物名 | `.github/workflows/manual-release.yml` | 当前会产出 `fluxon_quick_start_<version>_docker_image.tar.gz` |
| GitHub Release 文案 | `fluxon_release/release_notes/v<version>.md` | 当前 release 正文直接读取这个版本文件 |
| README 对外文案 | `README.md`、`README_CN.md` | 包括 badge、镜像标签示例和开发文档入口 |

建议先用一次全文搜索把旧版本号找全：

```bash
OLD=0.2.1  # replace with the previous release version
rg -n "$OLD" README.md README_CN.md fluxon_py fluxon_rs examples .github/workflows
```

版本号和文案改完后，先提交并推送 commit，再打 tag 并推送 tag。

发布文案当前使用每个版本单独文件。约定路径是：

```text
fluxon_release/release_notes/v<version>.md
```

例如 `v0.2.1` 对应：

```text
fluxon_release/release_notes/v0.2.1.md
```

如果这个文件不存在，`publish_release` 会直接失败，不会发布一个没有正文的 release。

## 2. Push `v<version>` tag 触发 `publish_release`

当前仓库的公开 release 构建入口是 GitHub Actions workflow `.github/workflows/manual-release.yml`，workflow 名称是 `publish_release`。它由 `v*` tag push 自动触发，也保留了 `workflow_dispatch` 作为重建入口。稳定主路径是：

```bash
git push origin <branch>
git tag v0.2.1
git push origin v0.2.1
```

workflow 在 GitHub runner 上会自动完成这些步骤：

1. 校验 tag、`fluxon_py/__init__.py`、`examples/fluxon_quick_start/build_image.py`、`fluxon_rs/setup.py` 和 release 相关 Cargo manifest 的版本号一致。
2. 校验 `fluxon_release/release_notes/v<version>.md` 存在。
3. 安装 Python 3.10、Docker 和打包依赖。
4. 构建 manylinux builder image。
5. 生成 CI 用的 `pack_fluxonkv_pylib_env.yaml`。
6. 运行 `python3 setup_and_pack/pack_release.py --release-dir fluxon_release`。
7. 运行 `python3 examples/fluxon_quick_start/build_image.py --mode existing_release --release-dir fluxon_release`。
8. 上传 workflow artifact。
9. 自动创建或更新同名 GitHub Release，并上传 release assets。

如果 tag 是 `v0.2.1`，但仓库公开版本号仍然是 `0.2.0`，或者 `fluxon_release/release_notes/v0.2.1.md` 不存在，workflow 会直接失败，不会发布一个契约不完整的 release。

本次 workflow 结束后，GitHub Actions artifact 和 GitHub Release assets 里都应至少包含：

- `fluxon_release.tar.gz`
- `fluxon_quick_start_<version>_docker_image.tar.gz`

`fluxon_release.tar.gz` 里会带上 `fluxon_release/` 目录，包含 core wheel、`pylib_src.tar.gz`、`install.py`、`ext_images.tar.gz` 和 `fluxon_release.sha256`。这些内部产物的构成细节分别见：

- [开发者 - 1 - 打包核心安装包](./开发者%20-%201%20-%20打包核心安装包.md)
- [开发者 - 2 - 打包中间件和镜像](./开发者%20-%202%20-%20打包中间件和镜像.md)

## 3. 什么时候需要手动触发

默认情况下，不需要再手动去 GitHub `Releases` 页面建 release。成功 push `v<version>` tag 后，workflow 会自动读取对应的 `fluxon_release/release_notes/v<version>.md`，然后完成这件事。

只有下面这些情况，才建议手动触发 `publish_release`：

- GitHub runner 临时故障，需要对已存在的 tag 重建产物。
- 之前的 workflow 在打包后半段失败，需要对同一个 tag 重新上传 assets。
- 你明确想验证 release workflow，但又不想新建 tag。

手动触发时，需要在 GitHub Actions 页面给出一个已经存在的 tag，例如 `v0.2.1`。这个 tag 仍然必须和仓库里的公开版本号一致。

如果只是改 GitHub Release 页面上的说明文字，需要修改对应的 `fluxon_release/release_notes/v<version>.md`，然后手动重跑 `publish_release`，这样既会更新 release 文案，也会重新上传 assets。

## 4. 发布文档站

文档站发布和二进制 release 是两条独立链路。当前文档站 workflow 是 `.github/workflows/docs-pages.yml`：

- 当 `README.md`、`README_CN.md`、`fluxon_doc_cn/**`、`fluxon_doc_en/**` 或 `scripts/build_doc_site.py` 变更并推到 `main` / `master` 时，会自动触发。
- 也可以在 GitHub Actions 页面手动触发 `docs-pages`。
- 该 workflow 会构建 `fluxon_release/doc_site/`，然后部署到 GitHub Pages。
- 该 workflow 不会上传 wheel、`fluxon_release.tar.gz` 或 Docker image。

如果这次 release 改了 README、安装文档、开发文档或 roadmap，通常要把 `docs-pages` 一起发掉。

## 5. 什么时候重跑

- 版本号、README badge、镜像标签、release 产物名变更后，要重新 push 一个对应版本的 tag，或手动重跑 `publish_release`。
- `setup_and_pack/`、`examples/fluxon_quick_start/`、`fluxon_py/`、`fluxon_rs/` 的发布相关内容变更后，要重新 push 一个对应版本的 tag，或手动重跑 `publish_release`。
- README、`fluxon_doc_cn/`、`fluxon_doc_en/` 或文档站导航变更后，要重跑 `docs-pages`。
- 只有 GitHub Release 页面上的说明文字改动时，不需要重跑任何 workflow。
