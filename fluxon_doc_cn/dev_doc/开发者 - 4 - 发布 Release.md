# 开发者 - 4 - 发布 Release

release 只有一个手动入口：从 GitHub Actions 运行 `create_release_tag`。它不读取或等待 main CI，创建 tag 后直接 dispatch `manual-release.yml`。发布 workflow 现场构建并校验全部产物，然后并行启动 GitHub Release、PyPI 和 Docker Hub 三个发布 job。本地 push tag 和第二个手动发布 workflow 不属于支持路径。

## 边界

| 本文覆盖 | 不覆盖 | 说明 |
|---|---|---|
| 通过 GitHub Actions 创建 release tag | 决定下一个版本号 | 版本策略由维护者决定 |
| 发布 GitHub Release 产物、`fluxon-ai` PyPI wheel 和 Quick Start Docker image | 其他 package index 或 image registry | 当前公开目的地是 GitHub、PyPI 和 Docker Hub |
| 产物确定性检查、只读 Codex 审核和目的地审批 | 功能测试或集成测试 | 发布路径不运行 main CI；Codex 输出也不是测试结果 |
| GitHub Pages 文档站入口 | 向远端机器 dispatch release | 远端部署属于 `deployment/manual_dispatch_release.py` |

## 1. 准备版本 PR

当前仓库没有单一的全局版本文件。发布前要核对这些公开入口：

| 对外对象 | 主要文件 | 说明 |
|---|---|---|
| Python 包版本 | `fluxon_py/__init__.py` | 根目录 `setup.py` 从这里读取版本号 |
| Rust crate 版本 | `fluxon_rs/Cargo.toml`、`fluxon_rs/*/Cargo.toml`、`fluxon_rs/setup.py` | release crate 与 wheel 版本必须一致 |
| 闭源 SDK open-surface 要求 | `fluxon_rs/fluxon_commu_contract/src/lib.rs`、`fluxon_release/closed_sdk/manifest.json` 和配套 SDK library | SDK 要求必须与 `FLUXON_COMMU_OPEN_SURFACE_VERSION` 一致；该契约版本独立于公开发布版本 |
| Quick Start image tag | `examples/fluxon_quick_start/build_image.py`、`examples/fluxon_quick_start/README.md` | Docker Hub 发布 `hanbaoaaa/fluxon_quick_start:<version>` |
| GitHub Release 文案 | `fluxon_release/release_notes/v<version>.md` | release 正文读取 tag 对应 revision |
| README release 文案 | `README.md`、`README_CN.md` | 包括 badge 和版本化 Docker 示例 |

先搜索旧版本在公开入口中的使用。不要机械修改用于特定版本行为的测试 fixture 或 YAML 示例。

```bash
OLD=0.2.1  # replace with the previous release version
rg -n "$OLD" README.md README_CN.md fluxon_py fluxon_rs examples fluxon_release
```

闭源通信 SDK 会分别报告 `sdk_version` 与 `required_open_surface_version`。运行时把后者与 `FLUXON_COMMU_OPEN_SURFACE_VERSION` 对照，不使用 Cargo package version。仅升级发布版本时保持该契约不变；修改 open-surface 常量或生成的 manifest 前必须先重建并验证 SDK library。发布 workflow 不运行 `fluxon_commu` 运行时契约测试，审批者需要从其他渠道确认相关测试证据。

运行本地元数据与 workflow 契约检查：

```bash
python3 fluxon_release/resolve_release_meta.py --git-ref refs/tags/v<version>
python3 -m unittest \
  setup_and_pack.tests.test_resolve_release_meta \
  setup_and_pack.tests.test_release_workflows
```

合入版本 PR 后即可发起 release。默认分支 CI 可以独立运行，但它的状态和产物不参与 tag 或发布门禁。

## 2. 一次性配置凭据与 environment

repository admin 必须配置以下外部控制项：

| 控制项 | 必需配置 | 用途 |
|---|---|---|
| repository Actions token | 不需要外部凭据；`create_release_tag` 为内置 `GITHUB_TOKEN` 授予 contents write 权限 | 创建 tag 并 dispatch 发布流程 |
| Codex 凭据 | 在 environment `OPENAI_API_KEY` 中设置 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL` | 执行只读 readiness 审核 |
| GitHub Release 审批 | 在 environment `github-release` 配置 required reviewers | 约束唯一持有 `contents: write` 的 job |
| PyPI 审批 | 在 environment `pypi` 配置 required reviewers；把 PyPI Trusted Publisher 绑定到 `manual-release.yml` 和该 environment | 约束经包契约校验的现场构建 wheel 的 OIDC 上传 |
| Docker 审批 | 在 environment `docker-image` 配置 required reviewers、`DOCKERHUB_USERNAME` 和 `DOCKERHUB_TOKEN` | 约束版本化 Docker Hub push |

只在 YAML 中引用 environment 不会自动增加 reviewer。管理员必须在 GitHub 设置中配置 protection rule。

tag workflow 使用仓库内置 `GITHUB_TOKEN` 创建 ref，并发送内部 `repository_dispatch`，其中只携带 source commit 和仓库推导出的 tag。`manual-release.yml` 只接受 `github-actions[bot]` 发起的事件，证明 tag 指向该 commit 且 commit 属于默认分支历史。无需外部发布身份、PAT、release-tag secret 或 CI run ID。

## 3. 从 GitHub Actions 发起 release

`.github/workflows/create-release-tag.yml` 是唯一手动 release 入口。

1. 打开 GitHub Actions，选择 `create_release_tag`。
2. workflow ref 选择默认分支。
3. 直接运行 workflow；该入口没有 release 参数。

workflow 从仓库中经过校验的版本来源推导准确的 `v<version>` tag，并从 `fluxon_release/release_notes/v<version>.md` 读取 release 正文。创建 tag 前，workflow 只校验 tag 形式、默认分支 ref、tag 不存在、release 元数据和 release notes。随后内置 token 创建 lightweight tag 并 dispatch 发布流程，不等待 main CI。

该入口明确允许 main CI 未运行、运行中或失败时创建 tag 并继续发布。审批者不能从 release workflow 的成功推导出功能测试、集成测试或运行时契约测试已经通过。

不要在本地运行 `git tag` 或 push release tag。release 输入有误时，修复版本 PR，并为新 tag 重新运行 `create_release_tag`。

## 4. 统一准备并审核三个目的地

`.github/workflows/manual-release.yml` 的 workflow 名称是 `publish_release`，只接受创建 tag 后发送的内部 `repository_dispatch`。该文件名沿用已有 PyPI Trusted Publisher 身份，workflow 本身不提供 `workflow_dispatch`。

workflow 分为以下阶段：

1. `verify-release` 校验 tag 与 source commit 身份、默认分支历史、版本元数据和 release notes，不读取 CI。
2. `pack-release` 现场构建 `fluxon_release.tar.gz`、Quick Start image archive 和 PyPI wheel candidate；`prepare-pypi-wheel` 校验该 wheel 的版本、兼容性 tag、大小、checksum 与 PyPI 元数据，并运行 `twine check`。
3. `release-readiness-review` 检查 release checksum，证明 GitHub Release 中的 wheel 与 PyPI wheel 完全相同，并使用只读 permission profile 和 `.github/codex/release-readiness-prompt.md` 运行 `openai/codex-action`。
4. 审核成功后，三个发布 job 同时具备运行条件。每个目的地在自己的受保护 environment 等待。

Codex 可以指出不一致和证据缺口。它必须明确披露 main CI 被跳过，不能把打包与元数据校验表述为功能测试；报告文字也不会被解析成自动发布授权。

## 5. 并行发布三个目的地

| Job | Environment | 发布对象 | 凭据边界 |
|---|---|---|---|
| `publish-github-release` | `github-release` | `fluxon_release.tar.gz` 与 `fluxon_quick_start_<version>_docker_image.tar.gz` | 只有该 job 获得 `contents: write` |
| `publish-pypi` | `pypi` | 发布 workflow 现场构建并通过包契约校验的 `fluxon_ai-*.whl` | 只有该 job 获得 `id-token: write`，不使用 `PYPI_TOKEN` |
| `publish-docker-image` | `docker-image` | `hanbaoaaa/fluxon_quick_start:<version>` | Docker Hub 凭据只存在于该 environment |

三个 job 依赖同一 readiness review，彼此之间没有依赖。批准或重跑其中一个目的地不会串行化另外两个。

PyPI 准备阶段检查 tag 身份、默认分支历史、distribution 与版本、`cp38-abi3-manylinux_2_28_x86_64` wheel tag、`Requires-Python >=3.10`、文件大小、checksum 和 `twine check`。用户安装命令是：

```bash
python3 -m pip install fluxon-ai
```

Docker job 会加载准确的受审 image archive，校验本地 image identity，再使用规范 Docker Hub repository 与 release version 重新打 tag，只 push 版本 tag，不更新 `latest`。

## 6. 不增加第二个入口的重跑方式

仓库不提供“手动发布已有 tag”的 workflow。preparation、Codex、审批或目的地上传发生临时失败时，在已有 `publish_release` run 上使用 GitHub Actions 的 rerun failed jobs。source 或 release 内容存在缺陷时，需要新 commit、新版本和新 tag；发布链路不等待或重跑 main CI。

不要移动已发布 tag。PyPI version 与版本化 Docker image tag 都按不可变 release 产物处理。产物或安装行为变化时使用新版本和新 tag。

## 7. 发布文档站

`.github/workflows/docs-pages.yml` 与三个 release 目的地分离，用于构建 `fluxon_release/doc_site/` 并部署 GitHub Pages。README、安装文档、开发者文档或 roadmap 改动时，要确认对应 run。

## 8. 重跑条件

- tag preflight 失败时修复版本 PR 并重跑 `create_release_tag`；不要用本地 tag 绕过。
- source 或 release 缺陷需要在新 commit 中修复并使用新版本；不要把已有 tag 移到其他 commit。
- preparation、Codex、审批、GitHub Release、PyPI 或 Docker Hub 发生临时失败时，rerun `publish_release` 中的失败 job。
- 已到达 PyPI 或 Docker Hub 的版本不能用不同内容覆盖。
- README、`fluxon_doc_cn/`、`fluxon_doc_en/` 或导航变化后重跑 `docs-pages`。
