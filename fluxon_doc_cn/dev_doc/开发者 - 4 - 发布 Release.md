# 开发者 - 4 - 发布 Release

release 只有一个手动入口：从 GitHub Actions 运行 `create_release_tag`。它复用准确 commit 已成功的默认分支 CI，创建 tag 并 dispatch `manual-release.yml`。发布 workflow 重新验证 CI、commit 与 tag 后统一准备并审核全部产物，然后并行启动 GitHub Release、PyPI 和 Docker Hub 三个发布 job。重复 tag CI、本地 push tag 和第二个手动发布 workflow 不属于支持路径。

## 边界

| 本文覆盖 | 不覆盖 | 说明 |
|---|---|---|
| 通过 GitHub Actions 创建 release tag | 决定下一个版本号 | 版本策略由维护者决定 |
| 发布 GitHub Release 产物、`fluxon-ai` PyPI wheel 和 Quick Start Docker image | 其他 package index 或 image registry | 当前公开目的地是 GitHub、PyPI 和 Docker Hub |
| 确定性检查、只读 Codex 审核和目的地审批 | 把 Codex 输出当作测试结果 | Codex 提供审核证据，确定性 job 才是权威门禁 |
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

闭源通信 SDK 会分别报告 `sdk_version` 与 `required_open_surface_version`。运行时把后者与 `FLUXON_COMMU_OPEN_SURFACE_VERSION` 对照，不使用 Cargo package version。仅升级发布版本时保持该契约不变；修改 open-surface 常量或生成的 manifest 前必须先重建并验证 SDK library。默认分支 CI suite 包含 `fluxon_commu` 运行时契约测试。

运行本地元数据与 workflow 契约检查：

```bash
python3 fluxon_release/resolve_release_meta.py --git-ref refs/tags/v<version>
python3 -m unittest \
  setup_and_pack.tests.test_resolve_release_meta \
  setup_and_pack.tests.test_release_ci_provenance \
  setup_and_pack.tests.test_release_workflows
```

合入版本 PR，等待默认分支上准确 commit 的 `ci_2_virt_node` push run 成功。

## 2. 一次性配置凭据与 environment

repository admin 必须配置以下外部控制项：

| 控制项 | 必需配置 | 用途 |
|---|---|---|
| repository Actions token | 不需要外部凭据；`create_release_tag` 为内置 `GITHUB_TOKEN` 授予 Actions read 与 contents write 权限 | 查找成功的默认分支 CI、创建 tag 并 dispatch 发布流程 |
| Codex 凭据 | 在 environment `OPENAI_API_KEY` 中设置 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL` | 执行只读 readiness 审核 |
| GitHub Release 审批 | 在 environment `github-release` 配置 required reviewers | 约束唯一持有 `contents: write` 的 job |
| PyPI 审批 | 在 environment `pypi` 配置 required reviewers；把 PyPI Trusted Publisher 绑定到 `manual-release.yml` 和该 environment | 约束受测 wheel 的 OIDC 上传 |
| Docker 审批 | 在 environment `docker-image` 配置 required reviewers、`DOCKERHUB_USERNAME` 和 `DOCKERHUB_TOKEN` | 约束版本化 Docker Hub push |

只在 YAML 中引用 environment 不会自动增加 reviewer。管理员必须在 GitHub 设置中配置 protection rule。

tag workflow 使用仓库内置 `GITHUB_TOKEN` 创建 ref，并发送内部 `repository_dispatch`，其中只携带已经验证的默认分支 CI run ID、source commit 和仓库推导出的 tag。`manual-release.yml` 只接受 `github-actions[bot]` 发起的事件，通过 GitHub API 重新读取 CI run，并要求它是在默认分支同一 commit 上成功完成的 `all_test.yml` push run，随后再证明新 tag 指向该 commit。无需外部发布身份、PAT、release-tag secret 或重复 tag CI。

## 3. 从 GitHub Actions 发起 release

`.github/workflows/create-release-tag.yml` 是唯一手动 release 入口。

1. 打开 GitHub Actions，选择 `create_release_tag`。
2. workflow ref 选择默认分支。
3. 直接运行 workflow；该入口没有 release 参数。

workflow 从仓库中经过校验的版本来源推导准确的 `v<version>` tag，并从 `fluxon_release/release_notes/v<version>.md` 读取 release 正文。创建 tag 前，workflow 会校验 tag 形式、默认分支 ref、tag 不存在、release 元数据、release notes，以及同一 commit 的默认分支 CI 已成功。随后内置 token 创建 lightweight tag 并 dispatch 发布流程，不再运行一次相同测试。

既有默认分支 CI 提供 `fluxon-ci-release-<commit SHA>` 与 `release-ci-provenance-<commit SHA>` 两份 artifact，统一保留 14 天。前者包含受测 wheel，后者记录完整 branch ref、ref 类型、commit、repository 和 workflow 路径。`create_release_tag` 会在创建 tag 前确认两份 artifact 存在且未过期；因此应在对应默认分支 CI 完成后的 14 天内发起 release。`manual-release.yml` 会验证 provenance，重新读取 CI run 与 jobs，并拒绝 event、branch、workflow、repository、结论、commit 或 tag 的任一不一致。

不要在本地运行 `git tag` 或 push release tag。release 输入有误时，修复版本 PR，并为新 tag 重新运行 `create_release_tag`。

## 4. 统一准备并审核三个目的地

`.github/workflows/manual-release.yml` 的 workflow 名称是 `publish_release`，只接受创建 tag 后发送的内部 `repository_dispatch`。该文件名沿用已有 PyPI Trusted Publisher 身份，workflow 本身不提供 `workflow_dispatch`。

workflow 分为以下阶段：

1. `verify-release` 重新读取并校验成功的默认分支 CI、provenance、tag 与 commit 身份、默认分支历史、版本元数据和 release notes。
2. `pack-release` 与 `prepare-pypi-wheel` 相互独立运行。前者构建 `fluxon_release.tar.gz` 与 Quick Start image archive；后者校验该默认分支 CI 产生的准确 wheel，并运行 `twine check`。
3. `release-readiness-review` 检查 release checksum、采集被复用的 CI 证据、纳入受测 wheel hash，并使用只读 permission profile 和 `.github/codex/release-readiness-prompt.md` 运行 `openai/codex-action`。
4. 审核成功后，三个发布 job 同时具备运行条件。每个目的地在自己的受保护 environment 等待。

Codex 可以指出不一致和证据缺口。它不能替代默认分支 CI、tag 与 commit 一致性校验、checksum 或 environment 审批，报告文字也不会被解析成自动发布授权。

## 5. 并行发布三个目的地

| Job | Environment | 发布对象 | 凭据边界 |
|---|---|---|---|
| `publish-github-release` | `github-release` | `fluxon_release.tar.gz` 与 `fluxon_quick_start_<version>_docker_image.tar.gz` | 只有该 job 获得 `contents: write` |
| `publish-pypi` | `pypi` | 准确默认分支 CI 产生的受测 `fluxon_ai-*.whl` | 只有该 job 获得 `id-token: write`，不使用 `PYPI_TOKEN` |
| `publish-docker-image` | `docker-image` | `hanbaoaaa/fluxon_quick_start:<version>` | Docker Hub 凭据只存在于该 environment |

三个 job 依赖同一 readiness review，彼此之间没有依赖。批准或重跑其中一个目的地不会串行化另外两个。

PyPI 准备阶段检查 tag 身份、默认分支历史、distribution 与版本、`cp38-abi3-manylinux_2_28_x86_64` wheel tag、`Requires-Python >=3.10`、文件大小、checksum 和 `twine check`。用户安装命令是：

```bash
python3 -m pip install fluxon-ai
```

Docker job 会加载准确的受审 image archive，校验本地 image identity，再使用规范 Docker Hub repository 与 release version 重新打 tag，只 push 版本 tag，不更新 `latest`。

## 6. 不增加第二个入口的重跑方式

仓库不提供“手动发布已有 tag”的 workflow。preparation、Codex、审批或目的地上传发生临时失败时，在已有 `publish_release` run 上使用 GitHub Actions 的 rerun failed jobs。CI 或 source 存在缺陷时，需要新 commit、成功的默认分支 CI、新版本和新 tag；发布链路不会为已有 tag 重跑测试。

不要移动已发布 tag。PyPI version 与版本化 Docker image tag 都按不可变 release 产物处理。产物或安装行为变化时使用新版本和新 tag。

## 7. 发布文档站

`.github/workflows/docs-pages.yml` 与三个 release 目的地分离，用于构建 `fluxon_release/doc_site/` 并部署 GitHub Pages。README、安装文档、开发者文档或 roadmap 改动时，要确认对应 run。

## 8. 重跑条件

- tag preflight 失败时修复版本 PR 并重跑 `create_release_tag`；不要用本地 tag 绕过。
- CI 或 release 缺陷需要在新 commit 中修复，并在默认分支 CI 成功后使用新版本；不要把已有 tag 移到其他 commit。
- preparation、Codex、审批、GitHub Release、PyPI 或 Docker Hub 发生临时失败时，rerun `publish_release` 中的失败 job。
- 已到达 PyPI 或 Docker Hub 的版本不能用不同内容覆盖。
- README、`fluxon_doc_cn/`、`fluxon_doc_en/` 或导航变化后重跑 `docs-pages`。
