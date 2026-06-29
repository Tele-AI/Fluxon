from __future__ import annotations

CI_SCENE_IDS: tuple[str, ...] = (
    "ci_top_attention_doc_page_build",
    "ci_top_attention_bin_kvtest",
    "ci_top_attention_cargo_fs_core",
    "ci_top_attention_cargo_util",
    "ci_top_attention_cargo_kv_unit",
    "ci_top_attention_cargo_cli",
    "ci_top_attention_cargo_commu",
    "ci_top_attention_cargo_commu_contract",
    "ci_top_attention_cargo_framework",
    "ci_top_attention_cargo_fs",
    "ci_top_attention_cargo_fs_s3_gateway",
    "ci_top_attention_cargo_limit_thirdparty",
    "ci_top_attention_cargo_mq",
    "ci_top_attention_cargo_observability",
    "ci_top_attention_cargo_ops",
    "ci_top_attention_cargo_pyo3",
    "ci_top_attention_log_mgmt",
    "ci_top_attention_mq_core",
)


def canonical_ci_scene_ids() -> tuple[str, ...]:
    # Keep one canonical declaration for the GitHub Actions CI scene list.
    return CI_SCENE_IDS
