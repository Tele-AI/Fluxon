from __future__ import annotations

CI_SCENE_IDS: tuple[str, ...] = (
    "ci_top_attention_doc_page_build",
    "ci_top_attention_bin_kvtest",
    "ci_top_attention_log_mgmt",
    "ci_top_attention_mq_core",
)


def canonical_ci_scene_ids() -> tuple[str, ...]:
    # Keep one canonical declaration for the GitHub Actions CI scene list.
    return CI_SCENE_IDS
