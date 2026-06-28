# Top Attention Test Index

This directory is a flat index of important existing tests and smoke commands
that now lives under `fluxon_test_stack/`. The entries stay thin: they forward
to canonical tests elsewhere in the repository instead of implementing new test
logic.

Each `*.py` entry here declares a sorted `TEST_REQUIREMENTS` list. The full
requirement universe lives in `requirements_all.py`, and `_test_requirements.py`
checks that per-entry declarations stay within that universe and fully cover it.
All indexed items now declare `ops` because execution is expected to go through
the shared Fluxon Ops / test-stack path.

This directory is listing-only. Runtime orchestration, status, and log UI are
owned by the `fluxon_test_stack` flow, not by this index directory. Listing and
quick execution entrypoints are now consolidated into `fluxon_test_stack/test_runner.py`.

Useful entry points:

- `python3 fluxon_test_stack/test_runner.py --action top_attention_list --top-attention-json --top-attention-all`
- `python3 fluxon_test_stack/test_runner.py --action top_attention_list --top-attention-json --top-attention-prefix mq`
- `python3 fluxon_test_stack/test_runner.py --action top_attention_run --top-attention-prefix mq`
- `python3 fluxon_test_stack/test_runner.py --action top_attention_quick`

Entries:

- `_pack_whl.py`: forwards to `setup_and_pack/pack_fluxon_pylib.py`
- `_pack_test_rsc.py`: forwards to `fluxon_test_stack/pack_test_stack_rsc.py`
- `_doc_page_build.py`: builds the docs through the configured Docker builder image
- `_bin_kvtest.py`: forwards to the Rust `kv_test` binary command. CI suites declare this wrapper through the `ci_top_attention_bin_kvtest` scene command.
- `_bin_external_client.py`: forwards to the Rust `external_client_test` binary
- `_ctrl_c_kv.py`: forwards to existing runtime Ctrl-C child-retirement coverage
- `_ctrl_c_mq.py`: forwards to `fluxon_py/tests/test_mq/test_example_ctrl_c_exit.py`
- `_config_fs.py`: FS Python config/schema coverage
- `_config_kv.py`: KV Python config/schema coverage
- `_config_mq.py`: existing MQ config/capacity semantic coverage
- `_py_runtime.py`: Python runtime/process runner coverage
- `_kv_py_core.py`: Python KV backend/core smoke coverage
- `_relay_mq.py`: MQ relay docker coverage
- `_mq_core.py`: non-Ctrl-C MQ correctness coverage
- `_largescale_mq.py`: TEST_STACK large-scale MQ benchmark wrapper (defaults to 30 owners at 5GiB, 300 producers, 8 consumers)
- `_mq_mpsc.py`: MPSC API channel coverage
- `_mq_mpmc.py`: MPMC API channel coverage
- `_mq_mpmc_bench.py`: heavier MPMC bench scripts
- `_fs_py_core.py`: Fluxon FS Python config/patcher coverage
- `_fs_transfer_tikv.py`: heavier Fluxon FS transfer integration coverage
- `_fs_remote_mount.py`: heavier Fluxon FS remote mount integration coverage
- `_test_stack_contract.py`: test-stack runner contract coverage
- `_deployment_codegen.py`: deployment code generation coverage
- `_log_mgmt.py`: shared-supervisor ops log rolling plus Rust KV log sharding coverage. CI suites declare this wrapper through the `ci_top_attention_log_mgmt` scene command.
- `_script_tools.py`: script utility coverage
- `_cargo_fs_core.py`: cargo tests for the Rust FS core crate. CI suites declare this wrapper through the `ci_top_attention_cargo_fs_core` scene command.
- `_cargo_util.py`: cargo tests for the Rust util crate. CI suites declare this wrapper through the `ci_top_attention_cargo_util` scene command, with runtime endpoints supplied through canonical `--case-config`.
- `_cargo_kv_unit.py`: cargo tests for the Rust KV crate. CI suites declare this wrapper through the `ci_top_attention_cargo_kv_unit` scene command, with transport feature selection sourced only from canonical `--case-config` (`scene_config.kv_transport_feature`).
- `_cargo_cli.py`: cargo tests for the Rust CLI crate
- `_cargo_commu.py`: cargo tests for the Rust communication facade crate
- `_cargo_commu_contract.py`: cargo tests for the Rust communication contract crate
- `_cargo_framework.py`: cargo tests for the Rust framework crate
- `_cargo_fs.py`: cargo tests for the Rust FS crate. This wrapper expects the prepared `fluxon_release/ext_images/tikv/*` runtime files.
- `_cargo_fs_s3_gateway.py`: cargo tests for the Rust FS S3 gateway crate. This wrapper expects the prepared `fluxon_release/ext_images/tikv/*` runtime files.
- `_cargo_limit_thirdparty.py`: cargo tests for the Rust third-party facade crate
- `_cargo_mq.py`: cargo tests for the Rust MQ crate
- `_cargo_observability.py`: cargo tests for the Rust observability crate
- `_cargo_ops.py`: cargo tests for the Rust ops crate
- `_cargo_pyo3.py`: cargo tests for the Rust PyO3 crate

Operational note:

- `_largescale_mq.py` generates a temporary `bench_mq` suite and then forwards
  to `fluxon_test_stack/test_runner.py`. The selected TEST_STACK profile must
  provide at least 308 common non-bastion deploy targets in `target_ip_map` for
  the default 300-producer/8-consumer topology; pass `--config` for the large
  cluster suite before running it.
- All `_cargo_*.py` wrappers are direct-process entrypoints. They do not forward
  `pytest` selectors or `cargo test` passthrough flags unless the wrapper
  explicitly defines that surface.

Known gap:

- There is no standalone canonical KV Ctrl-C integration test file in the tree.
  `_ctrl_c_kv.py` currently indexes the closest existing runtime Ctrl-C shutdown
  coverage. Add a direct KV master/client SIGINT integration test here only
  after a canonical implementation exists elsewhere.
- There is no standalone canonical MQ config unit test file in the tree.
  `_config_mq.py` currently indexes the closest existing MQ capacity/lease
  semantic coverage that exercises channel config fields.
- Candidates still worth classifying before adding to this top-attention index:
  `fluxon_py/tests/test_lib.py`,
  `setup_and_pack/tests/test_build_ext_config_contract.py`,
  `fluxon_py/tests/heavy_3rdparty_test/test_backend_heavy_3rdparty.py`,
  and selected compatibility tests under `fluxon_rs/moka/tests`.
