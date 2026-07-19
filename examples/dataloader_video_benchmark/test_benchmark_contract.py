#!/usr/bin/env python3
from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


EXAMPLE_DIR = Path(__file__).resolve().parent
MODULE_PATH = EXAMPLE_DIR / "benchmark.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("dataloader_video_benchmark", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec: {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


DL = _load_module()


class TestDataloaderPerfContract(unittest.TestCase):
    def test_parse_case_csv_and_frame_indices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            csv_path = Path(tmp_s) / "cases.csv"
            with csv_path.open("w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "idx",
                        "video_path",
                        "height",
                        "width",
                        "start_idx",
                        "end_idx",
                        "num_frames",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "idx": "7",
                        "video_path": "/data/a.mp4",
                        "height": "480",
                        "width": "832",
                        "start_idx": "10",
                        "end_idx": "18",
                        "num_frames": "5",
                    }
                )

            cases = DL.parse_case_csv(str(csv_path))

        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0].idx, 7)
        self.assertEqual(DL.frame_indices(cases[0]), [10, 12, 14, 16, 18])

    def test_build_jobs_alternates_backend_order(self) -> None:
        cases = [
            DL.DecodeCase(
                source="test",
                idx=1,
                video_path="/data/a.mp4",
                height=4,
                width=4,
                start_idx=0,
                end_idx=3,
                num_frames=2,
            ),
            DL.DecodeCase(
                source="test",
                idx=2,
                video_path="/data/b.mp4",
                height=4,
                width=4,
                start_idx=0,
                end_idx=3,
                num_frames=2,
            ),
        ]

        jobs = DL.build_jobs(
            cases,
            backends=["original", "fluxon"],
            rounds=1,
            alternate_backend_order=True,
        )

        self.assertEqual([job.backend for job in jobs], ["original", "fluxon", "fluxon", "original"])
        self.assertEqual(jobs[0].sample_id, jobs[1].sample_id)
        self.assertEqual(jobs[2].sample_id, jobs[3].sample_id)

    def test_summarize_reports_fluxon_speedup(self) -> None:
        rows = [
            {
                "backend": "original",
                "status": "ok",
                "elapsed_s": 2.0,
                "num_frames": 4,
            },
            {
                "backend": "fluxon",
                "status": "ok",
                "elapsed_s": 1.0,
                "num_frames": 4,
            },
        ]

        summary = DL.summarize(rows, wall_s=3.0)

        self.assertEqual(summary["backends"]["original"]["ok"], 1)
        self.assertEqual(summary["backends"]["fluxon"]["ok"], 1)
        self.assertEqual(summary["fluxon_vs_original"]["p50_speedup"], 2.0)

    def test_resolve_fluxon_agent_node_id_requires_external_agent(self) -> None:
        args = DL.argparse.Namespace(
            fluxon_agent_instance_key="fs-agent-1",
            fluxon_agent_node_id="",
        )
        self.assertEqual(DL.resolve_fluxon_agent_node_id(args), "fs-agent-1")

        args = DL.argparse.Namespace(
            fluxon_agent_instance_key="fs-agent-1",
            fluxon_agent_node_id="fs-agent-2",
        )
        with self.assertRaisesRegex(ValueError, "must match"):
            DL.resolve_fluxon_agent_node_id(args)

        args = DL.argparse.Namespace(
            fluxon_agent_instance_key="",
            fluxon_agent_node_id="",
        )
        with self.assertRaisesRegex(ValueError, "start fluxon_py.runtime.start_fs_agent"):
            DL.resolve_fluxon_agent_node_id(args)

    def test_cli_has_no_inline_fluxon_password_argument(self) -> None:
        parser = DL.build_arg_parser()
        option_strings = {
            option
            for action in parser._actions
            for option in action.option_strings
        }

        self.assertNotIn("--fluxon-request-password", option_strings)
        self.assertIn("--fluxon-request-password-file", option_strings)
        self.assertEqual(parser.get_default("manifest"), "")

    def test_sanitized_args_redacts_fluxon_password_file_path(self) -> None:
        args = DL.argparse.Namespace(
            fluxon_request_username="bench",
            fluxon_request_password_file="/run/secrets/fluxonfs-password",
            other="value",
        )

        self.assertEqual(
            DL.sanitized_args(args),
            {
                "fluxon_request_username": "bench",
                "fluxon_request_password_file": "<redacted>",
                "other": "value",
            },
        )

    def test_load_secret_file_requires_private_single_line_regular_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            secret_path = Path(tmp_s) / "password"
            secret_path.write_text("secret\n", encoding="utf-8")
            secret_path.chmod(0o600)

            self.assertEqual(
                DL.load_secret_file(str(secret_path), "test password file"),
                "secret",
            )

            secret_path.chmod(0o640)
            with self.assertRaisesRegex(ValueError, "group or world"):
                DL.load_secret_file(str(secret_path), "test password file")

            secret_path.chmod(0o600)
            secret_path.write_text("first\nsecond\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exactly one"):
                DL.load_secret_file(str(secret_path), "test password file")

            symlink_path = Path(tmp_s) / "password-link"
            symlink_path.symlink_to(secret_path)
            with self.assertRaisesRegex(ValueError, "failed to open"):
                DL.load_secret_file(str(symlink_path), "test password file")

            missing_path = Path(tmp_s) / "private-password-location"
            with self.assertRaises(ValueError) as raised:
                DL.load_secret_file(str(missing_path), "test password file")
            self.assertNotIn(str(missing_path), str(raised.exception))

    def test_validate_args_pairs_username_with_password_file(self) -> None:
        args = DL.build_arg_parser().parse_args(
            [
                "--backend",
                "fluxon",
                "--fluxon-kv-config",
                "/tmp/client.yaml",
                "--fluxon-remote-root",
                "/tmp/video-root",
                "--fluxon-request-username",
                "bench",
            ]
        )

        with self.assertRaisesRegex(ValueError, "password-file"):
            DL.validate_args(args)

    def test_run_jobs_uses_decode_batch_windows(self) -> None:
        class BatchBackend:
            def __init__(self) -> None:
                self.calls = []

            def decode_batch(self, cases):
                self.calls.append([case.idx for case in cases])
                return [
                    {
                        "status": "ok",
                        "open_s": 0.0,
                        "decode_s": 0.1,
                        "materialize_s": 0.0,
                        "elapsed_s": 0.1,
                        "shape": "1",
                        "dtype": "uint8",
                        "nbytes": 1,
                    }
                    for _case in cases
                ]

        cases = [
            DL.DecodeCase(
                source="test",
                idx=i,
                video_path=f"/data/{i}.mp4",
                height=4,
                width=4,
                start_idx=0,
                end_idx=3,
                num_frames=2,
            )
            for i in range(1, 4)
        ]
        jobs = DL.build_jobs(
            cases,
            backends=["fluxon"],
            rounds=1,
            alternate_backend_order=True,
        )
        backend = BatchBackend()

        rows, _wall_s = DL.run_jobs(
            jobs,
            backends={"fluxon": backend},
            workers=1,
            prefetch_factor=2,
            decode_batch_size=2,
            label="test",
        )

        self.assertEqual(backend.calls, [[1, 2], [3]])
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["status"] == "ok" for row in rows))


def main() -> None:
    unittest.main()


if __name__ == "__main__":
    main()
