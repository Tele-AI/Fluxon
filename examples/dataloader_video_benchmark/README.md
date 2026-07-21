# Dataloader Video Benchmark

`dataloader_video_benchmark` compares the original `decord.VideoReader`
path with the FluxonFS video reader path at the dataloader boundary.

It is an example benchmark, not a training dataset implementation. It replays
decode cases from CSV, timeout logs, or parquet metadata and reports decode
latency, QPS, output bandwidth, FluxonFS range-read counters, and reader-pool
stats.

## Entrypoints

- `benchmark.py`
  - runs original and Fluxon video decode backends
- `analyze_results.py`
  - summarizes benchmark CSV output
- `test_benchmark_contract.py`
  - local contract test for the example script

## Decode Case CSV

The simplest input is a CSV with these columns:

```csv
idx,video_path,height,width,start_idx,end_idx,num_frames
1,/abs/path/sample.mp4,480,832,0,63,16
```

`video_path` must be an absolute local path for the original backend. For the
Fluxon backend, the same path must be under `--fluxon-remote-root`; the example
converts it to `export_name + relpath` before calling FluxonFS.

## Original Baseline

```bash
python3 examples/dataloader_video_benchmark/benchmark.py \
  --backend original \
  --case-csv /tmp/cases.csv \
  --rounds 100 \
  --warmup-rounds 5 \
  --workers 4 \
  --prefetch-factor 2 \
  --num-threads 2 \
  --output-csv /tmp/original.csv \
  --output-json /tmp/original.summary.json
```

The original backend runs:

```text
decord.VideoReader(video_path).get_batch(indices).asnumpy()
```

## Fluxon Backend

Start a FluxonFS master and agent first, then run:

```bash
python3 examples/dataloader_video_benchmark/benchmark.py \
  --backend fluxon \
  --case-csv /tmp/cases.csv \
  --rounds 100 \
  --warmup-rounds 5 \
  --workers 4 \
  --prefetch-factor 2 \
  --decode-batch-size 3 \
  --num-threads 2 \
  --fluxon-kv-config /tmp/external.yaml \
  --fluxon-remote-root /tmp/video_root \
  --fluxon-agent-instance-key fs-agent-1 \
  --fluxon-reader-cache-size 32 \
  --fluxon-request-username bench \
  --fluxon-request-password-file /run/secrets/fluxonfs-password \
  --output-csv /tmp/fluxon.csv \
  --output-json /tmp/fluxon.summary.json
```

The password file must be a regular file containing one non-empty line and
must not grant group or world permissions (for example, mode `0600`). The
benchmark deliberately has no inline password argument, so the credential is
not exposed through the process command line.

`--decode-batch-size` controls the benchmark's sample-batch window. The Fluxon
backend passes that window to `FluxonFsVideoReaderPool.read_many_numpy_with_stats`.
Requests with the same video reader key are merged into one native decode call
and split back into per-sample rows.

For the current synthetic benchmark shape, `--decode-batch-size 3` is the
recommended first value: it coalesces the three clips per video while keeping
enough scheduling granularity for multi-worker runs.

## Analyze Results

```bash
python3 examples/dataloader_video_benchmark/analyze_results.py \
  --input-csv /tmp/fluxon.csv \
  --output-json /tmp/fluxon.analysis.json \
  --output-md /tmp/fluxon.analysis.md \
  --title fluxon_batch3_w4
```

The analyzer reports:

- backend QPS, frames/s, and output MiB/s
- p50/p95/p99 decode latency
- pairwise original-vs-Fluxon speedup when both backends are present
- FluxonFS remote read bytes, range-read calls, page cache hit rate, and reader
  pool hit rate

## Local Contract Test

```bash
python3 examples/dataloader_video_benchmark/test_benchmark_contract.py
```
