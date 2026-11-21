# Shopping Sessions

This branch benchmarks two Rust + Polars candidates against native commit `bfc2c1c`. The default Docker target runs the full Polars pipeline. Use the `dedup` target for Polars event deduplication with the native downstream stages. [Measured results and reproduction commands](POLARS_BENCHMARKS.md).

The Rust pipeline cleans the Shopping Sessions CSVs, resolves linked user IDs, builds sessions, and computes campaign attribution and assists. It writes `user_daily/part-0.parquet` and `campaign_daily/part-0.parquet` to the output directory. See [the official rules](rules.md) for the input and output contracts.

## Windows quick start

With Docker Desktop running, build the scorer image and install the Python checker:

```powershell
docker buildx build --platform linux/amd64 --target full --load -t shopping-sessions:polars-full .
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install polars
.\.venv\Scripts\python.exe scripts/run_fixtures.py fixtures -- powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/docker_run.ps1 -Image shopping-sessions:polars-full
```

The fixture runner checks both Parquet tables against the expected CSVs for all 36 official cases. To generate the public development dataset and download its expected results, run the following with Docker Desktop running:

```powershell
$base = 'https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files'
$work = (New-Item -ItemType Directory -Force .buildathon).FullName
foreach ($file in 'generator.zip', 'dev-answer.zip') {
    $archive = Join-Path $work $file
    curl.exe -fLsS "$base/$file" -o $archive
    if ($LASTEXITCODE -ne 0) { throw "Download failed for $file" }
    Expand-Archive -LiteralPath $archive -DestinationPath $work -Force
}
docker run --rm --platform linux/amd64 --mount "type=bind,source=$work,target=/work" --workdir /work/generator rust:1.85-slim-bookworm sh -ec 'cargo run --locked --release -- --seed 1 --events 1000000 --clicks 250000 --output /work/dev'
```

Run the development dataset and compare both tables:

```powershell
$env:PROFILE = '1'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/docker_run.ps1 -Image shopping-sessions:polars-full -DataDir .buildathon/dev -OutputDir out
.\.venv\Scripts\python.exe scripts/compare.py out .buildathon/dev-answer
```

The PowerShell runner defaults to 4 CPUs and 8 GB RAM. Use `-Cpus 8 -MemoryGb 24` to match the scorer's limits, or `-Cpus 8 -MemoryGb 4` to try a lower memory cap. To build and run directly with a local Rust toolchain:

```powershell
$env:DATA_DIR = '.buildathon/dev'
$env:OUTPUT_DIR = 'out'
cargo run --locked --release --manifest-path rust/Cargo.toml
```

## Linux and macOS

With Docker Buildx and `polars` installed for the local checker, use the starter's Makefile:

```sh
make fixtures                         # Build linux/amd64, run and compare every fixture
make dev                              # Requires the official Rust generator, generate, on PATH
make run-dev CPUS=4 MEM=8g             # Run against dev/
make check-dev ANSWER=dev-answer      # Directory extracted from the official dev-answer.zip
```

On Apple silicon, use `PLATFORM=linux/arm64` for native local iteration and verify `linux/amd64` for scoring. The Rust executable uses `DATA_DIR`, `OUTPUT_DIR`, and `NCPU`. It processes in memory and writes only the two output Parquet tables. Generated datasets, runtimes, and outputs are ignored by Git.

## Profiling and comparing

Set `PROFILE=1` to emit one JSON record per stage and a total record to stderr. On Linux, the stage records contain first, last, and peak process RSS sampled about every 5 ms. The total record reads the kernel's process high-water mark and cgroup peak, if available. The cgroup peak also includes charged file cache. A Docker memory cap constrains the container, including that cache.

See [the Polars comparison](POLARS_BENCHMARKS.md) for the 20M and 60M experiments, and [the native pipeline history](BENCHMARKS.md) for earlier measurements.

The DuckDB comparison implementation is still available in `pipeline/`. Build it with `docker build -f Dockerfile.duckdb -t shopping-sessions:duckdb .` and run the PowerShell wrapper with `-Image shopping-sessions:duckdb` and a separate output directory. Pass both output directories to `scripts/compare.py` for an exact comparison.

## Credits and license

The fixtures and test harness come from the [official Shopping Sessions starter](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions). The Rust pipeline uses Rayon for parallel processing, memchr for CSV scanning, ahash for hashing, mimalloc for allocation, and the Apache Arrow Rust crates (`arrow-array`, `arrow-schema`, and `parquet`) for output. It also uses chrono, serde_json, and anyhow. The comparison pipeline uses DuckDB and PyArrow. The result checker uses Polars.

Original contributions to this entry are released under the [MIT License](LICENSE).
