# Shopping Sessions

This DuckDB pipeline cleans the Shopping Sessions CSVs, resolves linked user IDs, builds sessions, and computes campaign attribution and assists. It writes `user_daily/part-0.parquet` and `campaign_daily/part-0.parquet` to the output directory. See [the official rules](rules.md) for the input and output contracts.

## Windows quick start

From the repository root, install Python 3.12 or newer and set up the local environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt polars
$env:SCRATCH_DIR = Join-Path (Get-Location) '.buildathon/scratch'
.\.venv\Scripts\python.exe scripts/run_fixtures.py fixtures -- .\.venv\Scripts\python.exe pipeline/pipeline.py
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

Build the Linux image, run the development dataset and compare both tables:

```powershell
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/docker_run.ps1 -DataDir .buildathon/dev -OutputDir out
.\.venv\Scripts\python.exe scripts/compare.py out .buildathon/dev-answer
```

The PowerShell runner defaults to 4 CPUs and 8 GB RAM for local runs. Use `-Cpus 8 -MemoryGb 24` to match the scorer's CPU and memory limits. To run the same data directly on Windows:

```powershell
$env:DATA_DIR = '.buildathon/dev'
$env:OUTPUT_DIR = 'out'
$env:SCRATCH_DIR = '.buildathon/scratch'
.\.venv\Scripts\python.exe pipeline/pipeline.py
```

## Linux and macOS

With Docker Buildx and `polars` installed for the local checker, use the starter's Makefile:

```sh
make fixtures                         # Build linux/amd64, run and compare every fixture
make dev                              # Requires the official Rust generator, generate, on PATH
make run-dev CPUS=4 MEM=8g             # Run against dev/
make check-dev ANSWER=dev-answer      # Directory extracted from the official dev-answer.zip
```

On Apple silicon, use `PLATFORM=linux/arm64` for native local iteration and verify `linux/amd64` for scoring. The Docker entry point also accepts the scorer's `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, and `MEM_GB` environment variables. The run needs no network. Generated datasets, runtimes, and outputs are ignored by Git.
