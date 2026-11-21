# Rust pipeline benchmark

Single profiled runs in Docker Desktop on an AMD Ryzen 7 9800X3D, using a `linux/amd64` release build, 8 vCPUs, a 4 GiB container limit, and no swap. The input and output were on Docker Linux volumes. The root filesystem and `/scratch` were read-only. The program read its input files in bounded chunks, kept intermediates in memory, and wrote the two Parquet outputs.

| Dataset | Generator seed | Events | Clicks | Elapsed | Process peak | Container peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Public dev | 1 | 1,000,000 | 250,000 | 0.187 s | 0.240 GiB | 0.291 GiB |
| Medium | 31 | 10,000,000 | 2,500,000 | 1.789 s | 0.569 GiB | 0.629 GiB |
| Full scale | 37 | 60,000,000 | 15,000,000 | 12.261 s | 2.748 GiB | 2.982 GiB |

Elapsed time is measured inside the program, from startup through Parquet output. It excludes the Docker CLI and container launch. These runs use generated data on a local machine. The production scorer uses separate datasets and hardware.

## Stage timings and memory

Each cell shows elapsed seconds and peak process RSS in GiB. The final column shows how much RSS remained when each 60M stage completed. Stage RSS is sampled about every 5 ms, while the process peak in the summary is the Linux kernel's high-water mark. The container peak also includes memory such as charged filesystem cache.

| Stage | 1M time / peak | 10M time / peak | 60M time / peak | 60M ending RSS |
| --- | ---: | ---: | ---: | ---: |
| Dimensions and linked IDs | 0.036 s / 0.031 | 0.166 s / 0.099 | 1.514 s / 0.408 | 0.313 GiB |
| Parse events | 0.078 s / 0.164 | 0.942 s / 0.453 | 6.167 s / 2.549 | 2.523 GiB |
| Deduplicate events | 0.022 s / 0.224 | 0.217 s / 0.555 | 1.520 s / 2.734 | 1.517 GiB |
| Sort events | 0.004 s / 0.208 | 0.035 s / 0.331 | 0.212 s / 1.517 | 1.517 GiB |
| Build sessions and user totals | 0.003 s / 0.215 | 0.020 s / 0.358 | 0.112 s / 1.561 | 1.238 GiB |
| Parse and sort clicks | 0.018 s / 0.223 | 0.188 s / 0.433 | 1.529 s / 1.556 | 1.331 GiB |
| Campaign attribution and assists | 0.010 s / 0.236 | 0.054 s / 0.420 | 0.204 s / 1.331 | 0.614 GiB |
| Write Parquet | 0.015 s / 0.227 | 0.168 s / 0.283 | 1.003 s / 0.630 | 0.325 GiB |

Event parsing used 50% of full-scale runtime. Memory peaked during deduplication while candidate records and the compact per-person event batches briefly coexisted. The first Rust build stopped during deduplication at a 4 GiB cap. At a 24 GiB cap it completed with 4.89 GiB process RSS. The current build uses mimalloc with immediate purging and releases the event batches before loading clicks.

## Correctness

- All 36 official fixtures passed in the release image.
- Both tables matched the official public answer exactly, with 117,038 `user_daily` rows and 27,400 `campaign_daily` rows.
- For the 10M event run, the independent DuckDB pipeline matched both Rust tables exactly, with 1,141,663 user rows and 28,014 campaign rows.
- The full-scale output matched the first Rust build across all 6,662,636 user rows and 28,014 campaign rows. Both output tables had unique keys and agreed on the totals of 10,086,639 sessions and 19,293,637,426 cents. The campaign table also passed the official assist-count checks.

## Reproduce

Build the release image as shown in the [README](README.md). The [official generator](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip) takes the seed, event count and click count shown above. Place the chosen dataset in a directory mounted at `/data` and set `PROFILE=1` for stage timing and RSS. The Windows runner uses the same image and emits the same JSON records to stderr.

```powershell
$env:PROFILE = '1'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/docker_run.ps1 -DataDir .buildathon/dev -OutputDir out -Cpus 8 -MemoryGb 4
```

For the measurements above, the input volume `shopping-sessions-bench` holds `events-1m`, `events-10m` and `events-60m`. The separate `shopping-sessions-bench-out` volume contains writable `rust-opt-1m`, `rust-opt-10m` and `rust-opt-60m` directories. Use `volume-subpath` to mount just the selected dataset and its output directory. This is the full-scale command:

```powershell
docker run --rm --platform linux/amd64 --network none --read-only `
    --tmpfs /tmp:rw,size=64m,mode=1777 --tmpfs /scratch:ro,size=64m `
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 `
    --cpus 8 --memory 4g --memory-swap 4g --user 1000:1000 -e NCPU=8 -e PROFILE=1 `
    --mount type=volume,source=shopping-sessions-bench,target=/data,volume-subpath=events-60m,readonly `
    --mount type=volume,source=shopping-sessions-bench-out,target=/output,volume-subpath=rust-opt-60m `
    shopping-sessions:rust
```
