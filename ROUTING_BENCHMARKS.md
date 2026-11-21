# Native Rust event routing experiment

Baseline `bfc2c1c0c56676c3fbf7985e34a0d78dd04b2a03`. Both release images used the same Linux volumes, logical CPUs `0-7`, CPU quota 8, a 24 GiB memory limit and no swap. The root filesystem and scratch were read-only. Docker Desktop had 23.05 GiB of VM memory. The timed runs used a warm filesystem cache. Docker start and finish timestamps came from `docker inspect`, with profiling off. CLI wall time includes a container-state probe, creation and attachment. Profiled runs were separate. Samples that overlapped another Shopping Sessions route were discarded. Other projects on the host could compete for CPU time and memory bandwidth.

## Release timings

| Input | Build | Container seconds, all runs | Container median | CLI wall median |
| --- | --- | --- | ---: | ---: |
| 20M, seed 53 | Baseline | 6.192, 6.219, 6.702 | 6.219 | 6.611 |
| 20M, seed 53 | Double routing and filter | 7.536, 6.146, 4.630 | 6.146 | 6.624 |
| 60M, seed 37 | Baseline | 14.390, 24.089, 24.669 | 24.089 | 24.605 |
| 60M, seed 37 | Double routing and filter | 14.662, 17.556, 17.632 | 17.556 | 17.980 |

The 20M container medians differed by 0.073 seconds. On the 60M input, the candidate won two of three adjacent pairs, while the baseline ranged from 14.390 to 24.669 seconds. Unrelated jobs were running on the host during testing. These results support keeping the implementation as a candidate for an isolated rerun before online submission.

## Profiled runs

Each cell gives stage seconds and peak process RSS in GiB. Process-wide RSS comes from the kernel. Triplets in the unprofiled table provide the container timing comparison.

| Stage | 20M baseline | 20M candidate | 60M baseline | 60M candidate |
| --- | ---: | ---: | ---: | ---: |
| Dimensions and links | 0.618 / 0.157 | 0.697 / 0.163 | 2.661 / 0.347 | 2.412 / 0.344 |
| Parse and route events | 2.219 / 0.947 | 2.175 / 1.145 | 9.634 / 2.512 | 9.945 / 3.227 |
| Deduplication total | 0.729 / 1.077 | 0.118 / 1.147 | 2.460 / 2.657 | 0.410 / 3.171 |
| Filter possible duplicate IDs |  | 0.035 / 1.147 |  | 0.141 / 3.171 |
| Exact sort and winner selection |  | 0.022 / 1.133 |  | 0.082 / 3.139 |
| Apply eliminations |  | 0.028 / 0.637 |  | 0.076 / 1.616 |
| Merge person blocks |  | 0.033 / 0.845 |  | 0.110 / 2.163 |
| Sort events | 0.098 / 0.556 | 0.080 / 0.782 | 0.419 / 1.496 | 0.260 / 2.025 |
| Sessionize | 0.067 / 0.593 | 0.071 / 0.798 | 0.218 / 1.523 | 0.198 / 2.057 |
| Parse and sort clicks | 0.535 / 0.640 | 0.476 / 0.831 | 2.969 / 1.517 | 2.235 / 1.884 |
| Attribution and assists | 0.141 / 0.540 | 0.130 / 0.666 | 0.408 / 1.382 | 0.352 / 1.390 |
| Write Parquet | 0.412 / 0.333 | 0.356 / 0.411 | 1.511 / 0.665 | 1.390 / 0.696 |
| Entire program | 4.818 / 1.081 | 4.104 / 1.149 | 20.279 / 2.659 | 17.204 / 3.239 |
| Container memory peak, GiB | 1.225 | 1.324 | 2.860 | 3.501 |

The candidate retains both compact event rows and the deduplication index during parsing. On 60M, the sampled combined parsing and deduplication time fell from 12.094 to 10.355 seconds, while the kernel process peak rose from 2.659 to 3.239 GiB.

## Duplicate counts and verification

| Input | Candidate rows | Exact comparison rows | Largest ID shard | Eliminated valid rows | Surviving events | Surviving purchases |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20M | 21280682 | 3321764 | 167555 | 1266903 | 19659050 | 561015 |
| 60M | 64454404 | 12821901 | 505725 | 4381372 | 58691654 | 1677315 |

On the 20M input, the same layout with the filter disabled sorted 21,280,682 index entries in 0.130 seconds. This run confirms the amount of sorting avoided independently of the data layout.

- `events-20m`, `base` versus `sort`. user_daily [2230266, 2230266] symmetric_diff 0 campaign_daily [28014, 28014] symmetric_diff 0
- `events-20m`, `sort` versus `filter`. user_daily [2230266, 2230266] symmetric_diff 0 campaign_daily [28014, 28014] symmetric_diff 0
- `events-20m`, `base` versus `filter`. user_daily [2230266, 2230266] symmetric_diff 0 campaign_daily [28014, 28014] symmetric_diff 0
- `events-20m-seed59`, `base` versus `filter`. user_daily [2235935, 2235935] symmetric_diff 0 campaign_daily [28014, 28014] symmetric_diff 0
- `events-60m`, `base` versus `filter`. user_daily [6662636, 6662636] symmetric_diff 0 campaign_daily [28014, 28014] symmetric_diff 0
- The release matched both official 1M outputs and all 36 official fixtures. Eight Rust tests passed. Targeted duplicate and purchase cases run at 1, 2 and 8 threads. Empty input at all three thread counts produced both Parquet schemas.

## Reproduce

Build `shopping-sessions:rust-routing-base-bfc2c1c` from the fixed baseline revision and `shopping-sessions:rust-routing-v2` from this branch. Both use the repository's root Dockerfile. The seed-53 and seed-37 datasets used `events-20m` and `events-60m` subpaths in the `shopping-sessions-bench` Linux volume. Give each image its own writable output subdirectory owned by UID 1000. Keep the input read-only.

```powershell
docker buildx build --platform linux/amd64 --load -t shopping-sessions:rust-routing-v2 .
$id = docker create --platform linux/amd64 --network none --read-only `
    --tmpfs /tmp:rw,size=64m,mode=1777 --tmpfs /scratch:ro,size=64m `
    --cpuset-cpus 0-7 --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 `
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 -e NCPU=8 -e MEM_GB=24 `
    --mount type=volume,source=shopping-sessions-bench,target=/data,volume-subpath=events-20m,readonly `
    --mount type=volume,source=shopping-sessions-bench-out,target=/output,volume-subpath=routing-events-20m-filter `
    shopping-sessions:rust-routing-v2
docker start -a $id
docker inspect $id --format '{{.State.StartedAt}} {{.State.FinishedAt}} {{.State.ExitCode}}'
docker rm $id
```

Add `-e PROFILE=1` for stage timings and memory. Add `-e DEDUP_FULL_SORT=1` on the candidate to run exact sorting on every index entry as a comparison for the filter. Take both Parquet tables from the candidate and baseline and compare each with bidirectional `EXCEPT ALL`, including `DATE` and nulls. Check that both sides have unique logical keys. Report wall time separately from the difference between Docker's start and finish timestamps.
