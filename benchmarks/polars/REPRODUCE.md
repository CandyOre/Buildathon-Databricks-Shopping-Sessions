# 本地复现 Rust + Polars 评测

在 `codex/polars-pipeline` 分支根目录打开 **PowerShell 7.4 或更新版本**，按下列顺序执行。宿主需要 Python 3.11+、Git、tar、curl 和启用 Linux 容器的 Docker Buildx。运行机应能分配逻辑 CPU 0–7，并支持 24 GiB 的容器内存限制和 cgroup v2 峰值统计。以下命令只在本地构建、运行和比较。

## 准备环境

```powershell
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$runId = Get-Date -Format 'yyyyMMdd-HHmmss'
$work = (New-Item -ItemType Directory -Force ".buildathon/repro-$runId").FullName
$inputVol = "shopping-sessions-polars-input-$runId"
$outputVol = "shopping-sessions-polars-output-$runId"

python -c "import sys; assert sys.version_info >= (3, 11), sys.version"
python -m venv "$work/venv"
$py = "$work/venv/Scripts/python.exe"
& $py -m pip install -r requirements.txt
& $py -c "import duckdb, pyarrow; print(duckdb.__version__, pyarrow.__version__)"
docker info --format '{{.OSType}} {{.Architecture}} CPUs={{.NCPU}} RAM_bytes={{.MemTotal}}'
docker buildx version
```

小规模检查在宿主调用锁定版本的 DuckDB 和 PyArrow。大规模检查在 Linux 卷所在的 Docker 环境读取 Parquet。以下四个镜像标签与检查、计时脚本一致。

## 构建三个参赛镜像和校验镜像

导出指定提交的 Git 树到单独目录，直接从该目录构建原生基线。`git archive --output` 将 tar 写入文件，保留 CSV 和源文件的原始字节。

```powershell
$base = 'bfc2c1c0c56676c3fbf7985e34a0d78dd04b2a03'
$nativeDir = (New-Item -ItemType Directory "$work/native").FullName
git archive --format=tar "--output=$work/native.tar" $base
tar -xf "$work/native.tar" -C $nativeDir
docker buildx build --platform linux/amd64 --load -t shopping-sessions:polars-base-bfc2c1c $nativeDir
docker buildx build --platform linux/amd64 --target dedup --load -t shopping-sessions:polars-dedup .
docker buildx build --platform linux/amd64 --target full --load -t shopping-sessions:polars-full .
docker buildx build --platform linux/amd64 -f Dockerfile.duckdb --load -t shopping-sessions:duckdb .
foreach ($tag in 'polars-base-bfc2c1c','polars-dedup','polars-full','duckdb') {
    docker image inspect "shopping-sessions:$tag" --format '{{.Id}} {{.Os}}/{{.Architecture}} {{.Size}} {{json .Config.Env}}'
}
```

`dedup` 与 `full` 共用锁定依赖的 release 二进制，镜像分别设置 `PIPELINE=dedup` 和 `PIPELINE=full`。根 Dockerfile 的默认 target 是 `full`。三个跑分镜像均可使用 `DATA_DIR`、`OUTPUT_DIR`、`NCPU` 和 `PROFILE` 环境变量。

## Rust 单元测试

运行以下命令分别测试原生提交与候选分支。候选测试包含跨文件事件副本回归。测试结束后再进入性能测量。

```powershell
$rustImage = 'rust:1.85-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36'
docker run --rm --platform linux/amd64 --cpus 8 --memory 24g --memory-swap 24g --mount "type=bind,source=$nativeDir,target=/baseline,readonly" $rustImage cargo test --locked --release --jobs 8 --manifest-path /baseline/rust/Cargo.toml --target-dir /tmp/native-test
docker buildx build --platform linux/amd64 --target build --load -t shopping-sessions:polars-build-tests .
docker run --rm --platform linux/amd64 --cpus 8 --memory 24g --memory-swap 24g --network none shopping-sessions:polars-build-tests cargo test --locked --release --jobs 8
```

## 生成输入与取得官方答案

下载官方生成器源码和 1M 答案。生成脚本先验证两个 ZIP 的 SHA-256，然后在固定摘要的 Rust 镜像内离线编译生成器、运行三个种子，并逐文件计算摘要。它要求一个新的卷名，可直接使用上面带时间戳的 `$inputVol`。

```powershell
$files = 'https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files'
foreach ($name in 'generator.zip','dev-answer.zip') {
    curl.exe -fLsS "$files/$name" -o "$work/$name"
    Get-FileHash "$work/$name" -Algorithm SHA256 | Format-List Path,Hash
}
& $py scripts/polars_data.py --archives $work --volume $inputVol --report "$work/inputs.json"
Expand-Archive -LiteralPath "$work/dev-answer.zip" -DestinationPath $work
& $py -c "import json,sys; a,b=(json.load(open(p,encoding='utf-8')) for p in sys.argv[1:]); assert a['archives_sha256']==b['archives_sha256'] and a['datasets']==b['datasets'], 'Input fingerprint mismatch';print('All dataset fingerprints match')" "$work/inputs.json" benchmarks/polars/inputs.json
docker volume create $outputVol
```

| 文件 | SHA-256 |
| --- | --- |
| `generator.zip` | `13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e` |
| `dev-answer.zip` | `7eb8e1f0b1f431d178d28ac9437ce85809c44eb6393fdd42fb59857f30834fa9` |

| 数据子目录 | Seed | 生成器事件数参数 | 点击数参数 |
| --- | ---: | ---: | ---: |
| `events-1m` | 1 | 1,000,000 | 250,000 |
| `events-20m` | 53 | 20,000,000 | 5,000,000 |
| `events-60m` | 37 | 60,000,000 | 15,000,000 |

若机器已有比赛数据卷，可在**生成命令**后追加 `--compare-volume shopping-sessions-bench`，生成器将本次新卷的每条“文件名、SHA-256”记录与原卷比较。三个规模的参考文件数和清单摘要见[输入指纹](inputs.json)。

## 官方验收、计时与逐列比较

```powershell
foreach ($variant in 'native','dedup','full') {
    & $py scripts/polars_check.py small $variant --data-volume $inputVol --answer "$work/dev-answer"
}
foreach ($size in '20m','60m') {
    & $py scripts/polars_bench.py $size "$work/$size.jsonl" --quiet --input-volume $inputVol --output-volume $outputVol
    foreach ($candidate in 'dedup','full') {
        & $py scripts/polars_check.py large $size $candidate --out-volume $outputVol --check-image shopping-sessions:duckdb
    }
}
```

`small` 对每个版本运行 36 组官方 fixtures、首副本无效与点击并列的定向用例，以及公开 1M 答案。`large` 在同一规模的输出卷内分别比较两个候选与原生基线。两个步骤均检查两张表的 schema、主键唯一性、各列空值、逐列差异以及双向 `EXCEPT ALL`。检查结果写入 `benchmarks/polars/checks/` 下对应版本的 JSON，重复执行会更新这些文件。

每种规模中，三个版本各预热一次。随后进行三轮常规计时，每轮轮换版本顺序。发布结果另取同轮独占 profiling 样本，20M 每版本三次，60M 每版本一次。`--quiet` 等待 Docker 上其他容器退出，并对有重叠的运行重测。计时前结束当前实验使用的 builder，并安排其他工作负载在本轮之外运行。脚本使用固定 CPU 0–7、8 CPU 配额、24 GiB 的容器上限、零 swap、只读输入卷与相同输出介质。各阶段 RSS 每约 5 ms 采样，整次进程峰值来自 `VmHWM`，容器峰值来自 cgroup 的 `memory.peak`。

每种规模的 JSONL 都使用新文件名。统计时选取 `accepted=true` 且 `run` 为 `round-1`、`round-2`、`round-3` 的记录，按版本分别计算 `container_s` 和 `cli_s` 的中位数。`container_s` 取 Docker 的 `FinishedAt − StartedAt`，`cli_s` 覆盖整条 `docker run`。阶段统计使用三个版本都有独占结果的完整轮次。20M 取前三轮的耗时中位数及最高内存，60M 使用第一轮实测值。若需为 60M 额外采满三轮，计时命令加 `--profile-rounds 3`，汇总命令加 `--profiles-60m 3`。

运行 `& $py scripts/polars_summary.py $work` 可从两份 JSONL 重新生成中位数、峰值与阶段对照表。

## 单独运行一次 profiling

以下示例从 20M 输入运行完整 Polars 版本，另开输出卷。`profile` JSON 写到宿主日志，两张 Parquet 表写入卷内的 `user_daily/` 与 `campaign_daily/`。

```powershell
$profileVol = "shopping-sessions-polars-profile-$runId"
docker volume create $profileVol
docker run --rm --network none --user 0:0 --mount "type=volume,source=$profileVol,target=/output" shopping-sessions:polars-full chown 1000:1000 /output
$flags = @('run','--rm','--platform','linux/amd64','--network','none','--read-only',
           '--tmpfs','/tmp:rw,size=64m,mode=1777','--tmpfs','/scratch:ro,size=64m',
           '--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','4096',
           '--cpuset-cpus','0-7','--cpus','8','--memory','24g','--memory-swap','24g',
           '--user','1000:1000','-e','NCPU=8','-e','MEM_GB=24','-e','PROFILE=1',
           '--mount',"type=volume,source=$inputVol,target=/data,volume-subpath=events-20m,readonly",
           '--mount',"type=volume,source=$profileVol,target=/output")
docker @flags shopping-sessions:polars-full 2> "$work/20m-full-profile.log"
Get-Content "$work/20m-full-profile.log"
```

## 单独验证冷构建

为测依赖获取、release 编译和镜像加载，在独立时段用唯一命名的 `docker-container` builder 启动全新 BuildKit 状态卷。先完成 builder 启动，再单独计时默认候选的 `build --load`。结束时仅停止本次创建的 builder，之后即可运行 `--quiet` 计时。日志保留每个 BuildKit 步骤耗时。

```powershell
$builder = "shopping-polars-cold-$runId"
docker buildx create --name $builder --driver docker-container --driver-opt 'memory=24g,memory-swap=24g,cpuset-cpus=0-7,cpu-period=100000,cpu-quota=800000' --bootstrap
try {
    $clock = [Diagnostics.Stopwatch]::StartNew()
    docker buildx build --builder $builder --no-cache --progress=plain --platform linux/amd64 --target full --load -t shopping-sessions:polars-cold . 2>&1 | Tee-Object "$work/cold-build.log"
    $clock.Stop()
    "Cold build wall seconds: $($clock.Elapsed.TotalSeconds)"
    docker image inspect shopping-sessions:polars-cold --format '{{.Id}} {{.Size}} bytes'
} finally {
    docker buildx stop $builder
}
```
