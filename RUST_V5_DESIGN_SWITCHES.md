# Rust v5 实验开关与复现

参赛入口是镜像默认命令。下表给出程序在环境变量未设置时的选择。布尔开关用 `1` 开启、`0` 关闭。编译参数、镜像环境、输入种子和采样方式都属于实验条件。固定这些条件后，用容器 `StartedAt` 至 `FinishedAt` 的时间比较版本。

## 从官方数据开始

以下命令在仓库根目录的 PowerShell 运行。宿主 Python 需有 DuckDB、Polars，Docker 需能运行 Linux/amd64 容器。`$study` 指向官方资产和实验卷记录目录。已有该目录时，直接读取 `volume` 并复用已生成的数据。

```powershell
$py = ".\.venv\Scripts\python.exe"
$study = ".buildathon/design-inputs"
$work = ".buildathon/design-repro"
$rev = (git rev-parse HEAD).Trim()
$a = "shopping-design:baseline"
$b = "shopping-design:candidate"
& $py scripts/v5_event_key_study.py $study init .buildathon/generator-v5-official-gmun.zip .buildathon/dev-answer-v5-official-gmun.zip .buildathon/starter-v5-official-gmun.zip
& $py scripts/v5_event_key_study.py $study generate dev s83-20m s37-20m s83-60m
$vol = (Get-Content "$study/volume" -Raw).Trim()
& $py scripts/design_bench.py $work build $a bf43e188783adbff8853002efacb8ae5fbfedc04
& $py scripts/design_bench.py $work build $b $rev
& $py scripts/v5_event_key_study.py $study provenance --images $a $b
& $py scripts/design_bench.py $work check --revision $rev
& $py scripts/verify_performance_design.py --image $b --assets $study --work "$work/verification"
```

`init` 校验三个官方压缩包的 SHA-256。四份输入的事件数和点击数分别为 1M/250k、20M/5M、20M/5M、60M/15M，种子依次为 1、83、37、83。`build` 从指定 Git 提交打包源码，留下提交、镜像 ID、源码包哈希。`check` 跑 portable 和 x86-64-v4 两套 release 单元测试，后者需要支持 v4 的机器。验收入口默认跑 36 个官方用例、v5 官方 1M 答案、4 个专项用例，支持重复传入 `--setting KEY=VALUE`；`--suite custom` 可只运行专项用例。

### 整程 A/B 和大表

```powershell
$ref = "$study/tables"
foreach ($ds in "s83-20m","s37-20m","s83-60m") {
    & $py scripts/bench_container.py $vol $ds $a --log "$ref/export.jsonl" --verify-cache --export "$ref/$ds/b"
}
& $py scripts/design_bench.py $work race $vol final $a $b --reference $ref
```

`final` 是新结果目录名。每份数据按 `ABBAAB` 交错。默认固定 CPU `0,2,4,6,8,10,12,14`，8 个逻辑核、24 GiB 内存及等额内存加交换上限。读写使用同一 Linux 卷，每次运行前确认所有输入页驻留。导出的候选结果与该数据的基线结果逐表、逐列做双向 `EXCEPT ALL`，同时检查字段类型、重复主键及非法空值。已有两份输出时也可独立运行 `python scripts/check_event_key_tables.py LEFT RIGHT`。

## 输入、解码与身份

| 开关 | 默认、适用条件与回退 |
| --- | --- |
| `PERF_IO=read/mmap`，`PERF_PAGE_RELEASE=1` | 默认分块读取，线程复用 128 KiB 读缓冲。Linux 的 `mmap` 为只读私有映射，映射失败回到读取路径。页释放只对映射中本任务完整覆盖的页调用 `MADV_DONTNEED`。两种路径都以行起始字节决定归属。 |
| `PERF_CHUNK=adaptive` 或字节数 | 默认 4 MiB。指定字节数限制在 4 KiB 至 4 MiB。自适应按数据量和线程数选择 64 KiB 至 2 MiB。事件和点击为大量细块合并桶元数据，文件头、跨界长行、末行归属仍按原始块处理。 |
| `PERF_SCANNER=span`，`PERF_DECODER=batch`，`PERF_BATCH=N` | 默认逐行切字段。`span` 在 AVX2 上跨行扫描事件和点击的逗号、引号、换行，超过 128 字节的行走通用切分。`batch` 批量处理完整行，批长 1–64，默认 16。常规 F1/F2 日期时钟可四路解码，其余形态逐项解析。 |
| `PERF_FAST_TIMES=1`，`PERF_HEX=simd` | 前者在满足 AVX2、SSSE3、SSE4.1 时成对转换事件时间与摄入时间，覆盖 ISO 与 13 位 epoch，特殊日期和数值回到精确解析。与 `batch` 合用时，事件批内优先成对转换。后者只在 AVX2 上切换 32 位小写十六进制后缀的校验及打包，标量查表得到同样的 128 位。 |
| `PERF_PARALLEL_SETUP=1`，`PERF_DIM_PARALLEL=1`，`PERF_DIM_NARROW=1` | 分别并行读 manifest/维表、并行建商品/活动索引、收窄商品索引。稠密商品槽的有效类别 ID 全部小于 255 时使用 `u8`，255 表示缺失；否则保留 `u32`。异常拼写保留精确索引。 |
| `PERF_USER_HEX=1`，`PERF_WIDE_CACHE=1` | 规范 `u[0-9a-f]{32}` 在链接及动态人员表用完整 128 位，保存原始拼写供代表比较和输出。后一个开关依赖前一个，缓存至多探测四槽，命中须比较完整键。 |
| `PERF_LINK_PREFETCH=1`，`PERF_LINK_COMPACT=1` | 短链接每 32 对预取并查槽，宽键和例外边直接处理。紧凑表按边数约 1.25 倍取整，首次最多探测 64 槽，容量不足时重建并重放全部边。代表始终取连通分量内字节序最小的拼写。 |
| `PERF_CLICK_KEYS=intern`，`PERF_CLICK_RECORDS=16` | 默认规范 `k[0-9a-f]{32}` 写块内 16 B 侧表，比较时恢复完整值；`intern` 改走共享字典。标准路径初存点击通常 24 B。`16` 允许紧凑人员用 8 B 点击句柄、4 B 相对毫秒、4 B 人员/活动编码。活动占 12 位，人员商占 20 位，越界进 24 B 路径。 |
| `PERF_OVERLAP_STREAMS=1` | 标准路径默认先处理事件，再读点击。此开关在链接表就绪后重叠事件处理与点击处理。原始用户路由有独立的读序开关。 |

标准事件路径保留 128 个分片。普通候选索引为 12 B，普通人员记录为 6 B，购买、异常摄入时间和宽键另计。规范 `e[0-9a-f]{32}` 保留完整高低两半。哈希只筛选候选，选胜比较完整键。语义无效但结构完整的最早副本仍能淘汰后来的有效副本。

## 原始用户路由

`PERF_ROUTE=1` 启用以下路径，默认使用标准路径。`auto` 最多读取四个事件文件各 64 KiB，当至少有 64 个完整样本行、其中至少 95% 的事件和用户键符合采样形态、窗口跨度不超过 `i32::MAX` 毫秒时才选路由。复现实验可用 `1` 或 `0` 固定选择。

下表除特别标明外，开关统一以 `PERF_ROUTE_` 开头。P1 按原始用户拼写分区，P2 按最终人员归并。

| 后缀 | 默认、编码和可比条件 |
| --- | --- |
| `EVENT_BUCKETS`、`USER_BUCKETS`、`CHUNKS` | 桶数默认按事件字节量选择，事件 8–2048 桶，人员 8–512 桶。手动桶数取不超过 8192 的二次幂。`CHUNKS` 建议每任务读 1–128 个块，默认 1，实际还受并行度、元数据预算和页写入模式影响。 |
| `PACK=1` | 通常事件候选 24 B，原始点击 32 B，符合编码范围可分别缩至 16 B。短后缀用保留前导零的 36 进制编码，宽键用全 128 位侧表加槽号。事件的 21 位时间码容纳常规整秒、无效标志或侧表引用。紧凑点击可容纳 31 位相对毫秒、15 位活动 ID。键、时间或侧表位置超出范围时走宽记录；侧表内存另计。 |
| `WRITES=direct/buffered/stream`、`SLAB=1`、`INPUT_PAGES=1` | 默认 `buffered`。测普通 Vec 写法时固定 `PACK=0, SLAB=0, INPUT_PAGES=0`。slab 前缀与单独的 16 B Vec 通道直接写，满前缀的普通记录再交给所选 writer。`INPUT_PAGES=1` 优先于 slab，事件和点击均由读取线程直接写入所属桶的 4 KiB 页，覆盖完整与紧凑通道。`direct` 逐条写，`buffered` 每桶暂存 8 条再连续复制，收尾写余数。`stream` 使用同一暂存策略，在 x86-64 上对目标地址和字节数均按 16 B 对齐的片段使用非临时写，其他片段普通复制，发布页前完成写屏障。 |
| `ARENA=1`、`REUSE=0/1` | 默认 `ARENA=0, REUSE=1`。arena 用 2 MiB 对齐区提供 4 KiB 页。`ARENA=1` 将 Vec 数据转入页供后续扫描，`INPUT_PAGES=1` 已直接产出页，会跳过这次搬运。`REUSE` 控制空页共享回收，同时影响去重临时空间复用。 |
| `WINNER=bitmap/map`、`BITMAP_FACTOR`、`EXACT=hash/linear` | 默认位图先筛，目标每行 8 槽，因子须为不超过 8192 的二次幂。`map` 让所有行进入精确表，此时位图因子不参与筛选。精确表默认哈希表，`linear` 为可复用的开放寻址表，碰撞仍比较完整键。 |
| `LINKS=overlap/serial`、`CLICKS=late/early` | 默认链接与输入处理重叠，事件选胜之后读点击。`early` 可与事件同时读取点击；合流前等待链接图就绪。 |
| `P1=0/1`、`GROUPS=map/flat`、`FORWARD=rows/blocks` | 默认 `P1=1, GROUPS=map, FORWARD=rows`。`P1=0` 把所有组转给 P2。开启后，确认无链接别名的组可在 P1 完成。`PERF_ROUTE_FORWARD=blocks` 在每个 P1 来源内按最终人员分组，每人向目标分区发一份 16 B 描述符，保存人员、事件/点击的末端位置及购买数。转发事件占 8 B/条，点击分列占 20 B/条，逐行模式对应 12 B/24 B。P2 读取描述符计算写入范围，同一人来自不同 P1 的块会合并。`flat` 允许 P2 用人员商直接索引，`map` 用哈希索引。 |
| `FUSED=1` | 完整人员组在 P1 或 P2 内完成归因，工人局部计数最后合并。默认先形成会话和点击数组，再批量归因。 |

## 会话、归因、写出

| 开关 | 默认与回退 |
| --- | --- |
| `PERF_TIME_SORT`、`PERF_MEDIUM_BINS`、`PERF_BIN_MASK` | `TIME_SORT=1` 启用混合排序，至多 16 个时间用插入排序，至少 2048 个可走基数排序。大输入编译版原有半小时分桶路径要求至少 2048 个事件且覆盖不超过 1024 桶。`MEDIUM_BINS=1` 为 64–2047 个事件试探活跃桶；太分散时回到排序。`BIN_MASK=1` 用位图扫描大组非空桶。 |
| `PERF_PURCHASE_BINS`、`PERF_SINGLE_PURCHASE`、`PERF_CATEGORY_DENSE` | 前两项均需设为 1，分别复用分桶得到的会话编号、直接定位单笔购买。`CATEGORY_DENSE=1` 为前 32 类用窄计数，加满后累计进位，其余类别用映射，按金额及类别字节序决胜。 |
| `PERF_ASSISTS=interval/mask/unsorted/simd`、`PERF_SMALL_CLICKS=N` | 默认 `interval`，小点击阈值 32，可设 1–64。`mask` 对最多 64 个会话按活动合并位掩码，`simd` 可在 AVX2 上比较至少 4 个会话时刻。`unsorted/simd` 在适用的分组路径上允许小组跳过预排序，包括路由的组内整理。最终会话数超过 64 时恢复时间顺序。 |
| `PERF_LARGE_ASSISTS=simd` | 对超过 64 个会话、跨度至多 32 个 UTC 日的人员组，AVX2 每次计算八天与同一活动覆盖区间的交集。其余组走精确的逐日累计。工作量诊断记录命中组数。 |
| `PERF_CAMPAIGNS=map/dense/columns/buffered` | 默认 `map`。活动数乘窗口天数至多 131072 时使用稠密槽；`map` 将结果物化为映射，`dense` 可直接交给 writer。`columns` 分列保存三个 `u16` 计数及 `i64` 收入，进位另存；`buffered` 每 32 个更新批量提交。超出稠密范围使用稀疏映射。 |
| `PERF_AGGREGATE_NAMES=1` | 在每个人员组产生最后一条日行时记录姓名编码和行程；写出时可直接按行程跨行组切分。`PROFILE_WORK=1` 记录实际覆盖行数、姓名行程数和字典字节。 |
| `PERF_PREPARE_NAMES=1`、`PERF_MERGE_GROUPS=1`、`PERF_GROUP_ROWS=N` | 提前生成姓名字典及行程、写前合并同一输出分片的列、调整用户行组行数。默认每组上限 65536 行，字符串页另有 8 MiB 目标。 |
| `PERF_USER_PLAIN=1`、`PERF_CAMPAIGN_PLAIN=1`、`PERF_WRITE_BUFFER=N` | 分别选择用户页及活动页的未压缩 codec，字典、RLE 与位打包仍由 writer 选择。默认使用 Snappy 的适用页面。用户 writer 的缓冲默认 256 KiB，可设 4 KiB–16 MiB。 |
| `PERF_WRITERS=N`、`PERF_CAMPAIGN_FIRST=1` | 用户文件固定八份，`WRITERS` 控制 1–8 个用户写任务，默认 8。活动表默认用单独任务；开启后由第一个用户写任务先写活动表。 |
| `PERF_POOL=spin/stages`、`PERF_SPIN=N`、`PERF_GRAIN=N` | 默认 Rayon。`spin` 用常驻队列处理并行 setup 与 writer。`stages` 还调度标准路径的事件解析、去重、人员会话、点击解析及排序分区，以及路由的事件/点击解析、去重、P1/P2。队列工人数取 Rayon 线程数，两池共享容器 CPU 配额。`SPIN` 对常驻队列有效，默认 0，最多 100000 次。setup/writer 默认每组一项，`stages` 的解析和分区阶段按每工人约四组、各阶段最小粒度计算默认组长，`GRAIN` 可覆盖它。 |
| `PERF_THP=1`、`PERF_RELEASE_AFTER_CLOSE=1`、`PERF_EXIT_AFTER_CLOSE=1` | 分别为合格内存区建议透明大页、将当前 writer 的日列保留至 close 后集中释放、在所有 writer 完成并输出诊断后退出。透明大页是否实际使用由内核决定。 |

## 整程计时与阶段诊断

`design_matrix.py` 可对同一镜像切换环境，`sweep` 对每项与其直接前驱跑 `ABBAAB`。矩阵的 `control` 显式选择旧排序、购买及归因设置；`defaults` 使用程序默认值。`profile --lean` 记录阶段时间；省略 `--lean` 同时打开 `PROFILE_WORK=1` 和 `PROFILE_LOCKS=1`。诊断会重新分类输入、采样内存及计时锁获取，正式耗时另跑。

默认 Rayon 下，`PROFILE=1` 的 `rayon_phase` 记录每个并行阶段的 wall time。其中 `event_dedup` 单独计量完整键选胜，是 `dedup_and_sessionize` 的子阶段。

阶段线程池把连续工作项组成一组，组内顺序处理并复用临时空间。常驻工人或协助执行的线程在组内再次提交阶段任务时就地运行。组内并行排序仍可借用 Rayon；等待子任务的 Rayon 工人会让出执行机会，长时间无进展时可协助自己的任务组。`PROFILE=1` 的 `stage_workers` 给出工作项数、实际组长、参与的常驻工人数和阶段时间。

```powershell
& $py scripts/design_matrix.py "$work/sweep" sweep $b mmap batch --study $study --cases s83-20m
& $py scripts/design_matrix.py "$work/phases" profile $b defaults --study $study --cases s83-20m --lean
& $py scripts/design_matrix.py "$work/counters" profile $b defaults --study $study --cases s83-20m
```

比较页写法时固定 `PERF_ROUTE_CHUNKS=16`。原生页会把目标组长至少提高到 16，再按工作量和元数据预算调整，同时改变组长会混入调度成本。用新提交构建的同一镜像、独立结果目录和下列配置，分别测三种写法、描述符和阶段线程池。`PROFILE=1` 的 `native_stream_store_bytes` 可确认实际经过非临时写的字节数；`forwarded_descriptor_bytes`、`forwarded_event_bytes`、`forwarded_click_bytes` 可核对转发量。矩阵复用已存在的结果文件，更换镜像或设置时另取目录名。

```powershell
@'
{
  "layout":["flat-groups",{"PERF_ROUTE_CHUNKS":"16","PERF_ROUTE_INPUT_PAGES":"0","PERF_ROUTE_WRITES":"direct"}],
  "page-direct":["layout",{"PERF_ROUTE_INPUT_PAGES":"1"}],
  "page-buffered":["page-direct",{"PERF_ROUTE_WRITES":"buffered"}],
  "page-stream":["page-buffered",{"PERF_ROUTE_WRITES":"stream"}],
  "forward-blocks":["layout",{"PERF_ROUTE_FORWARD":"blocks"}],
  "stage-pool":["layout",{"PERF_POOL":"stages"}]
}
'@ | Set-Content -Encoding ascii "$work/layout-trials.json"
& $py scripts/design_matrix.py "$work/layout-race" sweep $b page-direct page-buffered page-stream forward-blocks stage-pool --settings "$work/layout-trials.json" --study $study --cases s83-20m
```

`PROFILE=1` 记录各阶段 wall time、RSS、进程 I/O、缺页、线程 CPU ticks 与可用的硬件计数。内核拒绝 perf 事件时记录 errno。`PROFILE_LOCKS` 也可写作 `PERF_LOCKS`，累计的是各分片互斥锁获取区间。`PROFILE_WORK` 区分真实 read 返回字节、映射逻辑遍历字节、坏时间、引号及长行。路由重叠阶段的 elapsed、任务时长总和以及整程 wall time 各自统计。矩阵 `--cold` 单独进行页驱逐与驻留核验。

单测前缀工作可以用 `PERF_PROBE=read/lines/fields/decode/bucket`。该入口在事件前缀完成后退出，只用于诊断。示例输出写入独立日志。

```powershell
'{"probe":{"image":"shopping-design:candidate","env":{"PERF_PROBE":"decode"}}}' | Set-Content -Encoding ascii "$work/probe.json"
& $py scripts/bench_container.py $vol s83-20m probe --variants "$work/probe.json" --profile --verify-cache --log "$work/probe-runs.jsonl"
```

## 独立压力与编译变体

```powershell
& $py scripts/design_stress.py generate "$work/stress-inputs" --events 100000 --clicks 25000 --key-bytes 33
& $py scripts/design_stress.py install "$work/stress-inputs" $vol stress-design
& $py scripts/design_stress.py race $vol stress-design "$work/stress-runs" --a $a --b $b
& $py scripts/design_bench.py $work build shopping-design:static $rev --build-arg SCORER_STATIC=1 --build-arg RUNTIME_IMAGE=scratch
& $py scripts/design_bench.py $work build shopping-design:cascade $rev --build-arg SCORER_CASCADE=1
& $py scripts/design_bench.py $work build shopping-design:fat $rev --build-arg SCORER_LTO=fat
& $py scripts/design_bench.py $work build shopping-design:no-pgo $rev --build-arg SCORER_PGO=0
```

使用 `generate --winner-validity` 可单独生成两组 50% 重复且等字节的数据。每个键的两条记录跨文件、跨用户；等长时间戳在部分早晚副本间交换，使最早副本的有效性发生变化。脚本分别用独立预期核对两张表。

压力脚本发布到全新卷子目录并校验 SHA-256。低/高重复组行数、键长、其他列及总字节相同；多小文件组保留原始行流；超长行组只扩展两条 view 的商品字段。各组做 A/B 双向全列比较，等价布局再跨组比较。`--quick` 只跑一对。

运行时 `PERF_ISA=portable/v3/wide/cascade` 在镜像已构建且 CPU 特征满足时选对应程序。默认按 CPU 和事件 CSV 总字节调度，`PERF_LARGE_BYTES` 为 3 GiB。`SCORER_STATIC=1` 配合 `RUNTIME_IMAGE=scratch`，镜像自带 Rust 调度器。正式比较保持 mimalloc 和镜像的 `MIMALLOC_PURGE_DELAY=10000` 一致。默认 LTO 为 thin，v4 大输入程序使用官方 seed 91 的 60M 事件与 15M 点击重训的 PGO。原 profile 保留在 rust 目录，可用 `SCORER_PROFILE` 单独切回。重训时用独立种子，并记录训练提交和参数。`PERF_WIDE_SCAN=1` 在提供 AVX-512F/BW 的大输入入口上，对 65–128 B 事件行实际使用两个 64 B 载荷。`SCORER_CASCADE_PKU=0` 在编译 Cascade Lake 入口时禁用 PKU 指令，用于虚拟机未暴露内存保护键的机器，其他 ISA 检查仍执行。

```powershell
docker run --rm --network none --cpuset-cpus 0,2,4,6,8,10,12,14 --cpus 8 --memory 24g --memory-swap 24g --mount "type=volume,source=$vol,target=/data" "${vol}:generator" --seed 91 --events 60000000 --clicks 15000000 --threads 8 --output /data/design-s91-60m
& $py scripts/train_wide_profile.py $vol design-s91-60m rust/design-training.profdata --revision $rev --lto thin
docker buildx build --platform linux/amd64 --load -t shopping-design:trained --build-arg SCORER_PROFILE=design-training.profdata .
```

最后一条命令使用当前工作树内的训练文件。用 `design_bench.py build` 归档复现时，所选提交必须包含对应的 `rust/*.profdata`。
