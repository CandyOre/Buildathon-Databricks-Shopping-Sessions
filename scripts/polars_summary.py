"""Print Markdown tables from polars_bench.py JSONL logs (standard library only)."""
import argparse
import json
import math
import sys
from pathlib import Path
from statistics import median

from polars_bench import PROFILE_ROUNDS

SIZES = ("20m", "60m")
VARIANTS = ("native", "dedup", "full")
RUNS = [f"round-{i}" for i in (1, 2, 3)]
PROFILES = [f"profile-{i}" for i in (1, 2, 3)]
STAGES = {
    "load_dimensions_and_links": "维表和身份链接",
    "parse_events": "解析事件",
    "prepare_dedup_columns": "准备去重列",
    "dedup_events": "事件去重",
    "event_columns": "准备会话输入列",
    "sort_events": "事件排序",
    "sessionize_and_user_daily": "会话切分和用户日聚合",
    "parse_and_sort_clicks": "解析和排序点击",
    "attribution_and_assists": "原生归因和助攻",
    "campaign_columns": "准备活动输入列",
    "click_attribution": "Polars 点击归因",
    "assists_and_campaign_daily": "Polars 助攻和活动日汇总",
    "write_parquet": "写出 Parquet",
    "total": "程序全程",
}
FULL_ONLY = {"event_columns", "campaign_columns", "click_attribution", "assists_and_campaign_daily"}
COMPARABLE = {
    "解析事件至完成去重": ["parse_events", "prepare_dedup_columns", "dedup_events"],
    "去重准备与执行": ["prepare_dedup_columns", "dedup_events"],
    "会话建列、排序和用户日聚合": ["event_columns", "sort_events", "sessionize_and_user_daily"],
    "活动建列、归因、助攻和汇总": ["attribution_and_assists", "campaign_columns", "click_attribution",
                             "assists_and_campaign_daily"],
}


def number(row, field):
    value = row[field]
    if type(value) not in (float, int) or not math.isfinite(value) or value < 0:
        raise ValueError(f"invalid {field}: {value!r}")
    return value


def read_runs(directory, size, profile_count):
    selected = RUNS + PROFILES[:profile_count]
    path = directory / f"{size}.jsonl"
    runs = {v: {} for v in VARIANTS}
    with path.open(encoding="utf-8") as source:
        for line_no, line in enumerate(source, 1):
            row = json.loads(line)
            variant, label = row["variant"], row["run"]
            if row["size"] != size or variant not in runs or type(row["accepted"]) is not bool:
                raise ValueError(f"{path}:{line_no} has an invalid size, variant or accepted flag")
            if not row["accepted"] or label == "warmup" or label in PROFILES[profile_count:]:
                continue
            if label not in selected or label in runs[variant]:
                raise ValueError(f"{path}:{line_no} unexpected or duplicate accepted {variant} {label}")

            number(row, "container_s")
            number(row, "cli_s")
            stages = {stage["stage"]: stage for stage in row["stages"]}
            expected = set(STAGES)
            if variant == "full":
                expected.remove("attribution_and_assists")
            else:
                expected -= FULL_ONLY
            if variant == "native":
                expected.remove("prepare_dedup_columns")
            if label in RUNS:
                expected.clear()
            if len(stages) != len(row["stages"]) or set(stages) != expected:
                raise ValueError(f"{path}:{line_no} invalid stage set for {variant} {label}: {list(stages)}")
            for stage in stages.values():
                number(stage, "elapsed_s")
                if number(stage, "peak_rss_kib") == 0:
                    raise ValueError(f"{path}:{line_no} missing peak RSS")
            if stages and number(stages["total"], "container_peak_bytes") == 0:
                raise ValueError(f"{path}:{line_no} missing cgroup peak")
            row["stages"] = stages
            runs[variant][label] = row
    for variant, group in runs.items():
        missing = set(selected) - group.keys()
        if missing:
            raise ValueError(f"{path}: {variant} missing accepted runs {sorted(missing)}")
        if len({r["image_id"] for r in group.values()}) != 1:
            raise ValueError(f"{path}: image changed across accepted {variant} runs")
    return runs


def summarize(groups, counts):
    profiles = [[g[r]["stages"] for r in PROFILES if r in g] for g in groups]
    samples = "，".join(f"{s.upper()} 每版本 {counts[s]} 次" for s in SIZES)
    labels = {"native": "原生", "dedup": "仅去重", "full": "完整"}
    headings = [f"{s.upper()} {labels[v]}" for s in SIZES for v in VARIANTS]

    def header(title):
        print("\n" + " | ".join(["| " + title, *headings]) + " |")
        print("| --- |" + " ---: |" * len(headings))

    def row(name, values):
        print(" | ".join(["| " + name, *values]) + " |")

    print("## 容器计时和内存")
    print(f"\n两种规模各取三轮无采样运行的耗时中位数。内存峰值取自 profiling（{samples}）。")
    header("指标")
    for name, field in [("容器运行中位数（s）", "container_s"), ("Docker CLI 中位数（s）", "cli_s")]:
        row(name, [f"{median(g[r][field] for r in RUNS):.3f}" for g in groups])
    for name, field, divisor in [("进程最高 RSS（GiB）", "peak_rss_kib", 2**20),
                                 ("容器内存峰值（GiB）", "container_peak_bytes", 2**30)]:
        row(name, [f"{max(p['total'][field] for p in runs) / divisor:.3f}" for runs in profiles])
    print("\n## 分阶段耗时和 RSS")
    print(f"\n采样次数为 {samples}。每格给出耗时中位数（秒） / 最高 RSS（GiB）。单次采样直接列出该轮实测值。")
    header("阶段")
    for stage, title in STAGES.items():
        values = []
        for runs in profiles:
            if stage not in runs[0]:
                values.append("—")
                continue
            data = [p[stage] for p in runs]
            values.append(f"{median(d['elapsed_s'] for d in data):.3f} / "
                          f"{max(d['peak_rss_kib'] for d in data) / 2**20:.3f}")
        row(title, values)
    print("\n## 可比阶段耗时")
    print(f"\n每轮内对阶段求和，单位为秒。{samples}，多轮取中位数。")
    header("阶段范围")
    for title, stages in COMPARABLE.items():
        values = []
        for runs in profiles:
            times = []
            for data in runs:
                times.append(sum(data[s]["elapsed_s"] for s in stages if s in data))
            values.append(f"{median(times):.3f}")
        row(title, values)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path,
                        default=Path(__file__).resolve().parents[1] / "benchmarks" / "polars")
    for size in SIZES:
        parser.add_argument(f"--profiles-{size}", type=int, choices=range(1, 4), default=PROFILE_ROUNDS[size],
                            help=f"use the first N accepted profile rounds per {size} variant (default: {PROFILE_ROUNDS[size]})")
    args = parser.parse_args()
    counts = {s: getattr(args, f"profiles_{s}") for s in SIZES}
    try:
        groups = []
        for size in SIZES:
            data = read_runs(args.directory, size, counts[size])
            groups.extend(data[v] for v in VARIANTS)
        summarize(groups, counts)
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))


if __name__ == "__main__":
    main()
