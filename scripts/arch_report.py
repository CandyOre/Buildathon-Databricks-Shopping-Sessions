"""Report completed arch_study container timings, SQL checks and diagnostics."""
import argparse
import gzip
import json
from pathlib import Path, PurePosixPath
import re
from statistics import median
import sys
import tempfile


COLUMNS = {
    "user_daily": {"user_id", "day", "sessions", "events", "revenue_cents", "top_category"},
    "campaign_daily": {"campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                       "revenue_cents", "assisted_sessions"},
}


def read(path):
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def lines(path):
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def source(path, work):
    return path.relative_to(work).as_posix()


def checks(path, work):
    if not path.is_file():
        return None
    raw = path.read_text(encoding="utf-8").splitlines()
    tables = {t: {"sides": {}, "columns": {}} for t in COLUMNS}
    current = None
    for line in raw:
        side = re.fullmatch(r"(user_daily|campaign_daily) (side_[ab]): rows=(\d+) duplicate_key_rows=(\d+) invalid_null_rows=(\d+)", line)
        if side:
            table, name, rows, dup, nulls = side.groups()
            tables[table]["sides"][name] = dict(rows=int(rows), duplicate_keys=int(dup), invalid_nulls=int(nulls))
            continue
        full = re.fullmatch(r"(user_daily|campaign_daily) full rows: A EXCEPT ALL B=(\d+), B EXCEPT ALL A=(\d+)", line)
        if full:
            current, a, b = full.groups()
            tables[current]["full_row_differences"] = [int(a), int(b)]
            continue
        column = re.fullmatch(r"  (\w+): A EXCEPT ALL B=(\d+), B EXCEPT ALL A=(\d+)", line)
        if current and column:
            name, a, b = column.groups()
            tables[current]["columns"][name] = [int(a), int(b)]
    for name, t in tables.items():
        sides = t["sides"]
        t["match"] = (set(sides) == {"side_a", "side_b"}
                      and sides["side_a"]["rows"] == sides["side_b"]["rows"]
                      and all(s["duplicate_keys"] == s["invalid_nulls"] == 0 for s in sides.values())
                      and t.get("full_row_differences") == [0, 0]
                      and set(t["columns"]) == COLUMNS[name]
                      and all(v == [0, 0] for v in t["columns"].values()))
    return {"source": source(path, work), "match": bool(raw) and raw[-1] == "MATCH"
            and all(t["match"] for t in tables.values()), "tables": tables}


def study_run(work, stage, version, case, expected_input):
    root = work / stage / version
    if not (root / (case + ".input.json")).exists() and (root / "verified" / (case + ".input.json")).is_file():
        root /= "verified"
    path = root / (case + ".jsonl")
    rows = lines(path)
    if not rows:
        return None
    binding = read(root / (case + ".input.json"))
    status = "unbound" if not binding else "verified" if (
        binding == expected_input and binding.get("case") == case and binding.get("proof")
        and all(r["dataset"] == case and r["volume"] == binding["volume"] for r in rows)
    ) else "mismatch"
    return root, rows, {"source": source(path, work), "input_binding": status,
                        "input_sha256": binding.get("sha256") if binding else None}


def race(work, version, case, result):
    root, rows, info = result
    sides = {"P": [], version: []}
    snapshot = read(root / (case + ".summary.json"))
    if not all(r["image"] in sides for r in rows):
        raise ValueError(f"Unexpected race participants in {info['source']}")
    fields = ("image", "image_id", "architecture", "started", "finished", "seconds", "output", "limits",
              "resident_input_bytes", "input_page_bytes", "other_running_containers", "profile", "cache")
    records = []
    for i, r in enumerate(rows, 1):
        sides[r["image"]].append(r["seconds"])
        entry = {k: r[k] for k in fields if k in r}
        stem = str(i) + "-" + r["image"]
        dest = root / (case + "-tables")
        sql = dest / (stem + "-" + PurePosixPath(r["output"]).name + ".sql.log")
        if not sql.exists() and info["input_binding"] == "unbound":
            sql = dest / (stem + ".sql.log")
        entry["sql"] = checks(sql, work)
        records.append(entry)
    info.update(order=[r["image"] for r in rows], peer_seconds=sides["P"], scorer_seconds=sides[version],
                scorer_image=next((r["image_id"] for r in rows if r["image"] == version), None),
                records=records, sql_matches=sum(bool(r["sql"] and r["sql"]["match"]) for r in records))
    if info["order"] == ["P", version, version, "P", "P", version]:
        p, c = map(median, (sides["P"], sides[version]))
        info.update(peer_median_s=p, scorer_median_s=c, scorer_minus_peer_s=c-p,
                    scorer_reduction_pct=(1-c/p)*100 if p else None)
    isolated = all(r.get("profile") is False and r.get("other_running_containers") == []
                   and r.get("input_page_bytes", 0) > 0
                   and r.get("resident_input_bytes") == r["input_page_bytes"] for r in records)
    info["qualified"] = bool(isolated and snapshot and info["input_binding"] == "verified" and info["sql_matches"] == 6
                             and snapshot.get("input_receipt", {}).get("sha256") == info["input_sha256"]
                             and snapshot.get("peer") == sides["P"] and snapshot.get("scorer_times") == sides[version]
                             and snapshot.get("order") == info["order"] and "scorer_median_s" in info)
    return info


def diagnostic(work, case, result):
    root, rows, info = result
    if len(rows) != 1 or not rows[0]["profile"]:
        raise ValueError(f"Unexpected diagnostic records in {info['source']}")
    row = rows[0]
    info.update(image=row["image_id"], architecture=row.get("architecture"), container_s=row["seconds"],
                stages=[], peak_rss_kib=None, container_peak_bytes=None)
    for s in row.get("stages", []):
        # Keep scalar timing, counter and architecture evidence. Per-thread
        # scheduler maps do not belong in the compact comparison archive.
        info["stages"].append({k: v for k, v in s.items() if not isinstance(v, dict)})
        peak = s.get("peak_rss_kib")
        if peak is not None:
            info["peak_rss_kib"] = max(info["peak_rss_kib"] or 0, peak)
        if s.get("container_peak_bytes") is not None:
            info["container_peak_bytes"] = s["container_peak_bytes"]
    log = root / (case + ".log")
    if not info["stages"] and log.is_file():
        # Some complete scorer images write plain-text diagnostics.
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.startswith("[v3b] ms:"):
                continue
            values = {k: float(v) for k, v in re.findall(r"(\w+)=([0-9.]+)", line)}
            info["peak_rss_kib"] = values.pop("peak_rss_kb", None)
            info["stages"].extend({"stage": k, "elapsed_s": v/1000} for k, v in values.items())
            info["text_source"] = source(log, work)
            break
    return info


def phase_times(profile):
    stages = profile.get("stages", [])
    details = {"event_scan_s": "事件解析", "event_exchange_s": "事件交换",
               "event_winner_route_s": "去重与赢家路由", "click_scan_route_s": "点击解析与路由",
               "link_elapsed_s": "链接目录（与事件并行）", "link_barrier_s": "链接屏障等待",
               "p1_s": "P1 原始用户分区", "p2_s": "P2 最终用户分区", "final_people_s": "最终用户聚合"}
    times = []
    has_details = any(s["stage"] in ("arch_b_direct", "route_storage") for s in stages)
    for s in stages:
        if s["stage"] in ("route_storage", "raw_user_route", "arch_b_direct"):
            times.extend((label, s[key]) for key, label in details.items() if key in s)
        elif "elapsed_s" in s and not (has_details and s["stage"] in ("rayon_phase", "queue_phase")):
            times.append((s.get("phase", s["stage"]), s["elapsed_s"]))
    if profile.get("container_s") is not None:
        times.append(("诊断容器启动至退出", profile["container_s"]))
    return times


def official(work, version):
    path = work / "official" / version
    accepted = read(path / "accepted.json")
    for summary in sorted(path.glob("*/summary.json"), reverse=True):
        suite = summary.parent
        config = read(suite / "config.json")
        if not config or accepted and config.get("digest") != accepted:
            continue
        results = lines(suite / "results.jsonl")
        cases = {r["check"]: bool(r["passed"]) for r in results}
        examples = [v for k, v in cases.items() if k.startswith("official-") and k != "official-dev-1m"]
        extra = [v for k, v in cases.items() if k.startswith("design-")]
        totals = read(summary)
        return dict(source=source(summary, work), image=config["digest"], accepted=bool(accepted)
                    and len(examples) == 36 and all(examples) and cases.get("official-dev-1m") is True
                    and len(cases) == len(results) == totals["total"] == totals["passed"] and all(cases.values()),
                    official_passed=sum(examples), official_total=len(examples), custom_passed=sum(extra),
                    custom_total=len(extra), dev_1m=checks(suite/"official-dev-1m.log", work), cases=cases)
    return None


def collect(work, versions, cases):
    inputs = {p.name.removesuffix(".input.json"): read(p) for p in sorted(work.glob("*.input.json"))}
    versions = versions or sorted({p.name for kind in ("gate", "race", "profile", "official")
                                  for p in (work/kind).glob("*") if p.is_dir()})
    cases = cases or list(inputs)
    builds = {item["image"]: item for p in sorted(work.glob("*.image.json")) if (item := read(p))}
    data = {"inputs": {c: inputs.get(c) for c in cases}, "builds": builds,
            "official": {v: official(work, v) for v in versions}, "cases": {}}
    for case in cases:
        entries = {}
        for v in versions:
            entry = {}
            for stage in ("race", "profile", "gate"):
                run = study_run(work, stage, v, case, inputs.get(case))
                if not run:
                    continue
                if stage == "race":
                    entry[stage] = race(work, v, case, run)
                elif stage == "profile":
                    entry[stage] = diagnostic(work, case, run)
                else:
                    root, rows, info = run
                    info.update(image=rows[0]["image_id"], architecture=rows[0].get("architecture"),
                                sql=checks(root/(case+".sql.log"), work))
                    entry[stage] = info
            images = {x for x in (entry.get("race", {}).get("scorer_image"),
                                  entry.get("gate", {}).get("image"), entry.get("profile", {}).get("image"),
                                  (data["official"][v] or {}).get("image")) if x}
            if len(images) > 1:
                entry["image_mismatch"] = sorted(images)
                if "race" in entry:
                    entry["race"]["qualified"] = False
            entries[v] = entry
        data["cases"][case] = entries
    return data


def fmt(n):
    return f"{n:.6f}" if n is not None else "未测"


def markdown(data):
    out = ["## 容器启动至退出（秒）", "",
           "| 数据集 | 版本 | 对手三次 | 本方三次 | 对手中位 | 本方中位 | 本方减对手 | 六次验收 |",
           "|---|---|---|---|---:|---:|---:|---|"]
    for c, entries in data["cases"].items():
        for v, e in entries.items():
            r = e.get("race", {})
            series = lambda key: ", ".join(map(fmt, r.get(key, []))) or "未测"
            verdict = "镜像不一致" if e.get("image_mismatch") else "通过" if r.get("qualified") else "未验收"
            out.append(f"| {c} | {v} | {series('peer_seconds')} | {series('scorer_seconds')} | "
                       f"{fmt(r.get('peer_median_s'))} | {fmt(r.get('scorer_median_s'))} | "
                       f"{fmt(r.get('scorer_minus_peer_s'))} | {verdict} |")
    out.extend(["", "## 20M 阶段诊断（秒，子阶段可能重叠）", "",
                "| 数据集 | 版本 | 阶段 | 耗时 |", "|---|---|---|---:|"])
    for c, entries in data["cases"].items():
        if (data["inputs"].get(c) or {}).get("events") != 20_000_000:
            continue
        for v, e in entries.items():
            times = phase_times(e.get("profile", {}))
            for stage, time in times or [("未测", None)]:
                out.append(f"| {c} | {v} | {stage} | {fmt(time)} |")
    out.extend(["", "## 大表与内存诊断", "",
                "| 数据集 | 版本 | 用户日表 | 活动日表 | RSS 峰值 MiB | cgroup 峰值 MiB | 输入绑定 |",
                "|---|---|---|---|---:|---:|---|"])
    for c, entries in data["cases"].items():
        for v, e in entries.items():
            gate, diag = e.get("gate", {}), e.get("profile", {})
            tables = (gate.get("sql") or {}).get("tables", {})
            status = lambda t: ("通过" if tables[t]["match"] else "失败") if t in tables else "未测"
            mib = lambda k, div: f"{diag[k]/div:.1f}" if diag.get(k) is not None else "未测"
            out.append(f"| {c} | {v} | {status('user_daily')} | {status('campaign_daily')} | "
                       f"{mib('peak_rss_kib',1024)} | {mib('container_peak_bytes',1024**2)} | "
                       f"{gate.get('input_binding', diag.get('input_binding', '未测'))} |")
    out.extend(["", "## 官方验收与出处", ""])
    for v, o in data["official"].items():
        if o:
            dev = "通过" if (o["dev_1m"] or {}).get("match") else "未通过"
            out.append(f"- {v} 官方用例 {o['official_passed']}/{o['official_total']}，1M 答案{dev}，"
                       f"自定义 {o['custom_passed']}/{o['custom_total']}。镜像 {o['image']}。")
    for c, meta in data["inputs"].items():
        if meta:
            out.append(f"- {c} 种子 {meta['seed']}，输入 SHA-256 {meta['sha256']}，"
                       f"来源 {meta.get('proof', '未绑定')}。")
    for b in data["builds"].values():
        out.append(f"- {b['tag']} 提交 {b['commit']}，镜像 {b['image']}。")
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("work", type=Path)
    p.add_argument("--versions", nargs="+")
    p.add_argument("--cases", nargs="+")
    p.add_argument("--format", choices=("markdown", "json"), default="markdown")
    p.add_argument("--evidence", type=Path, help="Write a reproducible gzip of the compact JSON evidence")
    args = p.parse_args()
    if not args.work.is_dir():
        p.error("study directory does not exist")
    data = collect(args.work, args.versions, args.cases)
    if args.evidence:
        raw = json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=args.evidence.parent, delete=False) as temp:
            temp.write(gzip.compress(raw, mtime=0))
        Path(temp.name).replace(args.evidence)
    sys.stdout.reconfigure(encoding="utf-8")
    print(json.dumps(data, indent=2, ensure_ascii=False) if args.format == "json" else markdown(data))


if __name__ == "__main__":
    main()
