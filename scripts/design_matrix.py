"""Run isolated performance-design trials on the original v5 inputs."""
import argparse
import json
from pathlib import Path
from statistics import median
import subprocess
import sys

from design_bench import BENCH, CASES, ROOT, run

BASE = "shopping-sessions:submission-a0f08920c19c"
CONTROL = {"PERF_ASSISTS":"interval", "PERF_CAMPAIGNS":"map", "PERF_TIME_SORT":"0",
           "PERF_PURCHASE_BINS":"0", "PERF_SINGLE_PURCHASE":"0"}
# Each tuple gives the direct comparison parent and the settings changed for
# this treatment. Routing is introduced with all people forwarded first.
TRIALS = {
    "control": (None, CONTROL),
    "intern-clicks": ("control", {"PERF_CLICK_KEYS":"intern"}),
    "defaults": (None, {}),
    "stages": ("control", {"PERF_POOL":"stages"}),
    "stages-spin": ("stages", {"PERF_SPIN":"128"}),
    "mmap": ("control", {"PERF_IO":"mmap"}),
    "adaptive": ("mmap", {"PERF_CHUNK":"adaptive"}),
    "release-pages": ("adaptive", {"PERF_PAGE_RELEASE":"1"}),
    "simd-hex": ("control", {"PERF_HEX":"simd"}),
    "batch": ("control", {"PERF_DECODER":"batch"}),
    "batch4": ("batch", {"PERF_BATCH":"4"}),
    "batch32": ("batch", {"PERF_BATCH":"32"}),
    "dimensions": ("control", {"PERF_DIM_PARALLEL":"1"}),
    "categories": ("control", {"PERF_DIM_NARROW":"1"}),
    "wide-users": ("control", {"PERF_USER_HEX":"1"}),
    "wide-cache": ("wide-users", {"PERF_WIDE_CACHE":"1"}),
    "fast-times": ("control", {"PERF_FAST_TIMES":"1"}),
    "span": ("control", {"PERF_SCANNER":"span"}),
    "compact-clicks": ("control", {"PERF_CLICK_RECORDS":"16"}),
    "prefetch": ("control", {"PERF_LINK_PREFETCH":"1"}),
    "link-compact": ("control", {"PERF_LINK_COMPACT":"1"}),
    "purchase-bins": ("control", {"PERF_PURCHASE_BINS":"1"}),
    "time-sort": ("control", {"PERF_TIME_SORT":"1"}),
    "medium-bins": ("purchase-bins", {"PERF_MEDIUM_BINS":"1"}),
    "bin-mask": ("purchase-bins", {"PERF_BIN_MASK":"1"}),
    "single": ("control", {"PERF_SINGLE_PURCHASE":"1"}),
    "category-dense": ("control", {"PERF_CATEGORY_DENSE":"1"}),
    "assist-mask": ("control", {"PERF_ASSISTS":"mask"}),
    "unsorted": ("assist-mask", {"PERF_ASSISTS":"unsorted"}),
    "tiny16": ("unsorted", {"PERF_SMALL_CLICKS":"16"}),
    "simd-assist": ("unsorted", {"PERF_ASSISTS":"simd"}),
    "dense": ("control", {"PERF_CAMPAIGNS":"dense"}),
    "columns": ("dense", {"PERF_CAMPAIGNS":"columns"}),
    "buffered-counts": ("columns", {"PERF_CAMPAIGNS":"buffered"}),
    "serial-write": ("control", {"PERF_WRITERS":"1"}),
    "plain": ("control", {"PERF_USER_PLAIN":"1"}),
    "campaign-plain": ("control", {"PERF_CAMPAIGN_PLAIN":"1"}),
    "campaign-first": ("control", {"PERF_CAMPAIGN_FIRST":"1"}),
    "release-after": ("control", {"PERF_RELEASE_AFTER_CLOSE":"1"}),
    "write-buffer": ("control", {"PERF_WRITE_BUFFER":"1048576"}),
    "merge-groups": ("control", {"PERF_MERGE_GROUPS":"1"}),
    "large-groups": ("merge-groups", {"PERF_GROUP_ROWS":"1048576"}),
    "names": ("control", {"PERF_PREPARE_NAMES":"1"}),
    "setup": ("control", {"PERF_PARALLEL_SETUP":"1"}),
    "pool": ("setup", {"PERF_POOL":"spin"}),
    "spin": ("pool", {"PERF_SPIN":"128"}),
    "grain2": ("setup", {"PERF_GRAIN":"2"}),
    "exit": ("control", {"PERF_EXIT_AFTER_CLOSE":"1"}),
    "routed-serial": ("control", {"PERF_ROUTE":"1","PERF_ROUTE_LINKS":"serial","PERF_ROUTE_P1":"0",
                                  "PERF_ROUTE_WRITES":"direct"}),
    "routed-overlap": ("routed-serial", {"PERF_ROUTE_LINKS":"overlap"}),
    "routed-p1": ("routed-overlap", {"PERF_ROUTE_P1":"1"}),
    "overlap-streams": ("control", {"PERF_OVERLAP_STREAMS":"1"}),
    "flat-groups": ("routed-p1", {"PERF_ROUTE_GROUPS":"flat"}),
    "group-chunks": ("routed-p1", {"PERF_ROUTE_CHUNKS":"8"}),
    "flat-chunks": ("flat-groups", {"PERF_ROUTE_CHUNKS":"8"}),
    "flat-packed": ("flat-chunks", {"PERF_ROUTE_PACK":"1"}),
    "flat-slab": ("flat-chunks", {"PERF_ROUTE_SLAB":"1"}),
    "fused": ("flat-slab", {"PERF_ROUTE_FUSED":"1"}),
    "chunks16": ("flat-groups", {"PERF_ROUTE_CHUNKS":"16"}),
    "native-pages": ("chunks16", {"PERF_ROUTE_INPUT_PAGES":"1"}),
    "native-buffer": ("native-pages", {"PERF_ROUTE_WRITES":"buffered"}),
    "native-stream": ("native-buffer", {"PERF_ROUTE_WRITES":"stream"}),
    "native-no-reuse": ("native-buffer", {"PERF_ROUTE_REUSE":"0"}),
    "native-thp": ("native-buffer", {"PERF_THP":"1"}),
    "native-packed": ("native-buffer", {"PERF_ROUTE_PACK":"1"}),
    "descriptors": ("fused", {"PERF_ROUTE_FORWARD":"blocks"}),
    "sample": ("routed-p1", {"PERF_ROUTE":"auto"}),
    "packed-route": ("routed-p1", {"PERF_ROUTE_PACK":"1"}),
    "portable": ("control", {"PERF_ISA":"portable"}),
    "v3": ("control", {"PERF_ISA":"v3"}),
    "wide": ("control", {"PERF_ISA":"wide"}),
    "wide-scan": ("wide", {"PERF_WIDE_SCAN":"1"}),
    "cascade": ("wide", {"PERF_ISA":"cascade"}),
    "early-clicks": ("routed-p1", {"PERF_ROUTE_CLICKS":"early"}),
    "write-buckets": ("routed-p1", {"PERF_ROUTE_WRITES":"buffered"}),
    "stream": ("write-buckets", {"PERF_ROUTE_WRITES":"stream"}),
    "exact-linear": ("routed-p1", {"PERF_ROUTE_EXACT":"linear"}),
    "plain-map": ("routed-p1", {"PERF_ROUTE_WINNER":"map"}),
    "bitmap4": ("routed-p1", {"PERF_ROUTE_BITMAP_FACTOR":"4"}),
    "event512": ("routed-p1", {"PERF_ROUTE_EVENT_BUCKETS":"512"}),
    "user512": ("routed-p1", {"PERF_ROUTE_USER_BUCKETS":"512"}),
    "arena": ("write-buckets", {"PERF_ROUTE_ARENA":"1","PERF_ROUTE_REUSE":"0"}),
    "reuse": ("arena", {"PERF_ROUTE_REUSE":"1"}),
    "huge": ("reuse", {"PERF_THP":"1"}),
}
for prefix in ("read","lines","fields","decode","bucket"):
    TRIALS["prefix-"+prefix] = ("control", {"PERF_PROBE":prefix})


def settings(name):
    parent, changes = TRIALS[name]
    return {**(settings(parent) if parent else {}), **changes}


def call(work, volume, case, order, variants, label, *, export=False, profile=False, cold=False,
         case_config=False, record_peers=False):
    directory = work / label
    directory.mkdir(parents=True, exist_ok=True)
    conf = directory / (case+".variants.json" if case_config else "variants.json")
    if conf.exists() and json.loads(conf.read_text()) != variants:
        raise ValueError(f"Changed variants in {directory}")
    conf.write_text(json.dumps(variants, indent=2))
    path = directory / f"{case}.jsonl"
    if not path.exists():
        partial = path.with_suffix(".running")
        if partial.exists():
            raise FileExistsError(f"Incomplete previous trial: {partial}")
        flags = ["--variants", conf, "--verify-cache", "--log", partial]
        if record_peers:
            flags += ["--record-peers"]
        if export:
            flags += ["--export", directory / case]
        if profile:
            flags += ["--profile"]
        if cold:
            flags += ["--cache", "evict"]
        with path.with_suffix(".log").open("w",encoding="utf8") as log:
            run(sys.executable,BENCH,volume,case,*order,*flags,stdout=log,stderr=subprocess.STDOUT)
        partial.rename(path)
    rows = [json.loads(s) for s in path.read_text().splitlines()]
    assert [r["image"] for r in rows] == order and all(r["profile"] == profile for r in rows)
    assert all(r["resident_input_bytes"]==(0 if cold else r["input_page_bytes"]) for r in rows)
    return rows


def compare(path, reference, csv=False):
    log = path.with_suffix(".sql.log")
    with log.open("w",encoding="utf8") as f:
        run(sys.executable,ROOT/"scripts/check_event_key_tables.py",path,reference,
            *(["--expected-csv"] if csv else []),stdout=f,stderr=subprocess.STDOUT)


def race(work, volume, name, case, a, b, reference, cold=False):
    seq = list("ABBAAB")
    rows = call(work,volume,case,seq,{"A":a,"B":b},name,export=reference is not None,cold=cold)
    left, right = ([r["seconds"] for r in rows if r["image"]==k] for k in ("A","B"))
    if reference:
        compare(work/name/case,reference/case/"b")
    stats = dict(A=left,B=right,median_a=median(left),median_b=median(right),
        gain=1-median(right)/median(left),wins=sum(y<x for x,y in zip(left,right)),
        image_ids=[next(r["image_id"] for r in rows if r["image"]==k) for k in ("A","B")])
    print(name,case,json.dumps(stats),flush=True)
    (work/name/(case+".summary.json")).write_text(json.dumps(stats,indent=2))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("work",type=Path)
    p.add_argument("action",choices=("sweep","profile","qualify"))
    p.add_argument("image")
    p.add_argument("variants",nargs="+")
    p.add_argument("--settings",type=Path, help="additional trial definitions (parent, environment)")
    p.add_argument("--study",type=Path,default=Path(".buildathon/event-key-v5-a635381"))
    p.add_argument("--cases",nargs="+",default=CASES)
    p.add_argument("--reference",action="store_true",help="also export and compare the candidate's large tables")
    p.add_argument("--baseline-image",help="use an explicit image as the control")
    p.add_argument("--cold",action="store_true")
    p.add_argument("--lean",action="store_true",help="profile stage timings without detailed work or dictionary-lock instrumentation")
    args=p.parse_args()
    if args.settings:
        TRIALS.update(json.loads(args.settings.read_text()))
    work,study=args.work.resolve(),args.study.resolve()
    volume=(study/"volume").read_text().strip()
    for name in args.variants:
        candidate={"image":args.image,"env":settings(name)}
        parent=TRIALS[name][0] or "control"
        control={"image":args.image,"env":settings(parent)}
        if args.baseline_image:
            control={"image":args.baseline_image,"env":{}}
        if args.action=="qualify":
            call(work,volume,"dev",["B"],{"B":candidate},name,export=True)
            compare(work/name/"dev",study/"answer/dev-answer",csv=True)
            print(name, "official dev MATCH", flush=True)
            continue
        for case in args.cases:
            if args.action=="sweep":
                race(work,volume,name,case,control,candidate,study/"tables" if args.reference else None,args.cold)
            else:
                details = {} if args.lean else {"PROFILE_WORK":"1","PROFILE_LOCKS":"1"}
                detailed = {**candidate,"env":{**candidate["env"],**details}}
                call(work,volume,case,["B"],{"B":detailed},name,profile=True)
                print(name,case,"profiled",flush=True)


if __name__ == "__main__":
    main()
