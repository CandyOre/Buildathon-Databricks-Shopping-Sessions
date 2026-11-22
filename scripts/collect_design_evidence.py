"""Archive small machine-readable benchmark and verification records from a design study."""
import argparse
import gzip
import json
from pathlib import Path


def collect(work, assets):
    source = json.loads((assets/"provenance.json").read_text())
    data = {"inputs":source["inputs"],"limits":source["resources"],
            "official_archives":{k:source[k] for k in ("generator_sha256","answer_sha256","starter_sha256")},
            "images":{}, "timing":{}, "validation":{}, "sql":{}, "unit":{}, "dispatch":{},
            "comparisons":{}, "parquet":{}, "study_files":{}}
    for path in sorted(work.glob("*.image.json")):
        record=json.loads(path.read_text())
        data["images"][record["tag"]]=record
    for path in sorted(work.rglob("*.jsonl")):
        rows=[json.loads(line) for line in path.read_text(encoding="utf8").splitlines() if line]
        if not rows:
            continue
        rel=path.relative_to(work).as_posix()
        if "seconds" in rows[0]:
            data["timing"][rel]=rows
            if any(r.get("profile") for r in rows) and path.with_suffix(".log").exists():
                choices=[s for s in path.with_suffix(".log").read_text(encoding="utf8").splitlines()
                         if s.startswith("selected scorer ")]
                if choices:
                    data["dispatch"][rel]=choices
        elif "check" in rows[0]:
            record={"checks":{r["check"]:r["passed"] for r in rows}}
            config=path.with_name("config.json")
            if config.is_file():
                record["config"]=json.loads(config.read_text(encoding="utf8"))
            data["validation"][rel]=record
    for path in sorted(work.rglob("*.summary.json")):
        data["comparisons"][path.relative_to(work).as_posix()]=json.loads(path.read_text(encoding="utf8"))
    for path in sorted(work.rglob("pages-*.json")):
        data["parquet"][path.relative_to(work).as_posix()]=json.loads(path.read_text(encoding="utf-8-sig"))
    for path in sorted(work.rglob("*.sql.log")):
        body=path.read_text(encoding="utf8").splitlines()
        if not body:
            continue
        data["sql"][path.relative_to(work).as_posix()]=body
    for path in sorted(work.glob("unit*.log")):
        lines=[s for s in path.read_text(encoding="utf8").splitlines() if "test result:" in s]
        if lines:
            data["unit"][path.name]=lines
    for name in ("compositions.json","stress-inputs/datasets.json","stress-race/summary.json",
                 "winner-inputs/datasets.json","winner-race/summary.json","training.json"):
        if (work/name).is_file():
            data["study_files"][name]=json.loads((work/name).read_text(encoding="utf8"))
    return data


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("work",type=Path)
    p.add_argument("output",type=Path,help="destination .json.gz")
    p.add_argument("--assets",type=Path,default=Path(".buildathon/event-key-v5-a635381"))
    args=p.parse_args()
    data=collect(args.work,args.assets)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    raw=json.dumps(data,ensure_ascii=False,separators=(",",":")).encode()
    args.output.write_bytes(gzip.compress(raw,mtime=0))
    print(f'{sum(map(len,data["timing"].values()))} Docker runs, '
          f'{sum(len(v["checks"]) for v in data["validation"].values())} verification checks, '
          f'{len(data["sql"])} comparison logs')


if __name__=="__main__":
    main()
