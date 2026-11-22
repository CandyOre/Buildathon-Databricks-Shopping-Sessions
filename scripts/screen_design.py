"""Screen switchable variants in adjacent full-container pairs before six-run confirmation."""
import argparse
import json
from pathlib import Path
from statistics import median
import tempfile

from confirm_design import export
from design_matrix import TRIALS, call, compare, settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("image")
    parser.add_argument("names", nargs="+")
    parser.add_argument("--case", required=True)
    parser.add_argument("--study", type=Path, default=Path(".buildathon/event-key-v5-a635381"))
    parser.add_argument("--settings", type=Path)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--verify-retained", action="store_true",
                        help="validate previously timed B outputs on the study volume without rerunning the scorer")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("positive repeats required")
    if args.settings:
        TRIALS.update(json.loads(args.settings.read_text()))
    volume = (args.study / "volume").read_text().strip()
    if args.verify_retained:
        source=args.work/"pairs"/(args.case+".jsonl")
        rows=[json.loads(s) for s in source.read_text().splitlines()]
        dest=args.work/"checks"/args.case
        dest.mkdir(parents=True,exist_ok=True)
        for name in args.names:
            matched=[r for r in rows if r["image"]==name+"-B" and r["image_ref"]==args.image and r["volume"]==volume]
            if not matched:
                raise ValueError(f"No retained candidate output for {name}")
            with tempfile.TemporaryDirectory(dir=dest) as tmp:
                tables=Path(tmp)/"tables"
                export(volume,matched[-1],tables)
                try:
                    compare(tables,args.study/"tables"/args.case/"b")
                finally:
                    if tables.with_suffix(".sql.log").exists():
                        tables.with_suffix(".sql.log").replace(dest/(name+".sql.log"))
            print(name,args.case,"MATCH",flush=True)
        return
    variants = {}
    order = []
    for n in range(args.repeats):
        for i, name in enumerate(args.names):
            parent = TRIALS[name][0] or "control"
            pair = [name+"-A", name+"-B"]
            for label, variant in zip(pair,(parent,name)):
                variants[label] = dict(image=args.image, env=settings(variant))
            order.extend(reversed(pair) if (n+i)%2 else pair)
    rows = call(args.work.resolve(),volume,args.case,order,variants,"pairs",case_config=True)
    results = []
    for name in args.names:
        a, b = ([r["seconds"] for r in rows if r["image"]==name+"-"+label] for label in "AB")
        results.append(dict(name=name, parent=TRIALS[name][0], A=a, B=b,
                            seconds_saved=median(a)-median(b), gain=1-median(b)/median(a)))
    results.sort(key=lambda x: -x["gain"])
    dest = args.work/"pairs"/(args.case+".summary.json")
    dest.write_text(json.dumps(results, indent=2))
    for r in results:
        print(f'{r["name"]:18s} {r["gain"]:+7.1%}   {r["A"]} -> {r["B"]}')


if __name__ == "__main__":
    main()
