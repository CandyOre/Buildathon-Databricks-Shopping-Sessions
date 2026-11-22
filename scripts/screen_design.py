"""Screen switchable variants in adjacent full-container pairs before six-run confirmation."""
import argparse
import json
from pathlib import Path
from statistics import median

from design_matrix import TRIALS, call, settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("image")
    parser.add_argument("names", nargs="+")
    parser.add_argument("--case", required=True)
    parser.add_argument("--study", type=Path, default=Path(".buildathon/event-key-v5-a635381"))
    parser.add_argument("--settings", type=Path)
    parser.add_argument("--repeats", type=int, default=1)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("positive repeats required")
    if args.settings:
        TRIALS.update(json.loads(args.settings.read_text()))
    volume = (args.study / "volume").read_text().strip()
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
