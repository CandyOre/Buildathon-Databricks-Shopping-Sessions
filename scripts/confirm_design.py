"""Confirm an immutable candidate against one or more complete scorer images."""
import argparse
import json
from pathlib import Path
from statistics import median

from design_bench import CASES, run
from design_matrix import call, compare
from design_stress import HELPER, setting


def export(volume, row, target):
    target.mkdir(parents=True)
    run("docker","run","--rm","--network","none","--pull=never","--mount",
        f"type=volume,source={volume},volume-subpath=out,target=/output,readonly",
        "--mount",f"type=bind,source={target.resolve()},target=/export",HELPER,"sh","-ec",
        'cp -R -- "$1"/. /export/',"export",row["output"])


def trial(work, study, volume, case, name, variants, check_each):
    seq = list("ABBAAB")
    rows = call(work,volume,case,seq,variants,name)
    sides = ([r for r in rows if r["image"]==k] for k in "AB")
    a,b = sides
    reference = study/"tables"/case/"b"
    for records in (a,b):
        for i,row in enumerate(records,1):
            if not check_each and i != len(records):
                continue
            dest = work/name/(case+"-tables")/(row["image"]+str(i))
            if not dest.exists():
                export(volume,row,dest)
            compare(dest,reference)
    left,right = ([r["seconds"] for r in records] for records in (a,b))
    ma,mb = median(left),median(right)
    wins = sum(y<x for x,y in zip(left,right))
    stats = dict(case=case, A=left, B=right, median_A=ma, median_B=mb,
                 saved_s=ma-mb, gain=1-mb/ma, paired_wins=wins,
                 paired_savings_s=[x-y for x,y in zip(left,right)],
                 A_image=a[0]["image_id"], B_image=b[0]["image_id"],
                 agrees=((ma>mb)==(wins>=2)), verified_outputs=6 if check_each else 2)
    print(name, json.dumps(stats), flush=True)
    (work/name/(case+".summary.json")).write_text(json.dumps(stats,indent=2))
    return stats


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("work",type=Path)
    p.add_argument("--study",type=Path,default=Path(".buildathon/event-key-v5-a635381"))
    p.add_argument("--candidate",required=True)
    p.add_argument("--against",action="append",required=True,metavar="LABEL=IMAGE")
    p.add_argument("--setting",type=setting,action="append",default=[])
    p.add_argument("--cases",nargs="+",choices=CASES,default=CASES)
    p.add_argument("--check-each",action="store_true")
    opt=p.parse_args()
    work,study=opt.work.resolve(),opt.study.resolve()
    volume=(study/"volume").read_text().strip()
    for entry in opt.against:
        label,sep,image=entry.partition("=")
        if not sep or not label or not label.replace("-","").isalnum() or not image:
            p.error("--against requires a simple label and Docker image")
        variants={"A":{"image":image,"env":{}},"B":{"image":opt.candidate,"env":dict(opt.setting)}}
        for case in opt.cases:
            result=trial(work,study,volume,case,label,variants,opt.check_each)
            if not result["agrees"]:
                trial(work,study,volume,case,label+"-repeat",variants,opt.check_each)


if __name__=="__main__":
    main()
