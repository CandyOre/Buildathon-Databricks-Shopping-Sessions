"""Generate official inputs, validate complete scorer images, and time paired container runs."""
import argparse
import json
from pathlib import Path
from statistics import median
import subprocess
import sys

from confirm_design import export
from design_bench import ROOT, run
from design_matrix import call, compare
from v5_event_key_study import GEN, LIMITS


PEER = "sha256:6df7800724931c80750440ab7764b988586663d5ca77c7b42d0917fbaa64d008"
BASE = "sha256:670e51efc5a84de3db1dcc4d4401a6c8c2ebecfa8a5aa72f936914148df17e80"
CASES = {"s83-20m": (83, 20), "s83-60m": (83, 60),
         "intel-final-holdout107-s60m": (107, 60), "arch-s271-60m": (271, 60)}


def capture(*args):
    return run(*args, capture_output=True, text=True).stdout.strip()


def json_file(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf8")


def prepare(work, volume, study, cases):
    source = json.loads((study / "provenance.json").read_text())
    if source["generator_sha256"] != GEN:
        raise ValueError("Unexpected generator archive")
    generator = volume + ":generator"
    generator_id = capture("docker", "image", "inspect", "--format", "{{.Id}}", generator)
    if source["images"][generator] != generator_id:
        raise ValueError("Generator image differs from the official-input study")
    helper = "debian:bookworm-slim"
    root = f"type=volume,source={volume},target=/data"
    for case in cases:
        seed, size = CASES[case]
        exists = subprocess.run(["docker","run","--rm","--network","none","--mount",root,
                                 helper,"test","-d","/data/"+case])
        if exists.returncode == 1:
            # A failed generation leaves only a uniquely named temporary directory.
            import uuid
            temp = "arch-generating-" + uuid.uuid4().hex
            run("docker","run","--rm","--network","none",*LIMITS,"--mount",root,generator_id,
                "--seed",seed,"--events",size*1_000_000,"--clicks",size*250_000,
                "--threads",8,"--output","/data/"+temp)
            run("docker","run","--rm","--network","none","--mount",root,helper,
                "mv","-T","/data/"+temp,"/data/"+case)
        else:
            exists.check_returncode()
        mount = f"type=volume,source={volume},volume-subpath={case},target=/data,readonly"
        digest = capture("docker","run","--rm","--network","none","--read-only",*LIMITS,
                         "--mount",mount,"-w","/data",helper,"bash","-euo","pipefail","-c",
                         "find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum | sha256sum").split()[0]
        if case in source["inputs"] and digest != source["inputs"][case]:
            raise ValueError(f"Input fingerprint mismatch on {case}")
        info = dict(seed=seed, events=size*1_000_000, clicks=size*250_000, sha256=digest,
                    generator_image=generator_id, generator_archive=GEN, volume=volume)
        file = work/(case+".input.json")
        if file.exists() and json.loads(file.read_text()) != info:
            raise ValueError(f"Prepared data changed for {case}")
        json_file(file,info)
        print("Prepared", case, digest, flush=True)


def official(work, study, name, digest):
    path = work/"official"/name
    receipt = path/"accepted.json"
    if receipt.exists() and json.loads(receipt.read_text()) == digest:
        return
    run(sys.executable, ROOT/"scripts/verify_performance_design.py", "--assets",study,
        "--image",digest,"--work",path)
    json_file(receipt,digest)


def reference(work, volume, case):
    directory = work/"reference"/case
    rows = call(work,volume,case,["base"],{"base":{"image":BASE,"env":{}}},
                "reference",export=True,record_peers=True)
    if rows[0]["image_id"] != BASE:
        raise ValueError("Reference image changed")
    return directory


def gate(work, volume, study, name, digest, cases):
    official(work,study,name,digest)
    for case in cases:
        ref = reference(work,volume,case)
        label = "gate/"+name
        rows = call(work,volume,case,[name],{name:{"image":digest,"env":{}}},label,
                    export=True,record_peers=True)
        if rows[0]["image_id"] != digest:
            raise ValueError("Unexpected qualifying image")
        compare(work/label/case,ref)
        print("Qualified",name,case,flush=True)


def race(work, volume, name, digest, case):
    ref = work/"reference"/case
    if name != "base":
        gate_log = work/"gate"/name/(case+".sql.log")
        if not gate_log.is_file() or not gate_log.read_text().rstrip().endswith("MATCH"):
            raise ValueError(f"Qualify {name} on {case} before scoring")
        gate_row=json.loads((work/"gate"/name/(case+".jsonl")).read_text())
        if gate_row["image_id"] != digest:
            raise ValueError("Qualifying and timed images must match")
    else:
        reference(work,volume,case)
    variants = {"P":{"image":PEER,"env":{}},name:{"image":digest,"env":{}}}
    order = ["P",name,name,"P","P",name]
    label = "race/"+name
    rows = call(work,volume,case,order,variants,label,record_peers=True)
    for i,r in enumerate(rows,1):
        if r.get("other_running_containers") or r["image_id"] != variants[r["image"]]["image"]:
            raise ValueError("Unisolated run or image mismatch")
        dest=work/label/(case+"-tables")/(str(i)+"-"+r["image"])
        if not dest.exists():
            export(volume,r,dest)
        compare(dest,ref)
    p,b=([r["seconds"] for r in rows if r["image"]==side] for side in ("P",name))
    stats=dict(case=case,scorer=name,scorer_id=digest,peer_id=PEER,order=order,peer=p,scorer_times=b,
               peer_median=median(p),scorer_median=median(b),gap_s=median(b)-median(p),
               paired_wins=sum(y<x for x,y in zip(p,b)),matches=len(rows))
    json_file(work/label/(case+".summary.json"),stats)
    print(json.dumps(stats),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("work",type=Path)
    p.add_argument("action",choices=["prepare","reference","gate","race","profile"])
    p.add_argument("--study",type=Path,default=ROOT/".buildathon/event-key-v5-a635381")
    p.add_argument("--cases",choices=CASES,nargs="+",default=list(CASES))
    p.add_argument("--version",action="append",metavar="NAME=IMAGE")
    opts=p.parse_args()
    work=opts.work.resolve()
    work.mkdir(parents=True,exist_ok=True)
    study=opts.study.resolve()
    volume=(study/"volume").read_text().strip()
    if opts.action=="prepare":
        prepare(work,volume,study,opts.cases)
        return
    if opts.action=="reference":
        for case in opts.cases:
            reference(work,volume,case)
        return
    import re
    for item in opts.version or ["base="+BASE]:
        name, _, image=item.partition("=")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+",name) or not image or name=="P":
            p.error("Use a unique version label and image")
        digest=capture("docker","image","inspect","--format","{{.Id}}",image)
        if name=="base" and digest!=BASE:
            raise ValueError("Baseline digest changed")
        if opts.action=="gate":
            gate(work,volume,study,name,digest,opts.cases)
        else:
            for case in opts.cases:
                if opts.action=="race":
                    race(work,volume,name,digest,case)
                else:
                    diagnostic = {"XL_TIMING":"1","V1ND_LOG":"1"} if digest == PEER else {}
                    call(work,volume,case,[name],{name:{"image":digest,"env":diagnostic}},
                         "profile/"+name,profile=True,record_peers=True)


if __name__=="__main__":
    main()
