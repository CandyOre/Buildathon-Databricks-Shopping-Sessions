"""Generate official inputs, validate complete scorer images, and time paired container runs."""
import argparse
import json
from pathlib import Path
import re
from statistics import median
import subprocess
import sys
import uuid

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
    temp = path.with_name(path.name + "-" + uuid.uuid4().hex + ".tmp")
    temp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf8")
    temp.replace(path)


HELPER = "debian:bookworm-slim"


def inspect_input(volume, case):
    mount = f"type=volume,source={volume},volume-subpath={case},target=/data,readonly"
    # The SHA format matches the official-input study. Each source file has
    # one header and the generator terminates all rows with LF.
    script = r"""
test -f manifest.json
find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum | sha256sum
for dir in events clicks; do
    find "$dir" -maxdepth 1 -type f -name '*.csv' -print0 |
        xargs -0 -r -n 1 wc -l | awk '{ n += $1 - 1 } END { printf "%.0f\n", n }'
done
"""
    result = capture("docker", "run", "--rm", "--pull=never", "--network", "none",
                     "--read-only", *LIMITS, "--mount", mount, "-w", "/data",
                     HELPER, "bash", "-euo", "pipefail", "-c", script).splitlines()
    digest = result[0].split()[0]
    if len(result) != 3 or not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError(f"Invalid fingerprint for {case}")
    events, clicks = map(int, result[1:])
    if min(events, clicks) <= 0:
        raise ValueError(f"Empty generator inputs for {case}")
    return {"sha256": digest, "event_csv_rows": events, "click_csv_rows": clicks}


def replay(volume, case, generator_id, install):
    seed, size = CASES[case]
    token = uuid.uuid4().hex
    label = "shopping.arch-replay"
    # An unnamed volume is always a new Docker-owned volume. The label check
    # also prevents cleanup from addressing an unrelated persistent volume.
    fresh = capture("docker", "volume", "create", "--label", f"{label}={token}")
    if not re.fullmatch(r"[a-f0-9]{64}", fresh):
        raise ValueError(f"Unexpected temporary volume ID {fresh!r}")
    try:
        run("docker", "run", "--rm", "--pull=never", "--network", "none", *LIMITS,
            "--mount", f"type=volume,source={fresh},target=/verify", generator_id,
            "--seed", seed, "--events", size * 1_000_000, "--clicks", size * 250_000,
            "--threads", 8, "--output", "/verify/generated")
        expected = inspect_input(fresh, "generated")
        if install:
            # Publish only after a complete copy. A simultaneous publisher
            # may have finished first; prepare checks the winning tree below.
            run("docker", "run", "--rm", "--pull=never", "--network", "none", *LIMITS,
                "--mount", f"type=volume,source={fresh},volume-subpath=generated,target=/verify,readonly",
                "--mount", f"type=volume,source={volume},target=/study", HELPER,
                "sh", "-ec", 'mkdir "$1"; cp -a /verify/. "$1/"; mv -nT "$1" "$2"',
                "publish", "/study/arch-generating-" + token, "/study/" + case)
        return expected
    finally:
        owner = capture("docker", "volume", "inspect", "--format", '{{index .Labels "' + label + '"}}', fresh)
        if owner != token:
            raise RuntimeError("Refusing to remove a volume without this run's ownership label")
        run("docker", "volume", "rm", fresh)


def prepare(work, volume, study, cases):
    source = json.loads((study / "provenance.json").read_text())
    if source["generator_sha256"] != GEN:
        raise ValueError("Unexpected generator archive")
    generator = volume + ":generator"
    generator_id = capture("docker", "image", "inspect", "--format", "{{.Id}}", generator)
    if source["images"][generator] != generator_id:
        raise ValueError("Generator image differs from the official-input study")
    root = f"type=volume,source={volume},target=/data,readonly"
    receipts = {}
    for case in cases:
        seed, size = CASES[case]
        file = work / (case + ".input.json")
        previous = json.loads(file.read_text()) if file.exists() else {}
        params = dict(seed=seed, events=size*1_000_000, clicks=size*250_000,
                      generator_image=generator_id, generator_archive=GEN, volume=volume)
        if previous and any(previous.get(k) != v for k, v in params.items()):
            raise ValueError(f"Existing input receipt has different generator arguments for {case}")
        known = source["inputs"].get(case)
        proof = "official-input-study" if known else "independent-official-replay"
        verified = previous.get("proof") == proof and previous.get("case") == case
        exists = subprocess.run(["docker", "run", "--rm", "--pull=never", "--network", "none",
                                 "--mount", root, HELPER, "test", "-d", "/data/" + case])
        if exists.returncode not in (0, 1):
            exists.check_returncode()
        regenerated = None
        if exists.returncode or (not known and not verified):
            regenerated = replay(volume, case, generator_id, install=bool(exists.returncode))
        actual = inspect_input(volume, case)
        if ((known and actual["sha256"] != known)
            or (previous and actual["sha256"] != previous["sha256"])
            or (regenerated and actual != regenerated)):
            raise ValueError(f"Official generator or previous fingerprint disagrees for {case}")
        info = dict(case=case, proof=proof, **params, **actual)
        if verified and previous != info:
            raise ValueError(f"Verified input receipt no longer matches for {case}")
        if previous != info:
            json_file(file, info)
        receipts[case] = info
        print("Verified", case, proof, actual["sha256"], flush=True)
    return receipts


def official(work, study, name, digest):
    path = work/"official"/name
    receipt = path/"accepted.json"
    if receipt.exists() and json.loads(receipt.read_text()) == digest:
        return
    run(sys.executable, ROOT/"scripts/verify_performance_design.py", "--assets",study,
        "--image",digest,"--work",path)
    json_file(receipt,digest)


def bound_label(work, label, case):
    location = work / label
    binding = location / (case + ".input.json")
    if not binding.exists() and any((location / (case + ext)).exists() for ext in (".jsonl", ".running")):
        # Earlier experiments predate the input-to-run binding. Preserve
        # those records and qualify/timestamp this dataset in a fresh path.
        return label + "/verified"
    return label


def measured(work, receipt, order, variants, label, **options):
    volume, case = receipt["volume"], receipt["case"]
    label = bound_label(work, label, case)
    directory = work / label
    directory.mkdir(parents=True, exist_ok=True)
    binding = directory / (case + ".input.json")
    if binding.exists():
        if json.loads(binding.read_text()) != receipt:
            raise ValueError(f"Run in {directory} is already bound to a different input")
    else:
        if (directory / (case + ".jsonl")).exists():
            raise ValueError(f"Unbound legacy records in {directory}")
        json_file(binding, receipt)
    rows = call(work, volume, case, order, variants, label, **options)
    for row in rows:
        if (row["dataset"] != case or row["volume"] != volume
                or row["image_id"] != variants[row["image"]]["image"]):
            raise ValueError(f"Recorded image, dataset or volume differs in {directory}")
    return rows, directory


def reference(work, receipt):
    rows, directory = measured(work, receipt, ["base"], {"base":{"image":BASE,"env":{}}},
                               "reference", export=True, record_peers=True)
    if rows[0]["image_id"] != BASE:
        raise ValueError("Reference image changed")
    return directory / receipt["case"]


def gate(work, study, name, digest, receipts):
    expected = check_entry(name, digest)
    official(work,study,name,digest)
    for case, receipt in receipts.items():
        ref = reference(work,receipt)
        label = "gate/"+name
        rows, directory = measured(work,receipt,[name],{name:{"image":digest,"env":{}}},label,
                                   export=True,record_peers=True)
        if rows[0]["image_id"] != digest or rows[0].get("architecture") != expected:
            raise ValueError("Qualifying image or observed pipeline differs")
        compare(directory/case,ref)
        print("Qualified",name,case,flush=True)


def check_entry(name, digest):
    if name == "base" and digest == BASE:
        return None
    if name[-1:] not in {"A", "B"}:
        raise ValueError("Candidate labels must end in the selected A or B pipeline")
    env = json.loads(capture("docker", "image", "inspect", "--format", "{{json .Config.Env}}", digest))
    expected = name[-1]
    if f"PERF_ARCH={expected}" not in env:
        raise ValueError(f"{name} image default differs from requested pipeline {expected}")
    return expected


def race(work, receipt, name, digest):
    volume, case = receipt["volume"], receipt["case"]
    expected = check_entry(name, digest)
    ref = reference(work,receipt)
    if name != "base":
        gate_dir = work / bound_label(work, "gate/" + name, case)
        gate_log = gate_dir / (case+".sql.log")
        if not gate_log.is_file() or not gate_log.read_text().rstrip().endswith("MATCH"):
            raise ValueError(f"Qualify {name} on {case} before scoring")
        gate_row=json.loads((gate_dir/(case+".jsonl")).read_text())
        binding=json.loads((gate_dir/(case+".input.json")).read_text())
        if (binding != receipt or gate_row["image_id"] != digest
                or gate_row["volume"] != volume or gate_row["dataset"] != case
                or gate_row.get("architecture") != expected):
            raise ValueError("Qualifying and timed images, pipelines and input receipts must match")
    variants = {"P":{"image":PEER,"env":{}},name:{"image":digest,"env":{}}}
    order = ["P",name,name,"P","P",name]
    label = "race/"+name
    rows, directory = measured(work,receipt,order,variants,label,record_peers=True)
    for i,r in enumerate(rows,1):
        if (r["other_running_containers"] or r["image_id"] != variants[r["image"]]["image"]
                or r["input_page_bytes"] <= 0
                or (r["image"] == name and r.get("architecture") != expected)):
            raise ValueError("Unisolated run or image mismatch")
        dest=directory/(case+"-tables")/(str(i)+"-"+r["image"]+"-"+Path(r["output"]).name)
        if not dest.exists():
            export(volume,r,dest)
        compare(dest,ref)
    p,b=([r["seconds"] for r in rows if r["image"]==side] for side in ("P",name))
    stats=dict(case=case,scorer=name,scorer_id=digest,peer_id=PEER,order=order,peer=p,scorer_times=b,
               peer_median=median(p),scorer_median=median(b),gap_s=median(b)-median(p),
               paired_wins=sum(y<x for x,y in zip(p,b)),matches=len(rows),input_receipt=receipt)
    json_file(directory/(case+".summary.json"),stats)
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
    receipts=prepare(work,volume,study,opts.cases)
    if opts.action=="prepare":
        return
    if opts.action=="reference":
        for receipt in receipts.values():
            reference(work,receipt)
        return
    for item in opts.version or ["base="+BASE]:
        name, _, image=item.partition("=")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+",name) or not image or name=="P":
            p.error("Use a unique version label and image")
        digest=capture("docker","image","inspect","--format","{{.Id}}",image)
        if name=="base" and digest!=BASE:
            raise ValueError("Baseline digest changed")
        if opts.action=="gate":
            gate(work,study,name,digest,receipts)
        else:
            for case in opts.cases:
                if opts.action=="race":
                    race(work,receipts[case],name,digest)
                else:
                    diagnostic = {"XL_TIMING":"1","V1ND_LOG":"1"} if digest == PEER else {}
                    measured(work,receipts[case],[name],{name:{"image":digest,"env":diagnostic}},
                             "profile/"+name,profile=True,record_peers=True)


if __name__=="__main__":
    main()
