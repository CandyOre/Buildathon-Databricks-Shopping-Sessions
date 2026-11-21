"""Run serial Docker benchmarks. One JSONL file per dataset, including warmups."""
import argparse
import datetime as dt
import json
import subprocess
import time
import uuid
from pathlib import Path

IMAGES = {
    "native": "shopping-sessions:polars-base-bfc2c1c",
    "dedup": "shopping-sessions:polars-dedup",
    "full": "shopping-sessions:polars-full",
}
ORDER = list(IMAGES)
PROFILE_ROUNDS = {"20m": 3, "60m": 1}


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def ns(timestamp):
    whole, _, fraction = timestamp.removesuffix("Z").partition(".")
    utc = dt.datetime.fromisoformat(whole).replace(tzinfo=dt.timezone.utc)
    return int(utc.timestamp()) * 10**9 + int(fraction.ljust(9, "0"))


def peers():
    return len(docker("ps", "--quiet").splitlines())


def measure(args, label, variant, size):
    image = IMAGES[variant]
    profile = label.startswith("profile-")
    output = f"polars-{size}-{variant}"
    name = "shopping-bench-" + uuid.uuid4().hex[:12]
    before = peers()
    flags = ["--platform", "linux/amd64", "--network", "none", "--read-only",
             "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--tmpfs", "/scratch:ro,size=64m",
             "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "4096",
             "--cpuset-cpus", "0-7", "--cpus", "8", "--memory", "24g", "--memory-swap", "24g",
             "--user", "1000:1000", "-e", "NCPU=8", "-e", "MEM_GB=24",
             "--mount", f"type=volume,src={args.input_volume},dst=/data,volume-subpath=events-{size},readonly",
             "--mount", f"type=volume,src={args.output_volume},dst=/output,volume-subpath={output}"]
    if profile:
        flags.extend(["-e", "PROFILE=1"])
    started = time.perf_counter()
    result = subprocess.run(["docker", "run", "--name", name, *flags, image],
                            capture_output=True, text=True)
    wall = time.perf_counter() - started
    if result.returncode:
        raise RuntimeError(f"{name} ({result.returncode}): {result.stderr}")
    try:
        info, = json.loads(docker("inspect", name))
        state = info["State"]
        assert state["ExitCode"] == 0 and not state["OOMKilled"]
        stages = [json.loads(line[8:]) for line in result.stderr.splitlines()
                  if line.startswith("profile ")]
        if profile:
            assert stages[-1]["stage"] == "total" and stages[-1]["container_peak_bytes"]
        activity = docker("events", "--since", state["StartedAt"], "--until", state["FinishedAt"],
                        "--filter", "type=container", "--filter", "event=start", "--filter", "event=die",
                        "--format", "{{.Actor.Attributes.name}}").splitlines()
        return dict(size=size, run=label, variant=variant, image_id=info["Image"],
                    started=state["StartedAt"], finished=state["FinishedAt"],
                    container_s=(ns(state["FinishedAt"]) - ns(state["StartedAt"])) / 1e9,
                    cli_s=wall, peers_before=before, peers_after=peers(),
                    peers_during=len(set(activity) - {name}),
                    event_summary=result.stderr.strip().splitlines()[-1], stages=stages)
    finally:
        docker("rm", name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("size", choices=["20m", "60m"])
    parser.add_argument("log", type=Path, help="new JSONL file")
    parser.add_argument("--input-volume", default="shopping-sessions-bench")
    parser.add_argument("--output-volume", default="shopping-sessions-bench-out")
    parser.add_argument("--profile-rounds", type=int, choices=(1, 2, 3),
                        help="number of additional instrumented rounds after the three timed rounds")
    parser.add_argument("--quiet", action="store_true",
                        help="wait for an idle Docker host and retry runs overlapping other containers")
    args = parser.parse_args()
    profile_rounds = args.profile_rounds or PROFILE_ROUNDS[args.size]
    args.log.parent.mkdir(parents=True, exist_ok=True)
    paths = [f"/output/polars-{args.size}-{v}" for v in ORDER]
    with args.log.open("x", encoding="utf-8") as log:
        docker("run", "--rm", "--network", "none", "--read-only", "--user", "0:0",
               "--mount", f"type=volume,src={args.output_volume},dst=/output", IMAGES["native"],
               "sh", "-ec", 'for path do mkdir -p "$path"; chown 1000:1000 "$path"; done', "sh", *paths)
        runs = [("warmup", ORDER)]
        runs.extend((f"round-{i + 1}", ORDER[i:] + ORDER[:i]) for i in range(3))
        runs.extend((f"profile-{i + 1}", ORDER[i:] + ORDER[:i]) for i in range(profile_rounds))
        for label, variants in runs:
            for variant in variants:
                while True:
                    if args.quiet:
                        while peers():
                            time.sleep(3)
                    r = measure(args, label, variant, args.size)
                    overlap = r["peers_before"] or r["peers_during"] or r["peers_after"]
                    r["accepted"] = not (args.quiet and overlap)
                    print(json.dumps(r), file=log, flush=True)
                    if r["accepted"]:
                        break
                    print(f"Retrying {label} {variant} after Docker overlap", flush=True)
                print(f"{args.size} {label} {variant}: {r['container_s']:.3f}s", flush=True)


if __name__ == "__main__":
    main()
