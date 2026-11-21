"""Time complete, isolated Docker runs on a pre-generated Linux named volume."""
import argparse
import json
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from time import perf_counter


def docker(*args, capture=False):
    result = subprocess.run(["docker", *args], check=True, text=True, capture_output=capture)
    return result.stdout if capture else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("volume")
    parser.add_argument("dataset", help="input subdirectory on the volume")
    parser.add_argument("images", nargs="+", help="image tags in actual run order")
    parser.add_argument("--cpuset", default="0,2,4,6,8,10,12,14")
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--cache", choices=("warm", "evict"), default="warm")
    parser.add_argument("--export", type=Path, help="copy the last run's output to a new directory")
    args = parser.parse_args()
    if args.export and args.export.exists():
        parser.error("--export must refer to a new directory")
    args.log.parent.mkdir(parents=True, exist_ok=True)
    data_mount = f"type=volume,source={args.volume},volume-subpath={args.dataset},target=/data,readonly"
    docker("run", "--rm", "--network", "none", "--mount", data_mount, "--mount",
           f"type=volume,source={args.volume},target=/study", "debian:bookworm-slim", "sh", "-ec",
           "mkdir -p /study/out /study/scratch && chown 1000:1000 /study/out /study/scratch")
    if args.cache == "warm":
        docker("run", "--rm", "--network", "none", "--mount", data_mount, "debian:bookworm-slim",
               "sh", "-ec", "find /data -type f -exec cat {} + >/dev/null")
    for i, image in enumerate(args.images):
        resident_bytes = None
        if args.cache == "evict":
            resident_bytes = int(docker(
                "run", "--rm", "--network", "none", "--mount", data_mount, "debian:bookworm-slim",
                "sh", "-ec",
                """find /data -type f -exec dd 'if={}' iflag=nocache count=0 status=none \\;"""
                """ && find /data -type f -exec fincore -bn -o RES {} + | awk '{n+=$1} END{printf "%.0f", n}'""",
                capture=True).strip())
        run = "shopping-bench-" + uuid.uuid4().hex
        output = "/output/" + run
        command = ["run", "--name", run, "--platform", "linux/amd64", "--network", "none",
                   "--read-only", "--user", "1000:1000", "--cpuset-cpus", args.cpuset,
                   "--cpus", "8", "--memory", "24g", "--memory-swap", "24g",
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                   "--pids-limit", "4096", "--tmpfs", "/tmp:rw,size=64m,mode=1777",
                   "--mount", data_mount, "--mount",
                   f"type=volume,source={args.volume},volume-subpath=out,target=/output",
                   "--mount", f"type=volume,source={args.volume},volume-subpath=scratch,target=/scratch",
                   "-e", "NCPU=8", "-e", "MEM_GB=24", "-e", "OUTPUT_DIR=" + output]
        if args.profile:
            command += ["-e", "PROFILE=1"]
        try:
            start = perf_counter()
            result = subprocess.run(["docker", *command, image], text=True, capture_output=True)
            host_wall = perf_counter() - start
            print(result.stderr, end="", flush=True)
            result.check_returncode()
            container = json.loads(docker("inspect", run, capture=True))[0]
            state = container["State"]
            duration = (datetime.fromisoformat(state["FinishedAt"])
                        - datetime.fromisoformat(state["StartedAt"])).total_seconds()
            row = dict(image=image, image_id=container["Image"], dataset=args.dataset,
                       volume=args.volume, cpuset=args.cpuset, started=state["StartedAt"],
                       finished=state["FinishedAt"], seconds=duration, cli_seconds=host_wall,
                       profile=args.profile, output=output,
                       cache=args.cache, resident_bytes_after_eviction=resident_bytes,
                       stages=[json.loads(line[8:]) for line in result.stderr.splitlines()
                               if line.startswith("profile ")])
            with args.log.open("a", encoding="utf-8") as file:
                print(json.dumps(row), file=file)
            print(f"{image} {args.dataset}  container {duration:.6f}s  CLI {host_wall:.6f}s", flush=True)
            if args.export and i + 1 == len(args.images):
                args.export.parent.mkdir(parents=True, exist_ok=True)
                docker("cp", f"{run}:{output}", str(args.export))
        finally:
            subprocess.run(["docker", "rm", run], capture_output=True)


if __name__ == "__main__":
    main()
