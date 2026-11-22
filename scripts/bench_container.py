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


def input_residency(data_mount):
    check = (
        'pagesize=$(getconf PAGESIZE); '
        'find /data -type f -exec fincore -bn -o RES,SIZE {} + | '
        """awk -v p="$pagesize" '{r+=$1; n+=int(($2+p-1)/p)*p} END{printf "%.0f %.0f", r,n}'"""
    )
    output = docker("run", "--rm", "--network", "none", "--mount", data_mount,
                    "debian:bookworm-slim", "sh", "-ec", check, capture=True)
    resident, total = output.split()
    return int(resident), int(total)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("volume")
    parser.add_argument("dataset", help="input subdirectory on the volume")
    parser.add_argument("images", nargs="+", help="image tags in actual run order")
    parser.add_argument("--variants", type=Path, help="optional JSON mapping labels to an image and environment")
    parser.add_argument("--cpuset", default="0,2,4,6,8,10,12,14")
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--cache", choices=("warm", "evict"), default="warm")
    parser.add_argument("--verify-cache", action="store_true",
                        help="check resident input pages before each run and log the result")
    parser.add_argument("--export", type=Path, help="copy the last run's output to a new directory")
    args = parser.parse_args()
    variants = json.loads(args.variants.read_text(encoding="utf8")) if args.variants else {}
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
        input_page_bytes = None
        if args.cache == "evict":
            resident_bytes = int(docker(
                "run", "--rm", "--network", "none", "--mount", data_mount, "debian:bookworm-slim",
                "sh", "-ec",
                """find /data -type f -exec dd 'if={}' iflag=nocache count=0 status=none \\;"""
                """ && find /data -type f -exec fincore -bn -o RES {} + | awk '{n+=$1} END{printf "%.0f", n}'""",
                capture=True).strip())
        if args.verify_cache:
            resident_bytes, input_page_bytes = input_residency(data_mount)
            if args.cache == "warm" and resident_bytes != input_page_bytes:
                docker("run", "--rm", "--network", "none", "--mount", data_mount,
                       "debian:bookworm-slim", "sh", "-ec", "find /data -type f -exec cat {} + >/dev/null")
                resident_bytes, input_page_bytes = input_residency(data_mount)
            target = input_page_bytes if args.cache == "warm" else 0
            if resident_bytes != target:
                raise RuntimeError(f"input cache has {resident_bytes} resident bytes, expected {target}")
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
            variant = variants.get(image)
            if variant:
                for key, value in variant.get("env", {}).items():
                    command += ["-e", f"{key}={value}"]
            actual_image = variant["image"] if variant else image
            result = subprocess.run(["docker", *command, actual_image], text=True, capture_output=True)
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
                       cache=args.cache, resident_input_bytes=resident_bytes, input_page_bytes=input_page_bytes,
                       resident_bytes_after_eviction=resident_bytes if args.cache == "evict" else None,
                       stages=[json.loads(line[8:]) for line in result.stderr.splitlines()
                               if line.startswith("profile ")])
            if variant:
                row.update(image_ref=actual_image, environment=variant.get("env", {}))
            if args.profile:
                close = next((s.get("epoch_us") for s in row["stages"] if s["stage"]=="write_parquet"), None)
                if close:
                    row["write_to_exit_s"] = datetime.fromisoformat(state["FinishedAt"]).timestamp() - close / 1e6
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
