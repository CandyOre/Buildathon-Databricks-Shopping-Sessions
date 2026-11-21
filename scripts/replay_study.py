"""Rebuild the measured binaries and regenerate the three public-generator workloads."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parent.parent
BASE = "b7999c6b6b26d623a9622da40b7f491527b6309e"  # Starting Rust source, with the application's Cargo cache cleared on build.
GENERATOR_SHA256 = "13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e"
CASES = [(37, 20), (83, 20), (83, 60)]


def run(*args):
    subprocess.run(list(map(str, args)), cwd=ROOT, check=True)


def build(revision, tag, directory):
    archive = directory / (tag.split(":")[-1] + ".zip")
    run("git", "archive", "--format=zip", "-o", archive, revision, "Dockerfile", "rust")
    context = archive.with_suffix("")
    with ZipFile(archive) as files:
        files.extractall(context)
    run("docker", "buildx", "build", "--platform", "linux/amd64", "--load", "-t", tag, context)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generator_zip", type=Path, help="official generator.zip")
    parser.add_argument("output", type=Path, help="a new folder for logs and Parquet outputs")
    parser.add_argument("--cpuset", default="0,2,4,6,8,10,12,14")
    parser.add_argument("--pairs", type=int, default=1)
    parser.add_argument("--cold-pairs", type=int, default=0)
    args = parser.parse_args()
    if args.pairs < 1 or args.cold_pairs < 0:
        parser.error("pairs must be positive and cold-pairs nonnegative")
    if hashlib.sha256(args.generator_zip.read_bytes()).hexdigest() != GENERATOR_SHA256:
        parser.error("generator archive differs from the one used in the study")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    label = "shopping-replay-" + uuid.uuid4().hex[:12]
    baseline, candidate = label + ":baseline", label + ":candidate"
    print(f"Docker volume {label}; images {baseline} and {candidate}", flush=True)
    run("docker", "volume", "create", label)
    with tempfile.TemporaryDirectory() as temp:
        workspace = Path(temp)
        build(BASE, baseline, workspace)
        build("HEAD", candidate, workspace)
        with ZipFile(args.generator_zip) as files:
            files.extractall(workspace)
        generate = ["cargo build --release --locked --offline"]
        for seed, millions in CASES:
            generate.append(f"/tmp/build/release/generate --seed {seed} --events {millions*1000000} "
                            f"--clicks {millions*250000} --output /data/seed{seed}-{millions}m")
        run("docker", "run", "--rm", "--network", "none",
            "--mount", f"type=bind,source={workspace / 'generator'},target=/src,readonly",
            "--mount", f"type=volume,source={label},target=/data", "-w", "/src",
            "-e", "CARGO_TARGET_DIR=/tmp/build", "rust:1.85-slim-bookworm",
            "sh", "-ec", " && ".join(generate))
    for seed, millions in CASES:
        dataset = f"seed{seed}-{millions}m"
        bench = [sys.executable, ROOT / "scripts/bench_container.py", label, dataset]
        settings = ["--cpuset", args.cpuset, "--verify-cache"]
        reference = output / (dataset + "-baseline")
        tested = output / (dataset + "-candidate")
        run(*bench, baseline, *settings, "--export", reference, "--log", output / "timings.jsonl")
        run(*bench, candidate, *settings, "--export", tested, "--log", output / "timings.jsonl")
        for cache, pairs in [("warm", args.pairs - 1), ("evict", args.cold_pairs)]:
            if not pairs:
                continue
            order = []
            for i in range(pairs):
                order.extend((baseline, candidate) if (pairs-i) % 2 else (candidate, baseline))
            run(*bench, *order, *settings, "--cache", cache, "--log", output / "timings.jsonl")
        run(sys.executable, ROOT / "scripts/compare.py", "--columns", reference, tested)


if __name__ == "__main__":
    main()
