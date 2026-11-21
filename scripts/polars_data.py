#!/usr/bin/env python3
"""Build the pinned official generator, populate a fresh Docker volume and hash every file."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
from zipfile import ZipFile


ARCHIVES = {
    "generator.zip": "13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e",
    "dev-answer.zip": "7eb8e1f0b1f431d178d28ac9437ce85809c44eb6393fdd42fb59857f30834fa9",
}
DATASETS = [("events-1m", 1, 1_000_000, 250_000),
            ("events-20m", 53, 20_000_000, 5_000_000),
            ("events-60m", 37, 60_000_000, 15_000_000)]
IMAGE = "rust:1.85-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36"


def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, **kwargs)


def fingerprint(volume, directory, cpuset):
    # Sort paths bytewise before hashing; every input filename is generator-owned ASCII.
    command = ('cd "/data/$1" && export LC_ALL=C && '
               'find . -type f -print0 | sort -z | xargs -0 -r sha256sum')
    result = docker("run", "--rm", "--platform", "linux/amd64", "--network", "none",
                    "--cpuset-cpus", cpuset, "--mount", f"type=volume,source={volume},target=/data,readonly",
                    IMAGE, "bash", "-euo", "pipefail", "-c", command, "hash", directory,
                    stdout=subprocess.PIPE)
    if not result.stdout:
        raise ValueError(f"Empty input directory: {volume}/{directory}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", type=Path, default=Path(".buildathon"),
                        help="directory containing official generator.zip and dev-answer.zip")
    parser.add_argument("--volume", required=True, help="new volume for the generated input")
    parser.add_argument("--compare-volume", help="existing input volume with the same three subdirectories")
    parser.add_argument("--report", type=Path, help="write verified input fingerprints as JSON")
    parser.add_argument("--cpuset", default="0-7", help="logical CPU IDs used for generation and checksums")
    args = parser.parse_args()
    hashes = {}
    for filename, expected in ARCHIVES.items():
        with (args.archives / filename).open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != expected:
            raise SystemExit(f"{filename} has SHA256 {digest}; expected {expected}")
        hashes[filename] = digest
    if args.compare_volume:
        docker("volume", "inspect", args.compare_volume, stdout=subprocess.DEVNULL)
    volumes = docker("volume", "ls", "--format", "{{.Name}}", capture_output=True, text=True)
    if args.volume in volumes.stdout.splitlines():
        raise SystemExit(f"Volume {args.volume} already exists; supply a fresh volume name.")
    docker("volume", "create", args.volume)
    with tempfile.TemporaryDirectory(prefix="shopping-generator-") as temporary:
        with ZipFile(args.archives / "generator.zip") as archive:
            archive.extractall(temporary)
        generator = Path(temporary).resolve() / "generator"
        # Each mkdir also guards against competing generators writing the same dataset.
        script = """
            cargo build --locked --offline --release --manifest-path /generator/Cargo.toml --target-dir /tmp/target
            while [ "$#" -gt 0 ]; do
                mkdir "/data/$1"
                echo "Generating $1 (seed $2)"
                /tmp/target/release/generate --seed "$2" --events "$3" --clicks "$4" --threads 8 --output "/data/$1"
                shift 4
            done
        """
        dataset_args = [str(value) for dataset in DATASETS for value in dataset]
        docker("run", "--rm", "--platform", "linux/amd64", "--network", "none",
               "--cpuset-cpus", args.cpuset, "--cpus", "8", "--memory", "16g", "--memory-swap", "16g",
               "--mount", f"type=bind,source={generator},target=/generator,readonly",
               "--mount", f"type=volume,source={args.volume},target=/data",
               IMAGE, "sh", "-ec", script, "generate", *dataset_args)
    entries = {}
    for name, seed, events, clicks in DATASETS:
        actual = fingerprint(args.volume, name, args.cpuset)
        if args.compare_volume:
            expected = fingerprint(args.compare_volume, name, args.cpuset)
            if actual != expected:
                diff = set(actual.splitlines()).symmetric_difference(expected.splitlines())
                raise SystemExit(f"{name}: SHA256 manifest mismatch: {sorted(diff)[:4]!r}")
        entries[name] = dict(seed=seed, events=events, clicks=clicks,
                             files=len(actual.splitlines()), manifest_sha256=hashlib.sha256(actual).hexdigest())
        print(f"{name}: {entries[name]['files']} files, {entries[name]['manifest_sha256']}", flush=True)
    report = dict(archives_sha256=hashes, generator_image=IMAGE, datasets=entries,
                  manifest_format="LC_ALL=C, sorted ./relative/path names, lowercase sha256, two spaces, path, LF",
                  compared_volume=args.compare_volume)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    else:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
