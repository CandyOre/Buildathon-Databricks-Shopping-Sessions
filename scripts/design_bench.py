"""Build a recorded source revision and compare two intact scorer images on official v5."""
import argparse
import hashlib
import json
from pathlib import Path
from statistics import median
import subprocess
import sys
import zipfile
import shutil

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "scripts/bench_container.py"
CASES = ("s83-20m", "s37-20m", "s83-60m")


def run(*args, **kwargs):
    return subprocess.run(list(map(str, args)), cwd=ROOT, check=True, **kwargs)


def build(work, tag, revision, arguments):
    archive = work / (tag.split(":")[-1] + ".zip")
    context = archive.with_suffix("")
    run("git", "archive", "--format=zip", "-o", archive, revision, ".dockerignore", "Dockerfile", "rust")
    with zipfile.ZipFile(archive) as files:
        files.extractall(context)
    flags = [item for arg in arguments for item in ("--build-arg", arg)]
    with archive.with_suffix(".build.log").open("w", encoding="utf8") as log:
        run("docker", "buildx", "build", "--platform", "linux/amd64", "--load", "--progress", "plain",
            "-t", tag, *flags, context, stdout=log, stderr=subprocess.STDOUT)
    digest = run("docker", "image", "inspect", "--format", "{{.Id}}", tag,
                 text=True, capture_output=True).stdout.strip()
    sha = run("git", "rev-parse", revision, text=True, capture_output=True).stdout.strip()
    print(tag, sha, digest, flush=True)
    archive.with_suffix(".image.json").write_text(json.dumps(dict(tag=tag, commit=sha, image=digest,
        build_args=arguments, archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),indent=2))


def check(work, revision=None):
    if revision:
        sha = run("git","rev-parse",revision,text=True,capture_output=True).stdout.strip()
        ctx = work / ("unit-"+sha[:12])
        archive = ctx.with_suffix(".zip")
        run("git","archive","--format=zip","-o",archive,sha,"rust/Cargo.toml","rust/Cargo.lock","rust/src")
        with zipfile.ZipFile(archive) as files:
            files.extractall(ctx)
    else:
        ctx = work / "unit-context"
        (ctx / "rust").mkdir(parents=True,exist_ok=True)
        for name in ("Cargo.toml","Cargo.lock"):
            shutil.copyfile(ROOT / "rust" / name,ctx / "rust" / name)
        shutil.copytree(ROOT / "rust/src",ctx / "rust/src",dirs_exist_ok=True)
    recipe = """FROM rust:1.85-slim-bookworm
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/src/target,sharing=locked cargo clean --release -p shopping-sessions && cargo test --offline --locked --release && RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo test --release --locked --offline --features large-input
"""
    log_path = ctx.with_suffix(".log") if revision else work/"unit.log"
    with log_path.open("w",encoding="utf8") as log:
        run("docker","buildx","build","--platform","linux/amd64","--progress","plain","-f","-",ctx,
            input=recipe,text=True,stdout=log,stderr=subprocess.STDOUT)


def race(work, volume, name, a, b, cases, reference=None, one=False):
    dest = work / name
    dest.mkdir(parents=True, exist_ok=True)
    for case in cases:
        path = dest / (case + ".jsonl")
        if path.exists():
            raise FileExistsError(path)
        order = [a,b] if one else [a,b,b,a,a,b]
        export = ["--export", dest / case] if reference else []
        with path.with_suffix(".log").open("w", encoding="utf8") as log:
            run(sys.executable, BENCH, volume, case, *order, "--verify-cache", "--log", path, *export,
                stdout=log, stderr=subprocess.STDOUT)
        rows = [json.loads(s) for s in path.read_text().splitlines()]
        assert [r["image"] for r in rows] == order
        assert all(r["resident_input_bytes"] == r["input_page_bytes"] > 0 and r["profile"] is False for r in rows)
        left, right = ([r["seconds"] for r in rows if r["image"]==tag] for tag in (a,b))
        print(name, case, left, right, "median gain %", round(100*(1-median(right)/median(left)),2),
              "paired wins", sum(v<u for u,v in zip(left,right)), flush=True)
        if reference:
            with path.with_suffix(".sql.log").open("w", encoding="utf8") as log:
                run(sys.executable, ROOT/"scripts/check_event_key_tables.py", dest/case, reference/case/"b",
                    stdout=log, stderr=subprocess.STDOUT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    sub = parser.add_subparsers(dest="action", required=True)
    b = sub.add_parser("build")
    b.add_argument("tag")
    b.add_argument("revision")
    b.add_argument("--build-arg", action="append", default=[])
    sub.add_parser("check").add_argument("--revision")
    r = sub.add_parser("race")
    r.add_argument("volume")
    r.add_argument("name")
    r.add_argument("baseline")
    r.add_argument("candidate")
    r.add_argument("--cases", nargs="+", choices=CASES, default=CASES)
    r.add_argument("--reference", type=Path)
    r.add_argument("--one", action="store_true", help="one screening pair")
    args = parser.parse_args()
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    if args.action == "check":
        check(work,args.revision)
    elif args.action == "build":
        build(work,args.tag,args.revision,args.build_arg)
    else:
        race(work,args.volume,args.name,args.baseline,args.candidate,args.cases,args.reference,args.one)


if __name__ == "__main__":
    main()
