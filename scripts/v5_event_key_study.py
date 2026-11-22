"""Generate the original v5 inputs and reproduce the event-key A/B study."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
CPUSET = "0,2,4,6,8,10,12,14"
LIMITS = ["--cpuset-cpus", CPUSET, "--cpus", "8", "--memory", "24g", "--memory-swap", "24g"]
GEN = "78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a"
ANSWER = "c2439e0e2e7456d721852814e9928e077648803d5c57a1760a40c7d3b0864d20"
STARTER = "b84ff8726c43749043c33c250548a9ac9987b9132d28ac818a47515bd128f77c"
BASELINE = "a63538115ef0f316c6c7dd307b3698a764efb799"
CANDIDATE = "a0f08920c19c21610174852dd41129795f0d33e3"
CASES = {"dev": (1, 1), "s83-20m": (83, 20), "s37-20m": (37, 20), "s83-60m": (83, 60)}


def run(*args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, cwd=ROOT, **kwargs)


def extract(path, digest, target):
    path = Path(path)
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ValueError(f"Unexpected official archive: {path}")
    with zipfile.ZipFile(path) as archive:
        archive.extractall(target)


def init(work, archives):
    work.mkdir(parents=True, exist_ok=False)
    for path, digest, dest in zip(archives, (GEN, ANSWER, STARTER), ("source", "answer", "starter")):
        extract(path, digest, work / dest)
    volume = "shopping-v5-event-key-" + uuid.uuid4().hex[:12]
    run("docker", "volume", "create", volume)
    (work / "volume").write_text(volume, encoding="ascii")
    recipe = work / "source/Dockerfile"
    recipe.write_text("FROM rust:1.85-slim-bookworm\nWORKDIR /src\nCOPY generator/ ./\n"
                      "RUN cargo build --release --locked --offline\n"
                      'ENTRYPOINT ["/src/target/release/generate"]\n', encoding="ascii")
    run("docker", "buildx", "build", "--platform", "linux/amd64", "--load", "-t",
        volume + ":generator", recipe.parent)


def generate(work, volume, cases):
    for case in cases:
        seed, millions = CASES[case]
        marker = work / (case + ".generated")
        if marker.exists():
            continue
        run("docker", "run", "--rm", "--network", "none", *LIMITS,
            "--mount", f"type=volume,source={volume},target=/data", volume + ":generator",
            "--seed", seed, "--events", millions * 1_000_000, "--clicks", millions * 250_000,
            "--threads", 8, "--output", "/data/" + case)
        marker.write_text(f"{GEN} {seed} {millions}\n", encoding="ascii")


def snapshot(work, name, revision):
    raw = work / (name + ".zip")
    run("git", "archive", "--format=zip", "-o", raw, revision, ".dockerignore", "Dockerfile", "rust")
    with zipfile.ZipFile(raw) as archive:
        archive.extractall(work / name)
    return work / name


def build(work, volume, name, revision, mode):
    tag = name if not mode else name + "-" + mode
    context = snapshot(work, tag, revision)
    if mode:
        run(sys.executable, ROOT / "scripts/instrument_event_keys.py", context,
            "--variant", name.upper(), "--mode", mode)
    run("docker", "buildx", "build", "--platform", "linux/amd64", "--load", "-t", volume + ":" + tag, context)


def bench(work, volume, dataset, images, logfile, extra=()):
    run(sys.executable, ROOT / "scripts/bench_container.py", volume, dataset,
        *[volume + ":" + img for img in images], "--cpuset", CPUSET, "--verify-cache",
        "--log", work / logfile, *extra)


def unit_tests(work):
    recipe = """FROM rust:1.85-slim-bookworm
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/src/target,sharing=locked \
    cargo clean --release -p shopping-sessions && cargo test --locked --release && \
    RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo test --locked --release --features large-input
"""
    with (work / "rust-tests.log").open("w", encoding="utf-8") as log:
        run("docker", "buildx", "build", "--platform", "linux/amd64", "--progress", "plain",
            "-f", "-", work / "b", input=recipe, text=True, stdout=log, stderr=subprocess.STDOUT)


def provenance(work, volume, scorer_images=None):
    metadata = {"generator_sha256": GEN, "answer_sha256": ANSWER, "starter_sha256": STARTER}
    tags = ["rust:1.85-slim-bookworm", "debian:bookworm-slim", volume + ":generator",
            *(scorer_images if scorer_images is not None else [volume+":a", volume+":b"])]
    metadata["images"] = {
        tag: run("docker", "image", "inspect", "--format", "{{.Id}}", tag,
                 text=True, capture_output=True).stdout.strip() for tag in tags
    }
    common = ["docker", "run", "--rm", "--network", "none", "--read-only", *LIMITS]
    resources = run(*common, "--mount", f"type=volume,source={volume},target=/data,readonly",
                    "debian:bookworm-slim", "sh", "-ec",
                    "cat /sys/fs/cgroup/cpuset.cpus.effective /sys/fs/cgroup/cpu.max "
                    "/sys/fs/cgroup/memory.max; findmnt -n -o FSTYPE -T /data",
                    text=True, capture_output=True).stdout.splitlines()
    metadata["resources"] = dict(zip(("cpuset", "cpu_max", "memory_max", "filesystem"), resources, strict=True))
    metadata["inputs"] = {}
    for case in CASES:
        mount = f"type=volume,source={volume},volume-subpath={case},target=/data,readonly"
        digest = run(*common, "--mount", mount, "-w", "/data", "debian:bookworm-slim",
                     "bash", "-euo", "pipefail", "-c",
                     "find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum | sha256sum",
                     text=True, capture_output=True).stdout.split()[0]
        metadata["inputs"][case] = digest
    (work / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf8")


def replay(work, archives):
    def step(*args):
        run(sys.executable, Path(__file__).resolve(), work, *args)

    step("init", *archives)
    step("build", "a", BASELINE)
    step("generate", "s83-20m")
    step("baseline", "s83-20m")
    step("generate", "s37-20m", "s83-60m", "dev")
    step("build", "b", CANDIDATE)
    large_cases = [case for case in CASES if case != "dev"]
    for case in large_cases:
        step("bench", case)
    for case in CASES:
        for side in ("a", "b"):
            step("export", case, side)
    step("units")
    run(sys.executable, ROOT / "scripts/verify_v5_event_keys.py", work)
    step("provenance")
    for mode in ("count", "timing"):
        for side, revision in (("a", BASELINE), ("b", CANDIDATE)):
            step("build", side, revision, "--mode", mode)
        for case in large_cases:
            step("diagnose", case, mode)
    run(sys.executable, ROOT / "scripts/summarize_v5_event_keys.py", work, "--output", work / "summary.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path, help="a fresh study folder, reused by each subcommand")
    sub = parser.add_subparsers(dest="op", required=True)
    for op in ("init", "replay"):
        p = sub.add_parser(op)
        p.add_argument("archives", nargs=3, metavar="ZIP", help="official generator, answer, starter (in order)")
    p = sub.add_parser("generate")
    p.add_argument("cases", nargs="+", choices=CASES)
    p = sub.add_parser("build")
    p.add_argument("name", choices=["a", "b"])
    p.add_argument("revision")
    p.add_argument("--mode", choices=["count", "timing"])
    p = sub.add_parser("baseline")
    p.add_argument("dataset", choices=CASES)
    p = sub.add_parser("bench")
    p.add_argument("dataset", choices=CASES)
    p = sub.add_parser("export")
    p.add_argument("dataset", choices=CASES)
    p.add_argument("name", choices=["a", "b"])
    p = sub.add_parser("diagnose")
    p.add_argument("dataset", choices=CASES)
    p.add_argument("mode", choices=["count", "timing"])
    sub.add_parser("units")
    sub.add_parser("provenance").add_argument("--images", nargs="+", help="scorer images used in this study")
    args = parser.parse_args()
    work = args.work.resolve()
    if args.op == "replay":
        replay(work, args.archives)
        return
    if args.op == "init":
        init(work, args.archives)
        return
    volume = (work / "volume").read_text(encoding="ascii").strip()
    if args.op == "generate":
        generate(work, volume, args.cases)
    elif args.op == "build":
        build(work, volume, args.name, args.revision, args.mode)
    elif args.op == "export":
        bench(work, volume, args.dataset, [args.name], "exports.jsonl",
              ["--export", work / "tables" / args.dataset / args.name])
    elif args.op == "baseline":
        bench(work, volume, args.dataset, ["a"], "baseline.jsonl")
    elif args.op == "units":
        unit_tests(work)
    elif args.op == "provenance":
        provenance(work, volume, args.images)
    elif args.op == "bench":
        log = f"times-{args.dataset}.jsonl"
        if (work / log).exists():
            raise FileExistsError(work / log)
        bench(work, volume, args.dataset, "abbaab", log)
    elif args.op == "diagnose":
        log = f"{args.mode}-{args.dataset}.jsonl"
        if (work / log).exists():
            raise FileExistsError(work / log)
        order = "ab" if args.mode == "count" else "abbaab"
        bench(work, volume, args.dataset, [x + "-" + args.mode for x in order], log, ["--profile"])


if __name__ == "__main__":
    main()
