"""Train a wide-input scorer profile from an official dataset on a local Docker volume."""
import argparse
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUST = "rust@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36"
DEBIAN = "debian@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc"
RECIPE = f"""FROM {RUST} AS tools
RUN rustup component add llvm-tools-preview
FROM {RUST} AS instrument
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/src/target,sharing=locked cargo clean --release -p shopping-sessions && RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo rustc --release --locked --features large-input --bin shopping-sessions -- -Cprofile-generate=/profiles && cp target/release/shopping-sessions /train
FROM {DEBIAN} AS scorer
COPY --from=instrument /train /usr/local/bin/train
ENV MIMALLOC_PURGE_DELAY=10000
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/train"]
"""


def build(context, target, output):
    subprocess.run(["docker", "buildx", "build", "--platform", "linux/amd64", "--load",
                    "--target", target, "--iidfile", str(output), str(context)], check=True)
    return output.read_text().strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("volume", help="Local Linux volume containing the generated dataset")
    parser.add_argument("dataset", help="Dataset directory inside the volume, e.g. seed37-20m")
    parser.add_argument("output", type=Path, help="New merged .profdata file")
    parser.add_argument("--cpuset", default="0,2,4,6,8,10,12,14")
    parser.add_argument("--setting", action="append", default=[], metavar="PERF_KEY=VALUE")
    args = parser.parse_args()
    if any(not s.startswith("PERF_") or "=" not in s for s in args.setting):
        parser.error("--setting expects PERF_KEY=VALUE")
    settings = [v for s in args.setting for v in ("-e", s)]
    if args.output.exists():
        parser.error("select a new output file")
    with tempfile.TemporaryDirectory(prefix="shopping-pgo-") as temporary:
        work = Path(temporary)
        context = work / "source"
        context.mkdir()
        for name in ("Cargo.toml", "Cargo.lock"):
            shutil.copyfile(ROOT / "rust" / name, context / name)
        shutil.copytree(ROOT / "rust" / "src", context / "src")
        (context / "Dockerfile").write_text(RECIPE, encoding="utf-8")
        tools = build(context, "tools", work / "tools.id")
        scorer = build(context, "scorer", work / "scorer.id")
        counts = work / "counts"
        counts.mkdir()
        counts.chmod(0o777)
        mount = f"type=bind,source={counts},target=/profiles"
        dataset = f"type=volume,source={args.volume},volume-subpath={args.dataset},target=/data,readonly"
        subprocess.run(["docker", "run", "--rm", "--read-only", "--network", "none",
                        "--cpuset-cpus", args.cpuset, "--cpus", "8", "--memory", "24g",
                        "--memory-swap", "24g", "-e", "NCPU=8", "-e", "MEM_GB=24",
                        *settings, "-e", "LLVM_PROFILE_FILE=/profiles/wide-%m-%p.profraw", "--mount", dataset,
                        "--mount", mount, "--tmpfs", "/output:rw,uid=1000,gid=1000,mode=755", scorer],
                       check=True)
        merge = ('prof="$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata"; '
                 '"$prof" merge -sparse -o /profiles/large.profdata /profiles/wide-*.profraw')
        subprocess.run(["docker", "run", "--rm", "--read-only", "--network", "none", "--mount", mount,
                        "--entrypoint", "sh", tools, "-ec", merge], check=True)
        with (counts / "large.profdata").open("rb") as source, args.output.open("xb") as dest:
            shutil.copyfileobj(source, dest)
    with args.output.open("rb") as profile:
        print(hashlib.file_digest(profile, "sha256").hexdigest(), args.output)


if __name__ == "__main__":
    main()
