ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
FROM rust:1.85-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS build
ARG SCORER_LTO=thin
ARG SCORER_STATIC=0
ARG SCORER_CASCADE=0
ARG SCORER_CASCADE_PKU=1
ARG SCORER_PGO=1
ARG SCORER_PROFILE=large-b53443c7bca298bca2b33506bd4defa81419f2ee3e4cd8341728bfbcb3a7715a.profdata
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/${SCORER_PROFILE} /profile/large.profdata
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,sharing=locked \
    set -eu; export CARGO_PROFILE_RELEASE_LTO="$SCORER_LTO"; \
    static=""; if [ "$SCORER_STATIC" = 1 ]; then static=-Ctarget-feature=+crt-static; fi; \
    cargo clean --release -p shopping-sessions; \
    cargo rustc --locked --release -- $static; cp target/release/shopping-sessions /portable; \
    if [ "$(uname -m)" = "x86_64" ]; then \
      cargo rustc --locked --release -- -Ctarget-cpu=x86-64-v3 -Ctarget-feature=+aes $static; cp target/release/shopping-sessions /optimized; \
      pgo=""; if [ "$SCORER_PGO" = 1 ]; then pgo=-Cprofile-use=/profile/large.profdata; fi; \
      RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo rustc --release --locked --features large-input --bin shopping-sessions -- $pgo $static; cp target/release/shopping-sessions /large; \
      if [ "$SCORER_CASCADE" = 1 ]; then \
        cpu="-Ctarget-cpu=cascadelake"; if [ "$SCORER_CASCADE_PKU" = 0 ]; then cpu="$cpu -Ctarget-feature=-pku"; fi; \
        RUSTFLAGS="$cpu" cargo rustc --release --locked --features large-input --bin shopping-sessions -- $pgo $static; cp target/release/shopping-sessions /cascade; \
      else ln /large /cascade; fi; \
    else cp /portable /optimized; cp /portable /large; ln /large /cascade; fi
COPY rust/dispatch.rs /tmp/dispatch.rs
RUN set -eu; flags=""; if [ "$SCORER_STATIC" = 1 ]; then flags=-Ctarget-feature=+crt-static; fi; \
    SCORER_CASCADE="$SCORER_CASCADE" SCORER_CASCADE_PKU="$SCORER_CASCADE_PKU" rustc --edition=2024 -O $flags /tmp/dispatch.rs -o /shopping-sessions; \
    if [ "$SCORER_STATIC" = 1 ]; then command -v readelf >/dev/null; \
      for bin in /shopping-sessions /portable /optimized /large /cascade; do \
        readelf -l -d "$bin" >/tmp/scorer-elf; \
        if grep -Eq 'INTERP|\(NEEDED\)' /tmp/scorer-elf; then echo "dynamic runtime dependency in $bin"; exit 1; fi; \
      done; fi; \
    mkdir /scorers; cp /shopping-sessions /portable /optimized /large /scorers; \
    if [ "$SCORER_CASCADE" = 1 ]; then cp /cascade /scorers; fi

FROM ${RUNTIME_IMAGE}
COPY --from=build /scorers/ /usr/local/bin/
ENV MIMALLOC_PURGE_DELAY=10000
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
