ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM rust:1.85-slim-bookworm AS build
ARG SCORER_LTO=thin
ARG SCORER_STATIC=0
ARG SCORER_CASCADE=0
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/large-b53443c7bca298bca2b33506bd4defa81419f2ee3e4cd8341728bfbcb3a7715a.profdata /profile/large.profdata
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,sharing=locked \
    set -eu; export CARGO_PROFILE_RELEASE_LTO="$SCORER_LTO"; \
    static=""; if [ "$SCORER_STATIC" = 1 ]; then static=-Ctarget-feature=+crt-static; fi; \
    cargo clean --release -p shopping-sessions; \
    cargo rustc --locked --release -- $static; cp target/release/shopping-sessions /portable; \
    if [ "$(uname -m)" = "x86_64" ]; then \
      cargo rustc --locked --release -- -Ctarget-cpu=x86-64-v3 -Ctarget-feature=+aes $static; cp target/release/shopping-sessions /optimized; \
      RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo rustc --release --locked --features large-input --bin shopping-sessions -- -Cprofile-use=/profile/large.profdata $static; cp target/release/shopping-sessions /large; \
      if [ "$SCORER_CASCADE" = 1 ]; then \
        RUSTFLAGS="-Ctarget-cpu=cascadelake" cargo rustc --release --locked --features large-input --bin shopping-sessions -- $static; cp target/release/shopping-sessions /cascade; \
      else ln /large /cascade; fi; \
    else cp /portable /optimized; cp /portable /large; ln /large /cascade; fi
COPY rust/dispatch.rs /tmp/dispatch.rs
RUN set -eu; flags=""; if [ "$SCORER_STATIC" = 1 ]; then flags=-Ctarget-feature=+crt-static; fi; \
    SCORER_CASCADE="$SCORER_CASCADE" rustc --edition=2024 -O $flags /tmp/dispatch.rs -o /shopping-sessions; \
    if [ "$SCORER_STATIC" = 1 ]; then for bin in /shopping-sessions /portable /optimized /large /cascade; do \
      if readelf -l "$bin" | grep -q INTERP; then echo "dynamic interpreter in $bin"; exit 1; fi; done; fi; \
    mkdir /scorers; cp /shopping-sessions /portable /optimized /large /scorers; \
    if [ "$SCORER_CASCADE" = 1 ]; then cp /cascade /scorers; fi

FROM ${RUNTIME_IMAGE}
COPY --from=build /scorers/ /usr/local/bin/
ENV MIMALLOC_PURGE_DELAY=10000
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
