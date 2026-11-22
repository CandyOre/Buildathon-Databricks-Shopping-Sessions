FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/large-b53443c7bca298bca2b33506bd4defa81419f2ee3e4cd8341728bfbcb3a7715a.profdata /profile/large.profdata
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo clean --release -p shopping-sessions && \
    cargo build --locked --release && cp target/release/shopping-sessions /portable && \
    if [ "$(uname -m)" = "x86_64" ]; then cargo rustc --locked --release -- -Ctarget-cpu=x86-64-v3 -Ctarget-feature=+aes && cp target/release/shopping-sessions /optimized && RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo rustc --release --locked --features large-input --bin shopping-sessions -- -Cprofile-use=/profile/large.profdata && cp target/release/shopping-sessions /large; else cp /portable /optimized && cp /portable /large; fi
COPY rust/dispatch.rs /tmp/dispatch.rs
RUN rustc --edition=2024 -O /tmp/dispatch.rs -o /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
COPY --from=build /optimized /usr/local/bin/optimized
COPY --from=build /large /usr/local/bin/large
COPY --from=build /portable /usr/local/bin/portable
ENV MIMALLOC_PURGE_DELAY=10000
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
