FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo clean --release -p shopping-sessions && \
    cargo build --locked --release && cp target/release/shopping-sessions /portable && \
    if [ "$(uname -m)" = "x86_64" ]; then cargo rustc --locked --release -- -Ctarget-cpu=x86-64-v3 -Ctarget-feature=+aes && cp target/release/shopping-sessions /optimized && RUSTFLAGS="-Ctarget-cpu=x86-64-v4 -Ctarget-feature=+aes" cargo build --release --locked --features large-input && cp target/release/shopping-sessions /large; else cp /portable /optimized && cp /portable /large; fi
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
