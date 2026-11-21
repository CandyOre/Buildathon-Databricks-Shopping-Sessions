FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo clean --release -p shopping-sessions && \
    cargo build --locked --release && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV MIMALLOC_PURGE_DELAY=0
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
