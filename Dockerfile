FROM rust:1.85-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src/ ./src/
RUN cargo build --locked --release --jobs 8 && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS runtime
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV MIMALLOC_PURGE_DELAY=0
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]

FROM runtime AS dedup
ENV PIPELINE=dedup

FROM runtime AS full
ENV PIPELINE=full
