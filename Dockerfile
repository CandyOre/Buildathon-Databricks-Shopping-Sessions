FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --locked --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
