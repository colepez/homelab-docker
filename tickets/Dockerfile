# Two-stage build: compile in the official Rust image, run from slim Debian as a non-root user.
FROM rust:1-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked 2>/dev/null || cargo build --release

FROM debian:trixie-slim
RUN useradd -r -u 10001 tickets && mkdir /data && chown tickets /data
COPY --from=build /src/target/release/homelab-tickets /usr/local/bin/homelab-tickets
USER tickets
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD ["/usr/local/bin/homelab-tickets", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/homelab-tickets"]
