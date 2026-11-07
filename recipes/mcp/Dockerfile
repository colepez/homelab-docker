# Two-stage build: compile in the official Rust image, run from slim Debian as a non-root user.
FROM rust:1-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked 2>/dev/null || cargo build --release

FROM debian:trixie-slim
RUN useradd -r -u 10002 recipes-mcp
COPY --from=build /src/target/release/recipes-mcp /usr/local/bin/recipes-mcp
USER recipes-mcp
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD ["/usr/local/bin/recipes-mcp", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/recipes-mcp"]
