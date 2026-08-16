FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/stocks /usr/local/bin/stocks
RUN mkdir -p /data
VOLUME /data
EXPOSE 8080
ENV DB_PATH=/data/stocks.db
ENTRYPOINT ["/usr/local/bin/stocks"]
