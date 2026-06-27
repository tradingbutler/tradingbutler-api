FROM rust:1-slim AS build
WORKDIR /app
COPY Cargo.toml ./
COPY crates ./crates
RUN cargo build --release -p server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/server /usr/local/bin/server
ENV PORT=8080
EXPOSE 8080
CMD ["server"]
