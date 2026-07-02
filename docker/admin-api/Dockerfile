# syntax=docker/dockerfile:1

ARG BASE_IMAGE=tradingbutler-base:latest
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS builder

ARG TARGETARCH
ARG APP_NAME=admin-api

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY sdk ./sdk

RUN RUST_TARGET="$(/get_rust_target.sh ${TARGETARCH})" && \
    rustup target add "${RUST_TARGET}" && \
    cargo install --locked --bin ${APP_NAME} --path crates/${APP_NAME} --target "${RUST_TARGET}" && \
    cp target/"${RUST_TARGET}"/release/${APP_NAME} /app-bin

FROM gcr.io/distroless/static-debian13:nonroot AS runtime

COPY --from=builder /app-bin /app/service

USER nonroot

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/service", "healthcheck"]

ENTRYPOINT ["/app/service"]
CMD ["start"]
