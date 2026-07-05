-include .env
export

format:
	cargo fmt --all

lint: format
	cargo clippy --all --all-targets -- -D warnings -D dead_code

test:
	cargo test --all --all-features

dev:
	cargo build --all-features

collector:
	ID=collector1 HTTP_PORT=15000 cargo run -p collector -- start

admin-api:
	ID=adminapi1 HTTP_PORT=20001 cargo run -p admin-api -- start

rate-streamer:
	ID=ratestreamer1 HTTP_PORT=20002 cargo run -p rate-streamer -- start

json-writer:
	ID=jsonwriter1 HTTP_PORT=20003 cargo run -p json-writer -- start

prod:
	cargo build --release --all-features

docker-base:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  -f docker/base/Dockerfile \
	  -t tradingbutler-base \
	  --load .

docker-collector:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  --build-arg BASE_IMAGE=tradingbutler-base \
	  --build-arg APP_NAME=collector \
	  -f docker/collector/Dockerfile \
	  -t dimitrmok/tradingbutler-collector .

docker-json-writer:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  --build-arg BASE_IMAGE=tradingbutler-base \
	  --build-arg APP_NAME=json-writer \
	  -f docker/json-writer/Dockerfile \
	  -t dimitrmok/tradingbutler-json-writer .

docker-admin-api:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  --build-arg BASE_IMAGE=tradingbutler-base \
	  --build-arg APP_NAME=admin-api \
	  -f docker/admin-api/Dockerfile \
	  -t dimitrmok/tradingbutler-admin-api .

docker-rate-streamer:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  --build-arg BASE_IMAGE=tradingbutler-base \
	  --build-arg APP_NAME=rate-streamer \
	  -f docker/rate-streamer/Dockerfile \
	  -t dimitrmok/tradingbutler-rate-streamer .

.PHONY: dev collector admin-api rate-streamer json-writer prod format lint test \
        docker-base docker-collector docker-json-writer docker-admin-api docker-rate-streamer
