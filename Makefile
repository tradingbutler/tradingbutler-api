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
	cargo run -p collector

json-writer:
	cargo run -p json-writer

admin-api:
	HTTP_PORT=20001 cargo run -p admin-api

rate-streamer:
	HTTP_PORT=20002 cargo run -p rate-streamer

prod:
	cargo build --release --all-features

.PHONY: dev collector prod format lint test
