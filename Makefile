APP_NAME := sky-alpha-pro

.PHONY: tidy build test run db-migrate market-sync signal-generate signal-list agent-analyze agent-signals

tidy:
	go mod tidy

build:
	go build -o bin/$(APP_NAME) ./cmd/sky-alpha-pro

test:
	go test ./...

run:
	go run ./cmd/sky-alpha-pro serve --config ./configs/config.yaml

db-migrate:
	go run ./cmd/sky-alpha-pro db migrate --config ./configs/config.yaml

market-sync:
	go run ./cmd/sky-alpha-pro market sync --config ./configs/config.yaml

signal-generate:
	go run ./cmd/sky-alpha-pro signal generate --config ./configs/config.yaml

signal-list:
	go run ./cmd/sky-alpha-pro signal list --config ./configs/config.yaml

agent-analyze:
	go run ./cmd/sky-alpha-pro agent analyze --all --config ./configs/config.yaml

agent-signals:
	go run ./cmd/sky-alpha-pro agent signals --config ./configs/config.yaml
