APP_NAME := sky-alpha-pro

.PHONY: tidy build test run db-migrate market-sync

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
