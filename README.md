# Sky Alpha Pro

Week 1 bootstrap for the MVP backend:

- project skeleton
- config system (`viper`)
- structured logging (`zap`)
- PostgreSQL connection (`gorm`)
- schema management via GORM `AutoMigrate`

Week 2 delivery:

- API server entry command: `serve`
- health check endpoint: `GET /health` and `GET /api/v1/health`
- request logging middleware (method/path/status/latency/ip/user-agent)

Week 3 delivery:

- Polymarket Gamma/CLOB read-only integration
- market sync task: `market sync` and `POST /api/v1/markets/sync`
- market query API: `GET /api/v1/markets`
- market and price snapshots persisted into PostgreSQL (`markets`, `market_prices`)

Week 4 delivery:

- NWS / Open-Meteo / Visual Crossing integration
- normalized weather forecast and observation model
- weather APIs: `GET /api/v1/weather/forecast`, `GET /api/v1/weather/observation/:station`
- weather CLI: `weather forecast`, `weather observe`

Week 5 delivery:

- probability model v1 (temperature market probability estimation)
- edge calculation (`our_estimate - market_price`)
- signal generation and persistence (`signals` table)
- signal APIs: `GET /api/v1/signals`, `POST /api/v1/signals/generate`
- signal CLI: `signal generate`, `signal list`

## MVP Database Rule

MVP 阶段数据库表结构统一由 GORM `AutoMigrate` 管理：

- 不使用 SQL migration 文件
- 不保留 `migrations/` 目录
- 通过更新 `internal/model` 下的 GORM model 并执行 `db migrate` 生效

## Quick Start

```bash
make tidy
make build
make run
```

## Database Migration

```bash
make db-migrate
```

## API Health Check

Start server:

```bash
go run ./cmd/sky-alpha-pro serve --config ./configs/config.yaml
```

Check health:

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/api/v1/health
```

## Market Sync (W3)

Before first sync:

```bash
make db-migrate
```

Sync markets from Gamma/CLOB:

```bash
make market-sync
# or run as a periodic task
go run ./cmd/sky-alpha-pro market sync --interval 5m --config ./configs/config.yaml
```

List synced markets:

```bash
go run ./cmd/sky-alpha-pro market list --config ./configs/config.yaml
```

REST API:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/markets/sync
curl "http://127.0.0.1:8080/api/v1/markets?active=true&limit=20"
```

## Weather Query (W4)

CLI:

```bash
go run ./cmd/sky-alpha-pro weather forecast "New York, NY" --source all --days 5
go run ./cmd/sky-alpha-pro weather observe KNYC
```

REST API:

```bash
curl "http://127.0.0.1:8080/api/v1/weather/forecast?location=40.7829,-73.9654&source=all&days=5"
curl "http://127.0.0.1:8080/api/v1/weather/observation/KNYC"
```

## Signal Generation (W5)

Generate signals in batch:

```bash
go run ./cmd/sky-alpha-pro signal generate --limit 100
```

List latest signals:

```bash
go run ./cmd/sky-alpha-pro signal list --limit 20 --min-edge 5
```

REST API:

```bash
curl -X POST "http://127.0.0.1:8080/api/v1/signals/generate?limit=100"
curl "http://127.0.0.1:8080/api/v1/signals?limit=20&min_edge=5"
```

## Container

```bash
docker compose up --build
```

`docker-compose.yml` 默认会启动 PostgreSQL + 应用服务（`serve`）。

## CI Image Build

- GitHub Actions 工作流文件：`.github/workflows/release-image.yml`
- 当 `release/v*` 分支发生代码变更时，自动构建并推送镜像到 GHCR
- 版本号从分支名动态提取（例如 `release/v1.2.3` -> `1.2.3`），并生成动态标签：`<version>-build.<run_number>`

## Configuration

Default config file path lookup order:

1. `--config <path>`
2. `./config.yaml`
3. `./configs/config.yaml`
4. `$HOME/config.yaml`

Environment variable prefix is `SKY_ALPHA_`. See `.env.example`.
