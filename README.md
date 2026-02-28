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
- forecast target date uses `market.end_date - 1 day` (settlement-day alignment)
- stale forecast filtering (default: within 24h)
- per-market per-day signal de-duplication (update existing daily signal)
- signal APIs: `GET /api/v1/signals`, `POST /api/v1/signals/generate`
- signal CLI: `signal generate`, `signal list`

Week 6 delivery:

- Agent analysis service with tool-call trace (`get_market_prices`, `get_weather_forecast`, `calculate_edge`)
- Agent decision log persistence into `agent_logs`
- agent APIs: `POST /api/v1/agent/analyze`, `GET /api/v1/agent/signals`, `GET /api/v1/agent/signals/:id`
- agent CLI: `agent analyze`, `agent signals`

Week 7 delivery:

- EIP-712 order signing support
- CLOB order submit/cancel integration (`POST /order`, `DELETE /order/:id`)
- server-side risk checks (edge/liquidity/position size/open positions/daily loss/duplicate cooldown)
- trade error code mapping (`400/422/502/500`) for better caller handling
- trade APIs: `POST /api/v1/trades`, `DELETE /api/v1/trades/:id`, `GET /api/v1/trades`, `GET /api/v1/trades/:id`
- trade CLI: `trade buy`, `trade sell`, `trade cancel`, `trade list`

Week 8 delivery:

- position tracking API: `GET /api/v1/positions`
- PnL report API: `GET /api/v1/pnl`
- trade CLI: `trade positions`, `trade pnl`
- trade list API/CLI supports `status` + `market_id` filters

Week 9 delivery:

- Polygon chain scan service for competitor activity bootstrap
- competitor persistence and bot classification (`competitors`, `competitor_trades`)
- chain APIs:
  - `POST /api/v1/chain/scan`
  - `GET /api/v1/chain/competitors`
  - `GET /api/v1/chain/competitors/:address`
  - `GET /api/v1/chain/competitors/:address/trades`
- chain CLI: `chain scan`, `chain bots`, `chain watch`

Week 10 delivery:

- player tracking service based on tracked on-chain activity
- player ranking and position snapshots persisted into `players`, `player_positions`
- player APIs:
  - `POST /api/v1/players/sync`
  - `GET /api/v1/players`
  - `GET /api/v1/players/leaderboard`
  - `GET /api/v1/players/:address`
  - `GET /api/v1/players/:address/positions`
  - `GET /api/v1/players/:address/compare`
- player CLI: `player sync`, `player list`, `player show`, `player leaderboard`, `player compare`

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

## Agent Analysis (W6)

Analyze one market:

```bash
go run ./cmd/sky-alpha-pro agent analyze <market_id>
```

Analyze active markets:

```bash
go run ./cmd/sky-alpha-pro agent analyze --all --limit 20 --depth full
```

List agent signals:

```bash
go run ./cmd/sky-alpha-pro agent signals --limit 20 --min-edge 5
```

REST API:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/agent/analyze \
  -H "Content-Type: application/json" \
  -d '{"market_id":"<market_id>","depth":"full"}'
curl "http://127.0.0.1:8080/api/v1/agent/signals?limit=20&min_edge=5"
```

## Trade Execution (W7)

CLI:

```bash
go run ./cmd/sky-alpha-pro trade buy <market_id> --outcome YES --price 0.65 --size 10 --confirm
go run ./cmd/sky-alpha-pro trade sell <market_id> --outcome NO --price 0.42 --size 8 --confirm
go run ./cmd/sky-alpha-pro trade cancel <trade_id>
go run ./cmd/sky-alpha-pro trade list --limit 20 --status placed --market-id <market_id>
go run ./cmd/sky-alpha-pro trade positions --market-id <market_id>
go run ./cmd/sky-alpha-pro trade pnl --from 2026-02-01 --to 2026-02-28
```

REST API:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/trades \
  -H "Content-Type: application/json" \
  -d '{"market_id":"<market_id>","side":"BUY","outcome":"YES","price":0.65,"size":10,"confirm":true}'
curl "http://127.0.0.1:8080/api/v1/trades?limit=20&status=placed&market_id=<market_id>"
curl "http://127.0.0.1:8080/api/v1/trades/1"
curl -X DELETE "http://127.0.0.1:8080/api/v1/trades/1"
curl "http://127.0.0.1:8080/api/v1/positions?market_id=<market_id>"
curl "http://127.0.0.1:8080/api/v1/pnl?from=2026-02-01&to=2026-02-28"
```

Required config:

```yaml
trade:
  private_key: "<hex_private_key>"
  chain_id: 137
  max_order_size: 0 # 0 disables the limit
  confirmation_required: true
```

`private_key` 仅建议通过安全的环境变量/密钥管理系统注入，不要写入版本控制文件。
当前实现已对齐 CTF Exchange 的 EIP-712 订单结构；若直连真实 CLOB，还需要配置 `POLY_*` 认证头（API key/secret/passphrase）链路。

Chain scan config:

```yaml
chain:
  rpc_url: "https://polygon-rpc.com"
  chain_id: 137
  ctf_exchange_address: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
  negrisk_exchange_address: "0xC5d563A36AE78145C45a50134d48A1215220f80a"
  scan_lookback_blocks: 2000
  scan_max_tx: 2000
  bot_min_trades: 8
  bot_max_avg_interval_sec: 8.0
  watch_interval: 30s
```

## Chain Scan (W9)

CLI:

```bash
go run ./cmd/sky-alpha-pro chain scan --lookback-blocks 2000
go run ./cmd/sky-alpha-pro chain bots --limit 20
go run ./cmd/sky-alpha-pro chain bots --address 0xabc... --trades --trades-limit 30
go run ./cmd/sky-alpha-pro chain watch --interval 30s
```

REST API:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/chain/scan \
  -H "Content-Type: application/json" \
  -d '{"lookback_blocks":2000,"max_tx":2000}'
curl "http://127.0.0.1:8080/api/v1/chain/competitors?only_bots=true&limit=20"
curl "http://127.0.0.1:8080/api/v1/chain/competitors/0xabc..."
curl "http://127.0.0.1:8080/api/v1/chain/competitors/0xabc.../trades?limit=50"
```

## Player Tracking (W10)

CLI:

```bash
go run ./cmd/sky-alpha-pro player sync --limit 50
go run ./cmd/sky-alpha-pro player list --limit 20
go run ./cmd/sky-alpha-pro player show 0xabc... --positions-limit 20
go run ./cmd/sky-alpha-pro player leaderboard --type weather --limit 20
go run ./cmd/sky-alpha-pro player compare 0xabc...
```

REST API:

```bash
curl -X POST "http://127.0.0.1:8080/api/v1/players/sync?limit=50"
curl "http://127.0.0.1:8080/api/v1/players?limit=20"
curl "http://127.0.0.1:8080/api/v1/players/leaderboard?type=weather&limit=20"
curl "http://127.0.0.1:8080/api/v1/players/0xabc..."
curl "http://127.0.0.1:8080/api/v1/players/0xabc.../positions?limit=20"
curl "http://127.0.0.1:8080/api/v1/players/0xabc.../compare"
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
