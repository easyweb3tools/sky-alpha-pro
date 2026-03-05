# Sky Alpha Pro

AI-driven autonomous trading system for Polymarket weather prediction markets.

> Vertex AI decides. Go executes. Data closes the loop.

---

## Architecture

```text
┌─────────────────────────────────────┐
│     Scheduler (Go)                  │  Trigger + lifecycle + budget
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│     Vertex AI Control Plane         │  Plan / decide / adapt / validate
└──────────────┬──────────────────────┘
               ▼  MCP tool-calling
┌─────────────────────────────────────┐
│     Go MCP Runtime (Passive)        │  market / weather / signal / trade
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│     PostgreSQL + Metrics            │  State / results / evidence
└─────────────────────────────────────┘
```

- **Control Plane** — Vertex AI (`vertex_brain` mode) or deterministic fallback
- **Execution Plane** — Go services exposed as passive tools
- **Fact Plane** — PostgreSQL (`markets`, `forecasts`, `signals`, `trades`, `agent_*`)

## Quick Start

### Prerequisites

- Go 1.22+
- PostgreSQL 16+
- Google Cloud credentials (for Vertex AI)

### Local Development

```bash
cp .env.example .env    # Edit with your credentials
make tidy
make build
make db-migrate
make run                # Starts server with scheduler
```

### Docker

```bash
docker compose up
```

Starts PostgreSQL + application. To include the local Postgres container:

```bash
docker compose --profile with-postgres up
```

## Configuration

Config is loaded from YAML + environment variables (prefix `SKY_ALPHA_`).

Lookup order: `--config <path>` → `./config.yaml` → `./configs/config.yaml`

Key config files:
- `configs/config.yaml` — base configuration
- `.env` — secrets and overrides (see `.env.example`)

### Required Settings

| Setting | Description |
|---------|-------------|
| `SKY_ALPHA_AGENT_VERTEX_PROJECT` | GCP project ID for Vertex AI |
| `DB_PASSWORD` | PostgreSQL password |
| `SKY_ALPHA_CHAIN_RPC_URL` | Polygon RPC URL (for chain scan) |

### Signal Tuning

| Setting | Default | Description |
|---------|---------|-------------|
| `SKY_ALPHA_SIGNAL_MIN_EDGE_PCT` | `5.0` | Minimum raw edge % to generate signal |
| `SKY_ALPHA_SIGNAL_FORECAST_MAX_AGE_HOURS` | `24` | Max forecast age before considered stale |
| `SKY_ALPHA_SIGNAL_MIN_SIGMA` | `0.5` | Minimum standard deviation floor |

## Scheduler Jobs

The `serve` command automatically starts the scheduler. Jobs:

| Job | Default Interval | Description |
|-----|-----------------|-------------|
| `market_sync` | 5m | Sync weather markets from Polymarket Gamma/CLOB |
| `market_spec_fill` | 10m | Resolve city/threshold/comparator for active markets |
| `weather_forecast` | 15m | Fetch forecasts from NWS, Open-Meteo, Visual Crossing |
| `chain_scan` | 2m | Scan Polygon for competitor OrderFilled events |
| `agent_cycle` | 10m | Vertex AI-driven analysis → signal → trade cycle |
| `sim_cycle` | 5m | Paper trading simulation (disabled by default) |

## Opportunity Pipeline

```text
Gamma API → Markets DB → City Resolution → Weather Forecast → Signal Generation → Edge Filter → Trade
```

Each market goes through a funnel:

1. **Market Sync** — Fetch active weather markets from Polymarket
2. **Spec Fill** — Resolve city, temperature threshold, comparator from question text
3. **Weather Forecast** — Pull forecasts from 3 sources (NWS, Open-Meteo, Visual Crossing)
4. **Signal Generation** — Estimate probability via normal CDF model, compute `edge_exec`
5. **Edge Filter** — Require `|edge| >= min_edge_pct` after friction deduction
6. **Agent Decision** — Vertex AI evaluates signals and decides to trade or hold

## API Reference

### Health & Ops

```bash
GET  /health                    # Health check
GET  /ops/status                # Scheduler status
GET  /ops/inspection            # Full system inspection (+alerts +immediate_actions +h48_actions +prompt rollout diagnostics)
GET  /ops/agent/validations     # Agent cycle validation history
GET  /ops/agent/prompt-performance  # Prompt version A/B performance stats
GET  /ops/agent/strategy-changes    # Strategy change lifecycle (monitor/keep/rollback)
GET  /ops/events/summary        # Opportunity event pipeline summary
GET  /ops/candidates/summary    # Candidate pool summary
GET  /ops/candidates/transitions # Candidate state transition audit
POST /ops/events/emit           # Manual event injection (ops/debug)
```

### Markets

```bash
GET  /api/v1/markets            # List markets (?active=true&limit=20)
POST /api/v1/markets/sync       # Trigger market sync
```

### Weather

```bash
GET  /api/v1/weather/forecast           # ?location=NYC&source=all&days=5
GET  /api/v1/weather/observation/:station  # e.g. /observation/KNYC
```

### Signals

```bash
GET  /api/v1/signals                    # ?limit=20&min_edge=5
POST /api/v1/signals/generate           # ?limit=100
```

### Agent

```bash
POST /api/v1/agent/analyze              # {"market_id":"...","depth":"full"}
GET  /api/v1/agent/signals              # ?limit=20&min_edge=5
GET  /api/v1/agent/signals/:id
```

### Trading

```bash
POST   /api/v1/trades                   # Submit order
GET    /api/v1/trades                   # ?limit=20&status=placed&market_id=...
GET    /api/v1/trades/:id
DELETE /api/v1/trades/:id               # Cancel order
GET    /api/v1/positions                # ?market_id=...
GET    /api/v1/pnl                      # ?from=2026-02-01&to=2026-02-28
```

### Chain & Players

```bash
POST /api/v1/chain/scan                           # Trigger chain scan
GET  /api/v1/chain/competitors                    # ?only_bots=true&limit=20
GET  /api/v1/chain/competitors/:address/trades    # ?limit=50
POST /api/v1/players/sync                         # ?limit=50
GET  /api/v1/players/leaderboard                  # ?type=weather&limit=20
```

### Metrics

```bash
GET  /metrics                   # Prometheus metrics
```

## Operations

- API-first SRE inspection prompt: `docs/运维巡检Prompt.md`

## CLI Commands

```bash
sky-alpha-pro serve                          # Start API server + scheduler
sky-alpha-pro market sync                    # Sync markets
sky-alpha-pro market list                    # List markets
sky-alpha-pro weather forecast "NYC"         # Get forecast
sky-alpha-pro weather observe KNYC           # Get observation
sky-alpha-pro signal generate --limit 100    # Generate signals
sky-alpha-pro signal list --min-edge 5       # List signals
sky-alpha-pro agent analyze --all            # AI analysis
sky-alpha-pro trade buy <market> --outcome YES --price 0.65 --size 10 --confirm
sky-alpha-pro trade list --status placed
sky-alpha-pro trade positions
sky-alpha-pro trade pnl --from 2026-02-01
sky-alpha-pro chain scan --lookback-blocks 2000
sky-alpha-pro chain bots --limit 20
sky-alpha-pro player leaderboard --type weather
```

## Database

Schema managed by GORM `AutoMigrate` (no SQL migration files):

```bash
make db-migrate
```

Key tables: `markets`, `market_prices`, `forecasts`, `signals`, `signal_runs`, `trades`, `competitors`, `competitor_trades`, `players`, `player_positions`, `city_resolution_caches`, `agent_*`

## CI/CD

- **GitHub Actions**: `.github/workflows/release-image.yml`
- Trigger: push to `release/v*` branch
- Output: Docker image to GHCR with tag `<version>-build.<run_number>`

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.22 |
| AI | Google Vertex AI (Gemini) |
| Database | PostgreSQL 16 |
| ORM | GORM |
| Config | Viper |
| Logging | Zap |
| Metrics | Prometheus |
| Blockchain | go-ethereum (Polygon) |
| Container | Docker Compose |
