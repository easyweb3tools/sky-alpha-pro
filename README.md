# Sky Alpha Pro

Week 1 bootstrap for the MVP backend:

- project skeleton
- config system (`viper`)
- structured logging (`zap`)
- PostgreSQL connection (`gorm`)
- schema management via GORM `AutoMigrate`

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

## Container

```bash
docker compose up --build
```

`docker-compose.yml` 默认会先启动 PostgreSQL，再执行一次 `db migrate`。

## CI Image Build

- GitHub Actions 工作流文件：`.github/workflows/release-image.yml`
- 当 `release/v1.0.0` 分支发生代码变更时，自动构建并推送镜像到 GHCR
- 版本号从分支名动态提取（`1.0.0`），并生成动态标签：`1.0.0-build.<run_number>`

## Configuration

Default config file path lookup order:

1. `--config <path>`
2. `./config.yaml`
3. `./configs/config.yaml`
4. `$HOME/config.yaml`

Environment variable prefix is `SKY_ALPHA_`. See `.env.example`.
