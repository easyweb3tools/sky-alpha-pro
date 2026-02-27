# AGENTS Rules

## Database Schema Management (MVP)
- MVP 阶段统一使用 GORM `AutoMigrate` 管理数据库表结构。
- 禁止新增或维护 SQL migration 文件（例如 `migrations/*.sql`）。
- `db migrate` 命令必须调用应用内模型的 `AutoMigrate`。
- 如需新增字段/索引，请更新 GORM model 并通过 `db migrate` 生效。
