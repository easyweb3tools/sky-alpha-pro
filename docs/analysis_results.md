# 天气市场机会发现失败：精确诊断（基于生产配置）

> 日期：2026-03-05

---

## 一句话结论

**根因链**：城市解析只有规则匹配 → 大量市场缺 city → 无法关联预报数据 → 信号为 0 → 无机会。

---

## 生产配置验证结果

| 组件 | 状态 | 说明 |
|------|------|------|
| Scheduler | ✅ 已启用 | `ENABLED=true`, `RUN_ON_START=true` |
| market_sync | ✅ 5m 间隔 | 默认值，正常 |
| weather_forecast | ✅ 15m 间隔 | 默认值，正常 |
| chain_scan | ✅ 120s 间隔 | 已调低频率适配免费 RPC |
| agent_cycle | ✅ 10m 间隔 | vertex_brain 模式，12 tool calls 上限 |
| **market_spec_fill** | **❌ 未配置** | docker-compose 中无此 job，但 agent cycle 有 fallback 覆盖 |
| City Resolver Vertex AI | **❌ 未启用** | [.env](file:///Users/bruce/git/easyweb3/sky-alpha-pro/.env) 缺 `SKY_ALPHA_SIGNAL_CITY_RESOLVER_USE_VERTEX=true` |

---

## 根因链路图

```text
① Gamma API 拉取天气市场 → ✅ market_sync 正常，markets 表有数据
                              ↓
② 市场入库，city 字段 → 🟡 Gamma 通常不返回 city，大部分为空
                              ↓
③ City 解析 → 🔴 见下方详解
                              ↓
④ Weather forecast 拉取 → ⚠️ 依赖 cities，若 cities 全空则无数据
                              ↓
⑤ Signal 生成：city + forecast join → 🔴 见下方详解
                              ↓
⑥ Edge 过滤 (min_edge_pct=5.0) → 🟡 即使到达此步，5% 阈值可能过高
                              ↓
⑦ 无信号 → 无机会
```

---

## 🔴 根因 #1：城市解析全链路被限制为 rule-only

这是**唯一的一级根因**，其他问题都是下游连锁反应。

### 证据 1：[.env](file:///Users/bruce/git/easyweb3/sky-alpha-pro/.env) 未启用 Vertex AI 城市解析

生产 [.env](file:///Users/bruce/git/easyweb3/sky-alpha-pro/.env) 中**不存在** `SKY_ALPHA_SIGNAL_CITY_RESOLVER_USE_VERTEX=true`。

→ [city_resolver.go L71](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/city_resolver.go#L71)：`useVertex := os.Getenv("SKY_ALPHA_SIGNAL_CITY_RESOLVER_USE_VERTEX")` 为空 → `vertex = nil`

### 证据 2：Agent Cycle 强制禁用 Vertex 城市解析

即使 Vertex 被配置，agent cycle 也不会使用它：

→ [cycle.go L863](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/agent/cycle.go#L863)：
```go
func (s *Service) runToolMarketCityResolve(ctx context.Context, args map[string]any) cycleStepExecution {
    ctx = signal.WithCityResolverVertexDisabled(ctx)  // ← 强制禁用 Vertex
    ...
}
```

### 证据 3：规则匹配覆盖率有限

[city_resolver.go L295-311](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/city_resolver.go#L295-L311)：规则匹配仅依赖 `defaultCityAliases`（约 30 个美国城市 + 机场代码）。匹配逻辑是**子字符串包含**，需要市场 question 文本中完整出现城市名。

**对于 question 格式为** `"Will the high temperature in NYC exceed 80°F on March 10?"` → ✅ 可匹配 ("nyc" → "new york")

**对于 question 格式为** `"Temperature above 75 degrees in Charlotte on March 8?"` → ❌ Charlotte 不在别名列表中

### 影响链

```text
city 解析失败 → spec_status = "missing_city"
  → ensureMarketSpec 返回 skipReasonCityMissing
  → generateSignalForMarket 跳过此市场
  → signals = 0
```

---

## 🟡 根因 #2：Forecast→Signal 城市名不一致

即使 city 解析成功，[loadForecastValues](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/service.go#561-620) 的 join 条件也可能失败。

[signal/service.go L596](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/service.go#L596)：
```go
if normalizeForecastCity(r.City, r.Location) != city {
    continue  // ← forecast 的 city 必须与 spec 的 city 精确匹配
}
```

- NWS 返回的 `location` 可能是 `"New York, NY"` 或 GPS 坐标
- OpenMeteo 返回的 [city](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/service.go#58-62) 可能是 `"New York City"` vs spec 中的 `"new york"`
- [normalizeForecastCity](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/service.go#674-695) 只做 lowercase + trim，不做 alias 映射

---

## 🟡 根因 #3：`min_edge_pct = 5.0` 可能过高

天气预测市场通常定价效率较高，2-4% 的 edge 已是真实机会。5% 的门槛可能滤掉全部机会。

---

## 优先修复建议

### 🟥 P0：City 解析能力增强（阻塞全链路）

**方案 A（快速，改配置 + 1 行代码）**：

1. 生产 [.env](file:///Users/bruce/git/easyweb3/sky-alpha-pro/.env) 添加：`SKY_ALPHA_SIGNAL_CITY_RESOLVER_USE_VERTEX=true`
2. docker-compose 中添加环境变量传递
3. [cycle.go L863](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/agent/cycle.go#L863) **删除** `ctx = signal.WithCityResolverVertexDisabled(ctx)` — 让 agent cycle 也能使用 Vertex 城市解析

**方案 B（更可靠，扩展规则匹配）**：

扩充 `defaultCityAliases` 表，覆盖 Polymarket 天气市场中实际出现的所有城市名变体。这需要先从生产 DB 查询 `SELECT DISTINCT question FROM markets WHERE is_active = true AND COALESCE(city,'') = ''` 来了解哪些城市在漏匹配。

**建议两者都做**：方案 A 兜底处理长尾，方案 B 覆盖高频城市。

### 🟧 P1：Forecast 城市匹配增加 alias

[loadForecastValues](file:///Users/bruce/git/easyweb3/sky-alpha-pro/internal/signal/service.go#561-620) 的城市匹配逻辑增加 alias fallback，复用 `defaultCityAliases`，或在 query 阶段不过滤 city（在代码中用模糊匹配）。

### 🟨 P2：降低 edge 阈值

将 `SKY_ALPHA_SIGNAL_MIN_EDGE_PCT` 从 5.0 降到 2.0-3.0，先收集更多信号数据，后续根据回测调整。

---

## 验证方式（修复后）

连接生产 DB，用以下 SQL 追踪漏斗：

```sql
-- 1. 市场 city 覆盖率
SELECT
    COUNT(*) AS total_active,
    COUNT(CASE WHEN COALESCE(city,'') <> '' THEN 1 END) AS city_resolved,
    COUNT(CASE WHEN spec_status = 'ready' THEN 1 END) AS spec_ready
FROM markets WHERE is_active = true;

-- 2. 信号漏斗 skip 原因分布
SELECT skip_reasons_json, started_at, markets_total, spec_ready,
       forecast_ready, raw_edge_pass, exec_edge_pass, signals_generated, skipped
FROM signal_runs
ORDER BY started_at DESC LIMIT 10;

-- 3. 缺 city 的市场 question 样本
SELECT question, slug, market_type FROM markets
WHERE is_active = true AND COALESCE(city,'') = ''
LIMIT 20;
```
