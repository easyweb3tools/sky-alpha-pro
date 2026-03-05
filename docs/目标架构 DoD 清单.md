# Sky Alpha Pro 目标架构 DoD 清单

> 版本：v1.2  
> 日期：2026-03-05  
> 用途：按“Phase2 事件驱动 + Phase3 主控智能”推进目标架构落地

---

## 0. 总体完成定义（Definition of Done）

仅当以下三项同时满足，才视为目标架构完成：

1. Agent-First 控制面稳定运行，非人工兜底常态化。
2. 系统健康与策略价值均可通过 `/ops/*` 接口自动验收。
3. 失败可分层归因，运维无需深挖日志即可定位主因。

---

## 1. 当前状态快照（2026-03-05）

1. 已完成：
- 基础调度台账、结构化错误、`/ops/status`、`/ops/inspection`。
- Agent cycle 基础链路、记忆读写、验证报告落库。
- 价值看板基础字段、事件面字段、Prompt 灰度诊断与动作建议（`immediate_actions`/`h48_actions`）。
- Prompt active/candidate 灰度基础能力、策略变更台账、自动回滚（3轮无改善）。

2. 部分完成：
- Vertex 参与决策并可多步调用工具，但“外部会话完全主控 + streaming 工具编排”未完全落地。
- 分布式锁未做跨所有关键 job 的统一治理与告警闭环。

3. 待开发：
- 分布式锁与多副本安全统一化（不仅 agent_cycle）。
- 自动进化从“prompt rollout”扩展到“策略参数集”（多参数、目标函数、约束）。
- A/B 灰度的版本级收益归因与自动升降流量策略。

---

## 2. Phase2 DoD（事件驱动机会捕捉）

### P2-1 事件总线与去重（状态：已完成）

DoD：

1. 建立统一事件入口（`price_jump`、`forecast_update`、`expiry_window_entered`、`chain_activity_spike`、`manual_probe`）。
2. 事件字段标准化：`event_type`, `market_id`, `occurred_at`, `dedup_key`, `payload`。
3. 短窗口去重生效（建议 60s）。

验收：

1. 可通过接口看到 24h 事件吞吐、pending、dedup dropped。
2. 同类重复事件在窗口内不重复触发 Agent。

### P2-2 候选池状态机（状态：已完成）

DoD：

1. 市场状态支持：`cold/watch/hot/tradable/cool_down`。
2. 明确迁移条件与冷却策略。
3. 每次迁移有审计记录（来源事件/原因/时间）。

验收：

1. `/ops/candidates/*` 可查看状态分布与 top candidates。
2. 48h 内状态转移路径可追溯。

### P2-3 Agent 事件唤醒模式（状态：已完成）

DoD：

1. `agent_cycle` 支持 `run_mode=event_driven`。
2. 单轮按“候选市场集合”执行，而非全量扫市场。
3. 保留低频兜底轮询（可开关）。

验收：

1. 非机会时外部调用显著下降。
2. 触发到信号产出延迟可观测（p50/p90）。

### P2-4 机会预算与优先级（状态：已完成）

DoD：

1. 增加预算：`max_candidates_per_cycle`, `max_hot_markets_per_cycle`, `max_signal_writes_per_cycle`。
2. 统一排序分数：`abs(raw_edge) * liquidity_score * freshness_score * expiry_weight`。
3. 超预算有结构化错误码和审计。

验收：

1. 单轮不再无界扫描。
2. Top-N 市场处理顺序可解释。

### P2-5 事件面可观测性（状态：已完成）

DoD：

1. 新增指标：
- `sky_alpha_opportunity_events_total`
- `sky_alpha_candidate_pool_size`
- `sky_alpha_trigger_to_signal_latency_seconds`
- `sky_alpha_hot_market_hit_rate`
- `sky_alpha_event_dedup_dropped_total`
2. `/ops/inspection` 新增 `event_pipeline`、`candidate_funnel`、`opportunity_windows`。

验收：

1. 运维不再通过 SQL 才能判断“无机会 vs 漏机会”。

---

## 3. Phase3 DoD（主控智能与策略进化）

### P3-1 Vertex 真正主控会话闭环（状态：接近完成）

DoD：

1. Vertex 负责多步计划与工具调用编排，Go 仅做工具执行与风控。
2. 单轮预算：`max_tokens_per_cycle`, `max_tool_calls`, `max_external_requests`, `max_cycle_duration`。
3. 每轮落库：plan、steps、tool_calls、decision、no_op_reason。

验收：

1. 连续 24h 无预算失控。
2. 任一轮可回放执行轨迹。

当前进展：

1. 已支持 `run_mode=vertex_brain`，并落库 session/steps/plan/report/validation。
2. 已有预算限制（`max_tool_calls`、`max_external_requests`、`max_tokens_per_cycle`、`max_cycle_duration`）。
3. 已新增 `llm_tokens` 落库与 prompt performance 统计（`avg_llm_tokens`）。
4. 已在 `/ops/inspection` 增加 `vertex_brain_budget` 专项，统一输出预算压力与 decode 退化信号（token/duration/external/tool/plan_decode_failed）。
5. 已提供会话回放接口 `/ops/agent/sessions/:id/replay`，可直接查看 session + steps + brain_trace。
6. 已切换 `streamGenerateContent` + function-calling 的会话主控循环，并支持 malformed 一次自动纠偏重试。
7. 已新增 `/ops/inspection.control_plane`，用于监控 legacy analyze 是否旁路 Vertex 主控。

### P3-2 分布式调度锁（状态：部分完成）

DoD：

1. advisory lock 或 lease lock 落地。
2. 锁续约/释放/超时行为明确。
3. 抢锁失败与锁异常有指标和告警。

验收：

1. 双副本压测下关键 job 单轮只执行一次。

当前进展：

1. `agent_cycle` 已支持 PG advisory lock。
2. 尚未覆盖所有关键 job，也未形成统一锁指标/统一锁告警。

### P3-3 机会定义标准化（状态：部分完成）

DoD：

1. 决策统一使用 `edge_exec`。
2. `P_exec` 使用可成交价格（非 mid/last）。
3. `friction` 至少含 fee/spread/slippage。

验收：

1. 候选机会都能查询 `edge_exec` 构成项。

### P3-4 策略自动进化执行器（状态：接近完成）

DoD：

1. 每轮最多调整 1-2 个参数。
2. 每次变更必须绑定目标指标。
3. 连续 3 轮无改善自动回滚。

验收：

1. 可通过接口查看“变更 -> 效果 -> 保留/回滚”。

当前进展：

1. 已实现 Prompt rollout 的策略变更台账（`agent_strategy_changes`）。
2. 已实现目标指标绑定（`signals_per_run`）和“连续3轮无改善自动回滚”。
3. 已提供接口 `/ops/agent/strategy-changes` 与 inspection 聚合展示。
4. 已扩展到多参数自动调优（`max_tool_calls`、`max_external_requests`、`market_limit`）并落库到 `agent_strategy_params`。
5. 已增加参数联动约束（范围约束 + `max_external_requests >= max_tool_calls * 3`）。
6. 已增加回滚策略矩阵（`error_rate` 与 `signals_per_run` 双目标，连续3轮无改善自动回滚到旧值）。

### P3-5 Prompt/策略版本灰度（状态：部分完成）

DoD：

1. Prompt active/candidate 版本化。
2. 支持灰度与 A/B 对照。
3. 版本级效果指标自动沉淀。

验收：

1. 24h 内可完成一次灰度与自动回滚演练。

当前进展：

1. 已完成 active/candidate 版本选择与 rollout_pct 灰度。
2. 已沉淀版本效果统计接口 `/ops/agent/prompt-performance`。
3. 已在 `/ops/inspection` 输出 prompt rollout 对比和动作建议。
4. 尚未实现“自动升流量/降流量策略”与更细粒度收益归因（按市场类型/城市/窗口）。

---

## 4. 里程碑（建议）

1. M2（Phase2，1-2周）：事件总线 + 候选池 + 事件唤醒 + 事件面指标。  
2. M3（Phase3，2-4周）：Vertex 主控闭环 + 分布式锁 + 自动进化 + Prompt 灰度。  

---

## 5. 非目标（当前不纳入 DoD）

1. 完整前端运营平台（仅先提供 API）。
2. 高频撮合优化（当前以机会发现能力为主）。
3. 多主模型协同调度（先稳定单主模型）。

---

## 6. 每项交付物（强制）

1. 设计说明（目标/约束/权衡）
2. 接口契约（输入/输出/错误码）
3. 验收脚本或巡检 Prompt
4. 回滚方案
5. 至少一次真实环境演练记录
