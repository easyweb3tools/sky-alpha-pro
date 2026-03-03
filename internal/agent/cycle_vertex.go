package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/genai"
)

const cycleSystemPrompt = `You are Sky Alpha Pro's orchestration agent.
Generate one cycle execution plan for MCP tools.
Return strict JSON only with fields:
cycle_goal, decision, reasoning, plan, risk_controls, expected_outputs, memory_writeback.
Rules:
1) Prefer batch tools.
2) If markets_city_missing > 0, include market.city.resolve.batch before weather.forecast.batch.
3) Do not use trade.place.batch when trade_enabled=false.
4) Keep plan concise (<= 8 steps).`

func (v *vertexAIClient) PlanCycle(ctx context.Context, input cyclePromptInput, systemPrompt string) (*cyclePlanOutput, error) {
	if v == nil || v.client == nil {
		return nil, fmt.Errorf("vertex ai client is not configured")
	}
	sys := strings.TrimSpace(systemPrompt)
	if sys == "" {
		sys = cycleSystemPrompt
	}

	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	promptBody, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	resp, err := v.client.Models.GenerateContent(
		callCtx,
		v.model,
		genai.Text(buildCyclePlanPrompt(string(promptBody))),
		&genai.GenerateContentConfig{
			SystemInstruction: genai.NewContentFromText(sys, genai.RoleUser),
			Temperature:       genai.Ptr(v.temperature),
			MaxOutputTokens:   int32(v.maxOutputTokens),
			ResponseMIMEType:  "application/json",
		},
	)
	if err != nil {
		return nil, err
	}
	text := strings.TrimSpace(resp.Text())
	if text == "" {
		return nil, fmt.Errorf("vertex ai returned empty plan")
	}
	var out cyclePlanOutput
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil, fmt.Errorf("decode cycle plan json: %w", err)
	}
	out.Decision = strings.ToLower(strings.TrimSpace(out.Decision))
	if out.Decision == "" {
		out.Decision = "run"
	}
	for i := range out.Plan {
		out.Plan[i].Tool = strings.TrimSpace(out.Plan[i].Tool)
		out.Plan[i].OnFail = strings.ToLower(strings.TrimSpace(out.Plan[i].OnFail))
		if out.Plan[i].OnFail == "" {
			out.Plan[i].OnFail = "continue"
		}
		if out.Plan[i].Args == nil {
			out.Plan[i].Args = map[string]any{}
		}
	}
	return &out, nil
}

func buildCyclePlanPrompt(inputJSON string) string {
	return "Runtime context JSON:\n" + inputJSON + "\n\nOutput strict JSON only."
}
