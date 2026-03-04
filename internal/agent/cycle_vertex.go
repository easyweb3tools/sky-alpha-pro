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
	text = extractJSONObject(text)
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

type cycleValidationInput struct {
	SessionID string         `json:"session_id"`
	CycleID   string         `json:"cycle_id"`
	RunMode   string         `json:"run_mode"`
	Decision  string         `json:"decision"`
	Status    string         `json:"status"`
	Records   []CycleRecord  `json:"records"`
	Errors    []CycleIssue   `json:"errors,omitempty"`
	Warnings  []CycleIssue   `json:"warnings,omitempty"`
	Funnel    map[string]any `json:"funnel,omitempty"`
}

type cycleValidationOutput struct {
	Verdict   string   `json:"verdict"`
	Score     float64  `json:"score"`
	Summary   string   `json:"summary"`
	Strengths []string `json:"strengths"`
	Risks     []string `json:"risks"`
	Actions   []string `json:"actions"`
}

const cycleValidationSystemPrompt = `You are Sky Alpha Pro's cycle validator.
Assess one completed cycle and return strict JSON only:
verdict, score, summary, strengths, risks, actions.
Rules:
1) Use only provided facts.
2) score range 0-100.
3) verdict must be pass|warning|fail.
4) Keep concise and actionable.`

func (v *vertexAIClient) ValidateCycle(ctx context.Context, input cycleValidationInput) (*cycleValidationOutput, error) {
	if v == nil || v.client == nil {
		return nil, fmt.Errorf("vertex ai client is not configured")
	}
	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	body, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	resp, err := v.client.Models.GenerateContent(
		callCtx,
		v.model,
		genai.Text("Cycle result JSON:\n"+string(body)+"\n\nReturn strict JSON only."),
		&genai.GenerateContentConfig{
			SystemInstruction: genai.NewContentFromText(cycleValidationSystemPrompt, genai.RoleUser),
			Temperature:       genai.Ptr(float32(0.1)),
			MaxOutputTokens:   int32(minInt(v.maxOutputTokens, 1024)),
			ResponseMIMEType:  "application/json",
		},
	)
	if err != nil {
		return nil, err
	}
	text := strings.TrimSpace(resp.Text())
	if text == "" {
		return nil, fmt.Errorf("vertex ai returned empty validation")
	}
	text = extractJSONObject(text)
	var out cycleValidationOutput
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil, fmt.Errorf("decode cycle validation json: %w", err)
	}
	out.Verdict = strings.ToLower(strings.TrimSpace(out.Verdict))
	if out.Verdict == "" {
		out.Verdict = "warning"
	}
	if out.Score < 0 {
		out.Score = 0
	}
	if out.Score > 100 {
		out.Score = 100
	}
	out.Summary = strings.TrimSpace(out.Summary)
	return &out, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func extractJSONObject(raw string) string {
	txt := strings.TrimSpace(raw)
	if txt == "" {
		return txt
	}
	if strings.HasPrefix(txt, "```") {
		txt = strings.TrimPrefix(txt, "```json")
		txt = strings.TrimPrefix(txt, "```JSON")
		txt = strings.TrimPrefix(txt, "```")
		txt = strings.TrimSuffix(strings.TrimSpace(txt), "```")
		txt = strings.TrimSpace(txt)
	}
	start := strings.Index(txt, "{")
	end := strings.LastIndex(txt, "}")
	if start >= 0 && end > start {
		return txt[start : end+1]
	}
	return txt
}
