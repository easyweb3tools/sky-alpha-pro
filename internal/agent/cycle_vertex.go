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

func (v *vertexAIClient) PlanCycle(ctx context.Context, input cyclePromptInput, systemPrompt string) (*cyclePlanOutput, int, error) {
	if v == nil {
		return nil, 0, fmt.Errorf("vertex ai client is not configured")
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
		return nil, 0, err
	}

	var text string
	llmTokens := 0
	if v.apiKey != "" {
		raw, usage, err := v.generateWithAPIKey(callCtx, sys, buildCyclePlanPrompt(string(promptBody)), v.temperature, v.maxOutputTokens)
		if err != nil {
			return nil, 0, err
		}
		if usage != nil {
			llmTokens = usage.PromptTokens + usage.CompletionTokens
		}
		text = strings.TrimSpace(raw)
	} else {
		if v.client == nil {
			return nil, 0, fmt.Errorf("vertex ai client is not configured")
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
			return nil, 0, err
		}
		if resp.UsageMetadata != nil {
			llmTokens = int(resp.UsageMetadata.PromptTokenCount + resp.UsageMetadata.CandidatesTokenCount)
		}
		text = strings.TrimSpace(resp.Text())
	}
	if text == "" {
		return nil, 0, fmt.Errorf("vertex ai returned empty plan")
	}
	var out cyclePlanOutput
	if err := decodeVertexJSON(text, &out); err != nil {
		return nil, 0, fmt.Errorf("decode cycle plan json: %w", err)
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
	return &out, llmTokens, nil
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

type cycleActionOutput struct {
	Decision string         `json:"decision"`
	Tool     string         `json:"tool"`
	Args     map[string]any `json:"args"`
	Why      string         `json:"why"`
	OnFail   string         `json:"on_fail"`
	Summary  string         `json:"summary"`
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
	if v == nil {
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

	var text string
	if v.apiKey != "" {
		out, err := v.validateWithAPIKeyToolCall(callCtx, cycleValidationSystemPrompt, "Cycle result JSON:\n"+string(body))
		if err != nil {
			return nil, err
		}
		return out, nil
	} else {
		if v.client == nil {
			return nil, fmt.Errorf("vertex ai client is not configured")
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
		text = strings.TrimSpace(resp.Text())
	}
	if text == "" {
		return nil, fmt.Errorf("vertex ai returned empty validation")
	}
	var out cycleValidationOutput
	if err := decodeVertexJSON(text, &out); err != nil {
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

const cycleActionSystemPrompt = `You are Sky Alpha Pro's brain loop controller.
Given runtime context and step history, decide the NEXT single tool call.
You must respond via function calling:
- call_tool(tool, args, why, on_fail, summary)
- finish_cycle(summary, why)
Rules:
1) Use one tool per turn.
1.1) Never output code, print(), markdown, or plain text instructions.
2) Keep args minimal and valid.
3) Prefer tools in this list:
market.sync.batch, market.city.resolve.batch, market.cities.active,
weather.forecast.batch, signal.generate.batch, chain.scan.batch,
report.generate, report.validate.write.
4) End with finish_cycle when no more useful actions remain.`

func (v *vertexAIClient) NextCycleAction(ctx context.Context, step, maxSteps int, conversation []map[string]any) (*cycleActionOutput, int, error) {
	if v == nil {
		return nil, 0, fmt.Errorf("vertex ai client is not configured")
	}
	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	if len(conversation) == 0 {
		return nil, 0, fmt.Errorf("cycle conversation is empty")
	}

	var text string
	llmTokens := 0
	if v.apiKey != "" {
		action, usage, err := v.nextActionWithAPIKeyToolCall(callCtx, cycleActionSystemPrompt, conversation)
		if err != nil {
			return nil, 0, err
		}
		if usage != nil {
			llmTokens = usage.PromptTokens + usage.CompletionTokens
		}
		return action, llmTokens, nil
	} else {
		if v.client == nil {
			return nil, 0, fmt.Errorf("vertex ai client is not configured")
		}
		resp, err := v.client.Models.GenerateContent(
			callCtx,
			v.model,
			genai.Text("Cycle control input JSON:\n"+collapseConversationForPrompt(conversation)+"\n\nReturn strict JSON only."),
			&genai.GenerateContentConfig{
				SystemInstruction: genai.NewContentFromText(cycleActionSystemPrompt, genai.RoleUser),
				Temperature:       genai.Ptr(float32(0.1)),
				MaxOutputTokens:   int32(minInt(v.maxOutputTokens, 1024)),
				ResponseMIMEType:  "application/json",
			},
		)
		if err != nil {
			return nil, 0, err
		}
		if resp.UsageMetadata != nil {
			llmTokens = int(resp.UsageMetadata.PromptTokenCount + resp.UsageMetadata.CandidatesTokenCount)
		}
		text = strings.TrimSpace(resp.Text())
	}
	if text == "" {
		return nil, 0, fmt.Errorf("vertex ai returned empty action")
	}
	var out cycleActionOutput
	if err := decodeVertexJSON(text, &out); err != nil {
		return nil, 0, fmt.Errorf("decode cycle action json: %w", err)
	}
	out.Decision = strings.ToLower(strings.TrimSpace(out.Decision))
	if out.Decision == "" {
		out.Decision = "finish"
	}
	out.Tool = strings.TrimSpace(out.Tool)
	out.OnFail = strings.ToLower(strings.TrimSpace(out.OnFail))
	if out.OnFail == "" {
		out.OnFail = "continue"
	}
	if out.Args == nil {
		out.Args = map[string]any{}
	}
	return &out, llmTokens, nil
}

func collapseConversationForPrompt(conversation []map[string]any) string {
	if len(conversation) == 0 {
		return "{}"
	}
	b, err := json.Marshal(conversation)
	if err != nil {
		return "{}"
	}
	return string(b)
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

func decodeVertexJSON(raw string, out any) error {
	txt := extractJSONObject(raw)
	if err := json.Unmarshal([]byte(txt), out); err == nil {
		return nil
	}
	if repaired := repairTruncatedJSON(txt); repaired != "" && repaired != txt {
		if err := json.Unmarshal([]byte(repaired), out); err == nil {
			return nil
		}
	}
	return json.Unmarshal([]byte(txt), out)
}

func repairTruncatedJSON(in string) string {
	s := strings.TrimSpace(in)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	inStr := false
	escaped := false
	openObj := 0
	openArr := 0
	for _, r := range s {
		b.WriteRune(r)
		if inStr {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inStr = false
			}
			continue
		}
		if r == '"' {
			inStr = true
			continue
		}
		switch r {
		case '{':
			openObj++
		case '}':
			if openObj > 0 {
				openObj--
			}
		case '[':
			openArr++
		case ']':
			if openArr > 0 {
				openArr--
			}
		}
	}
	if inStr {
		b.WriteString(`"`)
	}
	for openArr > 0 {
		b.WriteByte(']')
		openArr--
	}
	for openObj > 0 {
		b.WriteByte('}')
		openObj--
	}
	return b.String()
}
