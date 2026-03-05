package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/genai"

	"sky-alpha-pro/pkg/config"
)

const (
	defaultVertexLocation        = "us-central1"
	defaultVertexModel           = "gemini-2.5-pro"
	defaultVertexTemperature     = float32(0.2)
	defaultVertexMaxOutputTokens = 2048
	defaultVertexTimeout         = 15 * time.Second
)

type vertexAIClient struct {
	client          *genai.Client
	httpClient      *http.Client
	apiKey          string
	project         string
	location        string
	model           string
	temperature     float32
	maxOutputTokens int
	timeout         time.Duration
}

type vertexPromptInput struct {
	Question         string
	City             string
	ForecastSummary  string
	MarketPriceYes   float64
	OurProbability   float64
	EdgePct          float64
	Confidence       float64
	Recommendation   string
	RiskFactors      []string
	DeterministicRsn string
}

type vertexAnalysisResult struct {
	Recommendation string   `json:"recommendation"`
	Reasoning      string   `json:"reasoning"`
	RiskFactors    []string `json:"risk_factors"`
}

type vertexUsage struct {
	PromptTokens     int
	CompletionTokens int
}

func newVertexAIClient(cfg config.AgentConfig, log *zap.Logger) (*vertexAIClient, error) {
	apiKey := strings.TrimSpace(cfg.VertexAPIKey)
	project := strings.TrimSpace(cfg.VertexProject)
	if apiKey == "" && project == "" {
		return nil, fmt.Errorf("agent.vertex_project is required for vertex ai")
	}
	location := strings.TrimSpace(cfg.VertexLocation)
	if location == "" {
		location = defaultVertexLocation
	}
	model := strings.TrimSpace(cfg.VertexModel)
	if model == "" {
		model = defaultVertexModel
	}
	temperature := cfg.VertexTemperature
	if temperature <= 0 {
		temperature = defaultVertexTemperature
	}
	maxTokens := cfg.VertexMaxOutputTokens
	if maxTokens <= 0 {
		maxTokens = defaultVertexMaxOutputTokens
	}
	timeout := cfg.VertexTimeout
	if timeout <= 0 {
		timeout = defaultVertexTimeout
	}

	var client *genai.Client
	if apiKey == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := genai.NewClient(ctx, &genai.ClientConfig{
			Backend:  genai.BackendVertexAI,
			Project:  project,
			Location: location,
		})
		if err != nil {
			return nil, err
		}
		client = c
	}

	if log != nil {
		log.Info("vertex ai client enabled",
			zap.Bool("api_key_mode", apiKey != ""),
			zap.String("project", project),
			zap.String("location", location),
			zap.String("model", model),
		)
	}

	return &vertexAIClient{
		client:          client,
		httpClient:      &http.Client{Timeout: timeout},
		apiKey:          apiKey,
		project:         project,
		location:        location,
		model:           model,
		temperature:     temperature,
		maxOutputTokens: maxTokens,
		timeout:         timeout,
	}, nil
}

func (v *vertexAIClient) ModelName() string {
	if v == nil {
		return ""
	}
	return v.model
}

func (v *vertexAIClient) Analyze(ctx context.Context, input vertexPromptInput) (*vertexAnalysisResult, vertexUsage, error) {
	if v == nil {
		return nil, vertexUsage{}, fmt.Errorf("vertex ai client is not configured")
	}
	if v.apiKey != "" {
		text, usage, err := v.generateWithAPIKey(ctx, "You are a weather prediction market analyst. Return only valid JSON. Keep recommendations conservative and do not fabricate data.", buildVertexPrompt(input), v.temperature, v.maxOutputTokens)
		if err != nil {
			return nil, vertexUsage{}, err
		}
		var out vertexAnalysisResult
		if err := json.Unmarshal([]byte(extractJSONObject(text)), &out); err != nil {
			return nil, vertexUsage{}, fmt.Errorf("decode vertex ai json: %w", err)
		}
		out.Recommendation = strings.TrimSpace(out.Recommendation)
		out.Reasoning = strings.TrimSpace(out.Reasoning)
		out.RiskFactors = normalizeStringSlice(out.RiskFactors)
		if out.Recommendation == "" || out.Reasoning == "" {
			return nil, vertexUsage{}, fmt.Errorf("vertex ai response missing required fields")
		}
		if usage == nil {
			return &out, vertexUsage{}, nil
		}
		return &out, *usage, nil
	}
	if v.client == nil {
		return nil, vertexUsage{}, fmt.Errorf("vertex ai client is not configured")
	}

	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	schema := &genai.Schema{
		Type: genai.TypeObject,
		Properties: map[string]*genai.Schema{
			"recommendation": {Type: genai.TypeString},
			"reasoning":      {Type: genai.TypeString},
			"risk_factors": {
				Type:  genai.TypeArray,
				Items: &genai.Schema{Type: genai.TypeString},
			},
		},
		Required: []string{"recommendation", "reasoning", "risk_factors"},
	}

	generateCfg := &genai.GenerateContentConfig{
		SystemInstruction: genai.NewContentFromText(
			"You are a weather prediction market analyst. Return only valid JSON that matches the provided schema. Keep recommendations conservative and do not fabricate data.",
			genai.RoleUser,
		),
		Temperature:      genai.Ptr(v.temperature),
		MaxOutputTokens:  int32(v.maxOutputTokens),
		ResponseMIMEType: "application/json",
		ResponseSchema:   schema,
	}

	resp, err := v.client.Models.GenerateContent(callCtx, v.model, genai.Text(buildVertexPrompt(input)), generateCfg)
	if err != nil {
		return nil, vertexUsage{}, err
	}
	text := strings.TrimSpace(resp.Text())
	if text == "" {
		return nil, vertexUsage{}, fmt.Errorf("vertex ai returned empty response")
	}

	var out vertexAnalysisResult
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil, vertexUsage{}, fmt.Errorf("decode vertex ai json: %w", err)
	}
	out.Recommendation = strings.TrimSpace(out.Recommendation)
	out.Reasoning = strings.TrimSpace(out.Reasoning)
	out.RiskFactors = normalizeStringSlice(out.RiskFactors)
	if out.Recommendation == "" || out.Reasoning == "" {
		return nil, vertexUsage{}, fmt.Errorf("vertex ai response missing required fields")
	}

	usage := vertexUsage{}
	if resp.UsageMetadata != nil {
		usage.PromptTokens = int(resp.UsageMetadata.PromptTokenCount)
		usage.CompletionTokens = int(resp.UsageMetadata.CandidatesTokenCount)
	}
	return &out, usage, nil
}

func (v *vertexAIClient) generateWithAPIKey(ctx context.Context, systemPrompt, userPrompt string, temperature float32, maxOutputTokens int) (string, *vertexUsage, error) {
	if v == nil || strings.TrimSpace(v.apiKey) == "" {
		return "", nil, fmt.Errorf("vertex api key is not configured")
	}
	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	project := strings.TrimSpace(v.project)
	location := strings.TrimSpace(v.location)
	if project == "" {
		return "", nil, fmt.Errorf("vertex api key mode requires agent.vertex_project")
	}
	if location == "" {
		location = defaultVertexLocation
	}
	endpoint := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent?key=%s",
		location,
		url.PathEscape(project),
		url.PathEscape(location),
		url.PathEscape(v.model),
		url.QueryEscape(v.apiKey),
	)

	payload := map[string]any{
		"contents": []map[string]any{
			{
				"role": "user",
				"parts": []map[string]any{
					{"text": userPrompt},
				},
			},
		},
		"generationConfig": map[string]any{
			"temperature":      temperature,
			"maxOutputTokens":  maxOutputTokens,
			"responseMimeType": "application/json",
		},
	}
	if strings.TrimSpace(systemPrompt) != "" {
		payload["systemInstruction"] = map[string]any{
			"role": "system",
			"parts": []map[string]any{
				{"text": systemPrompt},
			},
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil, fmt.Errorf("vertex api request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", nil, fmt.Errorf("decode vertex api response: %w", err)
	}
	text := ""
	if len(parsed.Candidates) > 0 && len(parsed.Candidates[0].Content.Parts) > 0 {
		text = strings.TrimSpace(parsed.Candidates[0].Content.Parts[0].Text)
	}
	if text == "" {
		return "", nil, fmt.Errorf("vertex api returned empty response")
	}
	usage := &vertexUsage{
		PromptTokens:     parsed.UsageMetadata.PromptTokenCount,
		CompletionTokens: parsed.UsageMetadata.CandidatesTokenCount,
	}
	return text, usage, nil
}

func (v *vertexAIClient) nextActionWithAPIKeyToolCall(ctx context.Context, systemPrompt string, conversation []map[string]any) (*cycleActionOutput, *vertexUsage, error) {
	if v == nil || strings.TrimSpace(v.apiKey) == "" {
		return nil, nil, fmt.Errorf("vertex api key is not configured")
	}
	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	project := strings.TrimSpace(v.project)
	location := strings.TrimSpace(v.location)
	if project == "" {
		return nil, nil, fmt.Errorf("vertex api key mode requires agent.vertex_project")
	}
	if location == "" {
		location = defaultVertexLocation
	}
	endpoint := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:streamGenerateContent?key=%s",
		location,
		url.PathEscape(project),
		url.PathEscape(location),
		url.PathEscape(v.model),
		url.QueryEscape(v.apiKey),
	)

	payload := map[string]any{
		"contents": conversation,
		"generationConfig": map[string]any{
			"temperature":     0.1,
			"maxOutputTokens": minInt(v.maxOutputTokens, 512),
		},
		"tools": []map[string]any{
			{
				"functionDeclarations": []map[string]any{
					{
						"name":        "call_tool",
						"description": "Call one MCP tool in this cycle step.",
						"parameters": map[string]any{
							"type": "OBJECT",
							"properties": map[string]any{
								"tool": map[string]any{"type": "STRING"},
								"args": map[string]any{"type": "OBJECT"},
								"why":  map[string]any{"type": "STRING"},
								"on_fail": map[string]any{
									"type": "STRING",
									"enum": []string{"continue", "abort", "retry"},
								},
								"summary": map[string]any{"type": "STRING"},
							},
							"required": []string{"tool"},
						},
					},
					{
						"name":        "finish_cycle",
						"description": "Finish this cycle when no more useful tool calls remain.",
						"parameters": map[string]any{
							"type": "OBJECT",
							"properties": map[string]any{
								"summary": map[string]any{"type": "STRING"},
								"why":     map[string]any{"type": "STRING"},
							},
						},
					},
				},
			},
		},
		"toolConfig": map[string]any{
			"functionCallingConfig": map[string]any{
				"mode": "ANY",
				"allowedFunctionNames": []string{
					"call_tool", "finish_cycle",
				},
			},
		},
	}
	if strings.TrimSpace(systemPrompt) != "" {
		payload["systemInstruction"] = map[string]any{
			"role": "system",
			"parts": []map[string]any{
				{"text": systemPrompt},
			},
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("vertex api request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	type streamChunk struct {
		Candidates []struct {
			FinishReason  string `json:"finishReason,omitempty"`
			FinishMessage string `json:"finishMessage,omitempty"`
			Content       struct {
				Parts []struct {
					Text         string `json:"text,omitempty"`
					FunctionCall *struct {
						Name string         `json:"name"`
						Args map[string]any `json:"args"`
					} `json:"functionCall,omitempty"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	var chunks []streamChunk
	if err := json.Unmarshal(body, &chunks); err != nil {
		return nil, nil, fmt.Errorf("decode vertex api response: %w", err)
	}
	if len(chunks) == 0 {
		return nil, nil, fmt.Errorf("vertex api returned empty stream")
	}
	usage := &vertexUsage{
		PromptTokens:     chunks[len(chunks)-1].UsageMetadata.PromptTokenCount,
		CompletionTokens: chunks[len(chunks)-1].UsageMetadata.CandidatesTokenCount,
	}

	var parts []struct {
		Text         string `json:"text,omitempty"`
		FunctionCall *struct {
			Name string         `json:"name"`
			Args map[string]any `json:"args"`
		} `json:"functionCall,omitempty"`
	}
	var finishReason string
	var finishMessage string
	textParts := make([]string, 0, 2)
	for _, ch := range chunks {
		if len(ch.Candidates) == 0 {
			continue
		}
		cand := ch.Candidates[0]
		if strings.TrimSpace(cand.FinishReason) != "" {
			finishReason = strings.TrimSpace(cand.FinishReason)
		}
		if strings.TrimSpace(cand.FinishMessage) != "" {
			finishMessage = strings.TrimSpace(cand.FinishMessage)
		}
		parts = append(parts, cand.Content.Parts...)
	}
	if len(parts) == 0 {
		if strings.EqualFold(finishReason, "MALFORMED_FUNCTION_CALL") {
			return &cycleActionOutput{
				Decision: "finish",
				Summary:  "vertex malformed function call: " + finishMessage,
				OnFail:   "continue",
				Args:     map[string]any{},
			}, usage, nil
		}
		return &cycleActionOutput{
			Decision: "finish",
			Summary:  "vertex returned no action parts",
			OnFail:   "continue",
			Args:     map[string]any{},
		}, usage, nil
	}
	for _, part := range parts {
		if strings.TrimSpace(part.Text) != "" {
			textParts = append(textParts, strings.TrimSpace(part.Text))
		}
		if part.FunctionCall == nil {
			continue
		}
		switch strings.TrimSpace(part.FunctionCall.Name) {
		case "call_tool":
			args := part.FunctionCall.Args
			tool, _ := args["tool"].(string)
			why, _ := args["why"].(string)
			onFail, _ := args["on_fail"].(string)
			summary, _ := args["summary"].(string)
			toolArgs := map[string]any{}
			if rawArgs, ok := args["args"].(map[string]any); ok {
				toolArgs = rawArgs
			}
			out := &cycleActionOutput{
				Decision: "call_tool",
				Tool:     strings.TrimSpace(tool),
				Args:     toolArgs,
				Why:      strings.TrimSpace(why),
				OnFail:   strings.ToLower(strings.TrimSpace(onFail)),
				Summary:  strings.TrimSpace(summary),
			}
			if out.OnFail == "" {
				out.OnFail = "continue"
			}
			return out, usage, nil
		case "finish_cycle":
			args := part.FunctionCall.Args
			summary, _ := args["summary"].(string)
			why, _ := args["why"].(string)
			return &cycleActionOutput{
				Decision: "finish",
				Summary:  strings.TrimSpace(summary),
				Why:      strings.TrimSpace(why),
				OnFail:   "continue",
				Args:     map[string]any{},
			}, usage, nil
		}
	}
	summary := "vertex returned no supported function call"
	if len(textParts) > 0 {
		summary = strings.Join(textParts, " | ")
	}
	if strings.EqualFold(finishReason, "MALFORMED_FUNCTION_CALL") && finishMessage != "" {
		summary = "vertex malformed function call: " + finishMessage
	}
	return &cycleActionOutput{
		Decision: "finish",
		Summary:  summary,
		OnFail:   "continue",
		Args:     map[string]any{},
	}, usage, nil
}

func (v *vertexAIClient) validateWithAPIKeyToolCall(ctx context.Context, systemPrompt, userPrompt string) (*cycleValidationOutput, error) {
	if v == nil || strings.TrimSpace(v.apiKey) == "" {
		return nil, fmt.Errorf("vertex api key is not configured")
	}
	callCtx := ctx
	cancel := func() {}
	if v.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, v.timeout)
	}
	defer cancel()

	project := strings.TrimSpace(v.project)
	location := strings.TrimSpace(v.location)
	if project == "" {
		return nil, fmt.Errorf("vertex api key mode requires agent.vertex_project")
	}
	if location == "" {
		location = defaultVertexLocation
	}
	endpoint := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent?key=%s",
		location,
		url.PathEscape(project),
		url.PathEscape(location),
		url.PathEscape(v.model),
		url.QueryEscape(v.apiKey),
	)

	payload := map[string]any{
		"contents": []map[string]any{
			{
				"role": "user",
				"parts": []map[string]any{
					{"text": userPrompt},
				},
			},
		},
		"generationConfig": map[string]any{
			"temperature":     0.1,
			"maxOutputTokens": minInt(v.maxOutputTokens, 512),
		},
		"tools": []map[string]any{
			{
				"functionDeclarations": []map[string]any{
					{
						"name":        "write_validation",
						"description": "Return one cycle validation result.",
						"parameters": map[string]any{
							"type": "OBJECT",
							"properties": map[string]any{
								"verdict": map[string]any{
									"type": "STRING",
									"enum":  []string{"pass", "warning", "fail"},
								},
								"score": map[string]any{"type": "NUMBER"},
								"summary": map[string]any{
									"type": "STRING",
								},
								"strengths": map[string]any{
									"type":  "ARRAY",
									"items": map[string]any{"type": "STRING"},
								},
								"risks": map[string]any{
									"type":  "ARRAY",
									"items": map[string]any{"type": "STRING"},
								},
								"actions": map[string]any{
									"type":  "ARRAY",
									"items": map[string]any{"type": "STRING"},
								},
							},
							"required": []string{"verdict", "score", "summary"},
						},
					},
				},
			},
		},
		"toolConfig": map[string]any{
			"functionCallingConfig": map[string]any{
				"mode":                 "ANY",
				"allowedFunctionNames": []string{"write_validation"},
			},
		},
	}
	if strings.TrimSpace(systemPrompt) != "" {
		payload["systemInstruction"] = map[string]any{
			"role": "system",
			"parts": []map[string]any{
				{"text": systemPrompt},
			},
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("vertex api request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					FunctionCall *struct {
						Name string         `json:"name"`
						Args map[string]any `json:"args"`
					} `json:"functionCall,omitempty"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode vertex api response: %w", err)
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("vertex api returned no validation action")
	}
	for _, part := range parsed.Candidates[0].Content.Parts {
		if part.FunctionCall == nil || strings.TrimSpace(part.FunctionCall.Name) != "write_validation" {
			continue
		}
		args := part.FunctionCall.Args
		out := &cycleValidationOutput{
			Verdict: strings.ToLower(strings.TrimSpace(anyToString(args["verdict"]))),
			Score:   anyToFloat64(args["score"]),
			Summary: strings.TrimSpace(anyToString(args["summary"])),
		}
		out.Strengths = anyToStringSlice(args["strengths"])
		out.Risks = anyToStringSlice(args["risks"])
		out.Actions = anyToStringSlice(args["actions"])
		if out.Verdict == "" {
			out.Verdict = "warning"
		}
		if out.Score < 0 {
			out.Score = 0
		}
		if out.Score > 100 {
			out.Score = 100
		}
		return out, nil
	}
	return nil, fmt.Errorf("vertex api returned no supported validation function call")
}

func anyToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return ""
	}
}

func anyToFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}

func anyToStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		s := strings.TrimSpace(anyToString(it))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func buildVertexPrompt(input vertexPromptInput) string {
	return fmt.Sprintf(
		`Analyze one weather market and return JSON.

Question: %s
City: %s
Forecast Summary: %s
Market Price YES: %.4f
Our Probability YES: %.4f
Edge (pct): %.2f
Base Confidence: %.2f
Base Recommendation: %s
Existing Risk Factors: %s
Deterministic Reasoning: %s

Output requirements:
1) recommendation: concise trade action text.
2) reasoning: one concise paragraph.
3) risk_factors: 0-5 short strings.`,
		strings.TrimSpace(input.Question),
		strings.TrimSpace(input.City),
		strings.TrimSpace(input.ForecastSummary),
		input.MarketPriceYes,
		input.OurProbability,
		input.EdgePct,
		input.Confidence,
		strings.TrimSpace(input.Recommendation),
		strings.Join(input.RiskFactors, ", "),
		strings.TrimSpace(input.DeterministicRsn),
	)
}
