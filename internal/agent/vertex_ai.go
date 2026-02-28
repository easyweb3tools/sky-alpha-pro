package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/genai"

	"sky-alpha-pro/pkg/config"
)

const (
	defaultVertexLocation        = "us-central1"
	defaultVertexModel           = "gemini-2.5-flash"
	defaultVertexTemperature     = float32(0.2)
	defaultVertexMaxOutputTokens = 600
	defaultVertexTimeout         = 15 * time.Second
)

type vertexAIClient struct {
	client          *genai.Client
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
	project := strings.TrimSpace(cfg.VertexProject)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  project,
		Location: location,
	})
	if err != nil {
		return nil, err
	}

	if log != nil {
		log.Info("vertex ai client enabled",
			zap.String("project", project),
			zap.String("location", location),
			zap.String("model", model),
		)
	}

	return &vertexAIClient{
		client:          client,
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
	if v == nil || v.client == nil {
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
