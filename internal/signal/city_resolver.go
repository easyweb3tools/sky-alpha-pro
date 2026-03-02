package signal

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/genai"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
)

type cityResolver struct {
	db       *gorm.DB
	log      *zap.Logger
	vertex   *cityVertexClient
	cacheMu  sync.RWMutex
	memCache map[string]string
}

type cityVertexClient struct {
	client  *genai.Client
	model   string
	timeout time.Duration
}

func newCityResolver(db *gorm.DB, log *zap.Logger) *cityResolver {
	return &cityResolver{
		db:       db,
		log:      log,
		vertex:   newCityVertexClient(log),
		memCache: make(map[string]string, 256),
	}
}

func (r *cityResolver) Resolve(ctx context.Context, m model.Market, knownCities []string) string {
	if city := normalizeCityToken(m.City); city != "" {
		return city
	}
	key := buildCityCacheKey(m.Question, m.Slug)
	if key == "" {
		return r.resolveByRules(m, knownCities)
	}
	if city := r.loadMem(key); city != "" {
		return city
	}
	if city := r.loadDBCache(ctx, key); city != "" {
		r.storeMem(key, city)
		return city
	}
	if city := r.resolveByRules(m, knownCities); city != "" {
		// Rule-based result is deterministic for same question/slug, cache it.
		r.persistCache(ctx, key, m.Question, m.Slug, city, "rule")
		r.storeMem(key, city)
		return city
	}
	if r.vertex == nil {
		return ""
	}
	city, err := r.vertex.ResolveCity(ctx, m.Question, m.Slug)
	if err != nil {
		if r.log != nil {
			r.log.Warn("vertex city resolve failed", zap.Error(err))
		}
		return ""
	}
	city = normalizeCityToken(city)
	if city == "" {
		return ""
	}
	r.persistCache(ctx, key, m.Question, m.Slug, city, "vertex_ai")
	r.storeMem(key, city)
	return city
}

func (r *cityResolver) resolveByRules(m model.Market, knownCities []string) string {
	q := normalizeText(m.Question + " " + strings.ReplaceAll(m.Slug, "-", " "))
	if q == "" {
		return ""
	}
	for _, city := range knownCities {
		if strings.Contains(q, city) {
			return city
		}
	}
	for _, alias := range defaultCityAliases {
		if strings.Contains(q, alias.Alias) {
			return alias.City
		}
	}
	return ""
}

func (r *cityResolver) loadMem(key string) string {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()
	return r.memCache[key]
}

func (r *cityResolver) storeMem(key string, city string) {
	if key == "" || city == "" {
		return
	}
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.memCache[key] = city
}

func (r *cityResolver) loadDBCache(ctx context.Context, key string) string {
	if r.db == nil || key == "" {
		return ""
	}
	var row model.CityResolutionCache
	err := r.db.WithContext(ctx).Where("cache_key = ?", key).Take(&row).Error
	if err != nil {
		return ""
	}
	return normalizeCityToken(row.City)
}

func (r *cityResolver) persistCache(ctx context.Context, key string, question string, slug string, city string, source string) {
	if r.db == nil || key == "" || city == "" {
		return
	}
	row := model.CityResolutionCache{
		CacheKey:  key,
		Question:  strings.TrimSpace(question),
		Slug:      strings.TrimSpace(slug),
		City:      city,
		Source:    source,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_ = r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "cache_key"}},
			DoUpdates: clause.Assignments(map[string]any{
				"city":       row.City,
				"source":     row.Source,
				"updated_at": row.UpdatedAt,
			}),
		}).
		Create(&row).Error
}

func buildCityCacheKey(question string, slug string) string {
	raw := strings.TrimSpace(strings.ToLower(question)) + "|" + strings.TrimSpace(strings.ToLower(slug))
	raw = strings.Join(strings.Fields(raw), " ")
	if raw == "|" || raw == "" {
		return ""
	}
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func normalizeText(v string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(v))), " ")
}

func newCityVertexClient(log *zap.Logger) *cityVertexClient {
	project := strings.TrimSpace(os.Getenv("SKY_ALPHA_AGENT_VERTEX_PROJECT"))
	if project == "" {
		return nil
	}
	location := strings.TrimSpace(os.Getenv("SKY_ALPHA_AGENT_VERTEX_LOCATION"))
	if location == "" {
		location = "us-central1"
	}
	modelName := strings.TrimSpace(os.Getenv("SKY_ALPHA_AGENT_VERTEX_MODEL"))
	if modelName == "" {
		modelName = "gemini-2.5-flash"
	}
	timeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  project,
		Location: location,
	})
	if err != nil {
		if log != nil {
			log.Warn("init city vertex client failed", zap.Error(err))
		}
		return nil
	}
	return &cityVertexClient{
		client:  client,
		model:   modelName,
		timeout: timeout,
	}
}

func (c *cityVertexClient) ResolveCity(ctx context.Context, question string, slug string) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("vertex client not configured")
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	prompt := fmt.Sprintf(`Extract the weather city from this Polymarket market.
Return JSON only with {"city":"<lowercase city or empty>"}.

question: %s
slug: %s`, strings.TrimSpace(question), strings.TrimSpace(slug))

	schema := &genai.Schema{
		Type: genai.TypeObject,
		Properties: map[string]*genai.Schema{
			"city": {Type: genai.TypeString},
		},
		Required: []string{"city"},
	}
	resp, err := c.client.Models.GenerateContent(callCtx, c.model, genai.Text(prompt), &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
		ResponseSchema:   schema,
		Temperature:      genai.Ptr(float32(0)),
		MaxOutputTokens:  48,
	})
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(resp.Text())
	if text == "" {
		return "", nil
	}
	var parsed struct {
		City string `json:"city"`
	}
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return "", err
	}
	return parsed.City, nil
}
