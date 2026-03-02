package signal

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/genai"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
)

type cityResolver struct {
	db         *gorm.DB
	log        *zap.Logger
	vertex     *cityVertexClient
	cacheMu    sync.RWMutex
	memCache   map[string]string
	failMu     sync.RWMutex
	failUntil  map[string]time.Time
	sf         singleflight.Group
	vertexSem  chan struct{}
	rateMu     sync.Mutex
	nextVertex time.Time
	globalMu   sync.RWMutex
	globalOff  time.Time
}

var errVertexGlobalBackoff = errors.New("vertex global backoff active")

type cityVertexClient struct {
	client  *genai.Client
	model   string
	timeout time.Duration
}

const signalCityResolverUseVertexEnv = "SKY_ALPHA_SIGNAL_CITY_RESOLVER_USE_VERTEX"

func newCityResolver(db *gorm.DB, log *zap.Logger) *cityResolver {
	useVertex := strings.EqualFold(strings.TrimSpace(os.Getenv(signalCityResolverUseVertexEnv)), "true")
	var vertexClient *cityVertexClient
	if useVertex {
		vertexClient = newCityVertexClient(log)
		if vertexClient == nil && log != nil {
			log.Warn("signal city resolver vertex enabled but unavailable; fallback to rule-only")
		}
	} else if log != nil {
		log.Info("signal city resolver uses rule-only mode", zap.String("env", signalCityResolverUseVertexEnv))
	}

	return &cityResolver{
		db:        db,
		log:       log,
		vertex:    vertexClient,
		memCache:  make(map[string]string, 256),
		failUntil: make(map[string]time.Time, 256),
		// Single worker to flatten bursts on low Vertex quota.
		vertexSem: make(chan struct{}, 1),
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
	if city, blocked := r.loadDBCache(ctx, key); blocked {
		return ""
	} else if city != "" {
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
	if !r.allowVertex(key) || !r.allowGlobalVertex() {
		return ""
	}
	v, err, _ := r.sf.Do(key, func() (any, error) {
		city, err := r.resolveCityByVertexWithRetry(ctx, m.Question, m.Slug)
		if err != nil {
			return "", err
		}
		return city, nil
	})
	if err != nil {
		if errors.Is(err, errVertexGlobalBackoff) {
			r.persistNegativeCache(ctx, key, m.Question, m.Slug, "vertex_global_backoff")
			return ""
		}
		if r.log != nil {
			r.log.Warn("vertex city resolve failed", zap.Error(err))
		}
		r.markVertexFailed(key)
		if isRateLimitVertexErr(err) {
			r.markGlobalVertexBackoff(10 * time.Minute)
			r.persistNegativeCache(ctx, key, m.Question, m.Slug, "vertex_rate_limited")
		} else {
			r.persistNegativeCache(ctx, key, m.Question, m.Slug, "vertex_failed")
		}
		return ""
	}
	city, _ := v.(string)
	city = normalizeCityToken(city)
	if city == "" {
		r.markVertexFailed(key)
		return ""
	}
	r.clearVertexFailed(key)
	r.persistCache(ctx, key, m.Question, m.Slug, city, "vertex_ai")
	r.storeMem(key, city)
	return city
}

func (r *cityResolver) resolveCityByVertexWithRetry(ctx context.Context, question string, slug string) (string, error) {
	var lastErr error
	backoff := 900 * time.Millisecond
	for attempt := 1; attempt <= 2; attempt++ {
		if !r.allowGlobalVertex() {
			return "", errVertexGlobalBackoff
		}
		if err := r.acquireVertexSlot(ctx); err != nil {
			return "", err
		}
		city, err := r.vertex.ResolveCity(ctx, question, slug)
		r.releaseVertexSlot()
		if err == nil {
			return city, nil
		}
		lastErr = err
		if !isRetryableVertexErr(err) || attempt == 2 {
			break
		}
		sleep := backoff + time.Duration(rand.Intn(250))*time.Millisecond
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(sleep):
		}
		backoff *= 2
	}
	return "", lastErr
}

func isRetryableVertexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "deadline_exceeded") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "502") ||
		strings.Contains(msg, "504") ||
		strings.Contains(msg, "bad gateway") ||
		strings.Contains(msg, "too many requests")
}

func isRateLimitVertexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests")
}

func (r *cityResolver) allowVertex(key string) bool {
	if key == "" {
		return true
	}
	r.failMu.RLock()
	defer r.failMu.RUnlock()
	until, ok := r.failUntil[key]
	return !ok || time.Now().UTC().After(until)
}

func (r *cityResolver) markVertexFailed(key string) {
	if key == "" {
		return
	}
	r.failMu.Lock()
	defer r.failMu.Unlock()
	r.failUntil[key] = time.Now().UTC().Add(5 * time.Minute)
}

func (r *cityResolver) clearVertexFailed(key string) {
	if key == "" {
		return
	}
	r.failMu.Lock()
	defer r.failMu.Unlock()
	delete(r.failUntil, key)
}

func (r *cityResolver) allowGlobalVertex() bool {
	r.globalMu.RLock()
	defer r.globalMu.RUnlock()
	return r.globalOff.IsZero() || time.Now().UTC().After(r.globalOff)
}

func (r *cityResolver) markGlobalVertexBackoff(d time.Duration) {
	if d <= 0 {
		return
	}
	r.globalMu.Lock()
	defer r.globalMu.Unlock()
	until := time.Now().UTC().Add(d)
	if until.After(r.globalOff) {
		r.globalOff = until
	}
}

func (r *cityResolver) acquireVertexSlot(ctx context.Context) error {
	select {
	case r.vertexSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.rateMu.Lock()
	now := time.Now().UTC()
	if r.nextVertex.IsZero() || now.After(r.nextVertex) {
		r.nextVertex = now
	}
	wait := time.Until(r.nextVertex)
	r.nextVertex = r.nextVertex.Add(1200 * time.Millisecond)
	r.rateMu.Unlock()

	if wait > 0 {
		select {
		case <-ctx.Done():
			r.releaseVertexSlot()
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

func (r *cityResolver) releaseVertexSlot() {
	select {
	case <-r.vertexSem:
	default:
	}
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

func (r *cityResolver) loadDBCache(ctx context.Context, key string) (string, bool) {
	if r.db == nil || key == "" {
		return "", false
	}
	var rows []model.CityResolutionCache
	if err := r.db.WithContext(ctx).
		Where("cache_key = ?", key).
		Limit(1).
		Find(&rows).Error; err != nil || len(rows) == 0 {
		return "", false
	}
	row := rows[0]
	src := strings.ToLower(strings.TrimSpace(row.Source))
	if strings.HasPrefix(src, "vertex_") && (strings.Contains(src, "failed") || strings.Contains(src, "rate_limited") || strings.Contains(src, "global_backoff")) {
		if row.UpdatedAt.After(time.Now().UTC().Add(-10 * time.Minute)) {
			return "", true
		}
	}
	return normalizeCityToken(row.City), false
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

func (r *cityResolver) persistNegativeCache(ctx context.Context, key string, question string, slug string, source string) {
	if r.db == nil || key == "" {
		return
	}
	row := model.CityResolutionCache{
		CacheKey:  key,
		Question:  strings.TrimSpace(question),
		Slug:      strings.TrimSpace(slug),
		City:      "",
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
