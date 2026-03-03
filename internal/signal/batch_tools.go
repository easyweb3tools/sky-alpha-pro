package signal

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type ResolveCitiesOptions struct {
	Limit       int
	OnlyMissing bool
}

type ResolveCitiesResult struct {
	Processed   int            `json:"processed"`
	Resolved    int            `json:"resolved"`
	Unresolved  int            `json:"unresolved"`
	SpecReady   int            `json:"spec_ready"`
	Sources     map[string]int `json:"sources"`
	SkipReasons map[string]int `json:"skip_reasons"`
	Items       []ResolvedCity `json:"items,omitempty"`
	Errors      []string       `json:"errors,omitempty"`
}

type ResolvedCity struct {
	MarketID   string  `json:"market_id"`
	City       string  `json:"city"`
	Source     string  `json:"source"`
	Confidence float64 `json:"confidence"`
	SkipReason string  `json:"skip_reason,omitempty"`
}

func (s *Service) ResolveMarketCities(ctx context.Context, opts ResolveCitiesOptions) (*ResolveCitiesResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit <= 0 {
		limit = 100
	}
	if s.cfg.MaxMarkets > 0 && limit > s.cfg.MaxMarkets {
		limit = s.cfg.MaxMarkets
	}

	query := s.db.WithContext(ctx).
		Model(&model.Market{}).
		Where("is_active = ?", true).
		Order("end_date ASC")
	if opts.OnlyMissing {
		query = query.Where("COALESCE(city, '') = '' OR COALESCE(spec_status, '') <> ?", "ready")
	}

	var markets []model.Market
	if err := query.Limit(limit).Find(&markets).Error; err != nil {
		return nil, err
	}

	result := &ResolveCitiesResult{
		Processed:   len(markets),
		Sources:     map[string]int{},
		SkipReasons: map[string]int{},
		Items:       make([]ResolvedCity, 0, len(markets)),
		Errors:      make([]string, 0),
	}
	if len(markets) == 0 {
		return result, nil
	}

	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		return nil, err
	}

	workers := s.cfg.Concurrency
	if workers <= 0 {
		workers = 4
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, market := range markets {
		m := market
		g.Go(func() error {
			spec, skipReason, specErr := s.ensureMarketSpec(gctx, m, knownCities)
			source := s.lookupCitySource(gctx, m)
			if source == "" && normalizeCityToken(m.City) != "" {
				source = "market_field"
			}
			if source == "" {
				source = "unknown"
			}
			confidence := cityResolutionConfidence(source, spec.City)

			mu.Lock()
			defer mu.Unlock()
			result.Sources[source]++
			if skipReason != "" {
				result.SkipReasons[skipReason]++
			}
			if spec.City != "" {
				result.Resolved++
			} else {
				result.Unresolved++
			}
			if spec.Status == "ready" {
				result.SpecReady++
			}
			result.Items = append(result.Items, ResolvedCity{
				MarketID:   m.ID,
				City:       spec.City,
				Source:     source,
				Confidence: confidence,
				SkipReason: skipReason,
			})
			if specErr != nil {
				result.Errors = append(result.Errors, specErr.Error())
				if errors.Is(specErr, context.Canceled) || errors.Is(specErr, context.DeadlineExceeded) {
					return specErr
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Service) ListActiveCities(ctx context.Context) ([]string, error) {
	var values []string
	if err := s.db.WithContext(ctx).
		Model(&model.Market{}).
		Distinct("city").
		Where("is_active = ?", true).
		Where("city <> ''").
		Pluck("city", &values).Error; err != nil {
		return nil, err
	}

	citySet := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	addCity := func(v string) {
		city := normalizeCityToken(v)
		if city == "" {
			return
		}
		if _, ok := citySet[city]; ok {
			return
		}
		citySet[city] = struct{}{}
		out = append(out, city)
	}

	for _, v := range values {
		addCity(v)
	}
	if len(out) > 0 {
		sort.Strings(out)
		return out, nil
	}

	if s.cityResolver == nil {
		return out, nil
	}

	var markets []model.Market
	if err := s.db.WithContext(ctx).
		Model(&model.Market{}).
		Select("id, question, slug, city, market_type").
		Where("is_active = ?", true).
		Order("updated_at DESC").
		Find(&markets).Error; err != nil {
		return nil, err
	}
	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range markets {
		addCity(s.cityResolver.resolveByRules(m, knownCities))
	}
	sort.Strings(out)
	return out, nil
}

func (s *Service) lookupCitySource(ctx context.Context, m model.Market) string {
	if s.db == nil {
		return ""
	}
	cacheKey := buildCityCacheKey(m.Question, m.Slug)
	if cacheKey == "" {
		return ""
	}
	type row struct {
		Source string `gorm:"column:source"`
	}
	var r row
	if err := s.db.WithContext(ctx).
		Table("city_resolution_caches").
		Select("source").
		Where("cache_key = ?", cacheKey).
		Take(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ""
		}
		return ""
	}
	return r.Source
}

func cityResolutionConfidence(source, city string) float64 {
	if normalizeCityToken(city) == "" {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "market_field":
		return 1.0
	case "rule":
		return 0.85
	case "vertex_ai":
		return 0.75
	case "vertex_rate_limited", "vertex_failed", "vertex_global_backoff", "unknown":
		return 0
	default:
		return 0.5
	}
}
