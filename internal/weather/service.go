package weather

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

type Service struct {
	cfg            config.WeatherConfig
	db             *gorm.DB
	log            *zap.Logger
	openMeteo      *OpenMeteoClient
	nws            *NWSClient
	visualCrossing *VisualCrossingClient
}

func NewService(cfg config.WeatherConfig, db *gorm.DB, log *zap.Logger) *Service {
	httpClient := &http.Client{Timeout: cfg.RequestTimeout}
	return &Service{
		cfg:            cfg,
		db:             db,
		log:            log,
		openMeteo:      NewOpenMeteoClient(cfg.OpenMeteoBaseURL, cfg.OpenMeteoGeocodingBaseURL, cfg.OpenMeteoModels, httpClient),
		nws:            NewNWSClient(cfg.NWSBaseURL, cfg.UserAgent, httpClient),
		visualCrossing: NewVisualCrossingClient(cfg.VisualCrossingBaseURL, cfg.VisualCrossingAPIKey, httpClient),
	}
}

func (s *Service) GetForecast(ctx context.Context, req ForecastRequest) (*ForecastResponse, error) {
	days := req.Days
	if days <= 0 {
		days = 7
	}
	if days > 16 {
		days = 16
	}

	lat, lon, normalizedLocation, err := s.openMeteo.ResolveCoordinates(ctx, req.Location)
	if err != nil {
		return nil, err
	}
	response := &ForecastResponse{
		Location:  normalizedLocation,
		Latitude:  lat,
		Longitude: lon,
		Forecasts: make([]ForecastEntry, 0),
		Errors:    make([]string, 0),
	}

	sources := normalizeSources(req.Source)
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(3)

	for _, source := range sources {
		source := source
		g.Go(func() error {
			switch source {
			case "openmeteo":
				items, e := s.openMeteo.GetDailyForecast(gctx, lat, lon, days)
				mu.Lock()
				defer mu.Unlock()
				if e != nil {
					response.Errors = append(response.Errors, "openmeteo: "+e.Error())
					return nil
				}
				for _, item := range items {
					item.Location = normalizedLocation
					response.Forecasts = append(response.Forecasts, item)
				}
			case "nws":
				items, e := s.nws.GetDailyForecast(gctx, lat, lon, days)
				mu.Lock()
				defer mu.Unlock()
				if e != nil {
					response.Errors = append(response.Errors, "nws: "+e.Error())
					return nil
				}
				for _, item := range items {
					item.Location = normalizedLocation
					response.Forecasts = append(response.Forecasts, item)
				}
			case "visualcrossing":
				items, e := s.visualCrossing.GetDailyForecast(gctx, req.Location, days)
				mu.Lock()
				defer mu.Unlock()
				if e != nil {
					response.Errors = append(response.Errors, "visualcrossing: "+e.Error())
					return nil
				}
				for _, item := range items {
					item.Location = normalizedLocation
					response.Forecasts = append(response.Forecasts, item)
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(response.Forecasts, func(i, j int) bool {
		if response.Forecasts[i].ForecastDate.Equal(response.Forecasts[j].ForecastDate) {
			return response.Forecasts[i].Source < response.Forecasts[j].Source
		}
		return response.Forecasts[i].ForecastDate.Before(response.Forecasts[j].ForecastDate)
	})

	if err := s.persistForecasts(ctx, response.Forecasts); err != nil {
		response.Errors = append(response.Errors, "persist: "+err.Error())
	}

	return response, nil
}

func (s *Service) GetLatestObservation(ctx context.Context, stationID string) (*Observation, error) {
	obs, err := s.nws.GetLatestObservation(ctx, strings.TrimSpace(stationID))
	if err != nil {
		return nil, err
	}
	if err := s.persistObservation(ctx, obs); err != nil {
		s.log.Warn("persist observation failed", zap.Error(err))
	}
	return obs, nil
}

func (s *Service) persistForecasts(ctx context.Context, forecasts []ForecastEntry) error {
	if len(forecasts) == 0 {
		return nil
	}
	now := time.Now().UTC()
	todayUTC := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	for _, f := range forecasts {
		lead := int(f.ForecastDate.UTC().Sub(todayUTC).Hours() / 24)
		row := model.Forecast{
			MarketID:      nil,
			StationID:     "",
			Location:      f.Location,
			City:          normalizeCityKey(f.Location),
			ForecastDate:  f.ForecastDate.UTC(),
			Source:        f.Source,
			TempHighF:     f.TempHighF,
			TempLowF:      f.TempLowF,
			PrecipIn:      f.PrecipIn,
			WindSpeedMPH:  f.WindSpeedMPH,
			WindGustMPH:   f.WindGustMPH,
			CloudCoverPct: f.CloudCover,
			HumidityPct:   f.HumidityPct,
			LeadDays:      lead,
			FetchedAt:     now,
		}
		if err := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "location"},
					{Name: "forecast_date"},
					{Name: "source"},
				},
				DoUpdates: clause.Assignments(map[string]any{
					"city":            row.City,
					"temp_high_f":     row.TempHighF,
					"temp_low_f":      row.TempLowF,
					"precip_in":       row.PrecipIn,
					"wind_speed_mph":  row.WindSpeedMPH,
					"wind_gust_mph":   row.WindGustMPH,
					"cloud_cover_pct": row.CloudCoverPct,
					"humidity_pct":    row.HumidityPct,
					"lead_days":       row.LeadDays,
					"fetched_at":      row.FetchedAt,
				}),
			}).
			Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) persistObservation(ctx context.Context, obs *Observation) error {
	if obs == nil {
		return nil
	}
	row := model.Observation{
		StationID:    obs.StationID,
		ObservedAt:   obs.ObservedAt.UTC(),
		TempF:        obs.TempF,
		HumidityPct:  obs.HumidityPct,
		WindSpeedMPH: obs.WindSpeedMPH,
		WindDir:      obs.WindDir,
		PressureMB:   obs.PressureMB,
		Precip1hIn:   obs.Precip1hIn,
		VisibilityMi: obs.VisibilityMi,
		Description:  obs.Description,
		FetchedAt:    time.Now().UTC(),
	}
	return s.db.WithContext(ctx).Create(&row).Error
}

func normalizeSources(source string) []string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "", "all":
		return []string{"openmeteo", "nws", "visualcrossing"}
	case "nws":
		return []string{"nws"}
	case "openmeteo":
		return []string{"openmeteo"}
	case "visualcrossing":
		return []string{"visualcrossing"}
	default:
		return []string{"openmeteo"}
	}
}

func normalizeCityKey(location string) string {
	v := strings.TrimSpace(location)
	if v == "" {
		return ""
	}
	parts := strings.Split(v, ",")
	if len(parts) >= 2 {
		if _, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64); err == nil {
			if _, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err2 == nil {
				return ""
			}
		}
	}
	if idx := strings.Index(v, ","); idx > 0 {
		v = v[:idx]
	}
	return strings.ToLower(strings.TrimSpace(v))
}
