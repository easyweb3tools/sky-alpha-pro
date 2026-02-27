package weather

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

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
		openMeteo:      NewOpenMeteoClient(cfg.OpenMeteoBaseURL, cfg.OpenMeteoGeocodingBaseURL, httpClient),
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
	for _, source := range sources {
		switch source {
		case "openmeteo":
			items, err := s.openMeteo.GetDailyForecast(ctx, lat, lon, days)
			if err != nil {
				response.Errors = append(response.Errors, "openmeteo: "+err.Error())
				continue
			}
			for _, item := range items {
				item.Location = normalizedLocation
				response.Forecasts = append(response.Forecasts, item)
			}
		case "nws":
			items, err := s.nws.GetDailyForecast(ctx, lat, lon, days)
			if err != nil {
				response.Errors = append(response.Errors, "nws: "+err.Error())
				continue
			}
			for _, item := range items {
				item.Location = normalizedLocation
				response.Forecasts = append(response.Forecasts, item)
			}
		case "visualcrossing":
			items, err := s.visualCrossing.GetDailyForecast(ctx, req.Location, days)
			if err != nil {
				response.Errors = append(response.Errors, "visualcrossing: "+err.Error())
				continue
			}
			for _, item := range items {
				item.Location = normalizedLocation
				response.Forecasts = append(response.Forecasts, item)
			}
		}
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
	rows := make([]model.Forecast, 0, len(forecasts))
	for _, f := range forecasts {
		rows = append(rows, model.Forecast{
			MarketID:      nil,
			StationID:     "",
			Location:      f.Location,
			ForecastDate:  f.ForecastDate.UTC(),
			Source:        f.Source,
			TempHighF:     f.TempHighF,
			TempLowF:      f.TempLowF,
			PrecipIn:      f.PrecipIn,
			WindSpeedMPH:  f.WindSpeedMPH,
			WindGustMPH:   f.WindGustMPH,
			CloudCoverPct: f.CloudCover,
			HumidityPct:   f.HumidityPct,
			FetchedAt:     time.Now().UTC(),
		})
	}
	return s.db.WithContext(ctx).Create(&rows).Error
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
