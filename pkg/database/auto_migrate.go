package database

import (
	"fmt"

	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

func AutoMigrate(db *gorm.DB) error {
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		return fmt.Errorf("create extension pgcrypto: %w", err)
	}

	return db.AutoMigrate(
		&model.Market{},
		&model.MarketPrice{},
		&model.Forecast{},
		&model.Observation{},
		&model.Signal{},
		&model.SignalRun{},
		&model.CityResolutionCache{},
		&model.Trade{},
		&model.Competitor{},
		&model.CompetitorTrade{},
		&model.Player{},
		&model.PlayerPosition{},
		&model.WeatherStation{},
		&model.AgentLog{},
		&model.PromptVersion{},
		&model.AgentSession{},
		&model.AgentStep{},
		&model.AgentMemory{},
		&model.AgentReport{},
		&model.AgentValidation{},
		&model.SchedulerRun{},
	)
}
