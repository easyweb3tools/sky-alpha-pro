package database

import (
	"time"

	"sky-alpha-pro/internal/model"

	"gorm.io/gorm"
)

const defaultPromptName = "agent_cycle"

const defaultPromptSystem = `You are Sky Alpha Pro's orchestration agent.
Generate concise tool actions for one cycle and prioritize data-readiness first.
If city/spec/forecast inputs are incomplete, fix those before signal generation.
Keep actions deterministic, auditable, and safe.`

const defaultPromptRuntimeTemplate = `{
  "goal": "single-cycle orchestration",
  "constraints": ["bounded tool calls", "no unsafe side effects"]
}`

const defaultPromptContextTemplate = `{
  "required": ["coverage", "freshness", "funnel_last_cycle", "memory_summary"]
}`

// EnsureDefaultActivePromptVersion seeds one active prompt on empty deployments.
// It is intentionally idempotent: if any active prompt exists, it does nothing.
func EnsureDefaultActivePromptVersion(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	var activeCount int64
	if err := db.Model(&model.PromptVersion{}).Where("is_active = ?", true).Count(&activeCount).Error; err != nil {
		return err
	}
	if activeCount > 0 {
		return nil
	}
	now := time.Now().UTC()
	row := model.PromptVersion{
		Name:            defaultPromptName,
		Version:         "v1.0.0",
		SystemPrompt:    defaultPromptSystem,
		RuntimeTemplate: defaultPromptRuntimeTemplate,
		ContextTemplate: defaultPromptContextTemplate,
		Stage:           "active",
		RolloutPct:      0,
		IsActive:        true,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	return db.Create(&row).Error
}

