package database

import (
	"testing"

	"sky-alpha-pro/internal/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEnsureDefaultActivePromptVersion_Idempotent(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.PromptVersion{}); err != nil {
		t.Fatalf("migrate prompt_versions: %v", err)
	}

	if err := EnsureDefaultActivePromptVersion(db); err != nil {
		t.Fatalf("seed default prompt #1: %v", err)
	}
	if err := EnsureDefaultActivePromptVersion(db); err != nil {
		t.Fatalf("seed default prompt #2: %v", err)
	}

	var total int64
	if err := db.Model(&model.PromptVersion{}).Count(&total).Error; err != nil {
		t.Fatalf("count prompts: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1 prompt row, got %d", total)
	}

	var active int64
	if err := db.Model(&model.PromptVersion{}).Where("is_active = ?", true).Count(&active).Error; err != nil {
		t.Fatalf("count active prompts: %v", err)
	}
	if active != 1 {
		t.Fatalf("expected 1 active prompt row, got %d", active)
	}
}

