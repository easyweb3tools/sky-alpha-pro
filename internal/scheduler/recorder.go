package scheduler

import (
	"context"
	"encoding/json"

	"gorm.io/datatypes"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type GormRunRecorder struct {
	db *gorm.DB
}

func NewGormRunRecorder(db *gorm.DB) *GormRunRecorder {
	if db == nil {
		return nil
	}
	return &GormRunRecorder{db: db}
}

func (r *GormRunRecorder) RecordSchedulerRun(ctx context.Context, rec RunRecord) error {
	if r == nil || r.db == nil {
		return nil
	}

	meta := datatypes.JSON([]byte("{}"))
	if len(rec.Meta) > 0 {
		if raw, err := json.Marshal(rec.Meta); err == nil {
			meta = datatypes.JSON(raw)
		}
	}

	row := model.SchedulerRun{
		JobName:        rec.JobName,
		StartedAt:      rec.StartedAt.UTC(),
		FinishedAt:     rec.FinishedAt.UTC(),
		DurationMS:     int(rec.Duration.Milliseconds()),
		Status:         rec.Status,
		RecordsSuccess: rec.RecordsSuccess,
		RecordsError:   rec.RecordsError,
		RecordsSkipped: rec.RecordsSkipped,
		ErrorCode:      rec.ErrorCode,
		ErrorMessage:   rec.ErrorMessage,
		MetaJSON:       meta,
		CreatedAt:      rec.FinishedAt.UTC(),
	}
	return r.db.WithContext(ctx).Create(&row).Error
}
