package opportunity

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/metrics"
)

const (
	stateCold     = "cold"
	stateWatch    = "watch"
	stateHot      = "hot"
	stateTradable = "tradable"
	stateCooldown = "cool_down"
)

type Service struct {
	db      *gorm.DB
	metrics *metrics.Registry
}

type EmitInput struct {
	EventType  string
	MarketID   string
	Severity   string
	DedupKey   string
	Payload    map[string]any
	OccurredAt time.Time
}

type EventSummary struct {
	WindowHours   int64              `json:"window_hours"`
	TotalEvents   int64              `json:"total_events"`
	PendingEvents int64              `json:"pending_events"`
	DedupDropped  int64              `json:"dedup_dropped"`
	ByType        []EventTypeCounter `json:"by_type"`
}

type EventTypeCounter struct {
	Type  string `json:"type"`
	Count int64  `json:"count"`
}

type CandidateSummary struct {
	States    map[string]int64 `json:"states"`
	Top       []CandidateItem  `json:"top_candidates"`
	UpdatedAt *string          `json:"updated_at,omitempty"`
}

type CycleResult struct {
	Consumed int `json:"consumed"`
	Decayed  int `json:"decayed"`
}

type CandidateItem struct {
	MarketID string  `json:"market_id"`
	State    string  `json:"state"`
	Score    float64 `json:"score"`
}

type CandidateTransitionItem struct {
	ID          uint64  `json:"id"`
	MarketID    string  `json:"market_id"`
	FromState   string  `json:"from_state"`
	ToState     string  `json:"to_state"`
	EventType   string  `json:"event_type"`
	Reason      string  `json:"reason"`
	ScoreBefore float64 `json:"score_before"`
	ScoreAfter  float64 `json:"score_after"`
	OccurredAt  string  `json:"occurred_at"`
}

type ScoreWeights struct {
	EdgeRefPct      float64
	WeightEdge      float64
	WeightLiquidity float64
	WeightFreshness float64
	WeightExpiry    float64
	FreshnessMaxAge time.Duration
}

func NewService(db *gorm.DB) *Service {
	return &Service{db: db}
}

func (s *Service) SetMetrics(reg *metrics.Registry) {
	if s == nil {
		return
	}
	s.metrics = reg
}

func (s *Service) Emit(ctx context.Context, in EmitInput) error {
	if s == nil || s.db == nil {
		return nil
	}
	eventType := strings.TrimSpace(strings.ToLower(in.EventType))
	if eventType == "" {
		return fmt.Errorf("event_type is required")
	}
	severity := strings.TrimSpace(strings.ToLower(in.Severity))
	if severity == "" {
		severity = "info"
	}
	occurredAt := in.OccurredAt.UTC()
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	dedupKey := strings.TrimSpace(in.DedupKey)
	if dedupKey == "" {
		dedupKey = buildDefaultDedupKey(eventType, in.MarketID, occurredAt)
	}
	payloadJSON := datatypes.JSON([]byte("{}"))
	if len(in.Payload) > 0 {
		if raw, err := json.Marshal(in.Payload); err == nil {
			payloadJSON = datatypes.JSON(raw)
		}
	}

	windowStart := occurredAt.Add(-60 * time.Second)
	var exists int64
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).
		Where("dedup_key = ? AND occurred_at >= ?", dedupKey, windowStart).
		Count(&exists).Error; err != nil {
		return err
	}
	var marketIDPtr *string
	if v := strings.TrimSpace(in.MarketID); v != "" {
		marketIDPtr = &v
	}
	if exists > 0 {
		dupEvent := model.OpportunityEvent{
			EventType:  eventType,
			MarketID:   marketIDPtr,
			Severity:   severity,
			DedupKey:   dedupKey,
			Payload:    payloadJSON,
			OccurredAt: occurredAt,
			CreatedAt:  time.Now().UTC(),
			Status:     "dedup_dropped",
		}
		_ = s.db.WithContext(ctx).Create(&dupEvent).Error
		if s.metrics != nil {
			s.metrics.AddOpportunityEvent(eventType, "dedup_dropped", 1)
		}
		return nil
	}

	event := model.OpportunityEvent{
		EventType:  eventType,
		MarketID:   marketIDPtr,
		Severity:   severity,
		DedupKey:   dedupKey,
		Payload:    payloadJSON,
		OccurredAt: occurredAt,
		CreatedAt:  time.Now().UTC(),
		Status:     "pending",
	}
	if err := s.db.WithContext(ctx).Create(&event).Error; err != nil {
		return err
	}
	if s.metrics != nil {
		s.metrics.AddOpportunityEvent(eventType, "pending", 1)
	}
	if marketIDPtr != nil {
		if err := s.bumpCandidate(ctx, *marketIDPtr, eventType, severity, occurredAt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) EmitMarketEvent(ctx context.Context, eventType, marketID, severity string, payload map[string]any) error {
	return s.Emit(ctx, EmitInput{
		EventType:  eventType,
		MarketID:   marketID,
		Severity:   severity,
		DedupKey:   strings.TrimSpace(eventType) + ":" + strings.TrimSpace(marketID),
		Payload:    payload,
		OccurredAt: time.Now().UTC(),
	})
}

func (s *Service) Summary(ctx context.Context, windowHours int64) (EventSummary, error) {
	if windowHours <= 0 {
		windowHours = 24
	}
	out := EventSummary{
		WindowHours: windowHours,
		ByType:      make([]EventTypeCounter, 0),
	}
	if s == nil || s.db == nil {
		return out, nil
	}
	cutoff := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).Where("occurred_at >= ?", cutoff).Count(&out.TotalEvents).Error; err != nil {
		return out, err
	}
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).Where("status = ?", "pending").Count(&out.PendingEvents).Error; err != nil {
		return out, err
	}
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).
		Where("status = ? AND occurred_at >= ?", "dedup_dropped", cutoff).
		Count(&out.DedupDropped).Error; err != nil {
		return out, err
	}
	type row struct {
		EventType string `gorm:"column:event_type"`
		Count     int64  `gorm:"column:count"`
	}
	var rows []row
	if err := s.db.WithContext(ctx).Table("opportunity_events").
		Select("event_type, COUNT(*) AS count").
		Where("occurred_at >= ?", cutoff).
		Group("event_type").
		Order("count DESC").
		Scan(&rows).Error; err != nil {
		return out, err
	}
	for _, r := range rows {
		out.ByType = append(out.ByType, EventTypeCounter{Type: r.EventType, Count: r.Count})
	}
	return out, nil
}

func (s *Service) CandidatePoolSummary(ctx context.Context, topN int) (CandidateSummary, error) {
	if topN <= 0 {
		topN = 20
	}
	out := CandidateSummary{
		States: map[string]int64{
			stateCold:     0,
			stateWatch:    0,
			stateHot:      0,
			stateTradable: 0,
			stateCooldown: 0,
		},
		Top: make([]CandidateItem, 0),
	}
	if s == nil || s.db == nil {
		return out, nil
	}
	type stateRow struct {
		State string `gorm:"column:state"`
		Count int64  `gorm:"column:count"`
	}
	var stateRows []stateRow
	if err := s.db.WithContext(ctx).Table("candidate_markets").
		Select("state, COUNT(*) AS count").
		Group("state").
		Scan(&stateRows).Error; err != nil {
		return out, err
	}
	for _, r := range stateRows {
		out.States[r.State] = r.Count
	}

	var rows []model.CandidateMarket
	if err := s.db.WithContext(ctx).Order("score DESC").Limit(topN).Find(&rows).Error; err != nil {
		return out, err
	}
	for _, r := range rows {
		out.Top = append(out.Top, CandidateItem{
			MarketID: r.MarketID,
			State:    r.State,
			Score:    r.Score,
		})
	}
	var latest model.CandidateMarket
	if err := s.db.WithContext(ctx).Order("updated_at DESC").Limit(1).Take(&latest).Error; err == nil {
		v := latest.UpdatedAt.UTC().Format(time.RFC3339)
		out.UpdatedAt = &v
	}
	return out, nil
}

func (s *Service) CandidateTransitions(ctx context.Context, marketID string, limit int) ([]CandidateTransitionItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if s == nil || s.db == nil {
		return []CandidateTransitionItem{}, nil
	}
	query := s.db.WithContext(ctx).
		Model(&model.CandidateStateTransition{}).
		Order("occurred_at DESC, id DESC").
		Limit(limit)
	if strings.TrimSpace(marketID) != "" {
		query = query.Where("market_id = ?", strings.TrimSpace(marketID))
	}
	var rows []model.CandidateStateTransition
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]CandidateTransitionItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, CandidateTransitionItem{
			ID:          row.ID,
			MarketID:    row.MarketID,
			FromState:   row.FromState,
			ToState:     row.ToState,
			EventType:   row.EventType,
			Reason:      row.Reason,
			ScoreBefore: row.ScoreBefore,
			ScoreAfter:  row.ScoreAfter,
			OccurredAt:  row.OccurredAt.UTC().Format(time.RFC3339),
		})
	}
	return out, nil
}

func (s *Service) RecomputeCandidateScores(ctx context.Context, weights ScoreWeights) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if weights.EdgeRefPct <= 0 {
		weights.EdgeRefPct = 10
	}
	if weights.FreshnessMaxAge <= 0 {
		weights.FreshnessMaxAge = 2 * time.Hour
	}
	weightSum := weights.WeightEdge + weights.WeightLiquidity + weights.WeightFreshness + weights.WeightExpiry
	if weightSum <= 0 {
		weights.WeightEdge = 0.55
		weights.WeightLiquidity = 0.20
		weights.WeightFreshness = 0.15
		weights.WeightExpiry = 0.10
		weightSum = 1.0
	}

	var rows []model.CandidateMarket
	if err := s.db.WithContext(ctx).
		Where("state IN ?", []string{stateWatch, stateHot, stateTradable}).
		Find(&rows).Error; err != nil {
		return 0, err
	}
	updated := 0
	now := time.Now().UTC()
	for _, row := range rows {
		edgeAbs, liquidity, freshness, expiry, err := s.loadCandidateScoreInputs(ctx, row.MarketID, now, weights.FreshnessMaxAge)
		if err != nil {
			return updated, err
		}
		edgeNorm := clamp01(edgeAbs / weights.EdgeRefPct)
		liquidityNorm := normalizeLiquidity(liquidity)
		freshnessNorm := clamp01(freshness)
		expiryNorm := clamp01(expiry)
		score01 := (weights.WeightEdge*edgeNorm +
			weights.WeightLiquidity*liquidityNorm +
			weights.WeightFreshness*freshnessNorm +
			weights.WeightExpiry*expiryNorm) / weightSum
		score := math.Round(score01*10000) / 100 // two decimals in 0~100 range
		if err := s.db.WithContext(ctx).Model(&model.CandidateMarket{}).
			Where("market_id = ?", row.MarketID).
			Update("score", score).Error; err != nil {
			return updated, err
		}
		updated++
	}
	if s.metrics != nil {
		s.refreshCandidatePoolMetrics(ctx)
	}
	return updated, nil
}

func (s *Service) ConsumePending(ctx context.Context, limit int) (int, *time.Time, error) {
	if limit <= 0 {
		limit = 200
	}
	if s == nil || s.db == nil {
		return 0, nil, nil
	}
	type eventHead struct {
		ID         uint64    `gorm:"column:id"`
		OccurredAt time.Time `gorm:"column:occurred_at"`
	}
	var heads []eventHead
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).
		Select("id, occurred_at").
		Where("status = ?", "pending").
		Order("occurred_at ASC, id ASC").
		Limit(limit).
		Find(&heads).Error; err != nil {
		return 0, nil, err
	}
	ids := make([]uint64, 0, len(heads))
	var firstAt *time.Time
	for i := range heads {
		ids = append(ids, heads[i].ID)
		if firstAt == nil {
			t := heads[i].OccurredAt.UTC()
			firstAt = &t
		}
	}
	if len(ids) == 0 {
		return 0, nil, nil
	}
	now := time.Now().UTC()
	if err := s.db.WithContext(ctx).Model(&model.OpportunityEvent{}).
		Where("id IN ?", ids).
		Updates(map[string]any{
			"status":       "processed",
			"processed_at": now,
		}).Error; err != nil {
		return 0, nil, err
	}
	return len(ids), firstAt, nil
}

func (s *Service) DecayCandidates(ctx context.Context, watchAfter, cooldownAfter time.Duration) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	now := time.Now().UTC()
	if watchAfter <= 0 {
		watchAfter = 45 * time.Minute
	}
	if cooldownAfter <= 0 {
		cooldownAfter = 30 * time.Minute
	}
	decayed := 0
	var watchRows []model.CandidateMarket
	if err := s.db.WithContext(ctx).
		Where("state = ? AND updated_at < ?", stateWatch, now.Add(-watchAfter)).
		Find(&watchRows).Error; err != nil {
		return decayed, err
	}
	for _, row := range watchRows {
		before := row.Score
		if err := s.db.WithContext(ctx).Model(&model.CandidateMarket{}).
			Where("market_id = ?", row.MarketID).
			Updates(map[string]any{
				"state":                stateCold,
				"score":                0.0,
				"last_state_change_at": now,
				"updated_at":           now,
			}).Error; err != nil {
			return decayed, err
		}
		decayed++
		s.recordTransition(ctx, row.MarketID, row.State, stateCold, "system_decay", "watch_timeout", before, 0, now)
	}

	var cooldownRows []model.CandidateMarket
	if err := s.db.WithContext(ctx).
		Where("state = ? AND updated_at < ?", stateCooldown, now.Add(-cooldownAfter)).
		Find(&cooldownRows).Error; err != nil {
		return decayed, err
	}
	for _, row := range cooldownRows {
		before := row.Score
		if err := s.db.WithContext(ctx).Model(&model.CandidateMarket{}).
			Where("market_id = ?", row.MarketID).
			Updates(map[string]any{
				"state":                stateCold,
				"score":                0.0,
				"cooldown_until":       nil,
				"last_state_change_at": now,
				"updated_at":           now,
			}).Error; err != nil {
			return decayed, err
		}
		decayed++
		s.recordTransition(ctx, row.MarketID, row.State, stateCold, "system_decay", "cooldown_timeout", before, 0, now)
	}
	if s.metrics != nil {
		s.refreshCandidatePoolMetrics(ctx)
	}
	return decayed, nil
}

func (s *Service) TopCandidateMarketIDs(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	if s == nil || s.db == nil {
		return []string{}, nil
	}
	var ids []string
	if err := s.db.WithContext(ctx).
		Table("candidate_markets").
		Select("market_id").
		Where("state IN ?", []string{stateHot, stateTradable}).
		Order("score DESC").
		Limit(limit).
		Pluck("market_id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *Service) SelectCandidateMarketIDs(ctx context.Context, limit int, maxHot int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	if s == nil || s.db == nil {
		return []string{}, nil
	}
	type row struct {
		MarketID string `gorm:"column:market_id"`
		State    string `gorm:"column:state"`
	}
	rows := make([]row, 0, limit*2)
	if err := s.db.WithContext(ctx).
		Table("candidate_markets").
		Select("market_id, state").
		Where("state IN ?", []string{stateTradable, stateHot}).
		Order("CASE WHEN state = 'tradable' THEN 0 ELSE 1 END ASC").
		Order("score DESC").
		Limit(limit * 4).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	ids := make([]string, 0, limit)
	hotUsed := 0
	for _, row := range rows {
		if len(ids) >= limit {
			break
		}
		state := strings.TrimSpace(strings.ToLower(row.State))
		switch state {
		case stateTradable:
			ids = append(ids, row.MarketID)
		case stateHot:
			if maxHot > 0 && hotUsed >= maxHot {
				continue
			}
			ids = append(ids, row.MarketID)
			hotUsed++
		}
	}
	return ids, nil
}

func (s *Service) bumpCandidate(ctx context.Context, marketID, eventType, severity string, occurredAt time.Time) error {
	state := stateWatch
	scoreDelta := 1.0
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "price_jump":
		state = stateHot
		scoreDelta = 3
	case "forecast_delta_large", "forecast_update":
		state = stateWatch
		scoreDelta = 2
	case "expiry_window_entered":
		state = stateHot
		scoreDelta = 2.5
	case "manual_probe":
		state = stateHot
		scoreDelta = 4
	}
	if strings.EqualFold(strings.TrimSpace(severity), "critical") {
		state = stateTradable
		scoreDelta += 2
	}

	now := time.Now().UTC()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model.CandidateMarket
		if err := tx.WithContext(ctx).Where("market_id = ?", marketID).Take(&row).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				newRow := model.CandidateMarket{
					MarketID:          marketID,
					State:             state,
					Score:             scoreDelta,
					TriggerCount24H:   1,
					LastTriggerAt:     &occurredAt,
					LastStateChangeAt: now,
					UpdatedAt:         now,
					Meta:              datatypes.JSON([]byte("{}")),
				}
				if err := tx.WithContext(ctx).Create(&newRow).Error; err != nil {
					return err
				}
				return s.recordTransitionWithTx(ctx, tx, marketID, "", state, eventType, "event_bump_create", 0, scoreDelta, occurredAt)
			}
			return err
		}
		prevState := strings.TrimSpace(strings.ToLower(row.State))
		nextState := state
		if prevState == stateTradable || prevState == stateHot {
			nextState = prevState
		}
		nextScore := row.Score + scoreDelta
		if nextScore > 100 {
			nextScore = 100
		}
		updates := map[string]any{
			"state":             nextState,
			"score":             nextScore,
			"trigger_count_24h": gorm.Expr("candidate_markets.trigger_count_24h + 1"),
			"last_trigger_at":   occurredAt,
			"updated_at":        now,
		}
		if nextState != prevState {
			updates["last_state_change_at"] = now
		}
		if err := tx.WithContext(ctx).Model(&model.CandidateMarket{}).
			Where("market_id = ?", marketID).
			Updates(updates).Error; err != nil {
			return err
		}
		if nextState != prevState {
			return s.recordTransitionWithTx(ctx, tx, marketID, prevState, nextState, eventType, "event_bump", row.Score, nextScore, occurredAt)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if s.metrics != nil {
		s.refreshCandidatePoolMetrics(ctx)
	}
	return nil
}

func buildDefaultDedupKey(eventType, marketID string, occurredAt time.Time) string {
	minute := occurredAt.UTC().Format("200601021504")
	if strings.TrimSpace(marketID) == "" {
		return eventType + "::" + minute
	}
	return eventType + ":" + marketID + ":" + minute
}

func (s *Service) recordTransition(ctx context.Context, marketID, fromState, toState, eventType, reason string, scoreBefore, scoreAfter float64, occurredAt time.Time) {
	if s == nil || s.db == nil {
		return
	}
	_ = s.recordTransitionWithTx(ctx, s.db.WithContext(ctx), marketID, fromState, toState, eventType, reason, scoreBefore, scoreAfter, occurredAt)
}

func (s *Service) recordTransitionWithTx(ctx context.Context, tx *gorm.DB, marketID, fromState, toState, eventType, reason string, scoreBefore, scoreAfter float64, occurredAt time.Time) error {
	from := strings.TrimSpace(strings.ToLower(fromState))
	to := strings.TrimSpace(strings.ToLower(toState))
	if tx == nil || from == to {
		return nil
	}
	row := model.CandidateStateTransition{
		MarketID:    marketID,
		FromState:   from,
		ToState:     to,
		EventType:   strings.TrimSpace(strings.ToLower(eventType)),
		Reason:      strings.TrimSpace(strings.ToLower(reason)),
		ScoreBefore: scoreBefore,
		ScoreAfter:  scoreAfter,
		OccurredAt:  occurredAt.UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	return tx.WithContext(ctx).Create(&row).Error
}

func (s *Service) refreshCandidatePoolMetrics(ctx context.Context) {
	if s == nil || s.db == nil || s.metrics == nil || !s.metrics.Enabled() {
		return
	}
	type row struct {
		State string `gorm:"column:state"`
		Count int64  `gorm:"column:count"`
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Table("candidate_markets").
		Select("state, COUNT(*) AS count").
		Group("state").
		Scan(&rows).Error; err != nil {
		return
	}
	stateCounts := map[string]int64{
		stateCold:     0,
		stateWatch:    0,
		stateHot:      0,
		stateTradable: 0,
		stateCooldown: 0,
	}
	for _, row := range rows {
		stateCounts[strings.TrimSpace(strings.ToLower(row.State))] = row.Count
	}
	s.metrics.SetCandidatePoolStateCounts(stateCounts)
}

func (s *Service) loadCandidateScoreInputs(ctx context.Context, marketID string, now time.Time, freshnessMaxAge time.Duration) (edgeAbs float64, liquidity float64, freshness float64, expiry float64, err error) {
	var edgeRow struct {
		Edge sql.NullFloat64 `gorm:"column:edge"`
	}
	if err = s.db.WithContext(ctx).
		Table("signals").
		Select("ABS(edge_exec_pct) AS edge").
		Where("market_id = ?", marketID).
		Order("created_at DESC").
		Limit(1).
		Scan(&edgeRow).Error; err != nil {
		return
	}
	if edgeRow.Edge.Valid {
		edgeAbs = edgeRow.Edge.Float64
	}

	var marketRow struct {
		Liquidity decimal.NullDecimal `gorm:"column:liquidity"`
		EndDate   time.Time           `gorm:"column:end_date"`
	}
	if err = s.db.WithContext(ctx).
		Table("markets").
		Select("liquidity, end_date").
		Where("id = ?", marketID).
		Take(&marketRow).Error; err != nil {
		return
	}
	if marketRow.Liquidity.Valid {
		liquidity = marketRow.Liquidity.Decimal.InexactFloat64()
	}

	var priceRow struct {
		Volume24h  decimal.NullDecimal `gorm:"column:volume_24h"`
		CapturedAt sql.NullTime        `gorm:"column:captured_at"`
	}
	if err = s.db.WithContext(ctx).
		Table("market_prices").
		Select("volume_24h, captured_at").
		Where("market_id = ?", marketID).
		Order("captured_at DESC").
		Limit(1).
		Scan(&priceRow).Error; err != nil {
		return
	}
	if priceRow.Volume24h.Valid {
		v := priceRow.Volume24h.Decimal.InexactFloat64()
		if v > liquidity {
			liquidity = v
		}
	}
	if priceRow.CapturedAt.Valid && !priceRow.CapturedAt.Time.IsZero() {
		age := now.Sub(priceRow.CapturedAt.Time.UTC())
		if age < 0 {
			age = 0
		}
		freshness = 1 - clamp01(age.Seconds()/freshnessMaxAge.Seconds())
	} else {
		freshness = 0
	}
	expiry = expiryWeight(marketRow.EndDate, now)
	return
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func normalizeLiquidity(v float64) float64 {
	if v <= 0 {
		return 0
	}
	// Saturates around 50k notional liquidity/volume.
	return clamp01(math.Log1p(v) / math.Log1p(50000))
}

func expiryWeight(endDate time.Time, now time.Time) float64 {
	if endDate.IsZero() {
		return 0.2
	}
	d := endDate.Sub(now)
	switch {
	case d <= 0:
		return 0
	case d <= 2*time.Hour:
		return 1.0
	case d <= 6*time.Hour:
		return 0.9
	case d <= 24*time.Hour:
		return 0.75
	case d <= 72*time.Hour:
		return 0.55
	default:
		return 0.35
	}
}
