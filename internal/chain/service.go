package chain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

const (
	defaultLookbackBlocks       = uint64(2000)
	defaultMaxTx                = 2000
	defaultListLimit            = 20
	defaultTradesLimit          = 50
	defaultBotMinTrades         = 8
	defaultBotMaxAvgIntervalSec = 8.0
	maxListLimit                = 500
	maxScanTx                   = 10000
)

var (
	ErrChainUnavailable = errors.New("chain service unavailable")
	ErrChainBadRequest  = errors.New("chain bad request")
	ErrChainNotFound    = errors.New("chain not found")
)

type ethClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]ethtypes.Log, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*ethtypes.Transaction, bool, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*ethtypes.Header, error)
	Close()
}

type Service struct {
	cfg    config.ChainConfig
	db     *gorm.DB
	log    *zap.Logger
	client ethClient
	mu     sync.Mutex
}

type observedTx struct {
	Address      string
	TxHash       string
	BlockNumber  uint64
	Timestamp    time.Time
	GasPriceGwei decimal.Decimal
}

func NewService(cfg config.ChainConfig, db *gorm.DB, log *zap.Logger) *Service {
	return &Service{cfg: cfg, db: db, log: log}
}

func newServiceWithClient(cfg config.ChainConfig, db *gorm.DB, log *zap.Logger, c ethClient) *Service {
	return &Service{cfg: cfg, db: db, log: log, client: c}
}

func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *Service) ensureClient(ctx context.Context) (ethClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return s.client, nil
	}
	rpcURL := strings.TrimSpace(s.cfg.RPCURL)
	if rpcURL == "" {
		return nil, fmt.Errorf("%w: chain.rpc_url is empty", ErrChainUnavailable)
	}
	c, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("%w: dial rpc: %v", ErrChainUnavailable, err)
	}
	s.client = c
	return s.client, nil
}

func (s *Service) Scan(ctx context.Context, opts ScanOptions) (*ScanResult, error) {
	client, err := s.ensureClient(ctx)
	if err != nil {
		return nil, err
	}
	started := time.Now().UTC()
	result := &ScanResult{StartedAt: started}

	latest, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: get latest block: %v", ErrChainUnavailable, err)
	}
	lookback := opts.LookbackBlocks
	if lookback == 0 {
		lookback = s.cfg.ScanLookbackBlocks
	}
	if lookback == 0 {
		lookback = defaultLookbackBlocks
	}
	from := uint64(0)
	if latest+1 > lookback {
		from = latest - lookback + 1
	}
	result.FromBlock = from
	result.ToBlock = latest

	addresses, err := s.resolveExchangeAddresses()
	if err != nil {
		return nil, err
	}
	logs, err := client.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(latest),
		Addresses: addresses,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: filter logs: %v", ErrChainUnavailable, err)
	}
	result.ObservedLogs = len(logs)

	maxTx := opts.MaxTx
	if maxTx <= 0 {
		maxTx = s.cfg.ScanMaxTx
	}
	if maxTx <= 0 {
		maxTx = defaultMaxTx
	}
	if maxTx > maxScanTx {
		maxTx = maxScanTx
	}

	hashes := make([]common.Hash, 0, len(logs))
	seen := make(map[common.Hash]struct{}, len(logs))
	logBlockByHash := make(map[common.Hash]uint64, len(logs))
	for _, lg := range logs {
		if _, ok := seen[lg.TxHash]; ok {
			continue
		}
		seen[lg.TxHash] = struct{}{}
		hashes = append(hashes, lg.TxHash)
		logBlockByHash[lg.TxHash] = lg.BlockNumber
		if len(hashes) >= maxTx {
			break
		}
	}

	chainID := s.cfg.ChainID
	if chainID <= 0 {
		chainID = 137
	}
	signer := ethtypes.LatestSignerForChainID(big.NewInt(chainID))
	headers := make(map[uint64]*ethtypes.Header)
	observed := make([]observedTx, 0, len(hashes))
	for _, h := range hashes {
		tx, pending, err := client.TransactionByHash(ctx, h)
		if err != nil || pending || tx == nil {
			continue
		}
		sender, err := ethtypes.Sender(signer, tx)
		if err != nil {
			continue
		}
		if sender == (common.Address{}) {
			continue
		}

		blockNumber := logBlockByHash[h]
		if blockNumber == 0 {
			continue
		}

		head := headers[blockNumber]
		if head == nil {
			head, err = client.HeaderByNumber(ctx, new(big.Int).SetUint64(blockNumber))
			if err != nil || head == nil {
				continue
			}
			headers[blockNumber] = head
		}
		gasPriceGwei := decimal.Zero
		if gp := tx.GasPrice(); gp != nil {
			gasPriceGwei = decimal.NewFromBigInt(gp, 0).Div(decimal.NewFromInt(1_000_000_000))
		}
		observed = append(observed, observedTx{
			Address:      strings.ToLower(sender.Hex()),
			TxHash:       strings.ToLower(h.Hex()),
			BlockNumber:  blockNumber,
			Timestamp:    time.Unix(int64(head.Time), 0).UTC(),
			GasPriceGwei: gasPriceGwei,
		})
	}
	result.ObservedTx = len(observed)

	inserted, duplicated, updatedComp, err := s.persistObserved(ctx, observed)
	if err != nil {
		return nil, err
	}
	result.InsertedTrades = inserted
	result.DuplicateTrades = duplicated
	result.UpdatedCompetitor = updatedComp
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func (s *Service) ListCompetitors(ctx context.Context, opts ListCompetitorsOptions) ([]CompetitorView, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	query := s.db.WithContext(ctx).Model(&model.Competitor{})
	if opts.OnlyBots {
		query = query.Where("is_bot = ?", true)
	}
	var rows []model.Competitor
	if err := query.Order("is_bot DESC").Order("bot_confidence DESC").Order("last_seen_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}

	items := make([]CompetitorView, 0, len(rows))
	for _, row := range rows {
		it := CompetitorView{
			Address:             row.Address,
			Label:               row.Label,
			IsBot:               row.IsBot,
			BotConfidence:       row.BotConfidence,
			TotalTrades:         row.TotalTrades,
			AvgTradeIntervalSec: row.AvgTradeIntervalSec,
			FirstSeenAt:         row.FirstSeenAt,
			LastSeenAt:          row.LastSeenAt,
			Notes:               row.Notes,
		}
		if row.TotalVolume.Valid {
			v := row.TotalVolume.Decimal.InexactFloat64()
			it.TotalVolume = &v
		}
		items = append(items, it)
	}
	return items, nil
}

func (s *Service) GetCompetitor(ctx context.Context, address string) (*CompetitorView, error) {
	row, err := s.loadCompetitorByAddress(ctx, address)
	if err != nil {
		return nil, err
	}
	item := CompetitorView{
		Address:             row.Address,
		Label:               row.Label,
		IsBot:               row.IsBot,
		BotConfidence:       row.BotConfidence,
		TotalTrades:         row.TotalTrades,
		AvgTradeIntervalSec: row.AvgTradeIntervalSec,
		FirstSeenAt:         row.FirstSeenAt,
		LastSeenAt:          row.LastSeenAt,
		Notes:               row.Notes,
	}
	if row.TotalVolume.Valid {
		v := row.TotalVolume.Decimal.InexactFloat64()
		item.TotalVolume = &v
	}
	return &item, nil
}

func (s *Service) ListCompetitorTrades(ctx context.Context, address string, limit int) ([]CompetitorTradeView, error) {
	competitor, err := s.loadCompetitorByAddress(ctx, address)
	if err != nil {
		if errors.Is(err, ErrChainNotFound) {
			return []CompetitorTradeView{}, nil
		}
		return nil, err
	}
	if limit <= 0 {
		limit = defaultTradesLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	var rows []model.CompetitorTrade
	if err := s.db.WithContext(ctx).
		Where("competitor_id = ?", competitor.ID).
		Order("timestamp DESC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, err
	}

	items := make([]CompetitorTradeView, 0, len(rows))
	for _, row := range rows {
		it := CompetitorTradeView{
			TxHash:      row.TxHash,
			BlockNumber: row.BlockNumber,
			MarketID:    row.MarketID,
			Side:        row.Side,
			Outcome:     row.Outcome,
			Timestamp:   row.Timestamp,
			CreatedAt:   row.CreatedAt,
		}
		if row.AmountUSDC.Valid {
			v := row.AmountUSDC.Decimal.InexactFloat64()
			it.AmountUSDC = &v
		}
		if row.GasPriceGwei.Valid {
			v := row.GasPriceGwei.Decimal.InexactFloat64()
			it.GasPriceGwei = &v
		}
		items = append(items, it)
	}
	return items, nil
}

func (s *Service) resolveExchangeAddresses() ([]common.Address, error) {
	parse := func(raw string) (common.Address, error) {
		clean := strings.TrimSpace(raw)
		if !common.IsHexAddress(clean) {
			return common.Address{}, fmt.Errorf("invalid address: %s", raw)
		}
		return common.HexToAddress(clean), nil
	}
	addresses := make([]common.Address, 0, 2)
	if strings.TrimSpace(s.cfg.CTFExchangeAddress) != "" {
		addr, err := parse(s.cfg.CTFExchangeAddress)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrChainBadRequest, err)
		}
		addresses = append(addresses, addr)
	} else {
		addresses = append(addresses, common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"))
	}
	if strings.TrimSpace(s.cfg.NegRiskExchangeAddress) != "" {
		addr, err := parse(s.cfg.NegRiskExchangeAddress)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrChainBadRequest, err)
		}
		addresses = append(addresses, addr)
	} else {
		addresses = append(addresses, common.HexToAddress("0xC5d563A36AE78145C45a50134d48A1215220f80a"))
	}
	return addresses, nil
}

func (s *Service) persistObserved(ctx context.Context, rows []observedTx) (inserted int, duplicated int, updatedCompetitor int, err error) {
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	touched := make(map[string]struct{}, len(rows))
	now := time.Now().UTC()

	txErr := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, row := range rows {
			competitor, err := s.ensureCompetitor(tx, row.Address, row.Timestamp, now)
			if err != nil {
				return err
			}
			trade := model.CompetitorTrade{
				CompetitorID: competitor.ID,
				TxHash:       row.TxHash,
				BlockNumber:  row.BlockNumber,
				GasPriceGwei: decimal.NewNullDecimal(row.GasPriceGwei),
				Timestamp:    row.Timestamp,
				CreatedAt:    now,
			}
			result := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "tx_hash"}},
				DoNothing: true,
			}).Create(&trade)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				inserted++
			} else {
				duplicated++
			}
			touched[row.Address] = struct{}{}
		}

		for addr := range touched {
			if err := s.refreshCompetitorStats(tx, addr, now); err != nil {
				return err
			}
			updatedCompetitor++
		}
		return nil
	})
	if txErr != nil {
		return inserted, duplicated, updatedCompetitor, txErr
	}
	return inserted, duplicated, updatedCompetitor, nil
}

func (s *Service) ensureCompetitor(tx *gorm.DB, address string, seenAt, now time.Time) (*model.Competitor, error) {
	var row model.Competitor
	err := tx.Where("address = ?", address).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		created := model.Competitor{
			Address:     address,
			TotalVolume: decimal.NewNullDecimal(decimal.Zero),
			CreatedAt:   now,
			UpdatedAt:   now,
			FirstSeenAt: &seenAt,
			LastSeenAt:  &seenAt,
		}
		if err := tx.Create(&created).Error; err != nil {
			return nil, err
		}
		return &created, nil
	}
	if err != nil {
		return nil, err
	}

	updates := map[string]any{"updated_at": now}
	if row.FirstSeenAt == nil || seenAt.Before(*row.FirstSeenAt) {
		updates["first_seen_at"] = seenAt
	}
	if row.LastSeenAt == nil || seenAt.After(*row.LastSeenAt) {
		updates["last_seen_at"] = seenAt
	}
	if err := tx.Model(&model.Competitor{}).Where("id = ?", row.ID).Updates(updates).Error; err != nil {
		return nil, err
	}
	if firstSeen, ok := updates["first_seen_at"]; ok {
		t := firstSeen.(time.Time)
		row.FirstSeenAt = &t
	}
	if lastSeen, ok := updates["last_seen_at"]; ok {
		t := lastSeen.(time.Time)
		row.LastSeenAt = &t
	}
	return &row, nil
}

func (s *Service) refreshCompetitorStats(tx *gorm.DB, address string, now time.Time) error {
	var competitor model.Competitor
	if err := tx.Where("address = ?", address).First(&competitor).Error; err != nil {
		return err
	}
	var trades []model.CompetitorTrade
	if err := tx.Where("competitor_id = ?", competitor.ID).Order("timestamp ASC").Find(&trades).Error; err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}

	first := trades[0].Timestamp
	last := trades[len(trades)-1].Timestamp
	totalVolume := decimal.Zero
	for _, tr := range trades {
		if tr.AmountUSDC.Valid {
			totalVolume = totalVolume.Add(tr.AmountUSDC.Decimal)
		}
	}
	avgInterval := averageIntervalSeconds(trades)

	minTrades := s.cfg.BotMinTrades
	if minTrades <= 0 {
		minTrades = defaultBotMinTrades
	}
	maxAvgInterval := s.cfg.BotMaxAvgIntervalSec
	if maxAvgInterval <= 0 {
		maxAvgInterval = defaultBotMaxAvgIntervalSec
	}
	isBot, confidence := classifyBot(len(trades), avgInterval, minTrades, maxAvgInterval)

	updates := map[string]any{
		"total_trades":           len(trades),
		"total_volume":           totalVolume,
		"avg_trade_interval_sec": avgInterval,
		"is_bot":                 isBot,
		"bot_confidence":         confidence,
		"first_seen_at":          first,
		"last_seen_at":           last,
		"updated_at":             now,
	}
	return tx.Model(&model.Competitor{}).Where("id = ?", competitor.ID).Updates(updates).Error
}

func averageIntervalSeconds(trades []model.CompetitorTrade) float64 {
	if len(trades) <= 1 {
		return 0
	}
	var total float64
	var n int
	for i := 1; i < len(trades); i++ {
		delta := trades[i].Timestamp.Sub(trades[i-1].Timestamp).Seconds()
		if delta <= 0 {
			continue
		}
		total += delta
		n++
	}
	if n == 0 {
		return 0
	}
	return total / float64(n)
}

func classifyBot(totalTrades int, avgIntervalSec float64, minTrades int, maxAvgIntervalSec float64) (bool, float64) {
	if totalTrades <= 0 {
		return false, 0
	}
	if minTrades <= 0 {
		minTrades = defaultBotMinTrades
	}
	if maxAvgIntervalSec <= 0 {
		maxAvgIntervalSec = defaultBotMaxAvgIntervalSec
	}
	tradeScore := math.Min(70, float64(totalTrades)/float64(minTrades)*35)
	intervalScore := 0.0
	if avgIntervalSec > 0 {
		intervalScore = math.Max(0, math.Min(30, (maxAvgIntervalSec-avgIntervalSec)/maxAvgIntervalSec*30))
	}
	confidence := math.Max(0, math.Min(99, tradeScore+intervalScore))
	isBot := totalTrades >= minTrades && avgIntervalSec > 0 && avgIntervalSec <= maxAvgIntervalSec
	if !isBot && confidence > 49 {
		confidence = 49
	}
	return isBot, confidence
}

func (s *Service) loadCompetitorByAddress(ctx context.Context, address string) (*model.Competitor, error) {
	addr := strings.ToLower(strings.TrimSpace(address))
	if addr == "" {
		return nil, fmt.Errorf("%w: address is required", ErrChainBadRequest)
	}
	if !common.IsHexAddress(addr) {
		return nil, fmt.Errorf("%w: invalid address", ErrChainBadRequest)
	}
	var row model.Competitor
	if err := s.db.WithContext(ctx).Where("address = ?", addr).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: competitor not found", ErrChainNotFound)
		}
		return nil, err
	}
	return &row, nil
}
