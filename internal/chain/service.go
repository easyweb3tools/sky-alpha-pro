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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	scanRPCConcurrency          = 20
)

var (
	ErrChainUnavailable = errors.New("chain service unavailable")
	ErrChainBadRequest  = errors.New("chain bad request")
	ErrChainNotFound    = errors.New("chain not found")

	// OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
	orderFilledEventTopic = crypto.Keccak256Hash([]byte("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"))
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
	Side         string
	TokenID      string
	AmountUSDC   decimal.Decimal
	HasAmount    bool
}

type decodedOrderFilled struct {
	Maker      string
	Side       string
	TokenID    string
	AmountUSDC decimal.Decimal
	HasAmount  bool
}

type txMeta struct {
	Sender       string
	Timestamp    time.Time
	GasPriceGwei decimal.Decimal
}

type marketTokenRef struct {
	MarketID string
	Outcome  string
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
		Topics:    [][]common.Hash{{orderFilledEventTopic}},
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
	eventsByHash := make(map[common.Hash][]decodedOrderFilled, len(logs))
	for _, lg := range logs {
		if _, ok := seen[lg.TxHash]; !ok {
			seen[lg.TxHash] = struct{}{}
			hashes = append(hashes, lg.TxHash)
			logBlockByHash[lg.TxHash] = lg.BlockNumber
		}
		decoded, decodeErr := decodeOrderFilledLog(lg)
		if decodeErr == nil {
			eventsByHash[lg.TxHash] = append(eventsByHash[lg.TxHash], *decoded)
		}
		if len(hashes) >= maxTx {
			break
		}
	}

	chainID := s.cfg.ChainID
	if chainID <= 0 {
		chainID = 137
	}
	signer := ethtypes.LatestSignerForChainID(big.NewInt(chainID))
	metas := s.fetchTxMetaConcurrently(ctx, client, signer, hashes, logBlockByHash)

	observed := make([]observedTx, 0, len(metas))
	for _, h := range hashes {
		meta, ok := metas[h]
		if !ok {
			continue
		}
		row := observedTx{
			Address:      meta.Sender,
			TxHash:       strings.ToLower(h.Hex()),
			BlockNumber:  logBlockByHash[h],
			Timestamp:    meta.Timestamp,
			GasPriceGwei: meta.GasPriceGwei,
		}
		if picked := pickOrderFill(eventsByHash[h], meta.Sender); picked != nil {
			if picked.Maker != "" {
				row.Address = picked.Maker
			}
			row.Side = picked.Side
			row.TokenID = picked.TokenID
			row.AmountUSDC = picked.AmountUSDC
			row.HasAmount = picked.HasAmount
		}
		if row.Address == "" {
			continue
		}
		observed = append(observed, row)
	}
	result.ObservedTx = len(observed)

	minTrades := opts.BotMinTrades
	if minTrades <= 0 {
		minTrades = s.cfg.BotMinTrades
	}
	if minTrades <= 0 {
		minTrades = defaultBotMinTrades
	}
	maxAvgInterval := opts.BotMaxAvgIntervalSec
	if maxAvgInterval <= 0 {
		maxAvgInterval = s.cfg.BotMaxAvgIntervalSec
	}
	if maxAvgInterval <= 0 {
		maxAvgInterval = defaultBotMaxAvgIntervalSec
	}

	inserted, duplicated, updatedComp, err := s.persistObserved(ctx, observed, minTrades, maxAvgInterval)
	if err != nil {
		return nil, err
	}
	result.InsertedTrades = inserted
	result.DuplicateTrades = duplicated
	result.UpdatedCompetitor = updatedComp
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func (s *Service) fetchTxMetaConcurrently(
	ctx context.Context,
	client ethClient,
	signer ethtypes.Signer,
	hashes []common.Hash,
	logBlockByHash map[common.Hash]uint64,
) map[common.Hash]txMeta {
	metas := make(map[common.Hash]txMeta, len(hashes))
	var metasMu sync.Mutex

	headers := make(map[uint64]*ethtypes.Header)
	var headersMu sync.Mutex

	fetchHeader := func(blockNumber uint64, gctx context.Context) (*ethtypes.Header, error) {
		headersMu.Lock()
		cached := headers[blockNumber]
		headersMu.Unlock()
		if cached != nil {
			return cached, nil
		}
		head, err := client.HeaderByNumber(gctx, new(big.Int).SetUint64(blockNumber))
		if err != nil {
			return nil, err
		}
		if head == nil {
			return nil, nil
		}
		headersMu.Lock()
		if existing := headers[blockNumber]; existing == nil {
			headers[blockNumber] = head
		} else {
			head = existing
		}
		headersMu.Unlock()
		return head, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(scanRPCConcurrency)

	for _, hash := range hashes {
		h := hash
		blockNumber := logBlockByHash[h]
		g.Go(func() error {
			tx, pending, err := client.TransactionByHash(gctx, h)
			if err != nil || pending || tx == nil {
				return nil
			}
			sender, err := ethtypes.Sender(signer, tx)
			if err != nil || sender == (common.Address{}) {
				return nil
			}
			head, err := fetchHeader(blockNumber, gctx)
			if err != nil || head == nil {
				return nil
			}

			gasPriceGwei := decimal.Zero
			if gp := tx.GasPrice(); gp != nil {
				gasPriceGwei = decimal.NewFromBigInt(gp, 0).Div(decimal.NewFromInt(1_000_000_000))
			}

			metasMu.Lock()
			metas[h] = txMeta{
				Sender:       strings.ToLower(sender.Hex()),
				Timestamp:    time.Unix(int64(head.Time), 0).UTC(),
				GasPriceGwei: gasPriceGwei,
			}
			metasMu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
	return metas
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
		items = append(items, mapCompetitorView(row))
	}
	return items, nil
}

func (s *Service) GetCompetitor(ctx context.Context, address string) (*CompetitorView, error) {
	row, err := s.loadCompetitorByAddress(ctx, address)
	if err != nil {
		return nil, err
	}
	item := mapCompetitorView(*row)
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
	parse := func(raw string, field string) (common.Address, error) {
		clean := strings.TrimSpace(raw)
		if clean == "" {
			return common.Address{}, fmt.Errorf("%w: %s is empty", ErrChainBadRequest, field)
		}
		if !common.IsHexAddress(clean) {
			return common.Address{}, fmt.Errorf("%w: invalid address for %s", ErrChainBadRequest, field)
		}
		return common.HexToAddress(clean), nil
	}
	ctf, err := parse(s.cfg.CTFExchangeAddress, "chain.ctf_exchange_address")
	if err != nil {
		return nil, err
	}
	negRisk, err := parse(s.cfg.NegRiskExchangeAddress, "chain.negrisk_exchange_address")
	if err != nil {
		return nil, err
	}
	return []common.Address{ctf, negRisk}, nil
}

func (s *Service) persistObserved(
	ctx context.Context,
	rows []observedTx,
	minTrades int,
	maxAvgIntervalSec float64,
) (inserted int, duplicated int, updatedCompetitor int, err error) {
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	touched := make(map[string]struct{}, len(rows))
	now := time.Now().UTC()

	txErr := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tokenMap, mapErr := s.loadMarketTokenMap(tx, rows)
		if mapErr != nil {
			return mapErr
		}

		for _, row := range rows {
			competitor, ensureErr := s.ensureCompetitor(tx, row.Address, row.Timestamp, now)
			if ensureErr != nil {
				return ensureErr
			}

			trade := model.CompetitorTrade{
				CompetitorID: competitor.ID,
				TxHash:       row.TxHash,
				BlockNumber:  row.BlockNumber,
				Side:         row.Side,
				GasPriceGwei: decimal.NewNullDecimal(row.GasPriceGwei),
				Timestamp:    row.Timestamp,
				CreatedAt:    now,
			}
			if row.HasAmount {
				trade.AmountUSDC = decimal.NewNullDecimal(row.AmountUSDC)
			}
			if ref, ok := tokenMap[row.TokenID]; ok {
				trade.MarketID = ref.MarketID
				trade.Outcome = ref.Outcome
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
			if refreshErr := s.refreshCompetitorStats(tx, addr, now, minTrades, maxAvgIntervalSec); refreshErr != nil {
				return refreshErr
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

func (s *Service) loadMarketTokenMap(tx *gorm.DB, rows []observedTx) (map[string]marketTokenRef, error) {
	uniq := make(map[string]struct{})
	tokenIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		token := strings.TrimSpace(row.TokenID)
		if token == "" {
			continue
		}
		if _, ok := uniq[token]; ok {
			continue
		}
		uniq[token] = struct{}{}
		tokenIDs = append(tokenIDs, token)
	}
	mapped := make(map[string]marketTokenRef, len(tokenIDs))
	if len(tokenIDs) == 0 {
		return mapped, nil
	}

	var markets []model.Market
	if err := tx.Model(&model.Market{}).
		Select("id", "token_id_yes", "token_id_no").
		Where("token_id_yes IN ? OR token_id_no IN ?", tokenIDs, tokenIDs).
		Find(&markets).Error; err != nil {
		return nil, err
	}
	for _, m := range markets {
		if strings.TrimSpace(m.TokenIDYes) != "" {
			mapped[m.TokenIDYes] = marketTokenRef{MarketID: m.ID, Outcome: "YES"}
		}
		if strings.TrimSpace(m.TokenIDNo) != "" {
			mapped[m.TokenIDNo] = marketTokenRef{MarketID: m.ID, Outcome: "NO"}
		}
	}
	return mapped, nil
}

func (s *Service) ensureCompetitor(tx *gorm.DB, address string, seenAt, now time.Time) (*model.Competitor, error) {
	candidate := model.Competitor{
		Address:     address,
		TotalVolume: decimal.NewNullDecimal(decimal.Zero),
		CreatedAt:   now,
		UpdatedAt:   now,
		FirstSeenAt: &seenAt,
		LastSeenAt:  &seenAt,
	}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}},
		DoNothing: true,
	}).Create(&candidate).Error; err != nil {
		return nil, err
	}

	var row model.Competitor
	if err := tx.Where("address = ?", address).First(&row).Error; err != nil {
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

func (s *Service) refreshCompetitorStats(
	tx *gorm.DB,
	address string,
	now time.Time,
	minTrades int,
	maxAvgIntervalSec float64,
) error {
	var competitor model.Competitor
	if err := tx.Where("address = ?", address).First(&competitor).Error; err != nil {
		return err
	}

	totalTrades := 0
	totalVolume := decimal.Zero
	var firstSeenAt *time.Time
	var lastSeenAt *time.Time
	var prevTimestamp *time.Time
	intervalTotalSec := 0.0
	intervalCount := 0

	batch := make([]model.CompetitorTrade, 0, 200)
	if err := tx.Model(&model.CompetitorTrade{}).
		Select("timestamp", "amount_usdc").
		Where("competitor_id = ?", competitor.ID).
		Order("timestamp ASC").
		FindInBatches(&batch, 200, func(_ *gorm.DB, _ int) error {
			for _, tr := range batch {
				totalTrades++
				if tr.AmountUSDC.Valid {
					totalVolume = totalVolume.Add(tr.AmountUSDC.Decimal)
				}
				current := tr.Timestamp
				if firstSeenAt == nil {
					t := current
					firstSeenAt = &t
				}
				t := current
				lastSeenAt = &t
				if prevTimestamp != nil {
					delta := current.Sub(*prevTimestamp).Seconds()
					if delta > 0 {
						intervalTotalSec += delta
						intervalCount++
					}
				}
				prevTimestamp = &t
			}
			return nil
		}).Error; err != nil {
		return err
	}
	if totalTrades == 0 {
		return nil
	}

	avgInterval := 0.0
	if intervalCount > 0 {
		avgInterval = intervalTotalSec / float64(intervalCount)
	}

	if minTrades <= 0 {
		minTrades = defaultBotMinTrades
	}
	if maxAvgIntervalSec <= 0 {
		maxAvgIntervalSec = defaultBotMaxAvgIntervalSec
	}
	isBot, confidence := classifyBot(totalTrades, avgInterval, minTrades, maxAvgIntervalSec)

	updates := map[string]any{
		"total_trades":           totalTrades,
		"total_volume":           totalVolume,
		"avg_trade_interval_sec": avgInterval,
		"is_bot":                 isBot,
		"bot_confidence":         confidence,
		"updated_at":             now,
	}
	if firstSeenAt != nil {
		updates["first_seen_at"] = *firstSeenAt
	}
	if lastSeenAt != nil {
		updates["last_seen_at"] = *lastSeenAt
	}
	return tx.Model(&model.Competitor{}).Where("id = ?", competitor.ID).Updates(updates).Error
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

func mapCompetitorView(row model.Competitor) CompetitorView {
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
	return item
}

func decodeOrderFilledLog(lg ethtypes.Log) (*decodedOrderFilled, error) {
	if len(lg.Topics) < 4 {
		return nil, fmt.Errorf("invalid topics")
	}
	if lg.Topics[0] != orderFilledEventTopic {
		return nil, fmt.Errorf("unexpected event topic")
	}
	if len(lg.Data) < 32*5 {
		return nil, fmt.Errorf("invalid event data")
	}
	maker := strings.ToLower(common.HexToAddress(lg.Topics[2].Hex()).Hex())

	makerAssetID := new(big.Int).SetBytes(lg.Data[0:32])
	takerAssetID := new(big.Int).SetBytes(lg.Data[32:64])
	makerAmountFilled := new(big.Int).SetBytes(lg.Data[64:96])
	takerAmountFilled := new(big.Int).SetBytes(lg.Data[96:128])
	// lg.Data[128:160] is fee and intentionally ignored for now.

	zero := big.NewInt(0)
	out := &decodedOrderFilled{Maker: maker}

	switch {
	case makerAssetID.Cmp(zero) == 0 && takerAssetID.Cmp(zero) > 0:
		out.Side = "BUY"
		out.TokenID = takerAssetID.String()
		out.AmountUSDC = decimal.NewFromBigInt(makerAmountFilled, -6)
		out.HasAmount = true
	case takerAssetID.Cmp(zero) == 0 && makerAssetID.Cmp(zero) > 0:
		out.Side = "SELL"
		out.TokenID = makerAssetID.String()
		out.AmountUSDC = decimal.NewFromBigInt(takerAmountFilled, -6)
		out.HasAmount = true
	default:
		if makerAssetID.Cmp(zero) > 0 {
			out.TokenID = makerAssetID.String()
		} else if takerAssetID.Cmp(zero) > 0 {
			out.TokenID = takerAssetID.String()
		}
	}

	return out, nil
}

func pickOrderFill(events []decodedOrderFilled, sender string) *decodedOrderFilled {
	if len(events) == 0 {
		return nil
	}
	for i := range events {
		if events[i].Maker == sender {
			return &events[i]
		}
	}
	return &events[0]
}
