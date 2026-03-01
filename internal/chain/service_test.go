package chain

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"errors"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

type mockEthClient struct {
	latestBlock     uint64
	logs            []ethtypes.Log
	txs             map[common.Hash]*ethtypes.Transaction
	headers         map[uint64]*ethtypes.Header
	blockNumberErr  error
	blockNumberErrs []error
	filterLogsErr   error
	maxLogRange     uint64
	filterLogCalls  int
}

func (m *mockEthClient) BlockNumber(context.Context) (uint64, error) {
	if len(m.blockNumberErrs) > 0 {
		err := m.blockNumberErrs[0]
		m.blockNumberErrs = m.blockNumberErrs[1:]
		if err != nil {
			return 0, err
		}
		return m.latestBlock, nil
	}
	if m.blockNumberErr != nil {
		return 0, m.blockNumberErr
	}
	return m.latestBlock, nil
}

func (m *mockEthClient) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]ethtypes.Log, error) {
	m.filterLogCalls++
	if m.filterLogsErr != nil {
		return nil, m.filterLogsErr
	}
	if m.maxLogRange > 0 && q.FromBlock != nil && q.ToBlock != nil {
		from := q.FromBlock.Uint64()
		to := q.ToBlock.Uint64()
		if to >= from && to-from+1 > m.maxLogRange {
			return nil, errors.New("400 Bad Request: Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range")
		}
	}
	out := make([]ethtypes.Log, 0, len(m.logs))
	for _, lg := range m.logs {
		if q.FromBlock != nil && lg.BlockNumber < q.FromBlock.Uint64() {
			continue
		}
		if q.ToBlock != nil && lg.BlockNumber > q.ToBlock.Uint64() {
			continue
		}
		if len(q.Addresses) > 0 {
			matched := false
			for _, addr := range q.Addresses {
				if addr == lg.Address {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if len(q.Topics) > 0 && len(q.Topics[0]) > 0 {
			if len(lg.Topics) == 0 {
				continue
			}
			topicMatched := false
			for _, topic := range q.Topics[0] {
				if lg.Topics[0] == topic {
					topicMatched = true
					break
				}
			}
			if !topicMatched {
				continue
			}
		}
		out = append(out, lg)
	}
	return out, nil
}

func (m *mockEthClient) TransactionByHash(_ context.Context, hash common.Hash) (*ethtypes.Transaction, bool, error) {
	tx := m.txs[hash]
	if tx == nil {
		return nil, false, nil
	}
	return tx, false, nil
}

func (m *mockEthClient) HeaderByNumber(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
	head := m.headers[number.Uint64()]
	if head == nil {
		return nil, nil
	}
	return head, nil
}

func (m *mockEthClient) Close() {}

func TestScanAndDetectBots(t *testing.T) {
	db := setupChainTestDB(t)
	client, botAddr := buildMockEthClient(t)

	svc := newServiceWithClient(config.ChainConfig{
		RPCURL:                 "http://mock",
		ChainID:                137,
		CTFExchangeAddress:     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
		NegRiskExchangeAddress: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
		ScanLookbackBlocks:     500,
		// intentionally high: validates opts override is applied
		BotMinTrades:         99,
		BotMaxAvgIntervalSec: 1,
	}, db, zap.NewNop(), client)

	res, err := svc.Scan(context.Background(), ScanOptions{
		BotMinTrades:         3,
		BotMaxAvgIntervalSec: 10,
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if res.ObservedLogs != 4 {
		t.Fatalf("expected observed logs 4 after topic filter, got %d", res.ObservedLogs)
	}
	if res.InsertedTrades != 4 {
		t.Fatalf("expected inserted 4, got %d", res.InsertedTrades)
	}
	if res.UpdatedCompetitor != 2 {
		t.Fatalf("expected updated competitors 2, got %d", res.UpdatedCompetitor)
	}

	bots, err := svc.ListCompetitors(context.Background(), ListCompetitorsOptions{OnlyBots: true, Limit: 10})
	if err != nil {
		t.Fatalf("list bots: %v", err)
	}
	if len(bots) != 1 {
		t.Fatalf("expected 1 bot, got %d", len(bots))
	}
	if bots[0].Address != botAddr {
		t.Fatalf("unexpected bot address: %s", bots[0].Address)
	}
	if !bots[0].IsBot {
		t.Fatalf("expected competitor marked as bot")
	}

	detail, err := svc.GetCompetitor(context.Background(), botAddr)
	if err != nil {
		t.Fatalf("get competitor: %v", err)
	}
	if detail.Address != botAddr {
		t.Fatalf("unexpected detail address: %s", detail.Address)
	}

	trades, err := svc.ListCompetitorTrades(context.Background(), botAddr, 10)
	if err != nil {
		t.Fatalf("list bot trades: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("expected 3 bot trades, got %d", len(trades))
	}
	if !sort.SliceIsSorted(trades, func(i, j int) bool {
		return trades[i].Timestamp.After(trades[j].Timestamp) || trades[i].Timestamp.Equal(trades[j].Timestamp)
	}) {
		t.Fatalf("expected trades sorted by timestamp desc")
	}
	for _, tr := range trades {
		if tr.MarketID == "" {
			t.Fatalf("expected market id decoded from token id")
		}
		if tr.Side == "" || tr.Outcome == "" {
			t.Fatalf("expected side/outcome decoded")
		}
		if tr.AmountUSDC == nil || *tr.AmountUSDC <= 0 {
			t.Fatalf("expected amount_usdc decoded")
		}
	}

	res2, err := svc.Scan(context.Background(), ScanOptions{BotMinTrades: 3, BotMaxAvgIntervalSec: 10})
	if err != nil {
		t.Fatalf("scan second run: %v", err)
	}
	if res2.InsertedTrades != 0 {
		t.Fatalf("expected second run inserted 0, got %d", res2.InsertedTrades)
	}
	if res2.DuplicateTrades != 0 {
		t.Fatalf("expected second run duplicate 0 with incremental scanning, got %d", res2.DuplicateTrades)
	}
}

func TestGetCompetitorInvalidAddress(t *testing.T) {
	db := setupChainTestDB(t)
	svc := newServiceWithClient(config.ChainConfig{}, db, zap.NewNop(), &mockEthClient{})

	_, err := svc.GetCompetitor(context.Background(), "invalid")
	if err == nil {
		t.Fatalf("expected invalid address error")
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanBlockNumberError(t *testing.T) {
	db := setupChainTestDB(t)
	svc := newServiceWithClient(config.ChainConfig{
		RPCURL:                 "http://mock",
		ChainID:                137,
		CTFExchangeAddress:     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
		NegRiskExchangeAddress: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
	}, db, zap.NewNop(), &mockEthClient{blockNumberErr: errors.New("rpc down")})

	_, err := svc.Scan(context.Background(), ScanOptions{})
	if err == nil {
		t.Fatalf("expected block number error")
	}
	if !errors.Is(err, ErrChainUnavailable) {
		t.Fatalf("expected ErrChainUnavailable, got %v", err)
	}
}

func TestScanBlockNumberRetriesOnRateLimit(t *testing.T) {
	db := setupChainTestDB(t)
	client := &mockEthClient{
		latestBlock: 200,
		blockNumberErrs: []error{
			errors.New("429 Too Many Requests"),
			nil,
		},
	}
	svc := newServiceWithClient(config.ChainConfig{
		RPCURL:                 "http://mock",
		ChainID:                137,
		CTFExchangeAddress:     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
		NegRiskExchangeAddress: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
		ScanLookbackBlocks:     10,
	}, db, zap.NewNop(), client)

	res, err := svc.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatalf("scan with transient 429: %v", err)
	}
	if res.ToBlock != 200 {
		t.Fatalf("expected to_block=200 after retry, got %d", res.ToBlock)
	}
}

func TestScanFallsBackToRecentWindowOnRPCRangeLimit(t *testing.T) {
	db := setupChainTestDB(t)
	client, _ := buildMockEthClient(t)
	client.maxLogRange = 10

	svc := newServiceWithClient(config.ChainConfig{
		RPCURL:                 "http://mock",
		ChainID:                137,
		CTFExchangeAddress:     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
		NegRiskExchangeAddress: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
		ScanLookbackBlocks:     500,
	}, db, zap.NewNop(), client)

	res, err := svc.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatalf("scan with fallback: %v", err)
	}
	if res.FromBlock != 191 {
		t.Fatalf("expected degraded from block 191, got %d", res.FromBlock)
	}
	if client.filterLogCalls < 2 {
		t.Fatalf("expected fallback path with at least 2 filter logs calls, got %d", client.filterLogCalls)
	}

	callsAfterFirst := client.filterLogCalls
	client.latestBlock = 205
	_, err = svc.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatalf("second scan with learned range cap: %v", err)
	}
	if client.filterLogCalls != callsAfterFirst+1 {
		t.Fatalf("expected second scan to avoid fallback retry and make one filter call, got delta=%d", client.filterLogCalls-callsAfterFirst)
	}
}

func TestPersistObservedAllowsNullMarketID(t *testing.T) {
	db := setupChainTestDB(t)
	svc := newServiceWithClient(config.ChainConfig{}, db, zap.NewNop(), &mockEthClient{})

	inserted, duplicated, updated, err := svc.persistObserved(context.Background(), []observedTx{
		{
			Address:     "0xaaa0000000000000000000000000000000000001",
			TxHash:      "0xdeadbeef",
			BlockNumber: 1,
			Timestamp:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}, 1, 10)
	if err != nil {
		t.Fatalf("persist observed: %v", err)
	}
	if inserted != 1 || duplicated != 0 || updated != 1 {
		t.Fatalf("unexpected counters inserted=%d duplicated=%d updated=%d", inserted, duplicated, updated)
	}

	type row struct {
		MarketID sql.NullString `gorm:"column:market_id"`
	}
	var got row
	if err := db.Raw("SELECT market_id FROM competitor_trades WHERE tx_hash = ?", "0xdeadbeef").Scan(&got).Error; err != nil {
		t.Fatalf("query competitor trade: %v", err)
	}
	if got.MarketID.Valid {
		t.Fatalf("expected market_id to be NULL when token mapping is missing")
	}
}

func setupChainTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.Competitor{}, &model.CompetitorTrade{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			token_id_yes TEXT,
			token_id_no TEXT
		)
	`).Error; err != nil {
		t.Fatalf("create markets table: %v", err)
	}
	if err := db.Exec(`
		INSERT INTO markets (id, token_id_yes, token_id_no) VALUES
			('00000000-0000-0000-0000-000000000111', '111', '112'),
			('00000000-0000-0000-0000-000000000222', '221', '222')
	`).Error; err != nil {
		t.Fatalf("seed markets: %v", err)
	}
	return db
}

func buildMockEthClient(t *testing.T) (*mockEthClient, string) {
	t.Helper()
	exchange := common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
	chainID := big.NewInt(137)
	signer := ethtypes.LatestSignerForChainID(chainID)

	botKey, err := crypto.HexToECDSA("4c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f")
	if err != nil {
		t.Fatalf("bot key: %v", err)
	}
	humanKey, err := crypto.HexToECDSA("6c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f")
	if err != nil {
		t.Fatalf("human key: %v", err)
	}
	botAddr := strings.ToLower(crypto.PubkeyToAddress(botKey.PublicKey).Hex())
	humanAddr := strings.ToLower(crypto.PubkeyToAddress(humanKey.PublicKey).Hex())

	tx1 := mustSignedTx(t, signer, botKey, exchange, 1)
	tx2 := mustSignedTx(t, signer, botKey, exchange, 2)
	tx3 := mustSignedTx(t, signer, botKey, exchange, 3)
	tx4 := mustSignedTx(t, signer, humanKey, exchange, 1)

	headers := map[uint64]*ethtypes.Header{
		100: {Number: new(big.Int).SetUint64(100), Time: uint64(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix())},
		101: {Number: new(big.Int).SetUint64(101), Time: uint64(time.Date(2026, 1, 1, 0, 0, 4, 0, time.UTC).Unix())},
		102: {Number: new(big.Int).SetUint64(102), Time: uint64(time.Date(2026, 1, 1, 0, 0, 8, 0, time.UTC).Unix())},
		160: {Number: new(big.Int).SetUint64(160), Time: uint64(time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC).Unix())},
	}

	irrelevantTopic := crypto.Keccak256Hash([]byte("Approval(address,address,uint256)"))

	return &mockEthClient{
		latestBlock: 200,
		logs: []ethtypes.Log{
			buildOrderFilledLog(tx1.Hash(), exchange, 100, botAddr, humanAddr, 0, 111, 5_500_000, 11_000_000, 0),
			buildOrderFilledLog(tx2.Hash(), exchange, 101, botAddr, humanAddr, 0, 111, 6_500_000, 13_000_000, 0),
			buildOrderFilledLog(tx3.Hash(), exchange, 102, botAddr, humanAddr, 0, 111, 4_500_000, 9_000_000, 0),
			buildOrderFilledLog(tx4.Hash(), exchange, 160, humanAddr, botAddr, 222, 0, 7_000_000, 3_500_000, 0),
			{Address: exchange, BlockNumber: 170, TxHash: tx4.Hash(), Topics: []common.Hash{irrelevantTopic}},
		},
		txs: map[common.Hash]*ethtypes.Transaction{
			tx1.Hash(): tx1,
			tx2.Hash(): tx2,
			tx3.Hash(): tx3,
			tx4.Hash(): tx4,
		},
		headers: headers,
	}, botAddr
}

func mustSignedTx(t *testing.T, signer ethtypes.Signer, key *ecdsa.PrivateKey, to common.Address, nonce uint64) *ethtypes.Transaction {
	t.Helper()
	tx := ethtypes.NewTransaction(nonce, to, big.NewInt(1), 21000, big.NewInt(2_000_000_000), []byte{0x1})
	signed, err := ethtypes.SignTx(tx, signer, key)
	if err != nil {
		t.Fatalf("sign tx: %v", err)
	}
	return signed
}

func buildOrderFilledLog(txHash common.Hash, exchange common.Address, block uint64, maker string, taker string, makerAsset, takerAsset, makerAmount, takerAmount, fee uint64) ethtypes.Log {
	makerAddr := common.HexToAddress(maker)
	takerAddr := common.HexToAddress(taker)
	dataHex := encodeOrderFilledData(
		new(big.Int).SetUint64(makerAsset),
		new(big.Int).SetUint64(takerAsset),
		new(big.Int).SetUint64(makerAmount),
		new(big.Int).SetUint64(takerAmount),
		new(big.Int).SetUint64(fee),
	)
	data, _ := hex.DecodeString(dataHex)
	return ethtypes.Log{
		Address:     exchange,
		BlockNumber: block,
		TxHash:      txHash,
		Topics: []common.Hash{
			orderFilledEventTopic,
			txHash,
			common.BytesToHash(common.LeftPadBytes(makerAddr.Bytes(), 32)),
			common.BytesToHash(common.LeftPadBytes(takerAddr.Bytes(), 32)),
		},
		Data: data,
	}
}

func encodeOrderFilledData(fields ...*big.Int) string {
	out := ""
	for _, f := range fields {
		b := common.LeftPadBytes(f.Bytes(), 32)
		out += hex.EncodeToString(b)
	}
	return out
}
