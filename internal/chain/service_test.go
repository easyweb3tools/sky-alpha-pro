package chain

import (
	"context"
	"crypto/ecdsa"
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
	latestBlock uint64
	logs        []ethtypes.Log
	txs         map[common.Hash]*ethtypes.Transaction
	headers     map[uint64]*ethtypes.Header
}

func (m *mockEthClient) BlockNumber(context.Context) (uint64, error) {
	return m.latestBlock, nil
}

func (m *mockEthClient) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]ethtypes.Log, error) {
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
		BotMinTrades:           3,
		BotMaxAvgIntervalSec:   10,
	}, db, zap.NewNop(), client)

	res, err := svc.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatalf("scan: %v", err)
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

	res2, err := svc.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatalf("scan second run: %v", err)
	}
	if res2.InsertedTrades != 0 {
		t.Fatalf("expected second run inserted 0, got %d", res2.InsertedTrades)
	}
	if res2.DuplicateTrades != 4 {
		t.Fatalf("expected second run duplicate 4, got %d", res2.DuplicateTrades)
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

func setupChainTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.Competitor{}, &model.CompetitorTrade{}); err != nil {
		t.Fatalf("migrate: %v", err)
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

	return &mockEthClient{
		latestBlock: 200,
		logs: []ethtypes.Log{
			{Address: exchange, BlockNumber: 100, TxHash: tx1.Hash()},
			{Address: exchange, BlockNumber: 101, TxHash: tx2.Hash()},
			{Address: exchange, BlockNumber: 102, TxHash: tx3.Hash()},
			{Address: exchange, BlockNumber: 160, TxHash: tx4.Hash()},
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
