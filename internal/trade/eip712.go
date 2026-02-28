package trade

import (
	"crypto/ecdsa"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	commonmath "github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

func ParsePrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	v := strings.TrimPrefix(strings.TrimSpace(hexKey), "0x")
	if v == "" {
		return nil, fmt.Errorf("trade private key is empty")
	}
	key, err := crypto.HexToECDSA(v)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return key, nil
}

func SignOrderEIP712(privateKey *ecdsa.PrivateKey, chainID int64, order SignableOrder) (string, error) {
	if privateKey == nil {
		return "", fmt.Errorf("private key is nil")
	}
	if chainID <= 0 {
		return "", fmt.Errorf("invalid chain id: %d", chainID)
	}

	domain := apitypes.TypedDataDomain{
		Name:    "PolymarketCLOB",
		Version: "1",
		ChainId: commonmath.NewHexOrDecimal256(chainID),
	}
	types := apitypes.Types{
		"EIP712Domain": {
			{Name: "name", Type: "string"},
			{Name: "version", Type: "string"},
			{Name: "chainId", Type: "uint256"},
		},
		"Order": {
			{Name: "marketId", Type: "string"},
			{Name: "side", Type: "string"},
			{Name: "outcome", Type: "string"},
			{Name: "price", Type: "string"},
			{Name: "size", Type: "string"},
			{Name: "nonce", Type: "uint256"},
			{Name: "expiration", Type: "uint256"},
		},
	}
	msg := apitypes.TypedDataMessage{
		"marketId":   order.MarketID,
		"side":       strings.ToUpper(order.Side),
		"outcome":    strings.ToUpper(order.Outcome),
		"price":      order.Price,
		"size":       order.Size,
		"nonce":      fmt.Sprintf("%d", order.Nonce),
		"expiration": fmt.Sprintf("%d", order.Expiration),
	}

	typedData := apitypes.TypedData{
		Types:       types,
		PrimaryType: "Order",
		Domain:      domain,
		Message:     msg,
	}
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return "", fmt.Errorf("hash typed data: %w", err)
	}

	signature, err := crypto.Sign(hash, privateKey)
	if err != nil {
		return "", fmt.Errorf("sign order: %w", err)
	}
	return hexutil.Encode(signature), nil
}
