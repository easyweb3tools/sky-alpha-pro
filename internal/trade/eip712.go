package trade

import (
	"crypto/ecdsa"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	commonmath "github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

const (
	eip712ProtocolName    = "Polymarket CTF Exchange"
	eip712ProtocolVersion = "1"
)

var exchangeAddressByChain = map[int64]common.Address{
	137:   common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
	80002: common.HexToAddress("0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40"),
}

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
	hash, err := hashOrderTypedData(chainID, order)
	if err != nil {
		return "", err
	}
	signature, err := crypto.Sign(hash, privateKey)
	if err != nil {
		return "", fmt.Errorf("sign order: %w", err)
	}
	return hexutil.Encode(signature), nil
}

func RecoverOrderSigner(chainID int64, order SignableOrder, signatureHex string) (common.Address, error) {
	hash, err := hashOrderTypedData(chainID, order)
	if err != nil {
		return common.Address{}, err
	}
	sig, err := hexutil.Decode(signatureHex)
	if err != nil {
		return common.Address{}, fmt.Errorf("decode signature: %w", err)
	}
	if len(sig) != crypto.SignatureLength {
		return common.Address{}, fmt.Errorf("invalid signature length: %d", len(sig))
	}
	if sig[64] >= 27 {
		sig[64] -= 27
	}
	pub, err := crypto.SigToPub(hash, sig)
	if err != nil {
		return common.Address{}, fmt.Errorf("recover signer: %w", err)
	}
	return crypto.PubkeyToAddress(*pub), nil
}

func hashOrderTypedData(chainID int64, order SignableOrder) ([]byte, error) {
	if chainID <= 0 {
		return nil, fmt.Errorf("invalid chain id: %d", chainID)
	}
	verifyingContract, ok := exchangeAddressByChain[chainID]
	if !ok {
		return nil, fmt.Errorf("unsupported chain id: %d", chainID)
	}

	domain := apitypes.TypedDataDomain{
		Name:              eip712ProtocolName,
		Version:           eip712ProtocolVersion,
		ChainId:           commonmath.NewHexOrDecimal256(chainID),
		VerifyingContract: verifyingContract.Hex(),
	}
	types := apitypes.Types{
		"EIP712Domain": {
			{Name: "name", Type: "string"},
			{Name: "version", Type: "string"},
			{Name: "chainId", Type: "uint256"},
			{Name: "verifyingContract", Type: "address"},
		},
		"Order": {
			{Name: "salt", Type: "uint256"},
			{Name: "maker", Type: "address"},
			{Name: "signer", Type: "address"},
			{Name: "taker", Type: "address"},
			{Name: "tokenId", Type: "uint256"},
			{Name: "makerAmount", Type: "uint256"},
			{Name: "takerAmount", Type: "uint256"},
			{Name: "expiration", Type: "uint256"},
			{Name: "nonce", Type: "uint256"},
			{Name: "feeRateBps", Type: "uint256"},
			{Name: "side", Type: "uint8"},
			{Name: "signatureType", Type: "uint8"},
		},
	}
	msg := apitypes.TypedDataMessage{
		"salt":          order.Salt,
		"maker":         order.Maker,
		"signer":        order.Signer,
		"taker":         order.Taker,
		"tokenId":       order.TokenID,
		"makerAmount":   order.MakerAmount,
		"takerAmount":   order.TakerAmount,
		"expiration":    order.Expiration,
		"nonce":         order.Nonce,
		"feeRateBps":    order.FeeRateBps,
		"side":          fmt.Sprintf("%d", order.Side),
		"signatureType": fmt.Sprintf("%d", order.SignatureType),
	}

	typedData := apitypes.TypedData{
		Types:       types,
		PrimaryType: "Order",
		Domain:      domain,
		Message:     msg,
	}
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return nil, fmt.Errorf("hash typed data: %w", err)
	}
	return hash, nil
}
