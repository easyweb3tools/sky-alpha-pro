package trade

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestParsePrivateKey(t *testing.T) {
	if _, err := ParsePrivateKey(""); err == nil {
		t.Fatalf("expected empty private key error")
	}
	if _, err := ParsePrivateKey("not-a-hex-key"); err == nil {
		t.Fatalf("expected invalid private key error")
	}
	key, err := ParsePrivateKey("4c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f")
	if err != nil {
		t.Fatalf("parse valid private key: %v", err)
	}
	if key == nil {
		t.Fatalf("expected parsed private key")
	}
}

func TestSignOrderEIP712AndRecover(t *testing.T) {
	privateKey, err := ParsePrivateKey("4c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f")
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	signer := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()

	order := SignableOrder{
		Salt:          "123456789",
		Maker:         signer,
		Signer:        signer,
		Taker:         "0x0000000000000000000000000000000000000000",
		TokenID:       "1000001",
		MakerAmount:   "6200000",
		TakerAmount:   "10000000",
		Expiration:    "1767225600",
		Nonce:         "42",
		FeeRateBps:    "0",
		Side:          0,
		SignatureType: 0,
	}

	signature, err := SignOrderEIP712(privateKey, 137, order)
	if err != nil {
		t.Fatalf("sign order: %v", err)
	}
	if !strings.HasPrefix(signature, "0x") || len(signature) != 132 {
		t.Fatalf("unexpected signature format: %s", signature)
	}

	recovered, err := RecoverOrderSigner(137, order, signature)
	if err != nil {
		t.Fatalf("recover signer: %v", err)
	}
	if !strings.EqualFold(recovered.Hex(), signer) {
		t.Fatalf("signer mismatch: got=%s want=%s", recovered.Hex(), signer)
	}
}

func TestSignOrderEIP712UnsupportedChain(t *testing.T) {
	privateKey, err := ParsePrivateKey("4c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f")
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	_, err = SignOrderEIP712(privateKey, 1, SignableOrder{})
	if err == nil {
		t.Fatalf("expected unsupported chain error")
	}
}
