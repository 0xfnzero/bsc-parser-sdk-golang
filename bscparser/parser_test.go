package bscparser

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestDecodeFourCalldata(t *testing.T) {
	parser := MustParser()
	addressType, _ := abi.NewType("address", "", nil)
	uintType, _ := abi.NewType("uint256", "", nil)
	args := abi.Arguments{
		{Type: addressType, Name: "token"},
		{Type: addressType, Name: "to"},
		{Type: uintType, Name: "funds"},
		{Type: uintType, Name: "minAmount"},
	}
	packed, err := args.Pack(
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
		big.NewInt(123),
		big.NewInt(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	input := append(common.FromHex("0x7f79f6df"), packed...)
	decoded, err := parser.DecodeFourCalldata(input)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "buyTokenAMAP" {
		t.Fatalf("unexpected method: %s", decoded.Name)
	}
	if decoded.Signature != "buyTokenAMAP(address,address,uint256,uint256)" {
		t.Fatalf("unexpected signature: %s", decoded.Signature)
	}
	if decoded.Selector != "0x7f79f6df" {
		t.Fatalf("unexpected selector: %s", decoded.Selector)
	}
}

func TestDecodeFourCreateTokenSingleArgument(t *testing.T) {
	parser := MustParser()
	bytesType, _ := abi.NewType("bytes", "", nil)
	args := abi.Arguments{{Type: bytesType, Name: "args"}}
	packed, err := args.Pack([]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	input := append(common.FromHex("0x73306244"), packed...)
	decoded, err := parser.DecodeFourCalldata(input)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Signature != "createToken(bytes)" {
		t.Fatalf("unexpected signature: %s", decoded.Signature)
	}
}

func TestDecodePancakeRouterCalldata(t *testing.T) {
	parser := MustParser()
	uintType, _ := abi.NewType("uint256", "", nil)
	addressType, _ := abi.NewType("address", "", nil)
	addressArrayType, _ := abi.NewType("address[]", "", nil)
	args := abi.Arguments{
		{Type: uintType, Name: "amountIn"},
		{Type: uintType, Name: "amountOutMin"},
		{Type: addressArrayType, Name: "path"},
		{Type: addressType, Name: "to"},
		{Type: uintType, Name: "deadline"},
	}
	path := []common.Address{
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
	}
	packed, err := args.Pack(
		big.NewInt(100),
		big.NewInt(90),
		path,
		common.HexToAddress("0x3333333333333333333333333333333333333333"),
		big.NewInt(123456),
	)
	if err != nil {
		t.Fatal(err)
	}

	input := append(common.FromHex("0x38ed1739"), packed...)
	decoded, err := parser.DecodePancakeRouterCalldata(input)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Protocol != ProtocolPancake {
		t.Fatalf("unexpected protocol: %s", decoded.Protocol)
	}
	if decoded.Signature != "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)" {
		t.Fatalf("unexpected signature: %s", decoded.Signature)
	}
	if decoded.Args["amountIn"].(*big.Int).Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("unexpected amountIn: %#v", decoded.Args["amountIn"])
	}
}

func TestParseFourAndPancakeLogs(t *testing.T) {
	parser := MustParser()

	purchase := parser.four.Events["TokenPurchase"]
	purchaseData, err := purchase.Inputs.NonIndexed().Pack(
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
		big.NewInt(10),
		big.NewInt(20),
		big.NewInt(30),
		big.NewInt(1),
		big.NewInt(40),
		big.NewInt(50),
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok, err := parser.ParseLog(types.Log{
		Address: common.HexToAddress(FourMemeAddress),
		Topics:  []common.Hash{purchase.ID},
		Data:    purchaseData,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || parsed.Protocol != ProtocolFour || parsed.Name != "TokenPurchase" {
		t.Fatalf("unexpected four event: %#v", parsed)
	}
	if parsed.Args["amount"].(*big.Int).Cmp(big.NewInt(20)) != 0 {
		t.Fatalf("unexpected amount: %#v", parsed.Args["amount"])
	}

	swap := parser.pancakePair.Events["Swap"]
	swapData, err := swap.Inputs.NonIndexed().Pack(big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4))
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok, err = parser.ParseLog(types.Log{
		Address: common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Topics: []common.Hash{
			swap.ID,
			common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
		},
		Data: swapData,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || parsed.Protocol != ProtocolPancake || parsed.Name != "Swap" {
		t.Fatalf("unexpected pancake event: %#v", parsed)
	}
	if parsed.Args["amount1Out"].(*big.Int).Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("unexpected amount1Out: %#v", parsed.Args["amount1Out"])
	}
}

func TestParseLogRejectsMissingIndexedTopics(t *testing.T) {
	parser := MustParser()
	swap := parser.pancakePair.Events["Swap"]
	swapData, err := swap.Inputs.NonIndexed().Pack(big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4))
	if err != nil {
		t.Fatal(err)
	}

	parsed, ok, err := parser.ParseLog(types.Log{
		Topics: []common.Hash{swap.ID},
		Data:   swapData,
	})
	if err == nil {
		t.Fatal("expected an error for a log with missing indexed topics")
	}
	if ok || parsed != nil {
		t.Fatalf("malformed log was reported as parsed: ok=%v parsed=%#v", ok, parsed)
	}
}

func TestParseLogNamesAnonymousEventArguments(t *testing.T) {
	parser := MustParser()
	pairCreated := parser.pancakeFactory.Events["PairCreated"]
	pair := common.HexToAddress("0x3333333333333333333333333333333333333333")
	data, err := pairCreated.Inputs.NonIndexed().Pack(pair, big.NewInt(7))
	if err != nil {
		t.Fatal(err)
	}

	parsed, ok, err := parser.ParseLog(types.Log{
		Topics: []common.Hash{
			pairCreated.ID,
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
			common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
		},
		Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || parsed == nil {
		t.Fatalf("PairCreated log was not parsed: %#v", parsed)
	}
	if value, exists := parsed.Args["arg3"]; !exists || value.(*big.Int).Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("unexpected anonymous argument: %#v", parsed.Args)
	}
	if _, exists := parsed.Args[""]; exists {
		t.Fatalf("anonymous argument retained an empty map key: %#v", parsed.Args)
	}
}

func TestFourSelectorName(t *testing.T) {
	name, ok := FourSelectorName("0x519ebb10")
	if !ok || name != "createToken(bytes,bytes)" {
		t.Fatalf("unexpected selector lookup: %s %v", name, ok)
	}

	expected := map[string]string{
		"0x7d17ff3d": "buyToken(address,address,uint256,uint256)",
		"0xe671499b": "buyToken(address,uint256,uint256)",
		"0x87f27655": "buyTokenAMAP(address,uint256,uint256)",
		"0x73306244": "createToken(bytes)",
		"0xf464e7db": "sellToken(address,uint256)",
		"0x06e7b98f": "sellToken(uint256,address,uint256,uint256,uint256,address)",
		"0x15a68a10": "sellToken(uint256,address,uint256)",
	}
	for selector, signature := range expected {
		name, ok := FourSelectorName(selector)
		if !ok || name != signature {
			t.Fatalf("unexpected selector lookup for %s: %s %v", selector, name, ok)
		}
	}
}

func TestPancakeRouterSelectorName(t *testing.T) {
	expected := map[string]string{
		"0x38ed1739": "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
		"0x7ff36ab5": "swapExactETHForTokens(uint256,address[],address,uint256)",
		"0xe8e33700": "addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256)",
		"0xbaa2abde": "removeLiquidity(address,address,uint256,uint256,uint256,address,uint256)",
	}
	for selector, signature := range expected {
		name, ok := PancakeRouterSelectorName(selector)
		if !ok || name != signature {
			t.Fatalf("unexpected pancake selector lookup for %s: %s %v", selector, name, ok)
		}
	}
}
