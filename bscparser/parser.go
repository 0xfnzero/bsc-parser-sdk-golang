package bscparser

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type Protocol string

const (
	ProtocolFour    Protocol = "four"
	ProtocolPancake Protocol = "pancake"

	FourMemeAddress = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"
)

type ParsedCall struct {
	Protocol  Protocol
	Name      string
	Signature string
	Selector  string
	Args      map[string]any
	RawArgs   []any
}

type ParsedLog struct {
	Protocol Protocol
	Name     string
	Address  common.Address
	Args     map[string]any
	Log      types.Log
}

type Parser struct {
	four           abi.ABI
	pancakeRouter  abi.ABI
	pancakePair    abi.ABI
	pancakeFactory abi.ABI
}

func NewParser() (*Parser, error) {
	four, err := abi.JSON(strings.NewReader(FourTradingABIJSON))
	if err != nil {
		return nil, err
	}
	router, err := abi.JSON(strings.NewReader(PancakeRouterABIJSON))
	if err != nil {
		return nil, err
	}
	pair, err := abi.JSON(strings.NewReader(PancakePairABIJSON))
	if err != nil {
		return nil, err
	}
	factory, err := abi.JSON(strings.NewReader(PancakeFactoryABIJSON))
	if err != nil {
		return nil, err
	}
	return &Parser{four: four, pancakeRouter: router, pancakePair: pair, pancakeFactory: factory}, nil
}

func MustParser() *Parser {
	parser, err := NewParser()
	if err != nil {
		panic(err)
	}
	return parser
}

func (p *Parser) DecodeFourCalldata(input []byte) (*ParsedCall, error) {
	return decodeCalldata(ProtocolFour, p.four, input)
}

func (p *Parser) DecodePancakeRouterCalldata(input []byte) (*ParsedCall, error) {
	return decodeCalldata(ProtocolPancake, p.pancakeRouter, input)
}

func (p *Parser) ParseLog(log types.Log) (*ParsedLog, bool, error) {
	if len(log.Topics) == 0 {
		return nil, false, nil
	}
	if event, err := p.four.EventByID(log.Topics[0]); err == nil {
		return unpackEvent(ProtocolFour, event, log)
	}
	if event, err := p.pancakePair.EventByID(log.Topics[0]); err == nil {
		return unpackEvent(ProtocolPancake, event, log)
	}
	if event, err := p.pancakeFactory.EventByID(log.Topics[0]); err == nil {
		return unpackEvent(ProtocolPancake, event, log)
	}
	return nil, false, nil
}

func (p *Parser) ParseReceipt(receipt *types.Receipt) ([]ParsedLog, error) {
	if receipt == nil {
		return nil, nil
	}
	out := make([]ParsedLog, 0, len(receipt.Logs))
	for _, item := range receipt.Logs {
		if item == nil {
			continue
		}
		parsed, ok, err := p.ParseLog(*item)
		if err != nil {
			return out, err
		}
		if ok && parsed != nil {
			out = append(out, *parsed)
		}
	}
	return out, nil
}

func decodeCalldata(protocol Protocol, contractABI abi.ABI, input []byte) (*ParsedCall, error) {
	if len(input) < 4 {
		return nil, errors.New("calldata shorter than 4-byte selector")
	}
	method, err := contractABI.MethodById(input[:4])
	if err != nil {
		return nil, err
	}
	values, err := method.Inputs.Unpack(input[4:])
	if err != nil {
		return nil, err
	}
	args := make(map[string]any, len(values))
	for i, value := range values {
		name := method.Inputs[i].Name
		if name == "" {
			name = fmt.Sprintf("arg%d", i)
		}
		args[name] = value
	}
	return &ParsedCall{
		Protocol:  protocol,
		Name:      method.RawName,
		Signature: method.Sig,
		Selector:  "0x" + hex.EncodeToString(input[:4]),
		Args:      args,
		RawArgs:   values,
	}, nil
}

func unpackEvent(protocol Protocol, event *abi.Event, log types.Log) (*ParsedLog, bool, error) {
	values, err := event.Inputs.NonIndexed().Unpack(log.Data)
	if err != nil {
		return nil, false, fmt.Errorf("decode %s event data: %w", event.RawName, err)
	}
	args := make(map[string]any, len(event.Inputs))
	indexed := make(abi.Arguments, 0, len(event.Inputs))
	valueIndex := 0
	for i, input := range event.Inputs {
		if input.Name == "" {
			input.Name = fmt.Sprintf("arg%d", i)
		}
		if input.Indexed {
			indexed = append(indexed, input)
			continue
		}
		args[input.Name] = values[valueIndex]
		valueIndex++
	}
	if err := abi.ParseTopicsIntoMap(args, indexed, log.Topics[1:]); err != nil {
		return nil, false, fmt.Errorf("decode %s event topics: %w", event.RawName, err)
	}
	return &ParsedLog{
		Protocol: protocol,
		Name:     event.Name,
		Address:  log.Address,
		Args:     args,
		Log:      log,
	}, true, nil
}
