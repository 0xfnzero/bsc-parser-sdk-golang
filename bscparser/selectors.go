package bscparser

var FourSelectorNames = map[string]string{
	"0x02ff2dcc": "buyToken(bytes,uint256,bytes)",
	"0x7d17ff3d": "buyToken(address,address,uint256,uint256)",
	"0xe671499b": "buyToken(address,uint256,uint256)",
	"0x43fc3523": "buyToken(uint256,address,uint256,uint256)",
	"0x90702094": "buyToken(uint256,address,address,uint256,uint256)",
	"0x7f79f6df": "buyTokenAMAP(address,address,uint256,uint256)",
	"0x87f27655": "buyTokenAMAP(address,uint256,uint256)",
	"0xedf9e251": "buyTokenAMAP(uint256,address,uint256,uint256)",
	"0x5e56c39a": "buyTokenAMAP(uint256,address,address,uint256,uint256)",
	"0x3e11741f": "sellToken(address,uint256,uint256)",
	"0xf464e7db": "sellToken(address,uint256)",
	"0x0da74935": "sellToken(uint256,address,uint256,uint256)",
	"0x15a68a10": "sellToken(uint256,address,uint256)",
	"0x06e7b98f": "sellToken(uint256,address,uint256,uint256,uint256,address)",
	"0xe63aaf36": "sellToken(uint256,address,address,uint256,uint256,uint256,address)",
	"0x930cc050": "sellToken(uint256,address,address,address,uint256,uint256)",
	"0x519ebb10": "createToken(bytes,bytes)",
	"0x73306244": "createToken(bytes)",
	"0x59f3f952": "collectFees(uint256,address,uint128,uint128)",
}

func FourSelectorName(selector string) (string, bool) {
	value, ok := FourSelectorNames[selector]
	return value, ok
}

var PancakeRouterSelectorNames = map[string]string{
	"0xe8e33700": "addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256)",
	"0xf305d719": "addLiquidityETH(address,uint256,uint256,uint256,address,uint256)",
	"0xbaa2abde": "removeLiquidity(address,address,uint256,uint256,uint256,address,uint256)",
	"0x02751cec": "removeLiquidityETH(address,uint256,uint256,uint256,address,uint256)",
	"0xaf2979eb": "removeLiquidityETHSupportingFeeOnTransferTokens(address,uint256,uint256,uint256,address,uint256)",
	"0x2195995c": "removeLiquidityWithPermit(address,address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
	"0xded9382a": "removeLiquidityETHWithPermit(address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
	"0x5b0d5984": "removeLiquidityETHWithPermitSupportingFeeOnTransferTokens(address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
	"0xfb3bdb41": "swapETHForExactTokens(uint256,address[],address,uint256)",
	"0x7ff36ab5": "swapExactETHForTokens(uint256,address[],address,uint256)",
	"0xb6f9de95": "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
	"0x18cbafe5": "swapExactTokensForETH(uint256,uint256,address[],address,uint256)",
	"0x791ac947": "swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
	"0x38ed1739": "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
	"0x5c11d795": "swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
	"0x4a25d94a": "swapTokensForExactETH(uint256,uint256,address[],address,uint256)",
	"0x8803dbee": "swapTokensForExactTokens(uint256,uint256,address[],address,uint256)",
}

func PancakeRouterSelectorName(selector string) (string, bool) {
	value, ok := PancakeRouterSelectorNames[selector]
	return value, ok
}
