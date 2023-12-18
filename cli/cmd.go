package cli

import (
	"context"
	"fmt"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

var log = logging.Logger("cli")

// custom CLI error

type ErrCmdFailed struct {
	msg string
}

func (e *ErrCmdFailed) Error() string {
	return e.msg
}

func NewCliError(s string) error {
	return &ErrCmdFailed{s}
}

// ApiConnector returns API instance
type ApiConnector func() api.FullNode

func GetFullNodeServices(ctx *cli.Context) (ServicesAPI, error) {
	if tn, ok := ctx.App.Metadata["test-services"]; ok {
		return tn.(ServicesAPI), nil
	}

	api, c, err := GetFullNodeAPIV1(ctx)
	if err != nil {
		return nil, err
	}

	return &ServicesImpl{api: api, closer: c}, nil
}

var GetAPIInfo = cliutil.GetAPIInfo
var GetRawAPI = cliutil.GetRawAPI
var GetAPI = cliutil.GetCommonAPI

var DaemonContext = cliutil.DaemonContext
var ReqContext = cliutil.ReqContext

var GetFullNodeAPI = cliutil.GetFullNodeAPI
var GetFullNodeAPIV1 = cliutil.GetFullNodeAPIV1
var GetGatewayAPI = cliutil.GetGatewayAPI

var GetStorageMinerAPI = cliutil.GetStorageMinerAPI
var GetMarketsAPI = cliutil.GetMarketsAPI
var GetWorkerAPI = cliutil.GetWorkerAPI

var CommonCommands = []*cli.Command{
	AuthCmd,
	LogCmd,
	WaitApiCmd,
	FetchParamCmd,
	PprofCmd,
	VersionCmd,
}

var Commands = []*cli.Command{
	WithCategory("basic", sendCmd),
	WithCategory("basic", walletCmd),
	WithCategory("basic", infoCmd),
	WithCategory("basic", clientCmd),
	WithCategory("basic", multisigCmd),
	WithCategory("basic", filplusCmd),
	WithCategory("basic", paychCmd),
	WithCategory("developer", AuthCmd),
	WithCategory("developer", MpoolCmd),
	WithCategory("developer", StateCmd),
	WithCategory("developer", ChainCmd),
	WithCategory("developer", LogCmd),
	WithCategory("developer", WaitApiCmd),
	WithCategory("developer", FetchParamCmd),
	WithCategory("developer", EvmCmd),
	WithCategory("network", NetCmd),
	WithCategory("network", SyncCmd),
	WithCategory("status", StatusCmd),
	PprofCmd,
	VersionCmd,
}

func WithCategory(cat string, cmd *cli.Command) *cli.Command {
	cmd.Category = strings.ToUpper(cat)
	return cmd
}

var lotusURLFlag = &cli.StringFlag{
	Name:  "lotus-url",
	Value: "https://api.node.glif.io",
}

var lotusTokenFlag = &cli.StringFlag{
	Name:  "lotus-token",
	Value: "",
}

var WalletURLFlag = &cli.StringFlag{
	Name:  "wallet-url",
	Value: "/ip4/127.0.0.1/tcp/5678",
}

var WalletTokenFlag = &cli.StringFlag{
	Name:  "wallet-token",
	Value: "",
}

var gasOverPremiumFlag = &cli.IntFlag{
	Name:  "gas-over-premium",
	Value: 5,
}

func NewLotusFullNodeRPCFromContext(cliCtx *cli.Context) (v0api.FullNode, jsonrpc.ClientCloser, error) {
	url := cliCtx.String(lotusURLFlag.Name)
	token := cliCtx.String(lotusTokenFlag.Name)
	fmt.Println("lotus url:", url)
	fmt.Println("lotus token:", token)
	ainfo := cliutil.APIInfo{
		Addr:  url,
		Token: []byte(token),
	}
	u, err := ainfo.DialArgs("v0")
	if err != nil {
		return nil, nil, err
	}
	fmt.Println(u, ainfo.Token)

	return client.NewFullNodeRPCV0(cliCtx.Context, u, ainfo.AuthHeader())
}

func NewWalletFullRPCFromContext(cliCtx *cli.Context) (v0api.Wallet, jsonrpc.ClientCloser, error) {
	url := cliCtx.String(WalletURLFlag.Name)
	token := cliCtx.String(WalletTokenFlag.Name)
	fmt.Println("wallet url:", url)
	fmt.Println("wallet token:", token)
	ainfo := cliutil.APIInfo{
		Addr:  url,
		Token: []byte(token),
	}
	u, err := ainfo.DialArgs("v0")
	if err != nil {
		return nil, nil, err
	}

	return client.NewWalletRPCV0(cliCtx.Context, u, ainfo.AuthHeader())
}

type signFunc func(ctx context.Context, signer address.Address, toSign []byte, meta api.MsgMeta) (*crypto.Signature, error)

func SignAndPushMessage(ctx context.Context, full v0api.FullNode, sign signFunc, in *types.Message, gasOverPremium int) (*types.SignedMessage, error) {
	msg := &types.Message{}
	*msg = *in

	msg, err := full.GasEstimateMessageGas(ctx, msg, nil, types.TipSetKey{})
	if err != nil {
		return nil, fmt.Errorf("GasEstimateMessageGas error: %w", err)
	}

	nonce, err := full.MpoolGetNonce(ctx, msg.From)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}
	msg.Nonce = nonce
	if gasOverPremium > 0 {
		msg.GasPremium = abi.NewTokenAmount(int64(gasOverPremium) * msg.GasPremium.Int64())
	}
	msg.GasFeeCap = big.Max(msg.GasPremium, msg.GasFeeCap)

	sb, err := SigningBytes(msg)
	if err != nil {
		return nil, err
	}

	mb, err := msg.ToStorageBlock()
	if err != nil {
		return nil, fmt.Errorf("serializing message: %w", err)
	}

	sig, err := sign(ctx, msg.From, sb, api.MsgMeta{
		Type:  api.MTChainMsg,
		Extra: mb.RawData(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign message: %w", err)
	}

	fmt.Println("sending message...")
	signedMsg := &types.SignedMessage{Message: *msg, Signature: *sig}
	_, err = full.MpoolPush(ctx, signedMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to push message: %w", err)
	}

	return signedMsg, nil
}

func SigningBytes(msg *types.Message) ([]byte, error) {
	if msg.From.Protocol() == address.Delegated {
		txArgs, err := ethtypes.EthTxArgsFromUnsignedEthMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to reconstruct eth transaction: %w", err)
		}
		rlpEncodedMsg, err := txArgs.ToRlpUnsignedMsg()
		if err != nil {
			return nil, fmt.Errorf("failed to repack eth rlp message: %w", err)
		}
		return rlpEncodedMsg, nil
	}

	return msg.Cid().Bytes(), nil
}
