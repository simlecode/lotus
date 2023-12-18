package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/crypto"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/gen/slashfilter/slashsvc"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var reportSlashCmd = &cli.Command{
	Name:  "report",
	Usage: "Report on slash events",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "from",
			Usage: "optionally specify the account to send messages from",
		},
		lotusURLFlag,
		lotusTokenFlag,
		WalletURLFlag,
		WalletTokenFlag,
		gasOverPremiumFlag,
	},
	Action: func(cctx *cli.Context) error {
		api, fclose, err := NewLotusFullNodeRPCFromContext(cctx)
		if err != nil {
			return err
		}
		defer fclose()

		var wapi v0api.Wallet
		var wclose jsonrpc.ClientCloser
		if cctx.IsSet(WalletURLFlag.Name) {
			wapi, wclose, err = NewWalletFullRPCFromContext(cctx)
			if err != nil {
				return err
			}
			defer wclose()
		}

		ctx := ReqContext(cctx)

		fromAddr, err := address.NewFromString(cctx.String("from"))
		if err != nil {
			return err
		}
		gasOverPremium := cctx.Int("gas-over-premium")

		storePath := ".report/report"
		if err := os.MkdirAll(storePath, 0755); err != nil {
			return err
		}
		if err = slashsvc.SlashConsensus(ctx, newSlashConsensusImpl(api, wapi, fromAddr, gasOverPremium), storePath, fromAddr.String()); err != nil {
			return err
		}

		<-ctx.Done()
		fmt.Println("context done", ctx.Err())

		return nil
	},
}

type slashConsensusImpl struct {
	api            v0api.FullNode
	wapi           v0api.Wallet
	sender         address.Address
	gasOverPremium int
}

var _ slashsvc.ConsensusSlasherApi = (*slashConsensusImpl)(nil)

func newSlashConsensusImpl(api v0api.FullNode, wapi v0api.Wallet, sender address.Address, gasOverPremium int) *slashConsensusImpl {
	return &slashConsensusImpl{
		api:            api,
		wapi:           wapi,
		sender:         sender,
		gasOverPremium: gasOverPremium,
	}
}

func (sci *slashConsensusImpl) ChainHead(ctx context.Context) (*types.TipSet, error) {
	return sci.api.ChainHead(ctx)
}
func (sci *slashConsensusImpl) ChainGetBlock(ctx context.Context, blk cid.Cid) (*types.BlockHeader, error) {
	return sci.api.ChainGetBlock(ctx, blk)
}
func (sci *slashConsensusImpl) MpoolPushMessage(ctx context.Context, msg *types.Message, spec *lapi.MessageSendSpec) (*types.SignedMessage, error) {
	signedMsg, err := SignAndPushMessage(ctx, sci.api, sci.wapi.WalletSign, msg, sci.gasOverPremium)
	if err != nil {
		return nil, err
	}
	return signedMsg, nil
}

func (sci *slashConsensusImpl) SyncIncomingBlocks(ctx context.Context) (<-chan *types.BlockHeader, error) {
	return sci.api.SyncIncomingBlocks(ctx)
}
func (sci *slashConsensusImpl) WalletDefaultAddress(ctx context.Context) (address.Address, error) {
	return sci.sender, nil
}

func (sci *slashConsensusImpl) WalletSign(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error) {
	return sci.wapi.WalletSign(ctx, k, msg, lapi.MsgMeta{
		Type:  lapi.MTChainMsg,
		Extra: msg,
	})
}
