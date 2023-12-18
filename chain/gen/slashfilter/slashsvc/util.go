package slashsvc

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/ipfs/go-cid"
)

type walletSign func(ctx context.Context, signer address.Address, toSign []byte, meta api.MsgMeta) (*crypto.Signature, error)

func signAndPushMessage(ctx context.Context, full v0api.FullNode, sign walletSign, in *types.Message, gasOverPremium int, needEstimate bool, updateNonce bool) (cid.Cid, error) {
	msg := &types.Message{}
	*msg = *in

	if needEstimate {
		var err error
		msg, err = full.GasEstimateMessageGas(ctx, msg, nil, types.TipSetKey{})
		if err != nil {
			return cid.Undef, fmt.Errorf("GasEstimateMessageGas error: %w", err)
		}
	}

	signedMsg, err := signData(ctx, full, sign, msg, gasOverPremium, updateNonce)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to sign message: %w", err)
	}

	fmt.Println("sending message...")
	msgCid, err := full.MpoolPush(ctx, signedMsg)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to push message: %w", err)
	}

	return msgCid, nil
}

func signData(ctx context.Context, full v0api.FullNode, sign walletSign, msg *types.Message, gasOverPremium int, updateNonce bool) (*types.SignedMessage, error) {
	if updateNonce {
		nonce, err := full.MpoolGetNonce(ctx, msg.From)
		if err != nil {
			return nil, fmt.Errorf("failed to get nonce: %w", err)
		}
		msg.Nonce = nonce
	}

	if gasOverPremium > 0 {
		msg.GasPremium = abi.NewTokenAmount(int64(gasOverPremium) * msg.GasPremium.Int64())
	}

	sb, err := signingBytes(msg)
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

	return &types.SignedMessage{
		Message:   *msg,
		Signature: *sig,
	}, nil
}

func signingBytes(msg *types.Message) ([]byte, error) {
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

type defWalletSign func(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error)

func tryReplaceMsg(ctx context.Context, full v0api.FullNode, sign defWalletSign, msg types.Message) (cid.Cid, error) {
	head, err := full.ChainHead(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get chain head: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	a, err := full.StateGetActor(ctx, msg.From, head.Key())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get actor: %w", err)
	}
	msg.Nonce = a.Nonce

	for {
		head2, err := full.ChainHead(ctx)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to get chain head: %w", err)
		}
		if head2.Height() > head.Height() {
			return cid.Undef, fmt.Errorf("chain head changed")
		}

		select {
		case <-ctx.Done():
			return cid.Undef, ctx.Err()
		default:
			pendingMsg, err := full.MpoolPending(ctx, head2.Key())
			if err != nil {
				return cid.Undef, fmt.Errorf("failed to get pending messages: %w", err)
			}

			maxPremium := msg.GasPremium
			for _, m := range pendingMsg {
				if m.Message.To == msg.To && m.Message.Method == msg.Method && m.Message.From != msg.From &&
					m.Message.GasPremium.GreaterThan(msg.GasPremium) && m.Message.GasPremium.Int64() <= 5000000000 {
					if maxPremium.LessThan(m.Message.GasPremium) {
						maxPremium = m.Message.GasPremium
						log.Warnf("ReportConsensusFault(replace) to messagepool, from %v, found bigger gas premium %v",
							m.Message.From, m.Message.GasPremium)
					}
				}
			}
			if maxPremium.LessThanEqual(msg.GasPremium) {
				time.Sleep(time.Second * 1)
				continue
			}
			time.Sleep(time.Second * 1)

			newGasPremium := abi.NewTokenAmount((maxPremium.Int64() + rand.Int63n(100000)) * 115 / 100)
			log.Warnf("ReportConsensusFault(replace) to messagepool premium: %v, %v, %v", msg.GasPremium, maxPremium, newGasPremium)
			msg.GasPremium = newGasPremium
			msg.GasFeeCap = big.Max(msg.GasFeeCap, msg.GasPremium)

			msgCid, err := signAndPushMessage(ctx, full, func(ctx context.Context, signer address.Address, toSign []byte, meta lapi.MsgMeta) (*crypto.Signature, error) {
				return sign(ctx, signer, toSign)
			}, &msg, 1, false, false)
			if err != nil {
				log.Warnf("ReportConsensusFault(replace) to messagepool failed: error:%s", err)
			} else {
				log.Infof("ReportConsensusFault(replace) push message success: CID:%s \n", msgCid)
			}
			time.Sleep(time.Millisecond * 500)
		}
	}
}
