package slashsvc

import (
	"context"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	levelds "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/gen/slashfilter"
	"github.com/filecoin-project/lotus/chain/types"
)

var log = logging.Logger("slashsvc")

type ConsensusSlasherApi interface {
	ChainHead(context.Context) (*types.TipSet, error)
	ChainGetBlock(context.Context, cid.Cid) (*types.BlockHeader, error)
	MpoolPushMessage(ctx context.Context, msg *types.Message, spec *lapi.MessageSendSpec) (*types.SignedMessage, error)
	SyncIncomingBlocks(context.Context) (<-chan *types.BlockHeader, error)
	WalletDefaultAddress(context.Context) (address.Address, error)
	WalletSign(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error)
}

func SlashConsensus(ctx context.Context, a ConsensusSlasherApi, p string, from string) error {
	var fromAddr address.Address
	slashBlocks := make(map[cid.Cid]*types.BlockHeader)

	ds, err := levelds.NewDatastore(p, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
	if err != nil {
		return xerrors.Errorf("open leveldb: %w", err)
	}
	sf := slashfilter.New(ds)
	if from == "" {
		defaddr, err := a.WalletDefaultAddress(ctx)
		if err != nil {
			return err
		}
		fromAddr = defaddr
	} else {
		addr, err := address.NewFromString(from)
		if err != nil {
			return err
		}

		fromAddr = addr
	}

	blocks, err := a.SyncIncomingBlocks(ctx)
	if err != nil {
		return xerrors.Errorf("sync incoming blocks failed: %w", err)
	}

	log.Infow("consensus fault reporter", "from", fromAddr)
	go func() {
		for block := range blocks {
			otherBlock, extraBlock, fault, err := slashFilterMinedBlock(ctx, sf, a, block)
			if err != nil {
				log.Errorf("slash detector errored: %s", err)
				continue
			}
			if fault {
				log.Errorf("<!!> SLASH FILTER DETECTED FAULT DUE TO BLOCKS %s and %s, height: %d", otherBlock.Cid(), block.Cid(), otherBlock.Height)
				bh1, err := cborutil.Dump(otherBlock)
				if err != nil {
					log.Errorf("could not dump otherblock:%s, err:%s", otherBlock.Cid(), err)
					continue
				}

				bh2, err := cborutil.Dump(block)
				if err != nil {
					log.Errorf("could not dump block:%s, err:%s", block.Cid(), err)
					continue
				}

				params := miner.ReportConsensusFaultParams{
					BlockHeader1: bh1,
					BlockHeader2: bh2,
				}
				if extraBlock != nil {
					be, err := cborutil.Dump(extraBlock)
					if err != nil {
						log.Errorf("could not dump block:%s, err:%s", block.Cid(), err)
						continue
					}
					params.BlockHeaderExtra = be
				}

				enc, err := actors.SerializeParams(&params)
				if err != nil {
					log.Errorf("could not serialize declare faults parameters: %s", err)
					continue
				}

				if _, ok := slashBlocks[block.Cid()]; ok {
					log.Infof("ReportConsensusFault %s already reported", block.Cid())
					continue
				}
				slashBlocks[block.Cid()] = block
				slashBlocks[otherBlock.Cid()] = otherBlock

				msg := &types.Message{
					To:     block.Miner,
					From:   fromAddr,
					Value:  types.NewInt(0),
					Method: builtin.MethodsMiner.ReportConsensusFault,
					Params: enc,
				}

				go waitPushMessage(ctx, a, msg, block.Height)
			}
		}
	}()

	return nil
}

func waitPushMessage(ctx context.Context, a ConsensusSlasherApi, msg *types.Message, slashHeight abi.ChainEpoch) error {
	otherAPI, closer, err := client.NewFullNodeRPCV0(ctx, "https://api.node.glif.io/rpc/v0", nil)
	if err != nil {
		log.Warnf("ReportConsensusFault connect other node error: %s", err)
	}
	if closer != nil {
		defer closer()
	}

	head, err := a.ChainHead(ctx)
	if err != nil {
		return err
	}

	maxWait := 12
	blkTime := time.Now().Unix() - int64(head.MinTimestamp())
	wait := maxWait - int(blkTime)
	if wait > 0 {
		time.Sleep(time.Duration(wait) * time.Second)
	}

	{
		tmpMsg := types.Message{}
		tmpMsg = *msg
		tmpMsg.GasLimit = int64(50923829 + rand.Int63n(10000000))
		tmpMsg.GasFeeCap = abi.NewTokenAmount(5000000)
		tmpMsg.GasPremium = abi.NewTokenAmount(4200000 + rand.Int63n(10000))
		msgCid, err := signAndPushMessage(ctx, otherAPI, func(ctx context.Context, signer address.Address, toSign []byte, meta lapi.MsgMeta) (*crypto.Signature, error) {
			return a.WalletSign(ctx, signer, toSign)
		}, &tmpMsg, 1, false, true)
		if err != nil {
			log.Warnf("ReportConsensusFault to messagepool failed: error:%s", err)
		} else {
			log.Infof("ReportConsensusFault push message success: CID:%s \n", msgCid)

			go func() {
				log.Infof("try replace message")
				_, err = tryReplaceMsg(ctx, otherAPI, a.WalletSign, tmpMsg)
				if err != nil {
					log.Warnf("ReportConsensusFault replace message failed: error:%s", err)
				}
			}()
		}
	}

	for {
		head, err := a.ChainHead(ctx)
		if err != nil {
			return err
		}
		if head.Height() > slashHeight {
			break
		}
		time.Sleep(time.Second * 1)
	}

	return nil
}

func slashFilterMinedBlock(ctx context.Context, sf *slashfilter.SlashFilter, a ConsensusSlasherApi, blockB *types.BlockHeader) (*types.BlockHeader, *types.BlockHeader, bool, error) {
	blockC, err := a.ChainGetBlock(ctx, blockB.Parents[0])
	if err != nil {
		return nil, nil, false, xerrors.Errorf("chain get block error:%s", err)
	}

	blockACid, fault, err := sf.MinedBlock(ctx, blockB, blockC.Height)
	if err != nil {
		return nil, nil, false, xerrors.Errorf("slash filter check block error:%s", err)
	}

	if !fault {
		return nil, nil, false, nil
	}

	blockA, err := a.ChainGetBlock(ctx, blockACid)
	if err != nil {
		return nil, nil, false, xerrors.Errorf("failed to get blockA: %w", err)
	}

	// (a) double-fork mining (2 blocks at one epoch)
	if blockA.Height == blockB.Height {
		return blockA, nil, true, nil
	}

	// (b) time-offset mining faults (2 blocks with the same parents)
	if types.CidArrsEqual(blockB.Parents, blockA.Parents) {
		return blockA, nil, true, nil
	}

	// (c) parent-grinding fault
	// Here extra is the "witness", a third block that shows the connection between A and B as
	// A's sibling and B's parent.
	// Specifically, since A is of lower height, it must be that B was mined omitting A from its tipset
	//
	//      B
	//      |
	//  [A, C]
	if types.CidArrsEqual(blockA.Parents, blockC.Parents) && blockA.Height == blockC.Height &&
		types.CidArrsContains(blockB.Parents, blockC.Cid()) && !types.CidArrsContains(blockB.Parents, blockA.Cid()) {
		return blockA, blockC, true, nil
	}

	log.Error("unexpectedly reached end of slashFilterMinedBlock despite fault being reported!")
	return nil, nil, false, nil
}
