package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
)

var disputeLog = logging.Logger("disputer")

const Confidence = 10

type minerDeadline struct {
	miner address.Address
	index uint64
}

var ChainDisputeSetCmd = &cli.Command{
	Name:  "disputer",
	Usage: "interact with the window post disputer",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "max-fee",
			Usage: "Spend up to X FIL per DisputeWindowedPoSt message",
		},
		&cli.StringFlag{
			Name:  "from",
			Usage: "optionally specify the account to send messages from",
		},
	},
	Subcommands: []*cli.Command{
		disputerStartCmd,
		disputerMsgCmd,
	},
}

var disputerMsgCmd = &cli.Command{
	Name:      "dispute",
	Usage:     "Send a specific DisputeWindowedPoSt message",
	ArgsUsage: "[minerAddress index postIndex]",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 3 {
			return IncorrectNumArgs(cctx)
		}

		ctx := ReqContext(cctx)

		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		toa, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("given 'miner' address %q was invalid: %w", cctx.Args().First(), err)
		}

		deadline, err := strconv.ParseUint(cctx.Args().Get(1), 10, 64)
		if err != nil {
			return err
		}

		postIndex, err := strconv.ParseUint(cctx.Args().Get(2), 10, 64)
		if err != nil {
			return err
		}

		fromAddr, err := getSender(ctx, api, cctx.String("from"))
		if err != nil {
			return err
		}

		dpp, aerr := actors.SerializeParams(&miner3.DisputeWindowedPoStParams{
			Deadline:  deadline,
			PoStIndex: postIndex,
		})

		if aerr != nil {
			return xerrors.Errorf("failed to serailize params: %w", aerr)
		}

		dmsg := &types.Message{
			To:     toa,
			From:   fromAddr,
			Value:  big.Zero(),
			Method: builtin3.MethodsMiner.DisputeWindowedPoSt,
			Params: dpp,
		}

		rslt, err := api.StateCall(ctx, dmsg, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("failed to simulate dispute: %w", err)
		}

		if rslt.MsgRct.ExitCode == 0 {
			mss, err := getMaxFee(cctx.String("max-fee"))
			if err != nil {
				return err
			}

			sm, err := api.MpoolPushMessage(ctx, dmsg, mss)
			if err != nil {
				return err
			}

			fmt.Println("dispute message ", sm.Cid())
		} else {
			fmt.Println("dispute is unsuccessful")
		}

		return nil
	},
}

func loadMinerFromFile(ctx context.Context, path string) ([]address.Address, int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	var addrs []address.Address
	if err := json.Unmarshal(data, &addrs); err != nil {
		return nil, 0, err
	}

	var max int
	for _, addr := range addrs {
		idStr := addr.String()[2:]
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, 0, err
		}
		if max < id {
			max = id
		}
	}

	log.Info("load local miners", len(addrs), "max", max)
	return addrs, max, nil
}

func loadMiners(ctx context.Context, api v0api.FullNode, path string) ([]address.Address, error) {
	var maddrs []address.Address
	start := 1000
	localMiners, max, err := loadMinerFromFile(ctx, path)
	if err == nil && start < max {
		start = max
		maddrs = localMiners
	}

	getActorFailedCount := 0
	for ; start < 2990000; start++ {
		if getActorFailedCount > 20 && max > 1000 && start > 2880000 {
			log.Infof("break at: %d", start)
			break
		}
		addr, err := address.NewIDAddress(uint64(start))
		if err != nil {
			return nil, err
		}
		actor, err := api.StateGetActor(ctx, addr, types.EmptyTSK)
		if err != nil {
			getActorFailedCount++
			continue
		}

		if actor.Code.String() != "bafk2bzacedo75pabe4i2l3hvhtsjmijrcytd2y76xwe573uku25fi7sugqld6" {
			continue
		}

		pow, err := api.StateMinerPower(ctx, addr, types.EmptyTSK)
		if err != nil {
			continue
		}
		if pow.MinerPower.QualityAdjPower.Int64() > 0 || pow.MinerPower.RawBytePower.Int64() > 0 {
			maddrs = append(maddrs, addr)
		}

		getActorFailedCount = 0

		if start%10000 == 0 {
			fmt.Printf("i %d, found %d miners\n", start, len(maddrs))
		}
	}

	data, err := json.Marshal(maddrs)
	if err == nil {
		_ = os.WriteFile(path, data, 0644)
	}

	return maddrs, nil
}

var disputerStartCmd = &cli.Command{
	Name:      "start",
	Usage:     "Start the window post disputer",
	ArgsUsage: "[minerAddress]",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "start-epoch",
			Usage: "only start disputing PoSts after this epoch ",
		},
		&cli.StringFlag{
			Name:  "miner-file",
			Value: "./miners.json",
			Usage: "path to miner file",
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

		// api, closer, err := GetFullNodeAPI(cctx)
		// if err != nil {
		// 	return err
		// }
		// defer closer()

		ctx := ReqContext(cctx)

		// fromAddr, err := getSender(ctx, api, cctx.String("from"))
		fromAddr, err := address.NewFromString(cctx.String("from"))
		if err != nil {
			return err
		}
		gasOverPremium := cctx.Int("gas-over-premium")

		// mss, err := getMaxFee(cctx.String("max-fee"))
		// if err != nil {
		// 	return err
		// }

		var minerList []address.Address
		minerFile := cctx.String("miner-file")
		if len(minerFile) > 0 {
			minerList, err = loadMiners(ctx, api, minerFile)
			if err != nil {
				return err
			}
		}

		startEpoch := abi.ChainEpoch(0)
		if cctx.IsSet("height") {
			startEpoch = abi.ChainEpoch(cctx.Uint64("height"))
		}

		disputeLog.Info("checking sync status")

		if err := SyncWait(ctx, api, false); err != nil {
			return xerrors.Errorf("sync wait: %w", err)
		}

		disputeLog.Info("setting up window post disputer")

		// subscribe to head changes and validate the current value

		headChanges, err := api.ChainNotify(ctx)
		if err != nil {
			return err
		}

		head, ok := <-headChanges
		if !ok {
			return xerrors.Errorf("Notify stream was invalid")
		}

		if len(head) != 1 {
			return xerrors.Errorf("Notify first entry should have been one item")
		}

		if head[0].Type != store.HCCurrent {
			return xerrors.Errorf("expected current head on Notify stream (got %s)", head[0].Type)
		}

		lastEpoch := head[0].Val.Height()
		lastStatusCheckEpoch := lastEpoch

		// build initial deadlineMap

		// minerList, err := api.StateListMiners(ctx, types.EmptyTSK)
		// if err != nil {
		// 	return err
		// }

		knownMiners := make(map[address.Address]struct{})
		deadlineMap := make(map[abi.ChainEpoch][]minerDeadline)
		for _, miner := range minerList {
			dClose, dl, err := makeMinerDeadline(ctx, api, miner)
			if err != nil {
				return xerrors.Errorf("making deadline: %w", err)
			}

			deadlineMap[dClose+Confidence] = append(deadlineMap[dClose+Confidence], *dl)

			knownMiners[miner] = struct{}{}
		}

		// when this fires, check for newly created miners, and purge any "missed" epochs from deadlineMap
		statusCheckTicker := time.NewTicker(time.Hour)
		defer statusCheckTicker.Stop()

		disputeLog.Info("starting up window post disputer")

		applyTsk := func(tsk types.TipSetKey) error {
			disputeLog.Infow("last checked epoch", "epoch", lastEpoch)
			dls, ok := deadlineMap[lastEpoch]
			delete(deadlineMap, lastEpoch)
			if !ok || startEpoch >= lastEpoch {
				// no deadlines closed at this epoch - Confidence, or we haven't reached the start cutoff yet
				return nil
			}

			dpmsgs := make([]*types.Message, 0)

			startTime := time.Now()
			proofsChecked := uint64(0)

			// TODO: Parallelizeable
			for _, dl := range dls {
				fullDeadlines, err := api.StateMinerDeadlines(ctx, dl.miner, tsk)
				if err != nil {
					return xerrors.Errorf("failed to load deadlines: %w", err)
				}

				if int(dl.index) >= len(fullDeadlines) {
					return xerrors.Errorf("deadline index %d not found in deadlines", dl.index)
				}

				disputableProofs := fullDeadlines[dl.index].DisputableProofCount
				proofsChecked += disputableProofs

				ms, err := makeDisputeWindowedPosts(ctx, api, dl, disputableProofs, fromAddr)
				if err != nil {
					return xerrors.Errorf("failed to check for disputes: %w", err)
				}

				dpmsgs = append(dpmsgs, ms...)

				dClose, dl, err := makeMinerDeadline(ctx, api, dl.miner)
				if err != nil {
					return xerrors.Errorf("making deadline: %w", err)
				}

				deadlineMap[dClose+Confidence] = append(deadlineMap[dClose+Confidence], *dl)
			}

			disputeLog.Infow("checked proofs", "count", proofsChecked, "duration", time.Since(startTime))

			// TODO: Parallelizeable / can be integrated into the previous deadline-iterating for loop
			for _, dpmsg := range dpmsgs {
				disputeLog.Infow("disputing a PoSt", "miner", dpmsg.To)
				// m, err := api.MpoolPushMessage(ctx, dpmsg, mss)
				if wapi == nil {
					signedMsg, err := SignAndPushMessage(ctx, api, func(ctx context.Context, signer address.Address, toSign []byte, meta lapi.MsgMeta) (*crypto.Signature, error) {
						return api.WalletSign(ctx, signer, toSign)
					}, dpmsg, gasOverPremium)
					if err != nil {
						disputeLog.Errorw("failed to dispute post message", "err", err.Error(), "miner", dpmsg.To)
					} else {
						disputeLog.Infow("submited dispute", "mcid", signedMsg.Cid(), "miner", dpmsg.To)
					}
					continue
				}
				signedMsg, err := SignAndPushMessage(ctx, api, wapi.WalletSign, dpmsg, gasOverPremium)
				if err != nil {
					disputeLog.Errorw("failed to dispute post message", "err", err.Error(), "miner", dpmsg.To)
				} else {
					disputeLog.Infow("submited dispute", "mcid", signedMsg.Cid(), "miner", dpmsg.To)
				}
			}

			return nil
		}

		disputeLoop := func() error {
			select {
			case notif, ok := <-headChanges:
				if !ok {
					return xerrors.Errorf("head change channel errored")
				}

				for _, val := range notif {
					switch val.Type {
					case store.HCApply:
						for ; lastEpoch <= val.Val.Height(); lastEpoch++ {
							err := applyTsk(val.Val.Key())
							if err != nil {
								return err
							}
						}
					case store.HCRevert:
						// do nothing
					default:
						return xerrors.Errorf("unexpected head change type %s", val.Type)
					}
				}
			case <-statusCheckTicker.C:
				disputeLog.Infof("running status check")

				// minerList, err = api.StateListMiners(ctx, types.EmptyTSK)
				// if err != nil {
				// 	return xerrors.Errorf("getting miner list: %w", err)
				// }
				minerList, err = loadMiners(ctx, api, minerFile)
				if err != nil {
					return xerrors.Errorf("getting miner list: %w", err)
				}

				for _, m := range minerList {
					_, ok := knownMiners[m]
					if !ok {
						dClose, dl, err := makeMinerDeadline(ctx, api, m)
						if err != nil {
							return xerrors.Errorf("making deadline: %w", err)
						}

						deadlineMap[dClose+Confidence] = append(deadlineMap[dClose+Confidence], *dl)

						knownMiners[m] = struct{}{}
					}
				}

				for ; lastStatusCheckEpoch < lastEpoch; lastStatusCheckEpoch++ {
					// if an epoch got "skipped" from the deadlineMap somehow, just fry it now instead of letting it sit around forever
					_, ok := deadlineMap[lastStatusCheckEpoch]
					if ok {
						disputeLog.Infow("epoch skipped during execution, deleting it from deadlineMap", "epoch", lastStatusCheckEpoch)
						delete(deadlineMap, lastStatusCheckEpoch)
					}
				}

				log.Infof("status check complete")
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}

		for {
			err := disputeLoop()
			if err == context.Canceled {
				disputeLog.Info("disputer shutting down")
				break
			}
			if err != nil {
				disputeLog.Errorw("disputer shutting down", "err", err)
				return err
			}
		}

		return nil
	},
}

// for a given miner, index, and maxPostIndex, tries to dispute posts from 0...postsSnapshotted-1
// returns a list of DisputeWindowedPoSt msgs that are expected to succeed if sent
func makeDisputeWindowedPosts(ctx context.Context, api v0api.FullNode, dl minerDeadline, postsSnapshotted uint64, sender address.Address) ([]*types.Message, error) {
	disputes := make([]*types.Message, 0)

	for i := uint64(0); i < postsSnapshotted; i++ {

		dpp, aerr := actors.SerializeParams(&miner3.DisputeWindowedPoStParams{
			Deadline:  dl.index,
			PoStIndex: i,
		})

		if aerr != nil {
			return nil, xerrors.Errorf("failed to serailize params: %w", aerr)
		}

		dispute := &types.Message{
			To:     dl.miner,
			From:   sender,
			Value:  big.Zero(),
			Method: builtin3.MethodsMiner.DisputeWindowedPoSt,
			Params: dpp,
		}

		rslt, err := api.StateCall(ctx, dispute, types.EmptyTSK)
		if err == nil && rslt.MsgRct.ExitCode == 0 {
			disputes = append(disputes, dispute)
		}

	}

	return disputes, nil
}

func makeMinerDeadline(ctx context.Context, api v0api.FullNode, mAddr address.Address) (abi.ChainEpoch, *minerDeadline, error) {
	dl, err := api.StateMinerProvingDeadline(ctx, mAddr, types.EmptyTSK)
	if err != nil {
		return -1, nil, xerrors.Errorf("getting proving index list: %w", err)
	}

	return dl.Close, &minerDeadline{
		miner: mAddr,
		index: dl.Index,
	}, nil
}

func getSender(ctx context.Context, api v0api.FullNode, fromStr string) (address.Address, error) {
	if fromStr == "" {
		return api.WalletDefaultAddress(ctx)
	}

	addr, err := address.NewFromString(fromStr)
	if err != nil {
		return address.Undef, err
	}

	has, err := api.WalletHas(ctx, addr)
	if err != nil {
		return address.Undef, err
	}

	if !has {
		return address.Undef, xerrors.Errorf("wallet doesn't contain: %s ", addr)
	}

	return addr, nil
}

func getMaxFee(maxStr string) (*lapi.MessageSendSpec, error) {
	if maxStr != "" {
		maxFee, err := types.ParseFIL(maxStr)
		if err != nil {
			return nil, xerrors.Errorf("parsing max-fee: %w", err)
		}
		return &lapi.MessageSendSpec{
			MaxFee:         types.BigInt(maxFee),
			GasOverPremium: 5,
		}, nil
	}

	return &lapi.MessageSendSpec{
		GasOverPremium: 5,
	}, nil
}
