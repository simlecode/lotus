package lpffi

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/lib/harmony/harmonytask"
	"github.com/filecoin-project/lotus/lib/harmony/resources"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type SectorRef struct {
	SpID         int64                   `db:"sp_id"`
	SectorNumber int64                   `db:"sector_number"`
	RegSealProof abi.RegisteredSealProof `db:"reg_seal_proof"`
}

func (sr SectorRef) ID() abi.SectorID {
	return abi.SectorID{
		Miner:  abi.ActorID(sr.SpID),
		Number: abi.SectorNumber(sr.SectorNumber),
	}
}

func (sr SectorRef) Ref() storiface.SectorRef {
	return storiface.SectorRef{
		ID:        sr.ID(),
		ProofType: sr.RegSealProof,
	}
}

type TaskStorage struct {
	sc *SealCalls

	alloc, existing storiface.SectorFileType
	ssize           abi.SectorSize
	pathType        storiface.PathType

	taskToSectorRef func(taskID harmonytask.TaskID) (SectorRef, error)
}

type ReleaseStorageFunc func() // free storage reservation

type StorageReservation struct {
	SectorRef SectorRef
	Release   ReleaseStorageFunc
	Paths     storiface.SectorPaths
	PathIDs   storiface.SectorPaths

	Alloc, Existing storiface.SectorFileType
}

func (sb *SealCalls) Storage(taskToSectorRef func(taskID harmonytask.TaskID) (SectorRef, error), alloc, existing storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) *TaskStorage {
	return &TaskStorage{
		sc:              sb,
		alloc:           alloc,
		existing:        existing,
		ssize:           ssize,
		pathType:        pathType,
		taskToSectorRef: taskToSectorRef,
	}
}

func (t *TaskStorage) HasCapacity() bool {
	ctx := context.Background()

	paths, err := t.sc.sectors.sindex.StorageBestAlloc(ctx, t.alloc, t.ssize, t.pathType)
	if err != nil {
		log.Errorf("finding best alloc in HasCapacity: %+v", err)
		return false
	}

	local, err := t.sc.sectors.localStore.Local(ctx)
	if err != nil {
		log.Errorf("getting local storage: %+v", err)
		return false
	}

	for _, path := range paths {
		if t.pathType == storiface.PathStorage && !path.CanStore {
			continue // we want to store, and this isn't a store path
		}
		if t.pathType == storiface.PathSealing && !path.CanSeal {
			continue // we want to seal, and this isn't a seal path
		}

		// check if this path is on this node
		var found bool
		for _, storagePath := range local {
			if storagePath.ID == path.ID {
				found = true
				break
			}
		}
		if !found {
			// this path isn't on this node
			continue
		}

		// StorageBestAlloc already checks that there is enough space; Not atomic like reserving space, but it's
		// good enough for HasCapacity
		return true
	}

	return false // no path found
}

func (t *TaskStorage) Claim(taskID int) error {
	ctx := context.Background()

	sectorRef, err := t.taskToSectorRef(harmonytask.TaskID(taskID))
	if err != nil {
		return xerrors.Errorf("getting sector ref: %w", err)
	}

	// storage writelock sector
	lkctx, cancel := context.WithCancel(ctx)

	allocate := storiface.FTCache

	lockAcquireTimuout := time.Second * 10
	lockAcquireTimer := time.NewTimer(lockAcquireTimuout)

	go func() {
		defer cancel()

		select {
		case <-lockAcquireTimer.C:
		case <-ctx.Done():
		}
	}()

	if err := t.sc.sectors.sindex.StorageLock(lkctx, sectorRef.ID(), storiface.FTNone, allocate); err != nil {
		// timer will expire
		return xerrors.Errorf("claim StorageLock: %w", err)
	}

	if !lockAcquireTimer.Stop() {
		// timer expired, so lkctx is done, and that means the lock was acquired and dropped..
		return xerrors.Errorf("failed to acquire lock")
	}
	defer func() {
		// make sure we release the sector lock
		lockAcquireTimer.Reset(0)
	}()

	// find anywhere
	//  if found return nil, for now
	s, err := t.sc.sectors.sindex.StorageFindSector(ctx, sectorRef.ID(), allocate, must.One(sectorRef.RegSealProof.SectorSize()), false)
	if err != nil {
		return xerrors.Errorf("claim StorageFindSector: %w", err)
	}

	lp, err := t.sc.sectors.localStore.Local(ctx)
	if err != nil {
		return err
	}

	// see if there are any non-local sector files in storage
	for _, info := range s {
		for _, l := range lp {
			if l.ID == info.ID {
				continue
			}

			// TODO: Create reservation for fetching; This will require quite a bit more refactoring, but for now we'll
			//  only care about new allocations
			return nil
		}
	}

	// acquire a path to make a reservation in
	pathsFs, pathIDs, err := t.sc.sectors.localStore.AcquireSector(ctx, sectorRef.Ref(), storiface.FTNone, allocate, storiface.PathSealing, storiface.AcquireMove)
	if err != nil {
		return err
	}

	// reserve the space
	release, err := t.sc.sectors.localStore.Reserve(ctx, sectorRef.Ref(), allocate, pathIDs, storiface.FSOverheadSeal)
	if err != nil {
		return err
	}

	sres := &StorageReservation{
		SectorRef: sectorRef,
		Release:   release,
		Paths:     pathsFs,
		PathIDs:   pathIDs,

		Alloc:    t.alloc,
		Existing: t.existing,
	}

	t.sc.sectors.storageReservations.Store(harmonytask.TaskID(taskID), sres)

	log.Debugw("claimed storage", "task_id", taskID, "sector", sectorRef.ID(), "paths", pathsFs)

	// note: we drop the sector writelock on return; THAT IS INTENTIONAL, this code runs in CanAccept, which doesn't
	// guarantee that the work for this sector will happen on this node; SDR CanAccept just ensures that the node can
	// run the job, harmonytask is what ensures that only one SDR runs at a time
	return nil
}

func (t *TaskStorage) MarkComplete(taskID int) error {
	// MarkComplete is ALWAYS called after the task is done or not scheduled
	// If Claim is called and returns without errors, MarkComplete with the same
	// taskID is guaranteed to eventually be called

	sectorRef, err := t.taskToSectorRef(harmonytask.TaskID(taskID))
	if err != nil {
		return xerrors.Errorf("getting sector ref: %w", err)
	}

	sres, ok := t.sc.sectors.storageReservations.Load(harmonytask.TaskID(taskID))
	if !ok {
		return xerrors.Errorf("no reservation found for task %d", taskID)
	}

	if sectorRef != sres.SectorRef {
		return xerrors.Errorf("reservation sector ref doesn't match task sector ref: %+v != %+v", sectorRef, sres.SectorRef)
	}

	log.Debugw("marking storage complete", "task_id", taskID, "sector", sectorRef.ID(), "paths", sres.Paths)

	// remove the reservation
	t.sc.sectors.storageReservations.Delete(harmonytask.TaskID(taskID))

	// release the reservation
	sres.Release()

	// note: this only frees the reservation, allocated sectors are declared in AcquireSector which is aware of
	//  the reservation
	return nil
}

var _ resources.Storage = &TaskStorage{}
