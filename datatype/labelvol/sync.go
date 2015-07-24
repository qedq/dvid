package labelvol

import (
	"encoding/binary"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	// These are the labels that are in the process of modification from merge, split, or other sync events.
	dirtyBlocks dvid.DirtyBlocks
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBufferSize = 100

// InitSync implements the datastore.Syncer interface
func (d *Data) InitSync(name dvid.InstanceName) []datastore.SyncSub {
	// This should only be called once for any synced instance.
	if d.IsSyncEstablished(name) {
		return nil
	}
	d.SyncEstablished(name)

	syncCh := make(chan datastore.SyncMessage, syncBufferSize)
	doneCh := make(chan struct{})

	go d.handleBlockEvent(syncCh, doneCh)

	subs := []datastore.SyncSub{
		datastore.SyncSub{
			Event:  datastore.SyncEvent{name, labels.ChangeBlockEvent},
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{name, labels.DeleteBlockEvent},
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
	}
	return subs
}

// Processes each change as we get it.
// TODO -- accumulate larger # of changes before committing to prevent
// excessive compaction time?  This assumes LSM storage engine, which
// might not always hold in future, so stick with incremental update
// until proven to be a bottleneck.
func (d *Data) handleBlockEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	store, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	for msg := range in {
		select {
		case <-done:
			return
		default:
			ctx := datastore.NewVersionedCtx(d, msg.Version)
			switch delta := msg.Delta.(type) {
			case imageblk.Block:
				d.ingestBlock(ctx, delta, batcher)
			case labels.DeleteBlock:
				d.deleteBlock(ctx, delta, batcher)
			default:
				dvid.Criticalf("Cannot sync labelvol from block event.  Got unexpected delta: %v\n", msg)
			}
		}
	}
}

func (d *Data) deleteBlock(ctx *datastore.VersionedCtx, block labels.DeleteBlock, batcher storage.KeyValueBatcher) {
	batch := batcher.NewBatch(ctx)

	// Iterate through this block of labels and get set of labels.
	blockBytes := len(block.Data)
	if blockBytes != int(d.BlockSize.Prod())*8 {
		dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			blockBytes, d.BlockSize.Prod())
		return
	}
	labelSet := make(map[uint64]struct{})
	for i := 0; i < blockBytes; i += 8 {
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		if label != 0 {
			labelSet[label] = struct{}{}
		}
	}

	// Go through all non-zero labels and delete the corresponding labelvol k/v pair.
	zyx := block.Index.ToIZYXString()
	for label := range labelSet {
		tk := NewTKey(label, zyx)
		batch.Delete(tk)
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("Bad sync in labelvol.  Couldn't commit block %s\n", zyx.Print())
	}
	return
}

// Note that this does not delete any removed labels in the block since we only get the CURRENT block
// and not PAST blocks.  To allow mutation of label blocks, not just ingestion, we need another function.
func (d *Data) ingestBlock(ctx *datastore.VersionedCtx, block imageblk.Block, batcher storage.KeyValueBatcher) {

	// Iterate through this block of labels and create RLEs for each label.
	blockBytes := len(block.Data)
	if blockBytes != int(d.BlockSize.Prod())*8 {
		dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			blockBytes, d.BlockSize.Prod())
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := block.Index.MinPoint(d.BlockSize)
	lastPt := block.Index.MaxPoint(d.BlockSize)

	var curStart dvid.Point3d
	var voxelLabel, curLabel, maxLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				voxelLabel = binary.LittleEndian.Uint64(block.Data[start : start+8])
				if maxLabel < voxelLabel {
					maxLabel = voxelLabel
				}
				start += 8

				// If we hit background or have switched label, save old run and start new one.
				if voxelLabel == 0 || voxelLabel != curLabel {
					// Save old run
					if curRun > 0 {
						labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					}
					// Start new one if not zero label.
					if voxelLabel != 0 {
						curStart = dvid.Point3d{x, y, z}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = voxelLabel
				} else {
					curRun++
				}
			}
			// Force break of any runs when we finish x scan.
			if curRun > 0 {
				labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
				curLabel = 0
				curRun = 0
			}
		}
	}

	// Store the RLEs for each label in this block.
	if maxLabel > 0 {
		batch := batcher.NewBatch(ctx)
		blockStr := block.Index.ToIZYXString()
		for label, rles := range labelRLEs {
			tk := NewTKey(label, blockStr)
			rleBytes, err := rles.MarshalBinary()
			if err != nil {
				dvid.Errorf("Bad encoding labelvol keys for label %d: %v\n", label, err)
				return
			}
			batch.Put(tk, rleBytes)
		}
		// compare-and-set MaxLabel and batch commit
		d.casMaxLabel(batch, ctx.VersionID(), maxLabel)
	}
}