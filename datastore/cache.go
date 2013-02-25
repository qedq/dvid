/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

import (
	"log"

	"github.com/janelia-flyem/dvid/dvid"
)

type cachedData struct {
	// The default version of the datastore
	Head UUID

	// Holds all UUIDs in open datastore.  When we construct keys, use the smaller
	// unique int per datastore instead of the full 16 byte value.  This can save
	// 14 bytes per key.
	Uuids map[string]int16
}

// BlockRequest encapsulates the essential data needed to write blocks to the datastore.
type BlockRequest struct {
	Coord    dvid.BlockCoord
	BlockKey Key
	Subvol   *dvid.Subvolume
}

type request struct {
	vs *VersionService
	br *BlockRequest
}

const NumBlockHandlers = 10

// Number of block write requests that can be buffered on each block handler
// before sender is blocked.
const BlockHandlerBufferSize = 1000

type BlockChannels []chan request

var blockChannels BlockChannels

func init() {
	blockChannels = make(BlockChannels, NumBlockHandlers, NumBlockHandlers)
	for channelNum, _ := range blockChannels {
		blockChannels[channelNum] = make(chan request, BlockHandlerBufferSize)
		go blockHandler(channelNum)
	}
}

// WriteBlock accepts all requests to write blocks using data from a given subvolume,
// and sends the request to a block handler goroutine.
func (vs *VersionService) WriteBlock(br *BlockRequest) error {
	// Try to spread block coordinates out among block handlers to get most out of
	// our concurrent processing.  However, we may want a handler to receive similar
	// spatial indices and be better able to batch them for sequential writes.
	channelN := (br.Coord[0]*2 + br.Coord[1]*3 + br.Coord[2]*5) % NumBlockHandlers

	// Package the write block data and send it down the chosen channel
	blockChannels[channelN] <- request{vs, br}
	return nil
}

// blockHandler is a goroutine that fulfills block write requests sent on the given
// channel number.
func blockHandler(channelNum int) {
	for {
		req := <-blockChannels[channelNum]
		vs := req.vs
		si := vs.SpatialIndex(req.br.Coord)
		subvol := req.br.Subvol
		log.Printf("blockHandler(%d) handling block coord %s\n", channelNum,
			req.br.Coord)

		// Get the block data
		data, err := vs.kvdb.getBytes(req.br.BlockKey)
		if err != nil {
			data = make([]byte, vs.BlockNumVoxels(), vs.BlockNumVoxels())
			//log.Printf("ERROR blockHandler(%d): could not getBytes on BlockKey(%s)\n",
			//	channelNum, req.br.BlockKey)
		}
		dataBytes := vs.BlockNumVoxels() * subvol.BytesPerVoxel
		if dataBytes != len(data) {
			log.Printf("ERROR blockHandler(%d): retrieved block has %d bytes not %d bytes\n",
				channelNum, len(data), dataBytes)
			continue
		}

		// Get min and max voxel coordinates of this block
		minBlockVoxel := vs.OffsetToBlock(si)
		maxBlockVoxel := minBlockVoxel.AddSize(vs.BlockMax)

		// Get min and max voxel coordinates of the entire subvolume
		minSubvolVoxel := subvol.Offset
		maxSubvolVoxel := minSubvolVoxel.AddSize(subvol.Size)

		// Bound the start and end voxel coordinates of the subvolume by the block limits.
		start := minSubvolVoxel.BoundMin(minBlockVoxel)
		end := maxSubvolVoxel.BoundMax(maxBlockVoxel)

		// Traverse the data from start to end voxel coordinates and write to the block.
		// TODO -- Optimize the inner loop and see if we actually get faster :)  Currently,
		// the code tries to make it very clear what transformations are happening.
		for z := start[2]; z <= end[2]; z++ {
			for y := start[1]; y <= end[1]; y++ {
				for x := start[0]; x <= end[0]; x++ {
					voxelCoord := dvid.VoxelCoord{x, y, z}
					i := subvol.VoxelCoordToDataIndex(voxelCoord)
					b := vs.VoxelCoordToBlockIndex(voxelCoord)
					bI := b * subvol.BytesPerVoxel // index into block.data
					for n := 0; n < subvol.BytesPerVoxel; n++ {
						data[bI+n] = subvol.Data[i+n]
					}
				}
			}
		}

	} // request loop
}