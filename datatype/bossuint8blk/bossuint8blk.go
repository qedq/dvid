/*
Package bossuint8blk implements DVID support for boss channel data
mostly fulling the uint8blk interface.
*/
package bossuint8blk

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/bossuint8blk"
	TypeName = "bossuint8blk"

	ExperimentInfo = "https://api.theboss.io/v1/collection/%s/experiment/%s"
	FrameInfo      = "https://api.theboss.io/v1/coord/%s"

	// collection, experiment, channel
	DownsampleInfo = "https://api.theboss.io/v1/downsample/%s/%s/%s?iso=true"

	// https://api.theboss.io/v1/cutout/:collection/:experiment/:channel/:res/:xmin:xmax/:ymin::ymax/:zmin::zmax
	CutOut = "https://api.theboss.io/v1/cutout/%s/%s/%s/%d/%d:%d/%d:%d/%d:%d?iso=true"
)

const helpMessage = `
API for datatypes derived from bossuint8blk (github.com/janelia-flyem/dvid/datatype/bossuint8blk)
=================================================================================================

Command-line:

$ dvid repo <UUID> new bossuint8blk <data name> <settings...>

	Adds uint8blk support to BOSS's channel data.  Expects
	env BOSS_APPLICATION_CREDENTIALS="<TOKEN>"

	Example:

	$ dvid repo 3f8c new bossuint8blk grayscale collection=Kasthuri experiment=em channel=images

    Arguments:

    UUID           Hexadecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Required Configuration Settings (case-insensitive keys)

    collection     Name of BOSS collection
    experiment     Name of BOSS experiment
    channel        Name of BOSS channel in experiment (must be 8bit)
    scale	   Downsample level to access channel (default: 0)
    scalehack	   Fakes the volume to be isotropic by multiplying Z dimension
    background     Integer value that signifies background in any element (default: 0)

    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info

    Retrieves characteristics of this data in JSON format.

    Example: 

    GET <api URL>/node/3f8c/grayscale/info

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of bossuint8blk data.

GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/segmentation/raw/0_1/512_256/0_0_100/jpg:80

    Returns a raw XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    By "raw", we mean that no additional processing is applied based on voxel
    resolutions to make sure the retrieved image has isotropic pixels.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream". 

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Example: "0_2" is XZ, and "0_1_2" is a 3d subvolume.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    compression   Allows retrieval or submission of 3d data in "snappy (default) or "lz4" format.  
                     The 2d data will ignore this and use the image-based codec.
  	scale         Default is 0.  For scale N, returns an image down-sampled by a factor of 2^N.
    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
    				(all API calls that can be throttled) are handled.  If the server can't initiate the API 
    				call right away, a 503 (Service Unavailable) status code is returned.

GET  <api URL>/node/<UUID>/<data name>/specificblocks[?queryopts]

    Retrieves blocks corresponding to those specified in the query string.  This interface
    is useful if the blocks retrieved are not consecutive or if the backend in non ordered.

    TODO: enable arbitrary compression to be specified

    Example: 

    GET <api URL>/node/3f8c/grayscale/specificblocks?blocks=x1,y1,z2,x2,y2,z2,x3,y3,z3
	
	This will fetch blocks at position (x1,y1,z1), (x2,y2,z2), and (x3,y3,z3).
	The returned byte stream has a list of blocks with a leading block 
	coordinate (3 x int32) plus int32 giving the # of bytes in this block, and  then the 
	bytes for the value.  If blocks are unset within the span, they will not appear in the stream,
	so the returned data will be equal to or less than spanX blocks worth of data.  

    The returned data format has the following format where int32 is in little endian and the bytes of
    block data have been compressed in JPEG format.

        int32  Block 1 coordinate X (Note that this may not be starting block coordinate if it is unset.)
        int32  Block 1 coordinate Y
        int32  Block 1 coordinate Z
        int32  # bytes for first block (N1)
        byte0  Bytes of block data in jpeg-compressed format.
        byte1
        ...
        byteN1

        int32  Block 2 coordinate X
        int32  Block 2 coordinate Y
        int32  Block 2 coordinate Z
        int32  # bytes for second block (N2)
        byte0  Bytes of block data in jpeg-compressed format.
        byte1
        ...
        byteN2

        ...

    If no data is available for given block span, nothing is returned.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.

    Query-string Options:

    compression   Allows retrieval of block data in default storage or as "uncompressed".
    blocks	  x,y,z... block string
    prefetch	  ("on" or "true") Do not actually send data, non-blocking (default "off") -- not currently supported
`

var (
	DefaultBlkSize int32  = 64
	BOSSToken      string = ""
)

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})

	// check for token on startup
	loadToken()
}

func loadToken() {
	token := os.Getenv("BOSS_APPLICATION_CREDENTIALS")
	if token != "" {
		BOSSToken = token
	}
}

func setAuthorization(req *http.Request) error {
	// Load environment variable (if not already loaded)
	loadToken()
	if BOSSToken == "" {
		return fmt.Errorf("no BOSS token: set BOSS_APPLICATION_CREDENTIALS")
	}
	authstr := fmt.Sprintf("Token %s", BOSSToken)
	req.Header.Set("Authorization", authstr)

	return nil
}

type Type struct {
	datastore.Type
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewType() *Type {
	return &Type{
		datastore.Type{
			Name:    "bossuint8blk",
			URL:     "github.com/janelia-flyem/dvid/datatype/bossuint8blk",
			Version: "0.1",
			Requirements: &storage.Requirements{
				Batcher: true,
			},
		},
	}
}

func (d *Data) SendBlockSimple(w http.ResponseWriter, x, y, z int32, data []byte) error {
	// Send block coordinate and size of data.
	if err := binary.Write(w, binary.LittleEndian, x); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, y); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, z); err != nil {
		return err
	}
	n := len(data)
	if err := binary.Write(w, binary.LittleEndian, int32(n)); err != nil {
		return err
	}

	// Send data itself
	if written, err := w.Write(data); err != nil || written != n {
		if err != nil {
			return err
		}
		return fmt.Errorf("could not write %d bytes of value: only %d bytes written", n, written)
	}
	return nil
}

// SendBlocksSpecific writes data to the blocks specified -- best for non-ordered backend
func (d *Data) SendBlocksSpecific(ctx *datastore.VersionedCtx, w http.ResponseWriter, compression string, blockstring string, isprefetch bool) (numBlocks int, err error) {
	w.Header().Set("Content-type", "application/octet-stream")

	if compression != "uncompressed" && compression != "jpeg" && compression != "" {
		err = fmt.Errorf("don't understand 'compression' query string value: %s", compression)
		return
	}

	// treat jpeg as the default
	formatstr := compression
	if compression != "uncompressed" {
		formatstr = "jpeg"
	}

	timedLog := dvid.NewTimeLog()
	defer timedLog.Infof("SendBlocks Specific ")

	// extract querey string
	if blockstring == "" {
		return
	}
	coordarray := strings.Split(blockstring, ",")
	if len(coordarray)%3 != 0 {
		err = fmt.Errorf("block query string should be three coordinates per block")
		return
	}
	numBlocks = len(coordarray) / 3

	// make a finished queue
	finishedRequests := make(chan error, len(coordarray)/3)
	var mutex sync.Mutex

	// iterate through each block and query
	for i := 0; i < len(coordarray); i += 3 {
		var xloc, yloc, zloc int
		xloc, err = strconv.Atoi(coordarray[i])
		if err != nil {
			return
		}
		yloc, err = strconv.Atoi(coordarray[i+1])
		if err != nil {
			return
		}
		zloc, err = strconv.Atoi(coordarray[i+2])
		if err != nil {
			return
		}

		go func(xloc, yloc, zloc int32, isprefetch bool, finishedRequests chan error) {
			var err error
			if !isprefetch {
				defer func() {
					finishedRequests <- err
				}()
			}
			// ?! handle blank blocks
			// (currently just fetch and deal with edge errors)
			timedLog := dvid.NewTimeLog()

			// retrieve value (jpeg or raw)
			block3d := d.BlockSize.(dvid.Point3d)
			offset := dvid.Point3d{xloc * block3d[0], yloc * block3d[1], zloc * block3d[2]}
			data, err := d.fetchData(d.BlockSize.(dvid.Point3d), offset, formatstr)

			timedLog.Infof("BOSS PROXY HTTP GET BLOCK, %d bytes\n", len(data))

			if err != nil {
				return
			}
			if !isprefetch {
				// lock shared resource
				mutex.Lock()
				defer mutex.Unlock()
				d.SendBlockSimple(w, xloc, yloc, zloc, data)
			}
		}(int32(xloc), int32(yloc), int32(zloc), isprefetch, finishedRequests)
	}

	if !isprefetch {
		// wait for everything to finish if not prefetching
		for i := 0; i < len(coordarray); i += 3 {
			// ?! TODO: handle 502 errors and other errors
			<-finishedRequests
			/*errjob := <-finishedRequests
			if errjob != nil {
				err = errjob
			}*/
		}
	}
	return
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Necessary information to select data from BOSS API.
	Collection string
	Experiment string
	Channel    string
	Frame      string
	Scale      int

	// ScaleHack tries to make the data look isotropic
	// z-coords will be stretched
	ScaleHackFactor float64

	// Load defaults into Values to match uint8blk interface
	Values dvid.DataValues

	// Block size for this repo
	// For now, just do 64,64,64 and not native blocks
	// TODO: support native block resolution
	BlockSize dvid.Point

	// resolution determined by BOSS downsampling service
	dvid.Resolution

	// leave field in metadata but no longer updated!!
	dvid.Extents

	// Background value for data (0 by default)
	Background uint8
}

func (d *Data) Extents() *dvid.Extents {
	return &(d.Properties.Extents)
}

// retrieveImageDetails determined the resolution for the specified scale level
func retrieveImageDetails(scalestr string, collection string, experiment string, frame string, channel string, blockSize dvid.Point, scalehack bool) (dvid.Resolution, dvid.Extents, float64, error) {
	var resolution dvid.Resolution
	var extents dvid.Extents
	scalehackfactor := float64(1.0)

	bossClient := http.Client{
		Timeout: time.Second * 60,
	}

	// assume nanometers is the only unit for now
	// TODO: read dynamically from the frame

	// request frame info to get voxel units
	req_url := fmt.Sprintf(DownsampleInfo, collection, experiment, channel)
	req, err := http.NewRequest(http.MethodGet, req_url, nil)
	if err != nil {
		return resolution, extents, scalehackfactor, err
	}

	// set authorization token
	err = setAuthorization(req)
	if err != nil {
		return resolution, extents, scalehackfactor, err
	}

	// perform request
	res, err := bossClient.Do(req)
	if err != nil {
		return resolution, extents, scalehackfactor, fmt.Errorf("request failed")
	}

	downbody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return resolution, extents, scalehackfactor, fmt.Errorf("request failed")
	}
	res.Body.Close()

	// read json
	var down struct {
		ScaleExtents map[string][]int32   `json:"extent"`
		VoxelSizes   map[string][]float32 `json:"voxel_size"`
	}
	if err = json.Unmarshal(downbody, &down); err != nil {
		return resolution, extents, scalehackfactor, err
	}

	var maxbound []int32
	ok := true
	if maxbound, ok = down.ScaleExtents[scalestr]; !ok {
		return resolution, extents, scalehackfactor, fmt.Errorf("Bounding box not found for scale")
	}
	var voxelsize []float32
	if voxelsize, ok = down.VoxelSizes[scalestr]; !ok {
		return resolution, extents, scalehackfactor, fmt.Errorf("Voxel size not found for scale")
	}

	// make data more istropic
	if scalehack {
		scalehackfactor = float64(voxelsize[2] / voxelsize[1])
		voxelsize[2] = voxelsize[2] / float32(scalehackfactor)
		maxbound[2] = int32(math.Round(float64(maxbound[2]) * scalehackfactor))
	}

	// init resolution datastructure
	resolution.VoxelSize = make(dvid.NdFloat32, 3)
	for d := 0; d < 3; d++ {
		resolution.VoxelSize[d] = voxelsize[d]
	}
	resolution.VoxelUnits = make(dvid.NdString, 3)
	for d := 0; d < 3; d++ {
		resolution.VoxelUnits[d] = "nanometers"
	}

	// init extents
	blockSize3d, ok := blockSize.(dvid.Point3d)
	minpoint := dvid.Point3d{0, 0, 0}
	extents.MinPoint = minpoint
	maxpoint := dvid.Point3d{maxbound[0] - 1, maxbound[1] - 1, maxbound[2] - 1}
	extents.MaxPoint = maxpoint
	extents.MinIndex = minpoint.ChunkIndexer(blockSize3d)
	extents.MaxIndex = maxpoint.ChunkIndexer(blockSize3d)

	return resolution, extents, scalehackfactor, nil
}

// --- TypeService interface ---

// NewData returns a pointer to new bossuint8blk data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	// inject jpeg compression type to make it appear as JPEG
	// TODO: support native BOSS format
	c.Set("Compression", "jpeg")

	// Make sure we have needed collection, experiment, and channel names.
	collection, found, err := c.GetString("collection")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make bossuint8blk data without collection name 'collection' setting.")
	}

	experiment, found, err := c.GetString("experiment")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make bossuint8blk data without experiment name 'experiment' setting.")
	}

	channel, found, err := c.GetString("channel")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make bossuint8blk data without channel name 'channel' setting.")
	}

	scalestr, found, err := c.GetString("scale")
	if err != nil {
		return nil, err
	}
	scale := 0
	if found {
		scale, err = strconv.Atoi(scalestr)
		if err != nil {
			return nil, err
		}
	}

	scalehackstr, found, err := c.GetString("scalehack")
	if err != nil {
		return nil, err
	}
	scalehack := false
	if found {
		if scalehackstr == "true" {
			scalehack = true
		}
	}

	// set imageblk properties
	s, found, err := c.GetString("background")
	if err != nil {
		return nil, err
	}
	background_final := uint8(0)
	if found {
		background, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return nil, err
		}
		background_final = uint8(background)
	}

	// treat block size as 64,64,64 for now
	blockSize, err := dvid.StringToPoint("64,64,64", ",")
	if err != nil {
		return nil, err
	}

	// create client
	bossClient := http.Client{
		Timeout: time.Second * 60,
	}

	// request experiment info
	req_url := fmt.Sprintf(ExperimentInfo, collection, experiment)
	req, err := http.NewRequest(http.MethodGet, req_url, nil)
	if err != nil {
		return nil, err
	}

	// set authorization token
	err = setAuthorization(req)
	if err != nil {
		return nil, err
	}

	// perform request
	res, err := bossClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed")
	}

	expbody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("request failed")
	}
	res.Body.Close()

	// load experiment info (ensure channel exists, retrieve frame name, and max res)
	var exp struct {
		Channels             []string `json:"channels"`
		Name                 string   `json:"name"`
		Description          string   `json:"description"`
		Collection           string   `json:"collection"`
		Coord_frame          string   `json:"coord_frame"`
		Num_hierarchy_levels int      `json:"num_hierarchy_levels"`
		Hierarchy_method     string   `json:"hierarchy_method"`
		Num_time_samples     int      `json:"num_time_samples"`
		Time_step            int      `json:"time_stemp"`
		Time_step_unit       string   `json:"time_step_unit"`
		Creator              string   `json:"creator"`
	}
	if err = json.Unmarshal(expbody, &exp); err != nil {
		return nil, err
	}

	// load frame
	frame := exp.Coord_frame

	if scale >= exp.Num_hierarchy_levels {
		return nil, fmt.Errorf("Provided scale is greater than the max level")
	}

	// check if the channel exists
	foundch := false
	for _, v := range exp.Channels {
		if v == channel {
			foundch = true
			break
		}
	}
	if !foundch {
		return nil, fmt.Errorf("specified channel does not exist")
	}

	// load extents and voxel size information
	resolution, extents, scalehackfactor, err := retrieveImageDetails(scalestr, collection, experiment, frame, channel, blockSize, scalehack)
	if err != nil {
		return nil, err
	}

	// initialize the bossuint8blk data
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data: basedata,
		Properties: Properties{
			Collection:      collection,
			Experiment:      experiment,
			Channel:         channel,
			Frame:           frame,
			Scale:           scale,
			ScaleHackFactor: scalehackfactor,
			BlockSize:       blockSize,
			Background:      background_final,
			Resolution:      resolution,
			Extents:         extents,
			Values: dvid.DataValues{
				{
					T:     dvid.T_uint8,
					Label: "uint8",
				},
			},
		},
		client: &bossClient,
	}
	return data, nil
}

// Do handles command-line requests to boss proxy
func (dtype *Type) Do(cmd datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("unknown command for type %s", dtype.GetTypeName())
}

// fetchData calls BOSS and returns binary data in supplied format.
// This function does not handle patching or writing to http.
func (d *Data) fetchData(size dvid.Point3d, offset dvid.Point3d, formatstr string) ([]byte, error) {
	timedLog := dvid.NewTimeLog()

	client, err := d.GetClient()
	if err != nil {
		return nil, err
	}

	offsetnew := offset.Duplicate().(dvid.Point3d)
	sizenew := size.Duplicate().(dvid.Point3d)

	// apply hack if enabled and relevant (automatic scale z)
	if d.Properties.ScaleHackFactor != 1.0 {
		offsetnew[2] = int32(math.Round(float64(offsetnew[2]) / d.Properties.ScaleHackFactor))
		sizenew[2] = int32(math.Round(float64(sizenew[2]) / d.Properties.ScaleHackFactor))
		if sizenew[2] == 0 {
			sizenew[2] = 1
		}
	}

	url := fmt.Sprintf(CutOut, d.Collection, d.Experiment, d.Channel, d.Scale, offsetnew[0], offsetnew[0]+sizenew[0], offsetnew[1], offsetnew[1]+sizenew[1], offsetnew[2], offsetnew[2]+sizenew[2])
	req, err := http.NewRequest(http.MethodGet, url, nil)

	// ?! support download formats in addition to JPEG
	req.Header.Set("Accept", "image/jpeg")

	// set authorization token
	err = setAuthorization(req)
	if err != nil {
		return nil, err
	}

	var data []byte
	// repeat loop if resource is busy (currently no request backoff)
	for {
		// perform request
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
			return nil, fmt.Errorf("request failed")
		}
		timedLog.Infof("PROXY HTTP to BOSS: %s, returned response %d", url, resp.StatusCode)

		// make sure resource is not busy
		if resp.StatusCode < 500 || resp.StatusCode >= 600 {
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("Unexpected status code %d on volume request (%q, channel %q)", resp.StatusCode, d.DataName(), d.Channel)
			}

			data, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			break
		}
	}

	// hack to re-scale request
	if d.Properties.ScaleHackFactor != 1.0 {
		// decompress JPEG
		b := bytes.NewBuffer(data)
		imgdata, err := jpeg.Decode(b)
		if err != nil {
			return nil, err
		}
		dataraw := imgdata.(*image.Gray).Pix

		// make expanded buffer
		dataexpand := make([]uint8, size[0]*size[1]*size[2])

		iter1 := 0
		for z := 0; z < int(size[2]); z++ {
			iter2 := int32(float64(z)/d.Properties.ScaleHackFactor) * (size[0] * size[1])
			for y := 0; y < int(size[1]); y++ {
				for x := 0; x < int(size[0]); x++ {
					dataexpand[iter1] = dataraw[iter2]
					iter1 += 1
					iter2 += 1
				}
			}
		}

		// compress back to jpeg
		var buf bytes.Buffer
		img := image.NewGray(image.Rect(0, 0, int(size[0]), int(size[1]*size[2])))
		img.Pix = dataexpand

		if err = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 80}); err != nil {
			return nil, err
		}
		data = buf.Bytes()
	}

	if formatstr == "jpeg" || formatstr == "jpg" {
		return data, nil
	}

	// decompress JPEG
	b := bytes.NewBuffer(data)
	imgdata, err := jpeg.Decode(b)
	if err != nil {
		return nil, err
	}
	data = imgdata.(*image.Gray).Pix

	switch formatstr {
	case "lz4":
		lz4data := make([]byte, lz4.CompressBound(data))
		_, err := lz4.Compress(data, lz4data)
		if err != nil {
			return nil, err
		}
		return lz4data, nil
	default: // raw binary
		return data, nil
	}
	return data, nil
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties. Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-imageblk data %q", src.DataName())
	}

	d.Collection = d2.Collection
	d.Experiment = d2.Experiment
	d.Channel = d2.Channel
	d.Frame = d2.Frame
	d.Scale = d2.Scale
	d.ScaleHackFactor = d2.ScaleHackFactor
	copy(d.Values, d2.Values)

	d.BlockSize = d2.BlockSize.Duplicate()
	d.Properties.Extents = d2.Properties.Extents.Duplicate()
	d.Resolution.VoxelSize = make(dvid.NdFloat32, 3)
	copy(d.Resolution.VoxelSize, d2.Resolution.VoxelSize)
	d.Resolution.VoxelUnits = make(dvid.NdString, 3)
	copy(d.Resolution.VoxelUnits, d2.Resolution.VoxelUnits)
	d.Background = d2.Background

	return nil
}

func (dtype *Type) Help() string {
	return helpMessage
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data
	Properties

	client *http.Client // HTTP client that provides Authorization headers
}

// Returns a potentially cached client that handles authorization to Google.
// Assumes a JSON Web Token has been loaded into Data or else returns an error.
func (d *Data) GetClient() (*http.Client, error) {
	if d.client != nil {
		return d.client, nil
	}

	d.client = &http.Client{
		Timeout: time.Second * 60,
	}
	return d.client, nil
}

// --- DataService interface ---

func (d *Data) Help() string {
	return helpMessage
}

// BackgroundBlock returns a block buffer that has been preinitialized to the background value.
func (d *Data) BackgroundBlock() []byte {
	numElements := d.BlockSize.Prod()
	bytesPerElement := int64(1)
	blockData := make([]byte, numElements*bytesPerElement)
	if d.Background != 0 && bytesPerElement == 1 {
		background := byte(d.Background)
		for i := range blockData {
			blockData[i] = background
		}
	}
	return blockData
}

// ?! support outside regions by 1) reading cropped volume and
// 2) creating a blank block, 3) filling in the blank block with something
// like the following:
/*
	blockOffset := blockBegX * bytesPerVoxel
	dX := int64(v.Size().Value(0)) * bytesPerVoxel
	dY := int64(v.Size().Value(1)) * dX
	dataOffset := int64(dataBeg.Value(0)) * bytesPerVoxel
	bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
	blockZ := blockBegZ

	for dataZ := int64(dataBeg.Value(2)); dataZ <= int64(dataEnd.Value(2)); dataZ++ {
		blockY := blockBegY
		for dataY := int64(dataBeg.Value(1)); dataY <= int64(dataEnd.Value(1)); dataY++ {
			dataI := dataZ*dY + dataY*dX + dataOffset
			blockI := blockZ*bY + blockY*bX + blockOffset
			copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
			blockY++
		}
		blockZ++
	}


*/

// ?! Use blank block if completely outside of ROI

// handleImageReq returns an image with appropriate Content-Type set.
// This function allows arbitrary offset and size and pads response if needed.
func (d *Data) handleImageReq(w http.ResponseWriter, r *http.Request, parts []string) error {
	if len(parts) < 7 {
		return fmt.Errorf("%q must be followed by shape/size/offset", parts[3])
	}
	shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		return err
	}

	var size dvid.Point
	if size, err = dvid.StringToPoint(sizeStr, "_"); err != nil {
		return err
	}
	offset, err := dvid.StringToPoint3d(offsetStr, "_")
	if err != nil {
		return err
	}

	switch plane.ShapeDimensions() {
	case 2:
		return fmt.Errorf("Not yet implemented")
		/* TODO handle 2D
		var formatStr string
		if len(parts) >= 8 {
			formatStr = parts[7]
		}
		if formatStr == "" {
			formatStr = DefaultTileFormat
		}

		return d.serveTile(w, r, geom, formatStr, false)
		*/
	case 3:
		if len(parts) >= 8 {
			return d.serveVolume(w, r, size.(dvid.Point3d), offset, parts[7])
		} else {
			return d.serveVolume(w, r, size.(dvid.Point3d), offset, "")
		}
	}
	return nil
}

func (d *Data) serveVolume(w http.ResponseWriter, r *http.Request, size dvid.Point3d, offset dvid.Point3d, formatstr string) error {
	w.Header().Set("Content-type", "application/octet-stream")

	// ?! check if completely outside -- blank out

	// ?! check if partially outside -- patch (which means no compression fetch)

	// ?! use compression flag only for specific blocks -- should call with formatstr
	//queryStrings := r.URL.Query()
	//compression := queryStrings.Get("compression")

	// If we are within volume, get data from Boss
	timedLog := dvid.NewTimeLog()
	res, err := d.fetchData(size, offset, formatstr)
	if err != nil {
		return err
	}
	timedLog.Infof("BOSS PROXY HTTP GET, %d bytes\n", len(res))

	if _, err = w.Write(res); err != nil {
		return err
	}

	return nil
}

// DoRPC handles the 'generate' command.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("Unknown command.  Data instance %q does not support any commands.  See API help.", d.DataName())
}

func (d *Data) MarshalJSON() ([]byte, error) {
	metabytes, err := json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
	if err != nil {
		return nil, err
	}

	// temporary hack to make "bossuint8blk" look like "uint8blk"
	// TODO: refactor DVID to allow for a list of supported interfaces
	metabytes = bytes.Replace(metabytes, []byte("bossuint8blk"), []byte("uint8blk"), 1)
	return metabytes, nil
}

func (d *Data) MarshalJSONExtents(ctx *datastore.VersionedCtx) ([]byte, error) {
	// grab extent property and load
	// TODO?: re-read extents and update meta (handles change in extents)
	/*extents, err := d.GetExtents(ctx)
	if err != nil {
		return nil, err
	}
	*/

	var extentsJSON imageblk.ExtentsJSON
	extentsJSON.MinPoint = d.Properties.MinPoint
	extentsJSON.MaxPoint = d.Properties.MaxPoint

	metabytes, err := json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
		Extents  imageblk.ExtentsJSON
	}{
		d.Data,
		d.Properties,
		extentsJSON,
	})
	if err != nil {
		return nil, err
	}

	// temporary hack to make "bossuint8blk" look like "uint8blk"
	// TODO: refactor DVID to allow for a list of supported interfaces
	metabytes = bytes.Replace(metabytes, []byte("bossuint8blk"), []byte("uint8blk"), 1)
	return metabytes, nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
	timedLog := dvid.NewTimeLog()

	action := strings.ToLower(r.Method)
	switch action {
	case "get":
		// Acceptable
	default:
		server.BadRequest(w, r, "bossuint8blk can only handle GET HTTP verbs at this time")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API request")
		return
	}
	queryStrings := r.URL.Query()

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())

	case "info":
		// Potential TODO: re-query and reset extents everytime called
		jsonBytes, err := d.MarshalJSONExtents(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
	case "raw":
		if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
			if server.ThrottledHTTP(w) {
				return
			}
			defer server.ThrottledOpDone()
		}
		if err := d.handleImageReq(w, r, parts); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: image (%s)", r.Method, r.URL)
	case "specificblocks":
		// GET <api URL>/node/<UUID>/<data name>/specificblocks?compression=&prefetch=false&blocks=x,y,z,x,y,z...
		compression := queryStrings.Get("compression")
		blocklist := queryStrings.Get("blocks")

		// TODO: support prefetching if possible
		isprefetch := false
		if prefetch := queryStrings.Get("prefetch"); prefetch == "on" || prefetch == "true" {
			isprefetch = true
			server.BadRequest(w, r, "BOSS proxy does not support prefetch")
		}

		if action == "get" {
			numBlocks, err := d.SendBlocksSpecific(ctx, w, compression, blocklist, isprefetch)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: %s", r.Method, r.URL)
			activity = map[string]interface{}{
				"num_blocks": numBlocks,
			}
		} else {
			server.BadRequest(w, r, "DVID does not accept the %s action on the 'specificblocks' endpoint", action)
			return
		}

	// ?! specific blocks, subvolblocks

	default:
		server.BadAPIRequest(w, r, d)
	}
	timedLog.Infof("HTTP %s: %s", r.Method, r.URL)
	return
}
