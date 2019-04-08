/*
Package bossuint8blk implements DVID support for boss channel data
mostly fulling the uint8blk interface.
*/
package bossuint8blk

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"os"
	"time"
	"bytes"
	"strconv"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/bossuint8blk"
	TypeName = "bossuint8blk"

	ExperimentInfo = "https://api.theboss.io/v1/collection/%s/experiment/%s"
	FrameInfo = "https://api.theboss.io/v1/coord/%s"

	// collection, experiment, channel
	DownsampleInfo = "https://api.theboss.io/v1/downsample/%s/%s/%s?iso=true"


	// https://api.theboss.io/v1/cutout/:collection/:experiment/:channel/:res/:xmin:xmax/:ymin::ymax/:zmin::zmax
	CutOut = "https://api.theboss.io/v1/cutout/%s/%s/%s/%d/%d:%d/%d:%d/%d:%d"
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
    level	   Downsample level to access channel (default: 0)
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


`

var (
	DefaultBlkSize   int32  = 64
	BOSSToken	string = ""
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


// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Necessary information to select data from BOSS API.
	Collection string
	Experiment string
	Channel string
	Frame string
	Scale	int

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
func retrieveImageDetails(scalestr string, collection string, experiment string, frame string, channel string, blockSize dvid.Point) (dvid.Resolution, dvid.Extents, error) {
	var resolution dvid.Resolution
	var extents dvid.Extents

	bossClient := http.Client{
		Timeout: time.Second * 60,
	}

	// assume nanometers is the only unit for now
	// TODO: read dynamically from the frame
	
	// request frame info to get voxel units
	req_url := fmt.Sprintf(DownsampleInfo, collection, experiment, channel) 
	req, err := http.NewRequest(http.MethodGet, req_url, nil)
	if err != nil {
		return resolution, extents, err
	}
	
	// set authorization token
	err = setAuthorization(req)
	if err != nil {
		return resolution, extents, err
	}

	// perform request
	res, err := bossClient.Do(req)
	if err != nil {
		return resolution, extents, fmt.Errorf("request failed")
	}

	downbody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return resolution, extents, fmt.Errorf("request failed")
	}
	res.Body.Close()

	// read json
	var down struct {
		ScaleExtents	map[string][]int32 `json:"extent"`
		VoxelSizes	map[string][]float32 `json:"voxel_size"`
	}
	if err = json.Unmarshal(downbody, &down); err != nil {
		return resolution, extents, err
	}

	var maxbound []int32
	ok := true
	if maxbound, ok =  down.ScaleExtents[scalestr]; !ok {
		return resolution, extents, fmt.Errorf("Bounding box not found for scale")
	}
	var voxelsize []float32
	if voxelsize, ok =  down.VoxelSizes[scalestr]; !ok {
		return resolution, extents, fmt.Errorf("Voxel size not found for scale")
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
	maxpoint := dvid.Point3d{maxbound[0]-1, maxbound[1]-1, maxbound[2]-1}
	extents.MaxPoint = maxpoint 
	extents.MinIndex = minpoint.ChunkIndexer(blockSize3d)
	extents.MaxIndex = maxpoint.ChunkIndexer(blockSize3d)

	return resolution, extents, nil
}

// --- TypeService interface ---

// NewData returns a pointer to new bossuint8blk data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
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
		Channels []string `json:"channels"`
		Name string `json:"name"`
		Description string `json:"description"`
		Collection string `json:"collection"`
		Coord_frame string `json:"coord_frame"`
		Num_hierarchy_levels int `json:"num_hierarchy_levels"`
		Hierarchy_method string `json:"hierarchy_method"`
		Num_time_samples int `json:"num_time_samples"`
		Time_step int `json:"time_stemp"`
		Time_step_unit string `json:"time_step_unit"`
		Creator string `json:"creator"`
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
	for _,  v := range exp.Channels {
		if v == channel {
			foundch = true
			break
		}
	}
	if !foundch {
		return nil, fmt.Errorf("specified channel does not exist")
	}

	// load extents and voxel size information
	resolution, extents, err := retrieveImageDetails(scalestr, collection, experiment, frame, channel, blockSize)
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
			Collection:     collection,
			Experiment:     experiment,
			Channel:	channel,
			Frame:		frame,
			Scale: 		scale,
			BlockSize:	blockSize,
			Background:	background_final,
			Resolution:	resolution,
			Extents:	extents,
		},
		client: &bossClient,
	}
	return data, nil
}

// Do handles command-line requests to boss proxy
func (dtype *Type) Do(cmd datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("unknown command for type %s", dtype.GetTypeName())
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
	metabytes = bytes.Replace(metabytes,  []byte("bossuint8blk"), []byte("uint8blk"), 1) 
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
	metabytes = bytes.Replace(metabytes,  []byte("bossuint8blk"), []byte("uint8blk"), 1) 
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

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())

	case "info":
		jsonBytes, err := d.MarshalJSONExtents(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
	// ?! extents -- which will re-query frame and re-set if changed
	// ?! raw, specific blocks -- need to check boundary and pad

	default:
		server.BadAPIRequest(w, r, d)
	}
	timedLog.Infof("HTTP %s: %s", r.Method, r.URL)
	return
}
