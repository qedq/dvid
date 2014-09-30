/*
	Package multichan16 tailors the voxels data type for 16-bit fluorescent images with multiple
	channels that can be read from V3D Raw format.  Note that this data type has multiple
	channels but segregates its channel data in (c, z, y, x) fashion rather than interleave
	it within a block of data in (z, y, x, c) fashion.  There is not much advantage at
	using interleaving; most forms of RGB compression fails to preserve the
	independence of the channels.  Segregating the channel data lets us use straightforward
	compression on channel slices.

	Specific channels of multichan16 data are addressed by adding a numerical suffix to the
	data name.  For example, if we have "mydata" multichan16 data, we reference channel 1
	as "mydata1" and channel 2 as "mydata2".  Up to the first 3 channels are composited
	into a RGBA volume that is addressible using "mydata" or "mydata0".
*/
package multichan16

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/multichan16"
	TypeName = "multichan16"
)

const HelpMessage = `
API for datatypes derived from multichan16 (github.com/janelia-flyem/dvid/datatype/multichan16)
===============================================================================================

Command-line:

$ dvid node <UUID> <data name> load <V3D raw filename>

    Adds multichannel data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").

    Example: 

    $ dvid node 3f8c mydata load local mydata.v3draw

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    filename      Filename of a V3D Raw format file.
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET <api URL>/node/3f8c/multichan16/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of multichan16 data.


GET  <api URL>/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]
POST <api URL>/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts orthogonal plane image data to named multichannel 16-bit data.

    Example: 

    GET <api URL>/node/3f8c/mydata2/xy/200,200/0,0,100/jpg:80  (channel 2 of mydata)

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data.  Optionally add a numerical suffix for the channel number.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in pixels in the format "dx_dy".
    offset        3d coordinate in the format "x_y_z".  Gives coordinate of top upper left voxel.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

`

// DefaultBlockMax specifies the default size for each block of this data type.
var (
	DefaultBlockSize int32 = 32

	typeService datastore.TypeService

	compositeValues = dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "red",
		},
		{
			T:     dvid.T_uint8,
			Label: "green",
		},
		{
			T:     dvid.T_uint8,
			Label: "blue",
		},
		{
			T:     dvid.T_uint8,
			Label: "alpha",
		},
	}
)

func init() {
	interpolable := true
	dtype := NewType(compositeValues, interpolable)

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	typeService = dtype
	datastore.Register(dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// Type just uses voxels data type by composition.
type Type struct {
	*voxels.Type
}

// NewType returns a pointer to a new voxels Type with default values set.
func NewType(values dvid.DataValues, interpolable bool) *Type {
	basetype := voxels.NewType(compositeValues, interpolable)
	basetype.Name = TypeName
	basetype.URL = RepoURL
	basetype.Version = Version
	return &Type{basetype}
}

// --- TypeService interface ---

// NewDataService returns a pointer to a new multichan16 with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	voxelData, err := dtype.Type.NewDataService(uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	basedata := voxelData.(*voxels.Data)
	basedata.Properties.Values = nil
	return &Data{Data: basedata}, nil
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// -------  ExtData interface implementation -------------

// Channel is an image volume that fulfills the voxels.ExtData interface.
type Channel struct {
	*voxels.Voxels

	// Channel 0 is the composite RGBA channel and all others are 16-bit.
	channelNum int32
}

func (c *Channel) String() string {
	return fmt.Sprintf("Channel %d of size %s @ offset %s", c.channelNum, c.Size(), c.StartPoint())
}

func (c *Channel) Interpolable() bool {
	return true
}

func (c *Channel) NewChunkIndex() dvid.ChunkIndexer {
	return &dvid.IndexCZYX{c.channelNum, dvid.IndexZYX{}}
}

// Index returns a channel-specific Index
func (c *Channel) Index(p dvid.ChunkPoint) dvid.Index {
	return &dvid.IndexCZYX{c.channelNum, dvid.IndexZYX(p.(dvid.ChunkPoint3d))}
}

// IndexIterator returns an iterator that can move across the voxel geometry,
// generating indices or index spans.
func (c *Channel) IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error) {
	// Setup traversal
	begVoxel, ok := c.StartPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData StartPoint() cannot handle Chunkable points.")
	}
	endVoxel, ok := c.EndPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData EndPoint() cannot handle Chunkable points.")
	}

	blockSize := chunkSize.(dvid.Point3d)
	begBlock := begVoxel.Chunk(blockSize).(dvid.ChunkPoint3d)
	endBlock := endVoxel.Chunk(blockSize).(dvid.ChunkPoint3d)

	return dvid.NewIndexCZYXIterator(c.channelNum, begBlock, endBlock), nil
}

// Data of multichan16 type embeds voxels and extends it with channels.
type Data struct {
	*voxels.Data

	// Number of channels for this data.  The names are referenced by
	// adding a number onto the data name, e.g., mydata1, mydata2, etc.
	NumChannels int
}

type propertiesT struct {
	voxels.Properties
	NumChannels int
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended propertiesT
	}{
		&(d.Data.Data),
		propertiesT{
			d.Data.Properties,
			d.NumChannels,
		},
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.NumChannels)); err != nil {
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
	if err := enc.Encode(d.NumChannels); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- DataService interface ---

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("multichan16.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// Do acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "load" {
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	if len(request.Command) < 5 {
		return fmt.Errorf("Poorly formatted load command.  See command-line help.")
	}
	return d.LoadLocal(request, reply)
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	_, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n",
			d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// All HTTP requests are interactive so let server tally request.
	// TODO: This command should be moved to web server handling when better
	// framework for datatype-specific API is implemented, allowing type-specific
	// logging of API calls, etc.
	server.GotInteractiveRequest()

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	var op voxels.OpType
	switch action {
	case "get":
		op = voxels.GetOp
	case "post":
		op = voxels.PutOp
	default:
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	if len(parts) < 4 {
		server.BadRequest(w, r, "Incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return
	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return
	default:
	}

	// Get the data name and parse out the channel number or see if composite is required.
	var channelNum int32
	channumStr := strings.TrimPrefix(parts[2], string(d.DataName()))
	if len(channumStr) == 0 {
		channelNum = 0
	} else {
		n, err := strconv.ParseInt(channumStr, 10, 32)
		if err != nil {
			server.BadRequest(w, r, "Error parsing channel number from data name '%s': %s",
				parts[2], err.Error())
			return
		}
		if int(n) > d.NumChannels {
			minChannelName := fmt.Sprintf("%s1", d.DataName())
			maxChannelName := fmt.Sprintf("%s%d", d.DataName(), d.NumChannels)
			server.BadRequest(w, r, "Data only has %d channels.  Use names '%s' -> '%s'", d.NumChannels,
				minChannelName, maxChannelName)
			return
		}
		channelNum = int32(n)
	}

	// Get the data shape.
	shapeStr := dvid.DataShapeString(parts[3])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		server.BadRequest(w, r, "Bad data shape given '%s'", shapeStr)
		return
	}

	switch dataShape.ShapeDimensions() {
	case 2:
		sizeStr, offsetStr := parts[4], parts[5]
		slice, err := dvid.NewSliceFromStrings(shapeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if op == voxels.PutOp {
			server.BadRequest(w, r, "DVID does not yet support POST of slices into multichannel data")
			return
		} else {
			if d.NumChannels == 0 || d.Data.Values() == nil {
				server.BadRequest(w, r, "Cannot retrieve absent data '%d'.  Please load data.", d.DataName())
				return
			}
			values := d.Data.Values()
			if len(values) <= int(channelNum) {
				server.BadRequest(w, r, "Must choose channel from 0 to %d", len(values))
				return
			}
			stride := slice.Size().Value(0) * values.BytesPerElement()
			dataValues := dvid.DataValues{values[channelNum]}
			data := make([]uint8, int(slice.NumVoxels()))
			v := voxels.NewVoxels(slice, dataValues, data, stride, d.ByteOrder)
			channel := &Channel{
				Voxels:     v,
				channelNum: channelNum,
			}
			img, err := voxels.GetImage(storeCtx, d, channel, nil)
			var formatStr string
			if len(parts) >= 7 {
				formatStr = parts[6]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img.Get(), formatStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
		}
	case 3:
		sizeStr, offsetStr := parts[4], parts[5]
		_, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if op == voxels.GetOp {
			server.BadRequest(w, r, "DVID does not yet support GET of volume data")
			return
		} else {
			server.BadRequest(w, r, "DVID does not yet support POST of volume data")
			return
		}
	default:
		server.BadRequest(w, r, "DVID does not yet support nD volumes")
		return
	}
	timedLog.Infof("HTTP %s: %s", r.Method, dataShape)
}

// LoadLocal adds image data to a version node.  See HelpMessage for example of
// command-line use of "load local".
func (d *Data) LoadLocal(request datastore.Request, reply *datastore.Response) error {
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceStr, filename string
	_ = request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceStr, &filename)

	// Get the uuid from a uniquely identifiable string
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return fmt.Errorf("Could not find node with UUID %s: %s", uuidStr, err.Error())
	}

	// Load the V3D Raw file.
	ext := filepath.Ext(filename)
	switch ext {
	case ".raw", ".v3draw":
	default:
		return fmt.Errorf("Unknown extension '%s' when expected V3D Raw file", ext)
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	unmarshaler := V3DRawMarshaler{}
	channels, err := unmarshaler.UnmarshalV3DRaw(file)
	if err != nil {
		return err
	}

	// Store the metadata
	d.NumChannels = len(channels)
	d.Properties.Values = make(dvid.DataValues, d.NumChannels)
	if d.NumChannels > 0 {
		d.ByteOrder = channels[0].ByteOrder()
		reply.Text = fmt.Sprintf("Loaded %s into data '%s': found %d channels\n",
			d.DataName(), filename, d.NumChannels)
		reply.Text += fmt.Sprintf(" %s", channels[0])
	} else {
		reply.Text = fmt.Sprintf("Found no channels in file %s\n", filename)
		return nil
	}
	for i, channel := range channels {
		d.Properties.Values[i] = channel.Voxels.Values()[0]
	}

	// Get repo and save it.
	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	if err := repo.Save(); err != nil {
		return err
	}

	// PUT each channel of the file into the datastore using a separate data name.
	storeCtx := datastore.NewVersionedContext(d, versionID)
	for _, channel := range channels {
		dvid.Infof("Processing channel %d... \n", channel.channelNum)
		err = voxels.PutVoxels(storeCtx, d, channel, nil)
		if err != nil {
			return err
		}
	}

	// Create a RGB composite from the first 3 channels.  This is considered to be channel 0
	// or can be accessed with the base data name.
	dvid.Infof("Creating composite image from channels...\n")
	err = d.storeComposite(storeCtx, channels)
	if err != nil {
		return err
	}

	timedLog.Infof("RPC load local '%s' completed", filename)
	return nil
}

// Create a RGB interleaved volume.
func (d *Data) storeComposite(ctx storage.Context, channels []*Channel) error {
	// Setup the composite Channel
	geom := channels[0].Geometry
	pixels := int(geom.NumVoxels())
	stride := geom.Size().Value(0) * 4
	composite := &Channel{
		Voxels:     voxels.NewVoxels(geom, compositeValues, channels[0].Data(), stride, d.ByteOrder),
		channelNum: channels[0].channelNum,
	}

	// Get the min/max of each channel.
	numChannels := len(channels)
	if numChannels > 3 {
		numChannels = 3
	}
	var min, max [3]uint16
	min[0] = uint16(0xFFFF)
	min[1] = uint16(0xFFFF)
	min[2] = uint16(0xFFFF)
	for c := 0; c < numChannels; c++ {
		channel := channels[c]
		data := channel.Data()
		beg := 0
		for i := 0; i < pixels; i++ {
			value := d.ByteOrder.Uint16(data[beg : beg+2])
			if value < min[c] {
				min[c] = value
			}
			if value > max[c] {
				max[c] = value
			}
			beg += 2
		}
	}

	// Do second pass, normalizing each channel and storing it into the appropriate byte.
	compdata := composite.Voxels.Data()
	for c := 0; c < numChannels; c++ {
		channel := channels[c]
		window := int(max[c] - min[c])
		if window == 0 {
			window = 1
		}
		data := channel.Data()
		beg := 0
		begC := c // Channel 0 -> R, Channel 1 -> G, Channel 2 -> B
		for i := 0; i < pixels; i++ {
			value := d.ByteOrder.Uint16(data[beg : beg+2])
			normalized := 255 * int(value-min[c]) / window
			if normalized > 255 {
				normalized = 255
			}
			compdata[begC] = uint8(normalized)
			beg += 2
			begC += 4
		}
	}

	// Set the alpha channel to 255.
	alphaI := 3
	for i := 0; i < pixels; i++ {
		compdata[alphaI] = 255
		alphaI += 4
	}

	// Store the result
	return voxels.PutVoxels(ctx, d, composite, nil)
}
