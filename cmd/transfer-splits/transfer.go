package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
)

func getSupervoxelRLE(url string, supervoxel uint64) (data []byte, err error) {
	rleURL := fmt.Sprintf("%s/sparsevol/%d?supervoxels=true&format=rles", url, supervoxel)
	var resp *http.Response
	resp, err = http.Get(rleURL)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("status code %d returned for %s", resp.StatusCode, rleURL)
		return
	}
	return ioutil.ReadAll(resp.Body)
}

func getSupervoxelMapping(url string, supervoxel uint64) (mapping uint64, err error) {
	payload := fmt.Sprintf("[%d]", supervoxel)
	var req *http.Request
	req, err = http.NewRequest("GET", url+"/mapping", bytes.NewBufferString(payload))
	if err != nil {
		return
	}
	client := &http.Client{}
	var resp *http.Response
	resp, err = client.Do(req)
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("status code %d returned for %s/mapping", resp.StatusCode, url)
		return
	}
	var jsonResp []byte
	jsonResp, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var jsonVal []uint64
	if err = json.Unmarshal(jsonResp, &jsonVal); err != nil {
		err = fmt.Errorf("Unable to parse JSON response after GET mapping: %v", err)
		return
	}
	if len(jsonVal) != 1 {
		err = fmt.Errorf("got bad result back from mapping: %v", jsonVal)
		return
	}
	return jsonVal[0], nil
}

func transferSplits(srcURL, destURL, splitLog string) {
	splits := strings.Split(splitLog, "\n")
	fmt.Printf("Initiating %d supervoxel splits.\n", len(splits))
	bodies := make(labels.Set)
	mapping := make(map[uint64]uint64) // how original supervoxel may be modified by splits
	for i, split := range splits {
		fmt.Printf("Executing: %s\n", split)
		// parse the split string
		var spOrig, spSplit, spRemain uint64
		n, err := fmt.Sscanf(split, "Presumptive supervoxel %d split into %d, remains into %d", &spOrig, &spSplit, &spRemain)
		if n != 3 || err != nil {
			fmt.Printf("error parsing superpixel split line %d (n = %d): %v\n %s\n", i, n, err, split)
			continue
		}

		// get split supervoxel RLE
		rles, err := getSupervoxelRLE(srcURL, spSplit)
		if err != nil {
			fmt.Printf("Error getting supervoxel %d RLE: %v\n", spSplit, err)
			continue
		}

		// POST supervoxel split using split supervoxel RLE
		destSupervoxel, ok := mapping[spOrig]
		if !ok {
			destSupervoxel = spOrig
		}
		body, err := getSupervoxelMapping(destURL, destSupervoxel)
		if err != nil {
			fmt.Printf("Error trying to find body for supervoxel %d: %v\n", destSupervoxel, err)
		}
		bodies[body] = struct{}{}

		postURL := fmt.Sprintf("%s/split-supervoxel/%d", destURL, destSupervoxel)
		if *dryrun {
			fmt.Printf("-- dryrun POST %s with len(rles) = %d\n", postURL, len(rles))
		} else {
			var resp *http.Response
			resp, err = http.Post(postURL, "application/octet-stream", bytes.NewBuffer(rles))
			if err != nil {
				fmt.Printf("POST error: %v\n", err)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				fmt.Printf("POST bad status %d received: %s\n", resp.StatusCode, postURL)
				continue
			}
			respJSON, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("Error receiving JSON after POST %s: %v\n", postURL, err)
				continue
			}
			var jsonVal struct {
				SplitSupervoxel  uint64
				RemainSupervoxel uint64
			}
			if err := json.Unmarshal(respJSON, &jsonVal); err != nil {
				fmt.Printf("Unable to parse JSON response after POST %s: %v\n", postURL, err)
				continue
			}
			mapping[spOrig] = jsonVal.RemainSupervoxel
		}
	}

	fmt.Printf("Completed transfer.\n")
	fmt.Printf("\nBodies in destination touched by supervoxel splits:\n%s\n", bodies)
}
