package reflector

import (
	"encoding/json"
	"io/ioutil"

	"reflector-s3-cleaner/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/sirupsen/logrus"
)

func SaveStreamData(streamData []shared.StreamData, path string) error {
	logrus.Printf("saving %d stream data objects to %s", len(streamData), path)
	file, err := json.MarshalIndent(streamData, "", "")
	if err != nil {
		return errors.Err(err)
	}

	err = ioutil.WriteFile(path, file, 0644)
	if err != nil {
		return errors.Err(err)
	}
	return nil
}

func LoadStreamData(path string) ([]shared.StreamData, error) {
	logrus.Printf("loading hashes from %s", path)
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Err(err)
	}
	var streamData []shared.StreamData
	err = json.Unmarshal(file, &streamData)
	if err != nil {
		return nil, errors.Err(err)
	}
	logrus.Printf("loaded %d stream data objects from %s", len(streamData), path)
	return streamData, nil
}

func SaveBlobs(blobs []shared.StreamBlobs, path string) error {
	logrus.Printf("saving %d sets of blobs to %s", len(blobs), path)
	file, err := json.MarshalIndent(blobs, "", "")
	if err != nil {
		return errors.Err(err)
	}

	err = ioutil.WriteFile(path, file, 0644)
	if err != nil {
		return errors.Err(err)
	}
	return nil
}
