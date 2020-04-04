package reflector

import (
	"encoding/json"
	"io/ioutil"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/sirupsen/logrus"
)

func SaveSDHashes(sdHashes []string, path string) error {
	logrus.Printf("saving %d hashes to %s", len(sdHashes), path)
	file, err := json.MarshalIndent(sdHashes, "", "")
	if err != nil {
		return errors.Err(err)
	}

	err = ioutil.WriteFile(path, file, 0644)
	if err != nil {
		return errors.Err(err)
	}
	return nil
}

func LoadSDHashes(path string) ([]string, error) {
	logrus.Printf("loading hashes from %s", path)
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Err(err)
	}
	var sdHashes []string
	err = json.Unmarshal(file, &sdHashes)
	if err != nil {
		return nil, errors.Err(err)
	}
	logrus.Printf("loaded %d hashes from %s", len(sdHashes), path)
	return sdHashes, nil
}
