package chainquery

import (
	"encoding/json"
	"io/ioutil"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/sirupsen/logrus"
)

func SaveHashes(hashes []string, path string) error {
	logrus.Printf("saving %d hashes to %s", len(hashes), path)
	file, err := json.MarshalIndent(hashes, "", "")
	if err != nil {
		return errors.Err(err)
	}

	err = ioutil.WriteFile(path, file, 0644)
	if err != nil {
		return errors.Err(err)
	}
	return nil
}

func LoadResolvedHashes(path string) ([]string, error) {
	logrus.Printf("loading hashes from %s", path)
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Err(err)
	}
	var hashes []string
	err = json.Unmarshal(file, &hashes)
	if err != nil {
		return nil, errors.Err(err)
	}
	logrus.Printf("loaded %d hashes from %s", len(hashes), path)
	return hashes, nil
}
