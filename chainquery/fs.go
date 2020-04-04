package chainquery

import (
	"encoding/json"
	"io/ioutil"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

func SaveHashes(hashes []string, path string) error {
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
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Err(err)
	}
	var hashes []string
	err = json.Unmarshal(file, &hashes)
	if err != nil {
		return nil, errors.Err(err)
	}
	return hashes, nil
}
