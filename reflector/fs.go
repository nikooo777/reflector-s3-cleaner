package reflector

import (
	"encoding/json"
	"io/ioutil"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

func SaveSDHashes(sdHashes []string, path string) error {
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
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Err(err)
	}
	var sdHashes []string
	err = json.Unmarshal(file, &sdHashes)
	if err != nil {
		return nil, errors.Err(err)
	}
	return sdHashes, nil
}
