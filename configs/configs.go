package configs

import (
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/tkanos/gonfig"
)

type DbConfig struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Database string `json:"database"`
	Password string `json:"password"`
}
type AWSS3Config struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	Endpoint  string `json:"endpoint"`
}
type Configs struct {
	Chainquery DbConfig    `json:"chainquery"`
	Reflector  DbConfig    `json:"reflector"`
	S3         AWSS3Config `json:"s3"`
}

var Configuration *Configs

func Init(configPath string) error {
	if Configuration != nil {
		return nil
	}
	c := Configs{}
	err := gonfig.GetConf(configPath, &c)
	if err != nil {
		return errors.Err(err)
	}
	Configuration = &c
	return nil
}
