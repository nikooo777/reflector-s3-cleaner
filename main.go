package main

import (
	"reflector-s3-cleaner/chainquery"
	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/reflector"

	"github.com/sirupsen/logrus"
)

func main() {
	err := configs.Init()
	if err != nil {
		panic(err)
	}

	cq, err := chainquery.Init()
	if err != nil {
		panic(err)
	}
	rf, err := reflector.Init()
	if err != nil {
		panic(err)
	}

	ids, err := rf.GetStreams(1000)
	if err != nil {
		panic(err)
	}
	hashes, err := rf.GetSDblobHashes(ids)
	if err != nil {
		panic(err)
	}
	for _, hash := range hashes {
		//logrus.Printf("%d - %s", id, hash)
		exists, err := cq.ClaimExists(hash)
		if err != nil {
			panic(err)
		}
		if !exists {
			logrus.Printf("stream %s does not exist in the blockchain", hash)
		}
	}
}
