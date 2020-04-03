package main

import (
	"math"

	"reflector-s3-cleaner/chainquery"
	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/reflector"
	"reflector-s3-cleaner/shared"

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

	ids, err := rf.GetStreams(50000000)
	if err != nil {
		panic(err)
	}
	totIds := len(ids)
	batches := int(math.Ceil(float64(totIds) / float64(shared.MysqlMaxBatchSize)))
	hashes := make(map[int64]string, shared.MysqlMaxBatchSize*batches)
	totalHashes := 0
	for i := 0; i < batches; i++ {
		ceiling := len(ids)
		if (i+1)*shared.MysqlMaxBatchSize < ceiling {
			ceiling = (i + 1) * shared.MysqlMaxBatchSize
		}
		logrus.Printf("getting hashes for batch %d of %d", i+1, batches)
		h, err := rf.GetSDblobHashes(ids[i*shared.MysqlMaxBatchSize : ceiling])
		if err != nil {
			panic(err)
		}
		for k, v := range h {
			hashes[k] = v
			totalHashes++
		}
	}

	sdHashes := make([]string, 0, totalHashes)
	for _, h := range hashes {
		sdHashes = append(sdHashes, h)
	}
	streamExists, err := cq.BatchedClaimsExist(sdHashes)
	existing := 0
	inexisting := 0
	for _, exists := range streamExists {
		if !exists {
			inexisting++
			//	logrus.Printf("stream %s does not exist in the blockchain", hash)
		} else {
			existing++
		}
	}
	logrus.Printf("%d existing and %d not on the blockchain (%.1f missing)", existing, inexisting, (float64(inexisting)/float64(existing))*100)
}
