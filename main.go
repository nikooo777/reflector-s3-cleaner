package main

import (
	"math"

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

	ids, err := rf.GetStreams(50000000)
	if err != nil {
		panic(err)
	}
	totIds := len(ids)
	batches := int(math.Ceil(float64(totIds) / 50000.0))
	hashes := make(map[int64]string, 50000*batches)
	for i := 0; i < batches; i++ {
		ceiling := len(ids)
		if (i+1)*50000 < ceiling {
			ceiling = (i + 1) * 50000
		}
		logrus.Printf("getting hashes for batch %d of %d", i+1, batches)
		h, err := rf.GetSDblobHashes(ids[i*50000 : ceiling])
		if err != nil {
			panic(err)
		}
		for k, v := range h {
			hashes[k] = v
		}
	}

	existing := 0
	inexisting := 0
	for _, hash := range hashes {
		//logrus.Printf("%d - %s", id, hash)
		exists, err := cq.ClaimExists(hash)
		if err != nil {
			panic(err)
		}
		if !exists {
			inexisting++
			//	logrus.Printf("stream %s does not exist in the blockchain", hash)
		} else {
			existing++
		}
	}
	logrus.Printf("%d existing and %d not on the blockchain (%.1f missing)", existing, inexisting, float64(inexisting)/float64(existing))
}
