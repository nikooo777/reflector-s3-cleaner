package main

import (
	"fmt"
	"math"
	"os"

	"reflector-s3-cleaner/chainquery"
	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/reflector"
	"reflector-s3-cleaner/shared"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	sdHashesPath         string
	existingHashesPath   string
	unresolvedHashesPath string
	loadReflectorData    bool
	loadChainqueryData   bool
	saveReflectorData    bool
	saveChainqueryData   bool
	checkExpired         bool
	checkSpent           bool
	limit                int64
)

func main() {
	cmd := &cobra.Command{
		Use:   "reflector-s3-cleaner",
		Short: "cleanup reflector storage",
		Run:   cleaner,
		Args:  cobra.RangeArgs(0, 0),
	}
	cmd.Flags().StringVar(&sdHashesPath, "sd_hashes", "sd_hashes.json", "path of sd_hashes")
	cmd.Flags().StringVar(&existingHashesPath, "existing_hashes", "existing_sd_hashes.json", "path of sd_hashes that exist on chain")
	cmd.Flags().StringVar(&unresolvedHashesPath, "unresolved_hashes", "unresolved_sd_hashes.json", "path of sd_hashes that don't exist on chain")
	cmd.Flags().BoolVar(&loadReflectorData, "load-reflector-data", false, "load results from file instead of querying the reflector database unnecessarily")
	cmd.Flags().BoolVar(&loadChainqueryData, "load-chainquery-data", false, "load results from file instead of querying the chainquery database unnecessarily")
	cmd.Flags().BoolVar(&saveReflectorData, "save-reflector-data", false, "save results to file once loaded from the reflector database")
	cmd.Flags().BoolVar(&saveChainqueryData, "save-chainquery-data", false, "save results to file once loaded from the chainquery database")
	cmd.Flags().BoolVar(&checkExpired, "check-expired", true, "check for streams referenced by an expired claim")
	cmd.Flags().BoolVar(&checkSpent, "check-spent", true, "check for streams referenced by a spent claim")
	cmd.Flags().Int64Var(&limit, "limit", 50000000, "how many streams to check (approx)")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func cleaner(cmd *cobra.Command, args []string) {
	if loadReflectorData == saveReflectorData && saveReflectorData == true {
		panic("You can't use --load-reflector-data and --save-reflector-data at the same time")
	}
	if loadChainqueryData == saveChainqueryData && saveChainqueryData == true {
		panic("You can't use --load-chainquery-data and --save-chainquery-data at the same time")
	}
	err := configs.Init("./config.json")
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

	var streamData []shared.StreamData
	if loadReflectorData {
		streamData, err = reflector.LoadStreamData(sdHashesPath)
		if err != nil {
			panic(err)
		}
	} else {
		streamData, err = rf.GetStreams(limit)
		if err != nil {
			panic(err)
		}

		if saveReflectorData {
			err = reflector.SaveStreamData(streamData, sdHashesPath)
			if err != nil {
				panic(err)
			}
		}
	}

	var unresolvedHashes []string
	var existingHashes []string
	if loadChainqueryData {
		existingHashes, err = chainquery.LoadResolvedHashes(existingHashesPath)
		if err != nil {
			panic(err)
		}
		unresolvedHashes, err = chainquery.LoadResolvedHashes(unresolvedHashesPath)
		if err != nil {
			panic(err)
		}
	} else {
		streamExists, err := cq.BatchedClaimsExist(streamData, checkExpired, checkSpent)
		if err != nil {
			panic(err)
		}

		unresolvedHashes = make([]string, 0, len(streamData))
		existingHashes = make([]string, 0, len(streamData))
		for hash, exists := range streamExists {
			if !exists {
				unresolvedHashes = append(unresolvedHashes, hash)
			} else {
				existingHashes = append(existingHashes, hash)
			}
		}
		if saveChainqueryData {
			err = chainquery.SaveHashes(unresolvedHashes, unresolvedHashesPath)
			if err != nil {
				panic(err)
			}
			err = chainquery.SaveHashes(existingHashes, existingHashesPath)
			if err != nil {
				panic(err)
			}
		}
	}

	logrus.Printf("%d existing and %d not on the blockchain (%.3f%% missing)", len(existingHashes),
		len(unresolvedHashes), (float64(len(unresolvedHashes))/float64(len(existingHashes)))*100)
}

func getHashesByIds(ids []int64, rf *reflector.ReflectorApi) ([]string, error) {
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
			return nil, err
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
	return sdHashes, nil
}
