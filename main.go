package main

import (
	"fmt"
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
	streamBlobsPath      string
	loadReflectorData    bool
	loadChainqueryData   bool
	saveReflectorData    bool
	saveChainqueryData   bool
	checkExpired         bool
	checkSpent           bool
	resolveBlobs         bool
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
	cmd.Flags().StringVar(&streamBlobsPath, "stream-blobs", "blobs_to_delete.json", "path of blobs set to delete")
	cmd.Flags().BoolVar(&loadReflectorData, "load-reflector-data", false, "load results from file instead of querying the reflector database unnecessarily")
	cmd.Flags().BoolVar(&loadChainqueryData, "load-chainquery-data", false, "load results from file instead of querying the chainquery database unnecessarily")
	cmd.Flags().BoolVar(&saveReflectorData, "save-reflector-data", false, "save results to file once loaded from the reflector database")
	cmd.Flags().BoolVar(&saveChainqueryData, "save-chainquery-data", false, "save results to file once loaded from the chainquery database")
	cmd.Flags().BoolVar(&checkExpired, "check-expired", true, "check for streams referenced by an expired claim")
	cmd.Flags().BoolVar(&checkSpent, "check-spent", true, "check for streams referenced by a spent claim")
	cmd.Flags().BoolVar(&resolveBlobs, "resolve-blobs", false, "resolve the blobs for the invalid streams")
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

	var invalidStreams []shared.StreamData
	var validStreams []shared.StreamData
	if loadChainqueryData {
		validStreams, err = chainquery.LoadResolvedHashes(existingHashesPath)
		if err != nil {
			panic(err)
		}
		invalidStreams, err = chainquery.LoadResolvedHashes(unresolvedHashesPath)
		if err != nil {
			panic(err)
		}
	} else {
		err := cq.BatchedClaimsExist(streamData, checkExpired, checkSpent)
		if err != nil {
			panic(err)
		}

		invalidStreams = make([]shared.StreamData, 0, len(streamData))
		validStreams = make([]shared.StreamData, 0, len(streamData))
		for _, stream := range streamData {
			if !stream.Exists || (checkExpired && stream.Expired) || (checkSpent && stream.Spent) {
				invalidStreams = append(invalidStreams, stream)
			} else {
				validStreams = append(validStreams, stream)
			}
		}
		if saveChainqueryData {
			err = chainquery.SaveHashes(invalidStreams, unresolvedHashesPath)
			if err != nil {
				panic(err)
			}
			err = chainquery.SaveHashes(validStreams, existingHashesPath)
			if err != nil {
				panic(err)
			}
		}
	}
	if resolveBlobs {
		blobsToDelete := make([]reflector.StreamBlobs, len(streamData))
		totalBlobs := 0
		for _, streamData := range invalidStreams {
			blobs, err := rf.GetBlobHashesForStream(streamData.StreamID)
			if err != nil {
				panic(err)
			}
			if blobs != nil {
				blobsToDelete = append(blobsToDelete, *blobs)
				totalBlobs += len(blobs.BlobIds)
			}
			logrus.Printf("found %d blobs for stream %s (%d total)", len(blobs.BlobHashes), streamData.SdHash, totalBlobs)
		}
		err = reflector.SaveBlobs(blobsToDelete, streamBlobsPath)
		if err != nil {
			panic(err)
		}
	}

	logrus.Printf("%d existing and %d not on the blockchain (%.3f%% missing)", len(validStreams),
		len(invalidStreams), (float64(len(invalidStreams))/float64(len(validStreams)+len(invalidStreams)))*100)
}
