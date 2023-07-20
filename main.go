package main

import (
	"fmt"
	"os"

	"github.com/nikooo777/reflector-s3-cleaner/blockchain"
	"github.com/nikooo777/reflector-s3-cleaner/chainquery"
	"github.com/nikooo777/reflector-s3-cleaner/configs"
	"github.com/nikooo777/reflector-s3-cleaner/reflector"
	"github.com/nikooo777/reflector-s3-cleaner/shared"
	"github.com/nikooo777/reflector-s3-cleaner/sqlite_store"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	loadData     bool
	resolveData  bool
	saveData     bool
	checkExpired bool
	checkSpent   bool
	resolveBlobs bool
	limit        int64
	doubleCheck  bool
)

func main() {
	cmd := &cobra.Command{
		Use:   "reflector-s3-cleaner",
		Short: "cleanup reflector storage",
		Run:   cleaner,
		Args:  cobra.RangeArgs(0, 0),
	}
	cmd.Flags().BoolVar(&loadData, "load-data", false, "load the data from SQLite instead of querying the databases unnecessarily")
	cmd.Flags().BoolVar(&resolveData, "resolve-data", false, "resolves the data against the chainquery database")
	cmd.Flags().BoolVar(&saveData, "save-data", false, "save results to an SQLite database")
	cmd.Flags().BoolVar(&checkExpired, "check-expired", true, "check for streams referenced by an expired claim")
	cmd.Flags().BoolVar(&checkSpent, "check-spent", true, "check for streams referenced by a spent claim")
	cmd.Flags().BoolVar(&resolveBlobs, "resolve-blobs", false, "resolve the blobs for the invalid streams")
	cmd.Flags().BoolVar(&doubleCheck, "double-check", false, "check against the blockchain to make sure the streams are actually invalid")
	cmd.Flags().Int64Var(&limit, "limit", 50000000, "how many streams to check (approx)")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func cleaner(cmd *cobra.Command, args []string) {
	localStore, err := sqlite_store.Init()
	if err != nil {
		logrus.Fatal(err)
	}

	err = configs.Init("./config.json")
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
	if loadData {
		streamData, err = localStore.LoadStreamData()
		if err != nil {
			panic(err)
		}
	} else {
		streamData, err = rf.GetStreams(limit)
		if err != nil {
			panic(err)
		}
	}

	if resolveData {
		err := cq.BatchedClaimsExist(streamData, checkExpired, checkSpent)
		if err != nil {
			panic(err)
		}
	}
	if saveData {
		err = localStore.StoreStreams(streamData)
		if err != nil {
			panic(err)
		}
	}
	if resolveBlobs {
		blobsToDeleteCount, err := rf.GetBlobHashesForStream(streamData)
		if err != nil {
			panic(err)
		}
		logrus.Infof("Found %d potential blobs to delete", blobsToDeleteCount)
		err = localStore.StoreBlobs(streamData)
		if err != nil {
			logrus.Errorf("Failed to store blobs: %s", err.Error())
		}
	}
	var validStreams, notOnChain, expired, spent, falseNegatives int64
	claimsThatExist := make([]string, 0)
	for i, sd := range streamData {
		if i%500000 == 0 {
			logrus.Infof("Processed %d/%d streams", i, len(streamData))
		}
		if !sd.Exists {
			notOnChain++
			continue
		}
		if sd.Expired {
			expired++
			continue
		}
		if sd.Spent {
			if doubleCheck && sd.ClaimID != nil {
				exists, err := blockchain.ClaimExists(*sd.ClaimID)
				if err != nil {
					logrus.Warnf("error checking claim: %s", err.Error())
				}
				if exists {
					falseNegatives++
					claimsThatExist = append(claimsThatExist, *sd.ClaimID)
					logrus.Errorf("claim actually exists: %s", *sd.ClaimID)
					err = localStore.UnflagStream(&sd)
					if err != nil {
						logrus.Errorf("error unflagging stream: %s", err.Error())
					}
				}
			}
			spent++
			continue
		}
		validStreams++
	}

	logrus.Printf("%d existing and %d not on the blockchain. %d expired, %d spent for a total of %d invalid streams (%.2f%% of the total)", validStreams,
		notOnChain, expired, spent, notOnChain+expired+spent, float64(notOnChain+expired+spent)/float64(len(streamData))*100)
	logrus.Printf("%d false negatives", falseNegatives corrected)
}
