package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"

	"github.com/nikooo777/reflector-s3-cleaner/blockchain"
	"github.com/nikooo777/reflector-s3-cleaner/chainquery"
	"github.com/nikooo777/reflector-s3-cleaner/configs"
	"github.com/nikooo777/reflector-s3-cleaner/purger"
	"github.com/nikooo777/reflector-s3-cleaner/reflector"
	"github.com/nikooo777/reflector-s3-cleaner/shared"
	"github.com/nikooo777/reflector-s3-cleaner/sqlite_store"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	loadData       bool
	resolveData    bool
	saveData       bool
	checkExpired   bool
	checkSpent     bool
	resolveBlobs   bool
	loadBlobs      bool
	performWipe    bool
	limit          int64
	doubleCheck    bool
	debug          bool
	cleanReflector bool
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
	cmd.Flags().BoolVar(&loadBlobs, "load-blobs", false, "load the blobs for invalid streams from the database")
	cmd.Flags().BoolVar(&performWipe, "wipe", false, "actually wipes blobs + flags streams as invalid in the database")
	cmd.Flags().BoolVar(&doubleCheck, "double-check", false, "check against the blockchain to make sure the streams are actually invalid")
	cmd.Flags().BoolVarP(&debug, "debug", "d", false, "enable debug logging")
	cmd.Flags().BoolVar(&cleanReflector, "cleanse", false, "remove all pruned blobs, sd_blobs, streams from the reflector_storage database")
	cmd.Flags().Int64Var(&limit, "limit", 50000000, "how many streams to check (approx)")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func cleaner(cmd *cobra.Command, args []string) {
	logrus.SetLevel(logrus.InfoLevel)
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
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
	pruner, err := purger.Init(configs.Configuration.S3)
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

	blobsToDeleteCount := int64(0)
	if resolveBlobs {
		logrus.Debugln("resolving blobs")
		blobsToDeleteCount, err = rf.GetBlobHashesForStream(streamData)
		if err != nil {
			panic(err)
		}
		logrus.Infof("Found %d potential blobs to delete", blobsToDeleteCount)
		err = localStore.StoreBlobs(streamData)
		if err != nil {
			logrus.Errorf("Failed to store blobs: %s", err.Error())
		}
	} else if loadBlobs {
		blobsToDeleteCount, err = localStore.LoadBlobs(streamData)
		if err != nil {
			logrus.Errorf("Failed to load stored blobs: %s", err.Error())
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
					streamData[i].Exists = true
					streamData[i].Expired = false
					streamData[i].Spent = false
					streamData[i].Resolved = true
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
	if cleanReflector {
		for i, sd := range streamData {
			if i%5000 == 0 {
				logrus.Infof("pruned %d/%d streams from reflector_data", i, len(streamData))
			}
			if sd.Spent || !sd.Exists {
				err := rf.DeleteStreamBlobs(streamData[i])
				if err != nil {
					logrus.Error(err)
				}
			}
		}
		return
	}
	if performWipe {
		logrus.Debugln("performing wipe")

		// Create channels for successes and failures
		successes := make(chan string, 10000)
		failures := make(chan purger.Failure, 10000)

		// Create channel for StreamData and start a goroutine to send all StreamData onto the channel
		streamDataChan := make(chan shared.StreamData, 64)

		var wg sync.WaitGroup
		maxThreads := runtime.NumCPU() * 4
		wg.Add(maxThreads)

		// Create a channel to listen for the interrupt signal (Ctrl+C).
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(streamDataChan)
			for i, sd := range streamData {
				if i%5000 == 0 {
					logrus.Infof("Queued %d/%d streams for pruning", i, len(streamData))
				}
				select {
				case streamDataChan <- sd:
				case <-interrupt:
					return
				}
			}
		}()

		// Start the PurgeStreams function in separate goroutines
		for i := 0; i < maxThreads; i++ {
			go pruner.PurgeStreams(streamDataChan, successes, failures, &wg)
		}

		// Start another goroutine to process the results
		resultsWg := sync.WaitGroup{}
		resultsWg.Add(1)
		go func() {
			defer resultsWg.Done()
			for s := range successes {
				err = localStore.FlagBlob(s)
				if err != nil {
					logrus.Errorf("Failed to flag blob %s: %s", s, err.Error())
				}
			}
		}()
		resultsWg.Add(1)
		go func() {
			defer resultsWg.Done()
			for f := range failures {
				//json pretty print
				prettier, err := json.Marshal(f.Hashes)
				if err == nil {
					logrus.Errorf("Failed to delete blobs %s: %s", string(prettier), f.Err.Error())
				}
			}
		}()

		// Wait for the PurgeStreamsV2 function to finish
		wg.Wait()

		// After waiting, close the channels
		close(successes)
		close(failures)
		resultsWg.Wait()
	}

	logrus.Printf("%d existing and %d not on the blockchain. %d expired, %d spent for a total of %d invalid streams (%.2f%% of the total)", validStreams,
		notOnChain, expired, spent, notOnChain+expired+spent, float64(notOnChain+expired+spent)/float64(len(streamData))*100)
	logrus.Printf("%d blobs to delete for up to %.1f TB of space", blobsToDeleteCount, float64(blobsToDeleteCount)*2/1024/1024)
	if doubleCheck {
		logrus.Printf("%d false negatives corrected", falseNegatives)
	}
}
