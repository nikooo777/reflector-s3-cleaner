package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflector-s3-cleaner/purger"
	"sync"
	"time"

	"reflector-s3-cleaner/chainquery"
	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/reflector"
	"reflector-s3-cleaner/shared"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lbryio/lbry.go/v2/extras/errors"
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

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func grin() {
	logrus.Infof("NEW RUN %s", time.Now().Format(time.UnixDate))
	file, err := os.Open("data/fixed.json")
	checkErr(err)
	defer file.Close()

	wg := sync.WaitGroup{}
	hashChan := make(chan string)
	confChan := make(chan string)
	failChan := make(chan purger.FailedDeletes)

	rf, err := reflector.Init()
	checkErr(err)

	awsCreds := purger.Credentials{
		AccessKey: configs.Configuration.AWS.Key,
		Secret:    configs.Configuration.AWS.Secret,
		Region:    configs.Configuration.AWS.Region,
		Bucket:    configs.Configuration.AWS.Bucket,
	}

	wg.Add(1)
	go func() {
		s3Deleter(awsCreds, hashChan, confChan, failChan)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		dbDeleter(rf, confChan)
		wg.Done()
	}()

	lineReader(file, hashChan)
	wg.Wait()

	logrus.Info("done")
}

func lineReader(file *os.File, hashChan chan string) {
	offset := 10000
	count := 0
	blobs := 0

	lastDone := "d28992fa008ac18ffb11badeb55274466d1c30a8f416a7f51ec056f8655325b027e26e3f3a9124a8f9928df38f232c0d"
	lastDonePassed := false

	rd := bufio.NewReader(file)
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErr(err)
		}

		var datum shared.StreamBlobs
		err = json.Unmarshal(line, &datum)
		checkErr(err)

		count += 1
		blobs += len(datum.BlobHashes)

		if count%10000 == 0 {
			logrus.Infof("Read %d lines from input file\n", count)
		}

		if count < offset {
			continue
		}

		for _, h := range datum.BlobHashes {
			if lastDonePassed {
				hashChan <- h
			} else if h == lastDone {
				lastDonePassed = true
			}
		}
	}

	close(hashChan)
	logrus.Infof("Total: %d stream, %d blobs\n", count, blobs)
}

func s3Deleter(creds purger.Credentials, hashChan chan string, confChan chan string, failChan chan purger.FailedDeletes) {
	batchSize := 1000
	hashes := make([]string, batchSize)
	i := 0
	done := false

	for !done {
		hash, ok := <-hashChan
		if !ok {
			done = true
		} else {
			hashes[i] = hash
			i++
		}

		if done || i == batchSize {
			hashes = hashes[:i]
			i = 0
			logrus.Infof("Deleting batch of %d from S3", len(hashes))
			confirmed, failed, err := deleteS3Batch(creds, hashes)
			if err != nil {
				logrus.Error(err)
			}
			logrus.Infof("%d confirmed and %d failed s3 deletions", len(confirmed), len(failed))
			for _, c := range confirmed {
				logrus.Infof("%s deleted from s3", c)
				confChan <- c
			}
			for _, f := range failed {
				logrus.Infof("%s failed to delete from s3", f)
				//failChan <- f
			}
		}
	}

	close(confChan)
	close(failChan)
}

func dbDeleter(rf *reflector.ReflectorApi, confChan chan string) {
	batchSize := 200
	hashes := make([]string, batchSize)
	i := 0
	done := false

	for !done {
		hash, ok := <-confChan
		if !ok {
			done = true
		} else {
			hashes[i] = hash
			i++
		}

		if done || i == batchSize {
			hashes = hashes[:i]
			i = 0
			logrus.Infof("Deleting batch of %d from database", len(hashes))
			rowsAffected, err := rf.MarkBlobsDeleted(hashes)
			if err != nil {
				logrus.Error(err)
			}
			logrus.Infof("%d marked as deleted in db", rowsAffected)
		}
	}
}

func deleteS3Batch(creds purger.Credentials, hashes []string) ([]string, []purger.FailedDeletes, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(creds.AccessKey, creds.Secret, ""),
		Region:      aws.String(creds.Region),
	})
	if err != nil {
		return nil, nil, errors.Err(err)
	}
	svc := s3.New(sess)

	objectsToDelete := make([]*s3.ObjectIdentifier, len(hashes))
	for i, hash := range hashes {
		objectsToDelete[i] = &s3.ObjectIdentifier{Key: aws.String(hash)}
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(creds.Bucket),
		Delete: &s3.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(false),
		},
	}

	res, err := svc.DeleteObjects(input)
	if err != nil {
		return nil, nil, errors.Err(err)
	}

	failed := make([]purger.FailedDeletes, len(res.Errors))
	for i, err := range res.Errors {
		failed[i] = purger.FailedDeletes{
			Hash: *err.Key,
			Err:  errors.Prefix(*err.Code, err.Message),
		}
	}

	confirmed := make([]string, len(hashes)-len(res.Errors))
	for i, o := range res.Deleted {
		confirmed[i] = *o.Key
	}

	return confirmed, failed, nil
}

func cleaner(cmd *cobra.Command, args []string) {
	var err error

	err = configs.Init("./config.json")
	checkErr(err)
	grin()
	return

	if loadReflectorData && saveReflectorData {
		panic("You can't use --load-reflector-data and --save-reflector-data at the same time")
	}
	if loadChainqueryData && saveChainqueryData {
		panic("You can't use --load-chainquery-data and --save-chainquery-data at the same time")
	}
	err = configs.Init("./config.json")
	checkErr(err)

	cq, err := chainquery.Init()
	checkErr(err)
	rf, err := reflector.Init()
	checkErr(err)

	var streamData []shared.StreamData
	if loadReflectorData {
		streamData, err = reflector.LoadStreamData(sdHashesPath)
		checkErr(err)
	} else {
		streamData, err = rf.GetStreams(limit)
		checkErr(err)

		if saveReflectorData {
			err = reflector.SaveStreamData(streamData, sdHashesPath)
			checkErr(err)
		}
	}

	var invalidStreams []shared.StreamData
	var validStreams []shared.StreamData
	if loadChainqueryData {
		validStreams, err = chainquery.LoadResolvedHashes(existingHashesPath)
		checkErr(err)
		invalidStreams, err = chainquery.LoadResolvedHashes(unresolvedHashesPath)
		checkErr(err)
	} else {
		err := cq.BatchedClaimsExist(streamData, checkExpired, checkSpent)
		checkErr(err)

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
			checkErr(err)
			err = chainquery.SaveHashes(validStreams, existingHashesPath)
			checkErr(err)
		}
	}

	if resolveBlobs {
		blobsToDelete := make([]shared.StreamBlobs, len(streamData))
		totalBlobs := 0
		for _, streamData := range invalidStreams {
			blobs, err := rf.GetBlobHashesForStream(streamData.StreamID)
			checkErr(err)
			if blobs.BlobHashes != nil {
				blobsToDelete = append(blobsToDelete, *blobs)
				totalBlobs += len(blobs.BlobIds)
			}
			logrus.Printf("found %d blobs for stream %s (%d total)", len(blobs.BlobHashes), streamData.SdHash, totalBlobs)
		}
		err = reflector.SaveBlobs(blobsToDelete, streamBlobsPath)
		checkErr(err)
	}

	logrus.Printf("%d existing and %d not on the blockchain (%.3f%% missing)", len(validStreams),
		len(invalidStreams), (float64(len(invalidStreams))/float64(len(validStreams)+len(invalidStreams)))*100)
}
