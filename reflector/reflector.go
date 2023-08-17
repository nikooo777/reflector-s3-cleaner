package reflector

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nikooo777/reflector-s3-cleaner/configs"
	"github.com/nikooo777/reflector-s3-cleaner/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/query"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type ReflectorApi struct {
	dbConn *sql.DB
}

var instance *ReflectorApi

func Init() (*ReflectorApi, error) {
	if instance != nil {
		return instance, nil
	}
	db, err := connect()
	if err != nil {
		return nil, err
	}
	instance = &ReflectorApi{
		dbConn: db,
	}
	return instance, nil
}

func connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", configs.Configuration.Reflector.User, configs.Configuration.Reflector.Password, configs.Configuration.Reflector.Host, configs.Configuration.Reflector.Database))
	return db, errors.Err(err)
}

func (c *ReflectorApi) GetSDblobHashes(blobsIDs []int64) (map[int64]string, error) {
	args := make([]interface{}, len(blobsIDs))
	for i, b := range blobsIDs {
		args[i] = b
	}
	rows, err := c.dbConn.Query(`SELECT id, hash FROM blob_ where id in(`+query.Qs(len(blobsIDs))+`)`, args...)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	idToHashMap := make(map[int64]string, len(blobsIDs))
	for rows.Next() {
		var id int64
		var hash string

		err = rows.Scan(&id, &hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		idToHashMap[id] = hash
	}
	return idToHashMap, nil
}

const batchSize = 10000

// GetStreams returns a slice of StreamData containing all necessary stream information
// limit is an indicator for the function for when to stop looking for new IDs
// it's not guaranteed that the amount of returned IDs matches the limit
func (c *ReflectorApi) GetStreams(limit int64) ([]shared.StreamData, error) {
	// get the most recent stream ID
	mostRecentStreamID, err := c.getMostRecentStreamID()
	if err != nil {
		return nil, err
	}
	logrus.Infof("most recent stream ID: %d", mostRecentStreamID)
	// this is an approximation of the limit. if the database has deleted entries, the actual limit will be lower as the ID of a stream is an auto incrementing value
	// when running across the whole dataset, a limit should be set very high to ensure that all streams are returned
	if mostRecentStreamID > limit {
		logrus.Warnf("most recent stream ID (%d) is higher than the limit (%d). Limiting to the latter", mostRecentStreamID, limit)
		mostRecentStreamID = limit
	}
	type offsets struct {
		start, end int64
	}
	allStreamData := make([]shared.StreamData, 0, limit)
	streamDataLock := sync.Mutex{}

	jobs := make(chan offsets, runtime.NumCPU())
	producerWg := sync.WaitGroup{}
	consumerWg := sync.WaitGroup{}

	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		for i := int64(0); i < mostRecentStreamID; i += batchSize {
			end := i + batchSize - 1
			if end > mostRecentStreamID {
				end = mostRecentStreamID
			}
			logrus.Debugf("adding job for %d to %d", i, end)
			jobs <- offsets{start: i, end: end}
		}
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for job := range jobs {
				logrus.Infof("getting stream data for ids between %d and %d", job.start, job.end)
				sd, err := c.getStreamDataV2(job.start, job.end)
				if err != nil {
					logrus.Fatal(err)
				}
				streamDataLock.Lock()
				allStreamData = append(allStreamData, sd...)
				streamDataLock.Unlock()
			}
		}()
	}

	producerWg.Wait()
	close(jobs)
	consumerWg.Wait()

	logrus.Infof("found %d streams out of the %d max expected", len(allStreamData), mostRecentStreamID)
	return allStreamData, nil
}

// getStreams returns a slice of StreamData containing all necessary stream information and an offset for the subsequent call which should be passed in as offset
func (c *ReflectorApi) getStreamDataV2(start int64, end int64) ([]shared.StreamData, error) {
	rows, err := c.dbConn.Query(`SELECT s.id, b.hash FROM stream s INNER JOIN blob_ b on s.sd_blob_id = b.id WHERE s.id > ? and s.id < ? order by s.id`, start, end)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	streamData := make([]shared.StreamData, 0, batchSize)
	for rows.Next() {
		var streamID int64
		var sdHash string
		err = rows.Scan(&streamID, &sdHash)
		if err != nil {
			return nil, errors.Err(err)
		}
		streamData = append(streamData, shared.StreamData{
			SdHash:   sdHash,
			StreamID: streamID,
		})
	}
	return streamData, nil
}

// getMostRecentStreamID returns the most recent stream ID
func (c *ReflectorApi) getMostRecentStreamID() (int64, error) {
	var streamID int64
	err := c.dbConn.QueryRow(`SELECT id FROM stream ORDER BY id DESC LIMIT 1`).Scan(&streamID)
	if err != nil {
		return 0, errors.Err(err)
	}
	return streamID, nil
}

// getBlobHashesForStream returns an object containing the blob hashes and ids for a given stream
func (c *ReflectorApi) getBlobHashesForStream(streamId int64) (map[string]shared.BlobInfo, error) {
	rows, err := c.dbConn.Query(`SELECT b.id, b.hash FROM blob_ b inner join stream_blob sb on b.id = sb.blob_id where sb.stream_id = ?`, streamId)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	streamBlobs := make(map[string]shared.BlobInfo)
	blobsFound := 0
	for rows.Next() {
		var id int64
		var hash string
		err = rows.Scan(&id, &hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		streamBlobs[hash] = shared.BlobInfo{
			BlobID:  id,
			Deleted: false,
		}
		blobsFound++
	}
	err = rows.Err()
	if err != nil {
		return nil, errors.Err(err)
	}
	if blobsFound == 0 {
		return nil, nil
	}
	return streamBlobs, nil
}

// DeleteStreamBlobs deletes blobs for a list of streams (granted that they're marked as deleted in memory)
// After deleting the blobs, stream_blob entries should have been deleted as well (on delete cascade)
// this allows for the deletion of the entry in `stream` which has to happen right before deleting the sd_blob
func (c *ReflectorApi) DeleteStreamBlobs(stream shared.StreamData) error {
	if stream.IsValid() {
		return errors.Err("stream is valid and should not be deleted!")
	}

	blobsToDelete := make([]interface{}, 0, len(stream.StreamBlobs))
	for sb, blobInfo := range stream.StreamBlobs {
		if !blobInfo.Deleted {
			logrus.Warnf("blob %s is not marked as deleted for stream %s (sd_hash), skipping!", sb, stream.SdHash)
			return nil
		}
		blobsToDelete = append(blobsToDelete, blobInfo.BlobID)
	}

	// delete blobs in a transaction
	tx, err := c.dbConn.Begin()
	if err != nil {
		return errors.Err(err)
	}

	// Construct and execute the DELETE query for blobs
	q := "DELETE FROM blob_ WHERE id IN (?" + strings.Repeat(",?", len(blobsToDelete)-1) + ")"
	_, err = tx.Exec(q, blobsToDelete...)
	if err != nil {
		_ = tx.Rollback() // Rollback transaction in case of error
		return errors.Err(err)
	}

	// Execute the DELETE query for associated stream_blob entries
	_, err = tx.Exec("DELETE FROM stream WHERE id = ?", stream.StreamID)
	if err != nil {
		_ = tx.Rollback() // Rollback transaction in case of error
		return errors.Err(err)
	}

	// Execute the DELETE query for sd_blob
	_, err = tx.Exec("DELETE FROM blob_ WHERE hash = ?", stream.SdHash)
	if err != nil {
		_ = tx.Rollback() // Rollback transaction in case of error
		return errors.Err(err)
	}

	err = tx.Commit() // Commit the transaction
	if err != nil {
		return errors.Err(err)
	}
	return nil
}

// GetBlobHashesForStream takes a slice of streams, feeds it into a channel, schedules workers to get the blob hashes for each stream, and returns a slice of StreamBlobs
func (c *ReflectorApi) GetBlobHashesForStream(streams []shared.StreamData) (int64, error) {
	streamsChan := make(chan shared.StreamData, runtime.NumCPU()*4)
	var streamBlobsWg sync.WaitGroup
	var streamsToBlobsMap = sync.Map{}
	blobsCount := int64(0)
	for i := 0; i < runtime.NumCPU()*4; i++ {
		streamBlobsWg.Add(1)
		go func() {
			defer streamBlobsWg.Done()
			for stream := range streamsChan {
				blobs, err := c.getBlobHashesForStream(stream.StreamID)
				if err != nil {
					logrus.Fatal(err)
				}
				if blobs != nil {
					logrus.Debugf("found %d blobs for stream %s (%d total)", len(blobs), stream.SdHash, atomic.LoadInt64(&blobsCount))
					streamsToBlobsMap.Store(stream.StreamID, blobs)
					atomic.AddInt64(&blobsCount, int64(len(blobs)))
				}
			}
		}()
	}
	for i, stream := range streams {
		if i%100 == 0 {
			logrus.Infof("queued %d/%d streams for blob hash retrieval", i, len(streams))
		}
		if stream.Resolved && !stream.IsValid() {
			streamsChan <- stream
		}
	}
	close(streamsChan)
	streamBlobsWg.Wait()

	for i, stream := range streams {
		val, found := streamsToBlobsMap.LoadAndDelete(stream.StreamID)
		if found {
			streams[i].StreamBlobs = val.(map[string]shared.BlobInfo)
		}
	}
	return blobsCount, nil
}
