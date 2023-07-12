package reflector

import (
	"database/sql"
	"fmt"
	"runtime"
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
			logrus.Infof("adding job for %d to %d", i, end)
			jobs <- offsets{start: i, end: end}
		}
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for job := range jobs {
				logrus.Infof("processing job for %d to %d", job.start, job.end)
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

// getStreams returns a slice of StreamData containing all necessary stream information and an offset for the subsequent call which should be passed in as offset
func (c *ReflectorApi) getStreamData(limit int64, offset int64) ([]shared.StreamData, int64, error) {
	upperLimit := offset + batchSize
	rows, err := c.dbConn.Query(`SELECT s.id , b.hash FROM stream s INNER JOIN blob_ b on s.sd_blob_id = b.id WHERE s.id > ? and s.id < ? order by s.id limit ?`, offset, upperLimit, limit)
	if err != nil {
		return nil, offset, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	streamData := make([]shared.StreamData, 0, batchSize)
	newOffset := offset
	for rows.Next() {
		var streamID int64
		var sdHash string
		err = rows.Scan(&streamID, &sdHash)
		if err != nil {
			return nil, 0, errors.Err(err)
		}
		streamData = append(streamData, shared.StreamData{
			SdHash:   sdHash,
			StreamID: streamID,
		})
		if streamID > newOffset {
			newOffset = streamID
		}
	}
	return streamData, newOffset, nil
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
func (c *ReflectorApi) getBlobHashesForStream(streamId int64) (map[string]int64, error) {
	rows, err := c.dbConn.Query(`SELECT b.id, b.hash FROM blob_ b inner join stream_blob sb on b.id = sb.blob_id where sb.stream_id = ?`, streamId)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	streamBlobs := make(map[string]int64)
	blobsFound := 0
	for rows.Next() {
		var id int64
		var hash string
		err = rows.Scan(&id, &hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		streamBlobs[hash] = id
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
					logrus.Printf("found %d blobs for stream %s (%d total)", len(blobs), stream.SdHash, atomic.LoadInt64(&blobsCount))
					streamsToBlobsMap.Store(stream.StreamID, blobs)
					atomic.AddInt64(&blobsCount, int64(len(blobs)))
				}
			}
		}()
	}
	for i, stream := range streams {
		if i%100 == 0 {
			logrus.Printf("queued %d/%d streams for blob hash retrieval", i, len(streams))
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
			streams[i].StreamBlobs = val.(map[string]int64)
		}
	}
	return blobsCount, nil
}
