package reflector

import (
	"database/sql"
	"fmt"

	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/shared"

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
	var curPos int64
	allStreamData := make([]shared.StreamData, 0, limit)
	stallCount := 0
	for {
		logrus.Printf("getting ids... cur pos: %d (%d fetched)", curPos, len(allStreamData))
		streamData, newOffset, err := c.getStreamData(limit, curPos)
		if err != nil {
			return nil, errors.Err(err)
		}
		if newOffset == curPos {
			curPos += batchSize
			stallCount++
			if stallCount == 100 {
				break
			}
		} else {
			stallCount = 0
			curPos = newOffset
		}
		allStreamData = append(allStreamData, streamData...)
		if int64(len(allStreamData)) >= limit {
			break
		}
	}

	return allStreamData, nil
}

// getStreams returns a slice of StreamData containing all necessary stream information and an offset for the subsequent call which should be passed in as offset
func (c *ReflectorApi) getStreamData(limit int64, offset int64) ([]shared.StreamData, int64, error) {
	upperLimit := offset + batchSize
	rows, err := c.dbConn.Query(`SELECT s.id, s.sd_blob_id , b.hash FROM stream s INNER JOIN blob_ b on s.sd_blob_id = b.id WHERE s.id > ? and s.id < ? order by s.id asc limit ?`, offset, upperLimit, limit)
	if err != nil {
		return nil, offset, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	streamData := make([]shared.StreamData, 0, batchSize)
	newOffset := offset
	for rows.Next() {
		var streamID int64
		var blobID int64
		var sdHash string
		err = rows.Scan(&streamID, &blobID, &sdHash)
		if err != nil {
			return nil, 0, errors.Err(err)
		}
		streamData = append(streamData, shared.StreamData{
			SdHash:   sdHash,
			StreamID: streamID,
			SdBlobID: blobID,
		})
		if streamID > newOffset {
			newOffset = streamID
		}
	}
	return streamData, newOffset, nil
}

// getStreams returns a slice of indexes referencing sdBlobs and an offset for the subsequent call which should be passed in as offset
func (c *ReflectorApi) GetBlobHashesForStream(sdBlobID int64) (*shared.StreamBlobs, error) {
	rows, err := c.dbConn.Query(`SELECT b.id, b.hash FROM blob_ b inner join stream_blob sb on b.id = sb.blob_id where sb.stream_id = ?`, sdBlobID)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	blobHashes := make([]string, 0, 50)
	blobIds := make([]int64, 0, 50)
	for rows.Next() {
		var id int64
		var hash string
		err = rows.Scan(&id, &hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		blobHashes = append(blobHashes, hash)
		blobIds = append(blobIds, id)
	}
	return &shared.StreamBlobs{
		BlobHashes: blobHashes,
		BlobIds:    blobIds,
	}, nil
}
