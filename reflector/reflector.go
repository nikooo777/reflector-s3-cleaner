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

// GetStreams returns a slice of indexes referencing sdBlobs
// limit is an indicator for the function for when to stop looking for new IDs
// it's not guaranteed that the amount of returned IDs matches the limit
func (c *ReflectorApi) GetStreams(limit int64) ([]int64, error) {
	var curPos int64
	allIDs := make([]int64, 0, limit)
	stallCount := 0
	for {
		logrus.Printf("getting ids... cur pos: %d (%d fetched)", curPos, len(allIDs))
		ids, newOffset, err := c.getStreams(curPos)
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
		allIDs = append(allIDs, ids...)
		if int64(len(allIDs)) >= limit {
			break
		}
	}

	return allIDs, nil
}

// getStreams returns a slice of indexes referencing sdBlobs and an offset for the subsequent call which should be passed in as offset
func (c *ReflectorApi) getStreams(offset int64) ([]int64, int64, error) {
	rows, err := c.dbConn.Query(`SELECT id, sd_blob_id FROM stream WHERE id > ? and id < ? order by id desc`, offset, offset+batchSize)
	if err != nil {
		return nil, offset, errors.Err(err)
	}
	defer shared.CloseRows(rows)
	ids := make([]int64, 0, batchSize)
	newOffset := offset
	for rows.Next() {
		var id int64
		var blobID int64
		err = rows.Scan(&id, &blobID)
		if err != nil {
			return nil, 0, errors.Err(err)
		}
		ids = append(ids, blobID)
		if id > newOffset {
			newOffset = id
		}
	}
	return ids, newOffset, nil
}
