package sqlite_store

import (
	"database/sql"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nikooo777/reflector-s3-cleaner/shared"
)

type Store struct {
	db *sql.DB
}

func Init() (*Store, error) {
	db, err := sql.Open("sqlite3", "./cleaner.sqlite")
	if err != nil {
		return nil, errors.Err(err)
	}
	//stream_id is the primary key of the stream table in reflector, use this to quickly identify streams
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS streams (
    sd_hash char(96) NOT NULL UNIQUE ,
    stream_id bigint(20) NOT NULL PRIMARY KEY,
    exists_in_blockchain tinyint(1) NOT NULL,
    expired tinyint(1) NOT NULL,
    spent tinyint(1) NOT NULL,
    resolved tinyint(1) NOT NULL DEFAULT 0,
    claim_id char(40) DEFAULT NULL
    )`)
	if err != nil {
		return nil, errors.Err(err)
	}
	// create blobs table that references the stream table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS blobs (
    blob_hash char(96) NOT NULL PRIMARY KEY,
    stream_id bigint(20) NOT NULL,
    blob_id bigint(20) NOT NULL,
    deleted tinyint(1) NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id)
	)`)
	if err != nil {
		return nil, errors.Err(err)
	}
	newStore := &Store{
		db: db,
	}
	return newStore, nil
}

func (s *Store) StoreStreams(streamData []shared.StreamData) error {
	// begin a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// prepare the statement
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO streams (sd_hash, stream_id, exists_in_blockchain, expired, spent, resolved, claim_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// insert records
	for _, sd := range streamData {
		//we don't need to know the claim_id of streams that will not be later deleted. this saves some space.
		if sd.IsValid() {
			sd.ClaimID = nil
		}
		_, err = stmt.Exec(sd.SdHash, sd.StreamID, sd.Exists, sd.Expired, sd.Spent, sd.Resolved, sd.ClaimID)
		if err != nil {
			return err
		}
	}

	// commit the transaction
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// UnflagStream sets the stream to spent=0, expired=0, exists_in_blockchain=1, resolved=1 and removes any blobs in the blobs table related to the stream_id of the stream.
func (s *Store) UnflagStream(streamData *shared.StreamData) error {
	// Begin a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// Prepare statement to update streams table
	updateStmt, err := tx.Prepare("UPDATE streams SET spent = 0, expired = 0, exists_in_blockchain = 1, resolved = 1 WHERE stream_id = ?")
	if err != nil {
		return err
	}

	// Execute update statement
	_, err = updateStmt.Exec(streamData.StreamID)
	if err != nil {
		return err
	}

	// Prepare statement to delete from blobs table
	deleteStmt, err := tx.Prepare("DELETE FROM blobs WHERE stream_id = ?")
	if err != nil {
		return err
	}

	// Execute delete statement
	_, err = deleteStmt.Exec(streamData.StreamID)
	if err != nil {
		return err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) LoadStreamData() ([]shared.StreamData, error) {
	// Query the database
	rows, err := s.db.Query("SELECT sd_hash, stream_id, exists_in_blockchain, expired, spent, resolved, claim_id FROM streams")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var streamData []shared.StreamData

	// Loop over rows
	for rows.Next() {
		var sd shared.StreamData
		// Scan the retrieved row into the StreamData struct
		if err := rows.Scan(&sd.SdHash, &sd.StreamID, &sd.Exists, &sd.Expired, &sd.Spent, &sd.Resolved, &sd.ClaimID); err != nil {
			return nil, err
		}
		streamData = append(streamData, sd)
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return streamData, nil
}

func (s *Store) StoreBlobs(streamData []shared.StreamData) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO blobs (stream_id, blob_hash, deleted, blob_id) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, sd := range streamData {
		if sd.StreamBlobs == nil {
			continue
		}
		for blobHash, blobId := range sd.StreamBlobs {
			_, err = stmt.Exec(sd.StreamID, blobHash, false, blobId)
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
