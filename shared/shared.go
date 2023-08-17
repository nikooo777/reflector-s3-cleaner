package shared

import (
	"database/sql"

	"github.com/sirupsen/logrus"
)

const MysqlMaxBatchSize = 10000

func CloseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		logrus.Errorln(err.Error())
	}
}

type BlobInfo struct {
	BlobID  int64
	Deleted bool
}
type StreamData struct {
	SdHash      string              `json:"sd_hash"`
	StreamID    int64               `json:"stream_id"`
	Exists      bool                `json:"exists"`
	Expired     bool                `json:"expired"`
	Spent       bool                `json:"spent"`
	Resolved    bool                `json:"resolved"`
	StreamBlobs map[string]BlobInfo `json:"stream_blobs"`
	ClaimID     *string             `json:"claim_id"`
}

func (stream *StreamData) IsValid() bool {
	if !stream.Exists || stream.Expired || stream.Spent {
		return false
	}
	return true
}
