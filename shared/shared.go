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

type StreamData struct {
	SdHash   string `json:"sd_hash"`
	StreamID int64  `json:"stream_id"`
	Exists   bool   `json:"exists"`
	Expired  bool   `json:"expired"`
	Spent    bool   `json:"spent"`
	Resolved bool   `json:"resolved"`
}

func (stream *StreamData) IsValid() bool {
	if !stream.Exists || stream.Expired || stream.Spent {
		return false
	}
	return true
}

type StreamBlobs struct {
	BlobHashes []string `json:"blob_hashes"`
	BlobIds    []int64  `json:"blob_ids"`
}
