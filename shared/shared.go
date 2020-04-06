package shared

import (
	"database/sql"

	"github.com/sirupsen/logrus"
)

const MysqlMaxBatchSize = 50000

func CloseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		logrus.Errorln(err.Error())
	}
}

type StreamData struct {
	SdHash   string `json:"sd_hash"`
	StreamID int64  `json:"stream_id"`
	SdBlobID int64  `json:"sd_blob_id"`
}
