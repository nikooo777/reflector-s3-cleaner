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
