package shared

import (
	"database/sql"

	"github.com/sirupsen/logrus"
)

func CloseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		logrus.Errorln(err.Error())
	}
}
