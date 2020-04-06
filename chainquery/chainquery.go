package chainquery

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"reflector-s3-cleaner/configs"
	"reflector-s3-cleaner/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/query"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/nullbio/null.v6"
)

type CQApi struct {
	dbConn *sql.DB
}

var instance *CQApi

type Claim struct {
	Name            string      `boil:"name" json:"name" toml:"name" yaml:"name"`
	ClaimID         string      `boil:"claim_id" json:"claim_id" toml:"claim_id" yaml:"claim_id"`
	ClaimType       int8        `boil:"claim_type" json:"claim_type" toml:"claim_type" yaml:"claim_type"`
	PublisherID     null.String `boil:"publisher_id" json:"publisher_id,omitempty" toml:"publisher_id" yaml:"publisher_id,omitempty"`
	SDHash          null.String `boil:"sd_hash" json:"sd_hash,omitempty" toml:"sd_hash" yaml:"sd_hash,omitempty"`
	TransactionTime null.Uint64 `boil:"transaction_time" json:"transaction_time,omitempty" toml:"transaction_time" yaml:"transaction_time,omitempty"`
	ValueAsJSON     null.String `boil:"value_as_json" json:"value_as_json,omitempty" toml:"value_as_json" yaml:"value_as_json,omitempty"`
	ValidAtHeight   uint        `boil:"valid_at_height" json:"valid_at_height" toml:"valid_at_height" yaml:"valid_at_height"`
	Height          uint        `boil:"height" json:"height" toml:"height" yaml:"height"`
	EffectiveAmount uint64      `boil:"effective_amount" json:"effective_amount" toml:"effective_amount" yaml:"effective_amount"`
	ContentType     null.String `boil:"content_type" json:"content_type,omitempty" toml:"content_type" yaml:"content_type,omitempty"`
	ThumbnailURL    null.String `boil:"thumbnail_url" json:"thumbnail_url,omitempty" toml:"thumbnail_url" yaml:"thumbnail_url,omitempty"`
	Title           null.String `boil:"title" json:"title,omitempty" toml:"title" yaml:"title,omitempty"`
	BidState        string      `boil:"bid_state" json:"bid_state" toml:"bid_state" yaml:"bid_state"`
	CreatedAt       time.Time   `boil:"created_at" json:"created_at" toml:"created_at" yaml:"created_at"`
	ModifiedAt      time.Time   `boil:"modified_at" json:"modified_at" toml:"modified_at" yaml:"modified_at"`
	ClaimAddress    string      `boil:"claim_address" json:"claim_address" toml:"claim_address" yaml:"claim_address"`
	IsCertValid     bool        `boil:"is_cert_valid" json:"is_cert_valid" toml:"is_cert_valid" yaml:"is_cert_valid"`
	Type            null.String `boil:"type" json:"type,omitempty" toml:"type" yaml:"type,omitempty"`
	ReleaseTime     null.Uint64 `boil:"release_time" json:"release_time,omitempty" toml:"release_time" yaml:"release_time,omitempty"`
}

func Init() (*CQApi, error) {
	if instance != nil {
		return instance, nil
	}
	db, err := connect()
	if err != nil {
		return nil, err
	}
	instance = &CQApi{
		dbConn: db,
	}
	return instance, nil
}

func connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", configs.Configuration.Chainquery.User, configs.Configuration.Chainquery.Password, configs.Configuration.Chainquery.Host, configs.Configuration.Chainquery.Database))
	return db, errors.Err(err)
}

func (c *CQApi) GetClaimFromSDHash(sdHash string) (*Claim, error) {
	rows, err := c.dbConn.Query(`SELECT name, claim_id, claim_type, publisher_id, sd_hash, transaction_time, value_as_json, valid_at_height, height, effective_amount, content_type, thumbnail_url, title, bid_state, created_at, modified_at, claim_address, is_cert_valid, type, release_time 
FROM claim 
where sd_hash = ?`, sdHash)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer shared.CloseRows(rows)

	claims := make([]Claim, 0, 1)
	for rows.Next() {
		var c Claim
		err = rows.Scan(
			&c.Name,
			&c.ClaimID,
			&c.ClaimType,
			&c.PublisherID,
			&c.SDHash,
			&c.TransactionTime,
			&c.ValueAsJSON,
			&c.ValidAtHeight,
			&c.Height,
			&c.EffectiveAmount,
			&c.ContentType,
			&c.ThumbnailURL,
			&c.Title,
			&c.BidState,
			&c.CreatedAt,
			&c.ModifiedAt,
			&c.ClaimAddress,
			&c.IsCertValid,
			&c.Type,
			&c.ReleaseTime,
		)
		if err != nil {
			return nil, errors.Err(err)
		}
		claims = append(claims, c)
	}
	if len(claims) == 0 {
		return nil, nil
	}
	if len(claims) > 1 {
		return nil, errors.Err("more claims (%d) found for this sd_hash %s", len(claims), sdHash)
	}
	return &claims[0], nil
}

func (c *CQApi) ClaimExists(sdHash string) (bool, error) {
	rows, err := c.dbConn.Query(`SELECT count(id) FROM claim where sd_hash = ?`, sdHash)
	if err != nil {
		return false, errors.Err(err)
	}
	defer shared.CloseRows(rows)

	for rows.Next() {
		var c int
		err = rows.Scan(&c)
		if err != nil {
			return false, errors.Err(err)
		}
		return c > 0, nil
	}
	return false, nil
}

func (c *CQApi) BatchedClaimsExist(streamData []shared.StreamData, checkExpired bool, checkSpent bool) (map[string]bool, error) {
	args := make([]interface{}, len(streamData))
	for i, sd := range streamData {
		args[i] = sd.SdHash
	}
	batches := int(math.Ceil(float64(len(args)) / float64(shared.MysqlMaxBatchSize)))
	existingHashes := make(map[string]bool, len(args))
	for i := 0; i < batches; i++ {
		ceiling := len(args)
		if (i+1)*shared.MysqlMaxBatchSize < ceiling {
			ceiling = (i + 1) * shared.MysqlMaxBatchSize
		}
		logrus.Printf("checking for existing hashes. Batch %d of %d", i+1, batches)
		err := c.batchedClaimsExist(args[i*shared.MysqlMaxBatchSize:ceiling], existingHashes, checkExpired, checkSpent)
		if err != nil {
			return nil, errors.Err(err)
		}
	}
	for _, sd := range streamData {
		_, ok := existingHashes[sd.SdHash]
		if !ok {
			existingHashes[sd.SdHash] = false
		}
	}
	return existingHashes, nil
}

func (c *CQApi) batchedClaimsExist(sdHashes []interface{}, existingHashes map[string]bool, checkExpired bool, checkSpent bool) error {
	rows, err := c.dbConn.Query(`SELECT sd_hash, bid_state FROM claim where sd_hash in (`+query.Qs(len(sdHashes))+`)`, sdHashes...)
	if err != nil {
		return errors.Err(err)
	}
	defer shared.CloseRows(rows)
	for rows.Next() {
		var h string
		var bidState string
		err = rows.Scan(&h, &bidState)
		if err != nil {
			return errors.Err(err)
		}
		existingHashes[h] = true
		if checkExpired && bidState == "Expired" {
			existingHashes[h] = false
		}
		if checkSpent && bidState == "Spent" {
			existingHashes[h] = false
		}
	}
	return nil
}
