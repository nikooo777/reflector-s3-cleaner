package purger

import (
	"reflector-s3-cleaner/shared"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lbryio/lbry.go/v2/extras/errors"
)

type Purger struct {
	session *session.Session
	client  *s3.S3
	bucket  string
}
type Credentials struct {
	AccessKey string
	Secret    string
	Region    string
	Bucket    string
}

func Init(awsCreds Credentials) (*Purger, error) {
	creds := credentials.NewStaticCredentials(awsCreds.AccessKey, awsCreds.Secret, "")
	sess, err := session.NewSession(&aws.Config{Region: aws.String(awsCreds.Region), Credentials: creds})
	if err != nil {
		return nil, errors.Err(err)
	}
	// Create S3 service client
	svc := s3.New(sess)

	return &Purger{
		session: sess,
		client:  svc,
		bucket:  awsCreds.Bucket,
	}, nil
}

type FailedDeletes struct {
	Hash string
	err  error
}
type DeleteResults struct {
	Failures  []FailedDeletes
	Successes []string
}

func (p *Purger) DeleteStream(streamBlobs shared.StreamBlobs) (*DeleteResults, error) {
	objectsToDelete := make([]*s3.ObjectIdentifier, 0, 1000)
	batches := 0
	for i, hash := range streamBlobs.BlobHashes {
		if i%999 == 0 {
			batches++
		}
		objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
			Key: aws.String(hash),
		})
	}

	failedDeletes := make([]FailedDeletes, 0, 100)
	confirmedDeletes := make([]string, 0, 100)
	for i := 0; i < batches; i++ {
		lowerIndex := i * 1000
		upperIndex := (i + 1) * 1000
		if len(objectsToDelete)-1 < upperIndex {
			upperIndex = len(objectsToDelete)
		}
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(p.bucket),
			Delete: &s3.Delete{
				Objects: objectsToDelete[lowerIndex:upperIndex],
				Quiet:   aws.Bool(false),
			},
		}
		res, err := p.client.DeleteObjects(input)
		if err != nil {
			return nil, errors.Err(err)
		}
		for _, err := range res.Errors {
			failedDeletes = append(failedDeletes, FailedDeletes{
				Hash: *err.Key,
				err:  errors.Prefix(*err.Code, errors.Err(err.Message)),
			})
		}
		for _, o := range res.Deleted {
			confirmedDeletes = append(confirmedDeletes, *o.Key)
		}
	}
	return &DeleteResults{
		Failures:  failedDeletes,
		Successes: confirmedDeletes,
	}, nil
}
