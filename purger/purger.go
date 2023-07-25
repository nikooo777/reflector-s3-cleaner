package purger

import (
	"github.com/nikooo777/reflector-s3-cleaner/configs"
	"github.com/nikooo777/reflector-s3-cleaner/shared"

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

func Init(awsCreds configs.AWSS3Config) (*Purger, error) {
	creds := credentials.NewStaticCredentials(awsCreds.AccessKey, awsCreds.SecretKey, "")
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
	Err  error
}
type DeleteResults struct {
	Failures  []FailedDeletes
	Successes []string
}

func (p *Purger) PurgeStreams(streams []shared.StreamData) (*DeleteResults, error) {
	delInput := &s3.Delete{
		Objects: []*s3.ObjectIdentifier{},
		Quiet:   aws.Bool(true),
	}

	results := &DeleteResults{
		Failures:  []FailedDeletes{},
		Successes: []string{},
	}

	for _, stream := range streams {
		if stream.IsValid() {
			continue
		}
		if stream.Spent {
			for blobHash, _ := range stream.StreamBlobs {
				delInput.Objects = append(delInput.Objects, &s3.ObjectIdentifier{Key: aws.String(blobHash)})

				if len(delInput.Objects) == 1000 {
					deletedKeys, err := p.deleteObjects(delInput)
					if err != nil {
						results.Failures = append(results.Failures, FailedDeletes{Hash: stream.SdHash, Err: err})
						continue
					}
					results.Successes = append(results.Successes, deletedKeys...)
					// clear the delete list for the next batch
					delInput.Objects = delInput.Objects[:0]
				}
			}
		}
	}

	// delete remaining objects
	if len(delInput.Objects) > 0 {
		deletedKeys, err := p.deleteObjects(delInput)
		if err != nil {
			return nil, err
		}
		results.Successes = append(results.Successes, deletedKeys...)
	}

	return results, nil
}

func (p *Purger) deleteObjects(delInput *s3.Delete) ([]string, error) {
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(p.bucket),
		Delete: delInput,
	}

	resp, err := p.client.DeleteObjects(input)
	if err != nil {
		return nil, err
	}

	var deletedKeys []string
	for _, deleted := range resp.Deleted {
		deletedKeys = append(deletedKeys, *deleted.Key)
	}

	return deletedKeys, nil
}
