package purger

import (
	"sync"

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
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(awsCreds.Region),
		Credentials:      creds,
		Endpoint:         aws.String(awsCreds.Endpoint),
		S3ForcePathStyle: aws.Bool(true),
	})
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

type Failure struct {
	Hashes []string
	Err    error
}
type DeleteResults struct {
	Failures  []Failure
	Successes []string
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

func (p *Purger) PurgeStreams(streams <-chan shared.StreamData, successes chan<- string, failures chan<- Failure, wg *sync.WaitGroup) {
	defer wg.Done()
	delInput := &s3.Delete{
		Objects: []*s3.ObjectIdentifier{},
	}

	for sd := range streams {
		if sd.IsValid() {
			continue
		}
		if sd.Spent || !sd.Exists {
			for blobHash, _ := range sd.StreamBlobs {
				delInput.Objects = append(delInput.Objects, &s3.ObjectIdentifier{Key: aws.String(blobHash)})

				if len(delInput.Objects) == 1000 {
					p.tryDeleteObjects(delInput, successes, failures)
				}
			}
		}
	}

	// delete remaining objects
	if len(delInput.Objects) > 0 {
		p.tryDeleteObjects(delInput, successes, failures)
	}
}

func (p *Purger) tryDeleteObjects(delInput *s3.Delete, successes chan<- string, failures chan<- Failure) {
	deletedKeys, err := p.deleteObjects(delInput)
	if err != nil {
		var failure Failure
		for _, key := range delInput.Objects {
			failure.Hashes = append(failure.Hashes, *key.Key)
		}
		failure.Err = err
		failures <- failure
	}
	for _, key := range deletedKeys {
		successes <- key
	}

	// Clear the delete list for the next batch
	delInput.Objects = delInput.Objects[:0]
}
