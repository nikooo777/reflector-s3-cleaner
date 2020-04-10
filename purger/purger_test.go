package purger

import (
	"os"
	"testing"

	"reflector-s3-cleaner/shared"

	"github.com/stretchr/testify/assert"
)

func TestPurger_DeleteStream(t *testing.T) {
	p, err := Init(Credentials{
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		Secret:    os.Getenv("S3_SECRET"),
		Region:    os.Getenv("S3_REGION"),
		Bucket:    os.Getenv("S3_BUCKET"),
	})
	assert.NoError(t, err)
	streamToDelete := shared.StreamBlobs{
		BlobHashes: []string{"nikonikoniko", "nikonikoniko2", "nikonikoniko3"},
		BlobIds:    nil,
	}
	res, err := p.DeleteStream(streamToDelete)
	assert.NoError(t, err)

	assert.Len(t, res.Failures, 0)
	assert.Len(t, res.Successes, 3)
}
