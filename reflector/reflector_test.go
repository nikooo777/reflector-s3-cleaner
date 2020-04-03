package reflector

import (
	"testing"

	"reflector-s3-cleaner/configs"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestReflectorApi_GetStreams(t *testing.T) {
	err := configs.Init()
	assert.NoError(t, err)

	rf, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, rf)

	streams, err := rf.GetStreams(1000)
	assert.NoError(t, err)
	assert.NotNil(t, streams)

	for _, stream := range streams {
		logrus.Println(stream)
	}

}
