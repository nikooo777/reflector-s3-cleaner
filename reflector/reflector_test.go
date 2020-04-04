package reflector

import (
	"testing"

	"reflector-s3-cleaner/configs"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestReflectorApi_GetStreams(t *testing.T) {
	err := configs.Init("../config.json")
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

func TestReflectorApi_GetSDblobHashes(t *testing.T) {
	err := configs.Init("../config.json")
	assert.NoError(t, err)

	rf, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, rf)

	idsToRetrieve := []int64{15137682, 62982738, 92067960}
	hashes, err := rf.GetSDblobHashes(idsToRetrieve)
	assert.NoError(t, err)
	assert.NotNil(t, hashes)

	expectedHashes := map[int64]string{
		15137682: "24a477c99598f1e79464d946bb2e5501f6f40baecd18bc8c137129e242d70f5aaf2e51f5216bbd8b682cc2842cc80f0d",
		62982738: "98733467bf2e247d9c28f090bebfb68af6f3982a8169e689fa7e053902e6b1bdb2de040e29fd764d65221def3a80666c",
		92067960: "ded8bb45eb5b80a577e92150b2de4f57b6f392e16be2d1fa7b1463cc75729998d825a38ed8df0c79a7b4ac05c6fab8ad",
	}
	for _, id := range idsToRetrieve {
		hash, ok := hashes[id]
		assert.True(t, ok)
		resolvedHash, ok := expectedHashes[id]
		assert.True(t, ok)
		assert.Equal(t, hash, resolvedHash)
	}

	hashes, err = rf.GetSDblobHashes([]int64{-1, -2, -3, 92067960})
	assert.NoError(t, err)
	assert.Len(t, hashes, 1)
}
