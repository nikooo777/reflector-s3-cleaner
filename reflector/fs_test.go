package reflector

import (
	"os"
	"testing"

	"reflector-s3-cleaner/shared"

	"github.com/stretchr/testify/assert"
)

func TestSaveAndLoadSDHashes(t *testing.T) {
	streamData := []shared.StreamData{
		{SdHash: "test1", StreamID: 1, SdBlobID: 1},
		{SdHash: "test2", StreamID: 2, SdBlobID: 2},
		{SdHash: "test3", StreamID: 3, SdBlobID: 3},
	}
	err := SaveStreamData(streamData, "test1.json")
	assert.NoError(t, err)
	defer os.Remove("test1.json")

	loadedStreamData, err := LoadStreamData("test1.json")
	assert.NoError(t, err)
	assert.NotNil(t, loadedStreamData)
	assert.Len(t, loadedStreamData, 3)
	assert.ElementsMatch(t, streamData, loadedStreamData)
}
