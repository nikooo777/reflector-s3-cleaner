package reflector

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSaveAndLoadSDHashes(t *testing.T) {
	hashes := []string{"test1", "test2", "test3"}
	err := SaveSDHashes(hashes, "test1.json")
	assert.NoError(t, err)
	defer os.Remove("test1.json")

	loadedHashes, err := LoadSDHashes("test1.json")
	assert.NoError(t, err)
	assert.NotNil(t, loadedHashes)
	assert.Len(t, loadedHashes, 3)
outer:
	for _, h := range loadedHashes {
		for _, expected := range hashes {
			if h != expected {
				continue
			} else {
				continue outer
			}
		}
		t.Fatalf("hash %s not found in initial set", h)
	}
}
