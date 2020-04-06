package chainquery

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSaveAndLoadSDHashes(t *testing.T) {
	existingHashes := []string{"test1", "test2", "test3"}
	unresolvedHashes := []string{"test4", "test5", "test6"}
	err := SaveHashes(existingHashes, "existing_sd_hashes.json")
	assert.NoError(t, err)
	defer os.Remove("existing_sd_hashes.json")
	err = SaveHashes(unresolvedHashes, "unresolved_sd_hashes.json")
	assert.NoError(t, err)
	defer os.Remove("unresolved_sd_hashes.json")

	loadedExistingHashes, err := LoadResolvedHashes("existing_sd_hashes.json")
	assert.NoError(t, err)
	assert.NotNil(t, loadedExistingHashes)
	assert.Len(t, loadedExistingHashes, 3)
	assert.True(t, slicesMatch(existingHashes, loadedExistingHashes))

	loadedUnresolvedHashes, err := LoadResolvedHashes("unresolved_sd_hashes.json")
	assert.NoError(t, err)
	assert.NotNil(t, loadedUnresolvedHashes)
	assert.Len(t, loadedUnresolvedHashes, 3)
	assert.ElementsMatch(t, unresolvedHashes, loadedUnresolvedHashes)
}
