package chainquery

import (
	"testing"

	"reflector-s3-cleaner/configs"

	"github.com/stretchr/testify/assert"
)

func TestCQApi_GetClaimFromSDHash(t *testing.T) {
	err := configs.Init()
	assert.NoError(t, err)

	cq, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, cq)

	c, err := cq.GetClaimFromSDHash("8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5")
	assert.NoError(t, err)
	assert.NotNil(t, c)
	c, err = cq.GetClaimFromSDHash("sdsds")
	assert.NoError(t, err)
	assert.Nil(t, c)
}

func TestCQApi_ClaimExists(t *testing.T) {
	err := configs.Init()
	assert.NoError(t, err)

	cq, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, cq)

	c, err := cq.ClaimExists("8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5")
	assert.NoError(t, err)
	assert.True(t, c)
	c, err = cq.ClaimExists("sdsds")
	assert.NoError(t, err)
	assert.False(t, c)
}
