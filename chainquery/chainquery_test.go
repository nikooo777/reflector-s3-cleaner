package chainquery

import (
	"testing"

	"github.com/nikooo777/reflector-s3-cleaner/configs"
	"github.com/nikooo777/reflector-s3-cleaner/shared"

	"github.com/stretchr/testify/assert"
)

func TestCQApi_GetClaimFromSDHash(t *testing.T) {
	err := configs.Init("../config.json")
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
	err := configs.Init("../config.json")
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

func TestCQApi_BatchedClaimsExist(t *testing.T) {
	err := configs.Init("../config.json")
	assert.NoError(t, err)

	cq, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, cq)

	hashesToResolve := []shared.StreamData{
		{"8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5", 0, false, false, false, false},
		{"98733467bf2e247d9c28f090bebfb68af6f3982a8169e689fa7e053902e6b1bdb2de040e29fd764d65221def3a80666c", 0, false, false, false, false},
		{"398504b5e6c65019cba01edf03fc9c2ad02606a80e76035e04fb5ec08ced6b5d8245484970bb51a06a787747030f3b7b", 0, false, false, false, false},
		{"92b4287fb0fb3a331c2e46045ca70fc8cb0a572412e1f07439455a1c6cc149421dd3d1b9504e27ea0271545b393e755d", 0, false, false, false, false},
	}

	expectedResults := []shared.StreamData{
		{"8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5", 0, true, true, false, true},
		{"98733467bf2e247d9c28f090bebfb68af6f3982a8169e689fa7e053902e6b1bdb2de040e29fd764d65221def3a80666c", 0, true, false, false, true},
		{"398504b5e6c65019cba01edf03fc9c2ad02606a80e76035e04fb5ec08ced6b5d8245484970bb51a06a787747030f3b7b", 0, false, false, false, true},
		{"92b4287fb0fb3a331c2e46045ca70fc8cb0a572412e1f07439455a1c6cc149421dd3d1b9504e27ea0271545b393e755d", 0, true, false, true, true},
	}

	err = cq.BatchedClaimsExist(hashesToResolve, true, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, hashesToResolve, expectedResults)

	hashesToResolve = []shared.StreamData{
		{"8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5", 0, false, false, false, false},
		{"98733467bf2e247d9c28f090bebfb68af6f3982a8169e689fa7e053902e6b1bdb2de040e29fd764d65221def3a80666c", 0, false, false, false, false},
		{"398504b5e6c65019cba01edf03fc9c2ad02606a80e76035e04fb5ec08ced6b5d8245484970bb51a06a787747030f3b7b", 0, false, false, false, false},
		{"92b4287fb0fb3a331c2e46045ca70fc8cb0a572412e1f07439455a1c6cc149421dd3d1b9504e27ea0271545b393e755d", 0, false, false, false, false},
	}
	expectedResults = []shared.StreamData{
		{"8ca439c2ca48512b5188bde83c631475b4727763fcd11933d0f3d62defdd35808c9d502766a6c088d4dff6e48d0335b5", 0, true, false, false, true},
		{"98733467bf2e247d9c28f090bebfb68af6f3982a8169e689fa7e053902e6b1bdb2de040e29fd764d65221def3a80666c", 0, true, false, false, true},
		{"398504b5e6c65019cba01edf03fc9c2ad02606a80e76035e04fb5ec08ced6b5d8245484970bb51a06a787747030f3b7b", 0, false, false, false, true},
		{"92b4287fb0fb3a331c2e46045ca70fc8cb0a572412e1f07439455a1c6cc149421dd3d1b9504e27ea0271545b393e755d", 0, true, false, false, true},
	}
	err = cq.BatchedClaimsExist(hashesToResolve, false, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, hashesToResolve, expectedResults)
}
