package reflector

import (
	"reflect"
	"testing"

	"reflector-s3-cleaner/configs"

	"github.com/stretchr/testify/assert"
)

func TestReflectorApi_GetStreams(t *testing.T) {
	err := configs.Init("../config.json")
	assert.NoError(t, err)

	rf, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, rf)

	streams, err := rf.GetStreams(10)
	assert.NoError(t, err)
	assert.NotNil(t, streams)
	assert.Len(t, streams, 10)
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

func TestReflectorApi_GetBlobHashesForStream(t *testing.T) {
	err := configs.Init("../config.json")
	assert.NoError(t, err)

	rf, err := Init()
	assert.NoError(t, err)
	assert.NotNil(t, rf)

	expectedBlobs := map[string]int64{
		"c349d4e23306c4229aaf658c7186a7c25a3099caf3fc9195ff15b41c18bce3c322d9939336ee57301a767bbde4020384": 80680687,
		"1b8e54c57d8c82383f8ef02c6ad1541472a2c981367a073a119627efc23272ff9ca376e0976f8ef90db8c343a15eaee2": 11382064,
		"62a9aa583168df73d05c2c1074cc45831bfcd54d99cb4732f9a47a544519a0ef5d0a1cc1965aa1e14658e146e717006b": 40766046,
		"a6088ba98d6931667a9c5c2e059dd98e0fbf1cc22f2ccebd47c64e7af7442f7447105dc72fc2d736f9f133cecf0683a0": 68595095,
		"8337c6dad7f9190a983d87c258a4dd0f6f3d984a6f0d1f41be5417fec225bf784bc69d3a3472c575aa9fc4fec81d6248": 54212068,
		"aa872c43e944af12f300529c4a6d3ac15b6c100d60bb82ab0a8a40d40c615643634b7b706c90adf4c81c22c886dbd74d": 70451734,
		"a7f1b88cefc604bbf3779fdbbdca5a1fc9c5b7de736a53e767f2bb6a2cf43da6842206a6cd5af04b6e7e3b5212757cc3": 69384447,
		"b00e7a63fc8bbf0dc03fed650cd5fdfbfd02fe191076b8dbc2eeed0d573b5ad4c635444674e1677718e06a6cae375524": 72732602,
		"c16027196e9024dc4f3f5a7e839e01778b7c4d8aba91609f8a183797dd4697d5596e25241ff0b190937a10afad8a5e2c": 79889998,
		"d41372dbdb504c0609730c66f2f198b38fa8701ef59bc96ce8423962cf0234c626770f24021acf36a222d51e84ac69ec": 87618584,
		"fdba9c57d3c1991c2ad8538ac99093552b8016d54d66789d5008df309822af8294125ee3364ac59dcdd5d14fca882da0": 104827338,
	}
	streamBlobs, err := rf.GetBlobHashesForStream(1)
	assert.NoError(t, err)
	assert.NotNil(t, streamBlobs)
	assert.True(t, reflect.DeepEqual(expectedBlobs, streamBlobs))

}
