package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIpfsOS(t *testing.T) {
	pinataKey := os.Getenv("PINATA_KEY")
	pinataSecret := os.Getenv("PINATA_SECRET")
	if pinataSecret == "" {
		return
	}
	// create random data
	rndData := make([]byte, 1024*10)
	rand.Read(rndData)
	assert := assert.New(t)
	storage := NewIpfsDriver(pinataKey, pinataSecret)
	sess := storage.NewSession("").(*IpfsSession)
	cid, err := sess.SaveData(context.TODO(), "", bytes.NewReader(rndData), nil, 0)
	assert.NoError(err)
	ipfsInfo, err := sess.ReadData(context.TODO(), cid)
	assert.NoError(err)
	ipfsData := new(bytes.Buffer)
	ipfsData.ReadFrom(ipfsInfo.Body)
	assert.Equal(rndData, ipfsData.Bytes())
}
