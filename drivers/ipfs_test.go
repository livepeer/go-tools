package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestIpfsOS(t *testing.T) {
	pinataKey := os.Getenv("PINATA_KEY")
	pinataSecret := os.Getenv("PINATA_SECRET")
	if pinataSecret == "" {
		fmt.Println("No IPFS credentials, test skipped")
		return
	}
	// create random data
	fileName := uuid.New().String() + ".ts"
	fileSize := int64(1024*10)
	rndData := make([]byte, fileSize)
	rand.Read(rndData)
	assert := assert.New(t)
	storage := NewIpfsDriver(pinataKey, pinataSecret)
	sess := storage.NewSession("").(*IpfsSession)
	cid, err := sess.SaveData(context.TODO(), fileName, bytes.NewReader(rndData), nil, 0)
	assert.NoError(err)
	// first, list file through API
	files, err := sess.ListFiles(context.TODO(), cid, "")
	assert.NoError(err)
	assert.Equal(1, len(files.Files()))
	assert.Equal(cid, files.Files()[0].ETag)
	assert.Equal(fileName, files.Files()[0].Name)
	assert.Equal(fileSize, *files.Files()[0].Size)
	// wait for file to appear on the gateway, it may take longer for public gateway, and the test may fail
	time.Sleep(5 * time.Second)
	ipfsInfo, err := sess.ReadData(context.TODO(), cid)
	assert.NoError(err)
	ipfsData := new(bytes.Buffer)
	ipfsData.ReadFrom(ipfsInfo.Body)
	assert.Equal(rndData, ipfsData.Bytes())
}
