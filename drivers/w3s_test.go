package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	require2 "github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"testing"
)

func TestW3sOS(t *testing.T) {
	require := require2.New(t)

	ucanKey := os.Getenv("W3S_UCAN_KEY")
	ucanProof := os.Getenv("W3S_UCAN_PROOF")
	if ucanKey == "" || ucanProof == "" {
		fmt.Println("No w3s credentials, test skipped")
		return
	}
	_, err := exec.LookPath("w3")
	if err != nil {
		fmt.Println("No w3 installed, test skipped")
		return
	}
	_, err = exec.LookPath("ipfs-car")
	if err != nil {
		fmt.Println("No ipfs-car installed, test skipped")
		return
	}

	// Configure Driver
	path := "/somepath/video/hls/"
	//path := "/"
	pubId := uuid.New().String()
	storage := NewW3sDriver(ucanKey, ucanProof, path, pubId)
	sess := storage.NewSession("").(*W3sSession)

	// Create random data
	fileName := uuid.New().String() + ".ts"
	fileSize := int64(1024 * 10)
	rndData := make([]byte, fileSize)
	rand.Read(rndData)

	// Store data
	cid, err := sess.SaveData(context.TODO(), fileName, bytes.NewReader(rndData), nil, 0)
	require.NoError(err)
	fmt.Println(cid)

	// TODO
	url := storage.Publish()
	fmt.Println(url)

	// TODO: Create CAR for each subdirectory
}
