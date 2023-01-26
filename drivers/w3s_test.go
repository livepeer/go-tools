package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	require2 "github.com/stretchr/testify/require"
	"io"
	"os"
	"os/exec"
	"testing"
)

type testFile struct {
	name string
	data io.Reader
}

func randFile() testFile {
	name := uuid.New().String() + ".ts"
	size := int64(1024 * 10)
	rndData := make([]byte, size)
	rand.Read(rndData)
	return testFile{name: name, data: bytes.NewReader(rndData)}
}

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

	pubId := uuid.New().String()

	// Add files to foo/video/hls dir
	sess := NewW3sDriver(ucanKey, ucanProof, "/foo/video/hls/", pubId).NewSession("").(*W3sSession)
	rndFile := randFile()
	_, err = sess.SaveData(context.TODO(), rndFile.name, rndFile.data, nil, 0)
	require.NoError(err)

	// Add files to bar/video/hls dir
	sess = NewW3sDriver(ucanKey, ucanProof, "/bar/video/hls/", pubId).NewSession("").(*W3sSession)
	rndFile = randFile()
	_, err = sess.SaveData(context.TODO(), rndFile.name, rndFile.data, nil, 0)
	require.NoError(err)
	rndFile = randFile()
	_, err = sess.SaveData(context.TODO(), rndFile.name, rndFile.data, nil, 0)
	require.NoError(err)

	// Add files to /bar/ dir
	sess = NewW3sDriver(ucanKey, ucanProof, "/bar/", pubId).NewSession("").(*W3sSession)
	rndFile = randFile()
	_, err = sess.SaveData(context.TODO(), rndFile.name, rndFile.data, nil, 0)
	require.NoError(err)

	// Add files to / dir
	sess = NewW3sDriver(ucanKey, ucanProof, "", pubId).NewSession("").(*W3sSession)
	rndFile = randFile()
	_, err = sess.SaveData(context.TODO(), rndFile.name, rndFile.data, nil, 0)
	require.NoError(err)

	// TODO
	url := NewW3sDriver(ucanKey, ucanProof, "/foo/video/hls/", pubId).Publish()
	fmt.Println(url)

	// TODO: Create CAR for each subdirectory
}
