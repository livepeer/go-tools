package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	require2 "github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
)

type testFile struct {
	dirPath string
	name    string
	data    []byte
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
	testFiles := []testFile{
		{dirPath: "/foo/video/hls/", name: randFilename(), data: randFiledata()},
		{dirPath: "/bar/video/hls/", name: randFilename(), data: randFiledata()},
		{dirPath: "/bar/video/hls/", name: randFilename(), data: randFiledata()},
		{dirPath: "/bar/", name: randFilename(), data: randFiledata()},
		{dirPath: "", name: randFilename(), data: randFiledata()},
	}

	// add a number of files in different locations
	for _, tf := range testFiles {
		sess := NewW3sDriver(ucanKey, ucanProof, tf.dirPath, pubId).NewSession("").(*W3sSession)
		_, err = sess.SaveData(context.TODO(), tf.name, bytes.NewReader(tf.data), nil, 0)
		require.NoError(err)
	}

	// publish the CAR and get the final w3s URL
	u, err := NewW3sDriver(ucanKey, ucanProof, "", pubId).Publish(context.TODO())
	require.NoError(err)

	// verify the test file data
	for _, tf := range testFiles {
		fileUrl, err := url.JoinPath(u, tf.dirPath, tf.name)
		require.NoError(err)

		resp, err := http.Get(fileUrl)
		require.NoError(err)

		d, err := io.ReadAll(resp.Body)
		require.NoError(err)
		resp.Body.Close()

		require.Equal(tf.data, d)
	}

	// TODO: Context and timeouts
	// TODO: unit test?
}

func randFilename() string {
	return uuid.New().String() + ".ts"
}

func randFiledata() []byte {
	size := int64(1024 * 10)
	rndData := make([]byte, size)
	rand.Read(rndData)
	return rndData
}
