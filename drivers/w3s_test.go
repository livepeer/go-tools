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

	_, present := os.LookupEnv("W3_PRINCIPAL_KEY")
	w3sUcanProof := os.Getenv("W3S_UCAN_PROOF")
	if !present || w3sUcanProof == "" {
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
		sess := NewW3sDriver(w3sUcanProof, tf.dirPath, pubId).NewSession("").(*W3sSession)
		_, err = sess.SaveData(context.TODO(), tf.name, bytes.NewReader(tf.data), FileProperties{}, 0)
		require.NoError(err)
	}

	// publish the CAR and get the w3s URL
	u, err := NewW3sDriver(w3sUcanProof, "", pubId).Publish(context.TODO())
	require.NoError(err)

	// convert to w3s link url
	URL, err := url.Parse(u)
	require.NoError(err)
	baseUrl, err := url.Parse(fmt.Sprintf("https://%s.ipfs.w3s.link/", URL.Host))
	require.NoError(err)

	// verify the test file data
	for _, tf := range testFiles {
		fileUrl := fmt.Sprintf("%s/%s/%s", baseUrl, tf.dirPath, tf.name)
		require.NoError(err)

		resp, err2 := http.Get(fileUrl)
		require.NoError(err2)

		d, err3 := io.ReadAll(resp.Body)
		require.NoError(err3)
		resp.Body.Close()

		require.Equal(tf.data, d)
	}
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

func TestBase64UrlToBase64(t *testing.T) {
	require := require2.New(t)

	tests := []struct {
		name string
		arg  string
		exp  string
	}{
		{
			name: "standard text",
			arg:  "c29tZSB0ZXh0",
			exp:  "c29tZSB0ZXh0",
		},
		{
			name: "binary no padding",
			arg:  "eSK_-HCmI596rRX4xY",
			exp:  "eSK/+HCmI596rRX4xQ==",
		},
		{
			name: "binary with padding",
			arg:  "_-HCmI596rRX4xY",
			exp:  "/+HCmI596rRX4xY=",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			res, err := base64UrlToBase64(tc.arg)
			require.NoError(err)
			require.Equal(tc.exp, res)
		})
	}
}
