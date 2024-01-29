package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readFile(session *FSSession, name string) []byte {
	fileInfoReader, _ := session.ReadData(context.Background(), name)
	defer fileInfoReader.Body.Close()
	buf := new(bytes.Buffer)
	io.Copy(buf, fileInfoReader.Body)
	return buf.Bytes()
}

func TestFsOS(t *testing.T) {
	// create random data
	rndData := make([]byte, 1024*1024+10)
	rand.Read(rndData)
	fileData := make([]byte, len(rndData))
	assert := assert.New(t)
	u, err := url.Parse("/tmp/")
	assert.NoError((err))
	storage := NewFSDriver(u)
	sess := storage.NewSession("driver-test").(*FSSession)
	out, err := sess.SaveData(context.TODO(), "name1/1.ts", bytes.NewReader(rndData), nil, 0)
	assert.NoError(err)
	path := out.URL
	defer os.Remove(path)
	assert.Equal("/tmp/driver-test/name1/1.ts", path)
	data := readFile(sess, "driver-test/name1/1.ts")
	assert.Equal(rndData, data)
	// check file contents
	file, _ := os.Open(path)
	_, _ = file.Read(fileData)
	assert.Equal(rndData, fileData)
	// check that file is listed
	files, err := sess.ListFiles(context.TODO(), "name1/", "")
	assert.NoError(err)
	assert.Equal(1, len(files.Files()))
	assert.Equal(0, len(files.Directories()))
	assert.Equal("1.ts", files.Files()[0].Name)

	// Test trim prefix when baseURI = nil
	storage = NewFSDriver(nil)
	sess = storage.NewSession("/tmp/").(*FSSession)
	out, err = sess.SaveData(context.TODO(), "driver-test/name1/1.ts", bytes.NewReader(rndData), nil, 0)
	assert.NoError(err)
	path = out.URL
	defer os.Remove(path)
	assert.Equal("/tmp/driver-test/name1/1.ts", path)
	data = readFile(sess, path)
	assert.Equal(rndData, data)
	// check file contents
	file, _ = os.Open(path)
	_, _ = file.Read(fileData)
	assert.Equal(rndData, fileData)
	// check that file is listed
	files, err = sess.ListFiles(context.TODO(), "driver-test//", "")
	assert.NoError(err)
	assert.Equal(0, len(files.Files()))
	assert.Equal(1, len(files.Directories()))
	assert.Equal("name1", files.Directories()[0])
}

func TestDeleteFile(t *testing.T) {
	file, err := os.CreateTemp("", "TestDeleteFileefix")
	require.NoError(t, err)

	// Defer a removal of the file so that we don't litter the filesystem when this test fails
	defer os.Remove(file.Name())

	// Confirm that the file exists
	_, err = os.Stat(file.Name())
	require.NoError(t, err)

	// Try to delete the file
	u, err := url.Parse(os.TempDir())
	require.NoError(t, err)
	sess := NewFSDriver(u).NewSession(os.TempDir())
	require.NoError(t, sess.DeleteFile(context.Background(), filepath.Base(file.Name())))

	// Check the file no longer exists
	_, err = os.Stat(file.Name())
	require.ErrorContains(t, err, "no such file or directory")
}
