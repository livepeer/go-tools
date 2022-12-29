package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func S3UploadTest(assert *assert.Assertions, fullUriStr, saveName string) {
	testData := make([]byte, 1024*10)
	rand.Read(testData)
	fullUri, _ := url.Parse(fullUriStr)
	os, err := ParseOSURL(fullUriStr, true)
	assert.NoError(err)
	session := os.NewSession("")
	outUriStr, err := session.SaveData(context.Background(), saveName, bytes.NewReader(testData), nil, 10*time.Second)
	assert.NoError(err)
	var data *FileInfoReader
	// for specific key session, saveName is empty, otherwise, it's the key
	data, err = session.ReadData(context.Background(), saveName)
	assert.NoError(err)
	assert.Equal(*data.Size, int64(len(testData)))
	osBuf := new(bytes.Buffer)
	osBuf.ReadFrom(data.Body)
	osData := osBuf.Bytes()
	assert.Equal(testData, osData)
	// also test that the object is accessible through full output path with same URL structure
	if saveName != "" {
		outUri, _ := url.Parse(outUriStr)
		password, _ := fullUri.User.Password()
		bucket := splitNonEmpty(fullUri.Path, '/')[0]
		if !strings.Contains(outUri.Host, bucket) {
			// if bucket is not included in domain name of output URI, then it's already in the path
			bucket = ""
		}
		unifiedUrl := fullUri.Scheme + "://" + path.Clean(fmt.Sprintf("%s:%s@%s/%s/%s", fullUri.User.Username(), password, fullUri.Host,
			bucket, outUri.Path))
		os, err := ParseOSURL(unifiedUrl, true)
		assert.NoError(err)
		session := os.NewSession("")
		data, err = session.ReadData(context.Background(), "")
		assert.NoError(err)
		assert.Equal(*data.Size, int64(len(testData)))
		osBuf := new(bytes.Buffer)
		osBuf.ReadFrom(data.Body)
		osData := osBuf.Bytes()
		assert.Equal(testData, osData)
	}
}

func TestAwsS3Upload(t *testing.T) {
	s3key := os.Getenv("AWS_S3_KEY")
	s3secret := os.Getenv("AWS_S3_SECRET")
	s3region := os.Getenv("AWS_S3_REGION")
	s3bucket := os.Getenv("AWS_S3_BUCKET")
	assert := assert.New(t)
	if s3key != "" && s3secret != "" && s3region != "" && s3bucket != "" {
		// test full path in URI
		testUriKey := "test/" + uuid.New().String() + ".ts"
		fullUrl := fmt.Sprintf("s3://%s:%s@%s/%s/%s", s3key, s3secret, s3region, s3bucket, testUriKey)
		S3UploadTest(assert, fullUrl, "")
		// test key in SaveData arg
		fullUrl = fmt.Sprintf("s3://%s:%s@%s/%s", s3key, s3secret, s3region, s3bucket)
		S3UploadTest(assert, fullUrl, testUriKey)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}

func TestMinioS3Upload(t *testing.T) {
	s3key := os.Getenv("MINIO_S3_KEY")
	s3secret := os.Getenv("MINIO_S3_SECRET")
	s3bucket := os.Getenv("MINIO_S3_BUCKET")
	assert := assert.New(t)
	if s3key != "" && s3secret != "" {
		// test full path in URI
		testUriKey := "test/" + uuid.New().String() + ".ts"
		fullUrl := fmt.Sprintf("s3+http://%s:%s@localhost:9000/%s/%s", s3key, s3secret, s3bucket, testUriKey)
		S3UploadTest(assert, fullUrl, "")
		// test key in SaveData arg
		fullUrl = fmt.Sprintf("s3+http://%s:%s@localhost:9000/%s", s3key, s3secret, s3bucket)
		S3UploadTest(assert, fullUrl, testUriKey)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}

func TestStorjS3Read(t *testing.T) {
	s3key := os.Getenv("STORJ_S3_KEY")
	s3secret := os.Getenv("STORJ_S3_SECRET")
	s3bucket := os.Getenv("STORJ_S3_BUCKET")
	s3Path := os.Getenv("STORJ_S3_PATH")
	assert := assert.New(t)
	if s3key != "" && s3secret != "" && s3bucket != "" && s3Path != "" {
		fullUrl := fmt.Sprintf("s3+https://%s:%s@gateway.storjshare.io/%s", s3key, s3secret, s3bucket)
		os, err := ParseOSURL(fullUrl, true)
		assert.NoError(err)
		session := os.NewSession("")
		data, err := session.ReadData(context.Background(), s3Path)
		assert.NoError(err)
		osBuf := new(bytes.Buffer)
		osBuf.ReadFrom(data.Body)
		osData := osBuf.Bytes()
		assert.True(len(osData) > 0)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}
