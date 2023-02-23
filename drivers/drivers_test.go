package drivers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3URL(t *testing.T) {
	assert := assert.New(t)
	os, err := ParseOSURL("s3://user:xxxxxxxxx%2Bxxxxxxxxx%2Fxxxxxxxx%2Bxxxxxxxxxxxxx@us-west-2/example-bucket/", true)
	assert.Equal(nil, err)
	s3, iss3 := os.(*S3OS)
	assert.Equal(true, iss3)
	assert.Equal("user", s3.awsAccessKeyID)
	assert.Equal("xxxxxxxxx+xxxxxxxxx/xxxxxxxx+xxxxxxxxxxxxx", s3.awsSecretAccessKey)
	assert.Equal("https://example-bucket.s3.amazonaws.com", s3.host)
	assert.Equal("us-west-2", s3.region)
	assert.Equal("example-bucket", s3.bucket)
	assert.Equal("", s3.keyPrefix)

	// test full URI
	os, err = ParseOSURL("s3://user:xxxxxxxxx%2Bxxxxxxxxx%2Fxxxxxxxx%2Bxxxxxxxxxxxxx@us-west-2/example-bucket/key_part1/key_part2/key.ts", true)
	assert.Equal(nil, err)
	s3, iss3 = os.(*S3OS)
	assert.Equal(true, iss3)
	assert.Equal("user", s3.awsAccessKeyID)
	assert.Equal("xxxxxxxxx+xxxxxxxxx/xxxxxxxx+xxxxxxxxxxxxx", s3.awsSecretAccessKey)
	assert.Equal("https://example-bucket.s3.amazonaws.com", s3.host)
	assert.Equal("us-west-2", s3.region)
	assert.Equal("example-bucket", s3.bucket)
	assert.Equal("key_part1/key_part2/key.ts", s3.keyPrefix)
}

func TestFsPath(t *testing.T) {
	assert := assert.New(t)
	testPath := func(path string) {
		os, err := ParseOSURL(path, true)
		assert.Equal(nil, err)
		_, isfs := os.(*FSOS)
		assert.Equal(true, isfs)
	}
	testPath("/tmp/test/stream.ts")
	testPath("tmp/test/stream.ts")
}

func TestIpfsUrls(t *testing.T) {
	assert := assert.New(t)
	os, err := ParseOSURL("ipfs://pinata.cloud", true)
	assert.Equal(nil, err)
	_, isfs := os.(*IpfsOS)
	assert.Equal(true, isfs)
	_, err = ParseOSURL("ipfs://", true)
	assert.NotNil(err)
}

func TestCustomS3URL(t *testing.T) {
	assert := assert.New(t)
	os, err := ParseOSURL("s3+http://user:password@example.com:9000/bucket-name", true)
	s3, iss3 := os.(*S3OS)
	assert.Equal(true, iss3)
	assert.Equal(nil, err)
	assert.Equal("http://example.com:9000", s3.host)
	assert.Equal("bucket-name", s3.bucket)
	assert.Equal("user", s3.awsAccessKeyID)
	assert.Equal("password", s3.awsSecretAccessKey)
	assert.Equal("ignored", s3.region)
}

func TestCustomS3URLWithRegion(t *testing.T) {
	assert := assert.New(t)
	os, err := ParseOSURL("s3+http://user:password@example.com:9000/bucket-name/key", true)
	s3, iss3 := os.(*S3OS)
	assert.Equal(true, iss3)
	assert.Equal(nil, err)
	assert.Equal("http://example.com:9000", s3.host)
	assert.Equal("bucket-name", s3.bucket)
	assert.Equal("ignored", s3.region)
	assert.Equal("user", s3.awsAccessKeyID)
	assert.Equal("password", s3.awsSecretAccessKey)
}

func TestGSURL(t *testing.T) {
	assert := assert.New(t)
	// Don't worry, I invalidated this
	testGSToken := `{
		"type": "service_account",
		"project_id": "livepeerjs-231617",
		"private_key_id": "835d25ed984195fab1e551d9c9c351921b9512cb",
		"private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCdhlk4y6a/U5qu\ngGQ5vqxfwcJ6qJKNtMRTa5DEOny0XKwsbfUEF6YMbsMx1ZGhgiAH2w+KMukpdPMd\nAU6DxNDrractn2i+rSTL8mcRXALGlBLkpHWfQZvSojcqo5CVktpUsv31PJirGEuC\n0HuSVcSuM/RFG4B3tcMtxNB8yh9AOTYqe3H9YNywLig+vTJMh9tGkKA5FFwN0gAh\nSDfuDpkmewbsysgQmNqytvjP5yRtSfX8G5URDt/6Ge5Dbb763LhLPtmxg9hTizYo\nSbvytj/tHtIZBeYvPMZq78m7B8rNemiVDHMxE1+WoBjbixIkjGz5GTXX0NRv222q\nqqyGb5/NAgMBAAECggEACDq1TQwIl0y3E0A2XDTrnOoKrq1BUMFdk00WiEXU73g6\n72xEJVVV8abUsDUL0V/yq+5kCrB7sVSAgeaoUyZ0UqelCPNfvbxeZIAypbvEklq4\nfPThhzMegJvEXYgjfMjp+oxKS6ZBhIi1oy0gk4XDC2W/8F9OMBLREkJKsQY/KTPu\n9n5QkkRA1bhI2d1sj+HHOwKwfyxzhwLXvni4F1X2V8C1YLQ/9kb+kpul5+j5QRMr\nwW8rNsihXXcR5U0q8X4OEqGGsVb3/2e/lAw5KAvJ4MPAgwrjmLbPViuU01KQXS1J\ndgix5KPeOZ8ejWfvxXP0EoqzGCq7K+61xy2Zg1JCGQKBgQDLICfxNWn/xf8QIvIC\ndxSCnWZWydyy5sStfManJhaJcm2JdL95uLI+b3oCE6dLiOxtu4lzrxKELjyJNBV1\nbxnc/JfWG0FURl7c8G+VTGJiSaRH5WhBvziD3kphgKpjL6cHXSJR/tT+ElBBjK9o\nOIvTeMi6uh710mMD+1crUSpwaQKBgQDGh3Rqn7Z6EYYRMx7hczZci8ktKYiyyEBH\niL/1O6MR39Pn81VWHhr/0EwfbPG3k9xDsoKRGGwE54Q9Ymlff4k9d/5qsEl7VM4P\n/pK1vipMXyVtyPYRfAsPdonaotRQvK0uA1tlTpmc1JKGRa4+zQLlnooLh29o1lfa\nR+g/u4pHxQKBgAk4gXeqpBAvTb/OxkukWjL/sCiaa0FXxm/VrTLjQLymjCBkQ1jk\nMHszFkfH2p1MLudgTwIIXX/QlYDo81xsWbE1ajMW86U+uImxBG+zkvfBPgrheBUb\n+BXMXnYEoDd2b0+fQ7KTLdoGvMvs9f12K6rC3eHUFxmznjkNDMzzl0iZAoGANfdm\nTwGhYedXkV9bGp/t/BRHmI48yZSj3I4w2CHg/x/gA6Ji5SkD39wohTZhMqzv6Dsj\nQPvpiR/CE8mnqT0K+nme4DORlgQEi9aA3QSXjPEkRIanVTNp8kcfzB4NJvFTBjoF\nYzGNklM6jWNtrUafbfm9vsqPH2l8sipv2LtLKJ0CgYB//iLmjLBS/Qqsiqz7Y5GK\n24jrL6ZPPpjJ6ms7oPHiHPhRqnnxPaoZ/eakUFdr1diWk6vxZUv/6Fnhr313i14A\nMQwn/d3abKyHpj5DI6/5c4OjLoLHFcBZgWkv+/cZcNhkau92pMQUUWZqJdPjK2EQ\nYJE8pVOLiszikCAO1IL8mA==\n-----END PRIVATE KEY-----\n",
		"client_email": "dummy-service-account@livepeerjs-231617.iam.gserviceaccount.com",
		"client_id": "112693155080511457268",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dummy-service-account%40livepeerjs-231617.iam.gserviceaccount.com"
	}`

	// Set up test file
	file, err := ioutil.TempFile("", "gs-code")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())
	file.WriteString(testGSToken)

	// Set up test URL from filesystem when trusted
	u, _ := url.Parse(fmt.Sprintf("gs://bucket-name?keyfile=%s", file.Name()))
	prepared, err := PrepareOSURL(u.String())
	assert.Equal(nil, err)
	os, err := ParseOSURL(prepared, true)
	assert.Equal(nil, err)
	gs, ok := os.(*GsOS)
	assert.Equal(true, ok)
	assert.Equal("https://bucket-name.storage.googleapis.com", gs.S3OS.host)
	assert.Equal("bucket-name", gs.S3OS.bucket)

	// Also test embedding the thing in the URL itself
	u = &url.URL{
		Scheme: "gs",
		Host:   "bucket-name",
		User:   url.User(testGSToken),
	}
	os, err = ParseOSURL(u.String(), false)
	assert.Equal(nil, err)
	gs, ok = os.(*GsOS)
	assert.Equal(true, ok)
	assert.Equal("https://bucket-name.storage.googleapis.com", gs.S3OS.host)
	assert.Equal("bucket-name", gs.S3OS.bucket)
}

func TestW3sURL(t *testing.T) {
	assert := assert.New(t)

	// given
	os.Setenv("W3_PRINCIPAL_KEY", "C7+sElIGGz25QwiLkOZhkNV7dosvJAZuBfDlRnHuCt8=")
	pubId := "abcdef12345"
	proof := "EaJlcm9vdHOAZ3ZlcnNpb24BmgIBcRIguVaNefyQMACKNgi3XA46t5ijCH19S_ndLpkGhZ0kWiOnYXNYRO2hA0CVYBCNOU9IW-u-IUqhZ9gSHPzFMB7tzLYBE0tjOUrg11K3p3bC31kprHJ769ISMQSJDMRvWCGamwks2rsWJA4GYXZlMC45LjFjYXR0gaJjY2FuYSpkd2l0aHg4ZGlkOmtleTp6Nk1rdGdRNGZHOWNFTTdVY3dOTUhuRUJ0a1ZXYmQ2QUJLRFh3VTFKMlpvdVpodnBjYXVkWCLtAeoGmhaC2aAQPNKXr4AK7MOo8OR_9RkLNIZ6_SgZUq2_Y2V4cPZjaXNzWCLtAdNhO-TS5YOYwp4wQuxsFq9Hi2uBoldfmfxUxf3HWuhRY3ByZoDoAgFxEiAdz1OG9whG7Z5aT42jkEMcBiczAba5WgpZ5NO6okLTKKhhc1hE7aEDQJqxaum4RfYm8EF9W2G2SSoI6rI58lC6buIUoSZaThMs0JA3blC7PPrTgL06AqWOaaAnQKN4b9TuBezi3llLnQhhdmUwLjkuMWNhdHSBomNjYW5hKmR3aXRoeDhkaWQ6a2V5Ono2TWt0Z1E0Zkc5Y0VNN1Vjd05NSG5FQnRrVldiZDZBQktEWHdVMUoyWm91Wmh2cGNhdWRYIu0BYFbRZFVNOcB-ZhrKuhujUFU3l9oaQa68-YMRNtYtqDpjZXhw9mNmY3SBoWVzcGFjZaJkbmFtZWR0ZXN0bGlzUmVnaXN0ZXJlZPVjaXNzWCLtAeoGmhaC2aAQPNKXr4AK7MOo8OR_9RkLNIZ6_SgZUq2_Y3ByZoHYKlglAAFxEiC5Vo15_JAwAIo2CLdcDjq3mKMIfX1L-d0umQaFnSRaIw"
	path := "/video/hls"

	// when
	os, err := ParseOSURL(fmt.Sprintf("w3s://%s@%s%s", proof, pubId, path), false)

	// then
	assert.Equal(nil, err)
	w3s, isW3s := os.(*W3sOS)
	assert.Equal(true, isW3s)
	assert.Equal(proof, w3s.ucanProof)
	assert.Equal(pubId, w3s.pubId)
	assert.Equal(path, w3s.dirPath)
}

func TestDescribeDriversJson(t *testing.T) {
	assert := assert.New(t)
	handlersJson := DescribeDriversJson()
	var driverDescr struct {
		Drivers []OSDriverDescr `json:"storage_drivers"`
	}
	err := json.Unmarshal(handlersJson, &driverDescr)
	assert.NoError(err)
	assert.Equal(len(AvailableDrivers), len(driverDescr.Drivers))
	for i, h := range AvailableDrivers {
		assert.Equal(h.Description(), driverDescr.Drivers[i].Description)
		assert.Equal(h.UriSchemes(), driverDescr.Drivers[i].UriSchemes)
	}
}

func TestItChoosesTheCorrectContentTypes(t *testing.T) {
	extType, err := TypeByExtension(".m3u8")
	require.NoError(t, err)
	require.Equal(t, "application/x-mpegurl", extType)

	extType, err = TypeByExtension(".ts")
	require.NoError(t, err)
	require.Equal(t, "video/mp2t", extType)

	extType, err = TypeByExtension(".mp4")
	require.NoError(t, err)
	require.Equal(t, "video/mp4", extType)

	extType, err = TypeByExtension(".json")
	require.NoError(t, err)
	require.Equal(t, "application/json", extType)
}
