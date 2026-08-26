package drivers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func rejectingHTTPClient(err error, calls *atomic.Int32) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, err
	})}
}

func TestParseOSURLWithHTTPClientUsesClientForPolicyPost(t *testing.T) {
	blocked := errors.New("blocked request")
	var calls atomic.Int32
	client := rejectingHTTPClient(blocked, &calls)

	driver, err := ParseOSURLWithHTTPClient("s3+http://access:secret@example.com/bucket", false, client)
	require.NoError(t, err)
	sess := driver.NewSession("stream").(*s3Session)
	require.Same(t, client, sess.httpClient)

	_, err = sess.SaveData(context.Background(), "segment.ts", strings.NewReader("segment"), nil, time.Second)
	require.ErrorIs(t, err, blocked)
	require.Equal(t, int32(1), calls.Load())
}

func TestParseOSURLWithHTTPClientRejectsNonClientAwareSchemes(t *testing.T) {
	client := &http.Client{}
	for _, input := range []string{
		"relative/path",
		"/absolute/path",
		"file:///tmp/segments",
		"http://example.com/segments",
		"https://example.com/segments",
		"ipfs://access:secret@pinata.cloud",
		"gs://credentials@bucket",
		"w3s://proof@publication/path",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseOSURLWithHTTPClient(input, false, client)
			require.ErrorContains(t, err, "does not support a supplied HTTP client")
		})
	}
}

func TestParseOSURLWithHTTPClientUsesClientForFullS3API(t *testing.T) {
	blocked := errors.New("blocked request")
	var calls atomic.Int32
	client := rejectingHTTPClient(blocked, &calls)

	driver, err := ParseOSURLWithHTTPClient("s3+http://access:secret@example.com/bucket", true, client)
	require.NoError(t, err)
	sess := driver.NewSession("stream").(*s3Session)

	_, err = sess.SaveData(context.Background(), "segment.ts", strings.NewReader("segment"), nil, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), blocked.Error())
	require.Positive(t, calls.Load())
}

func TestParseOSURLWithHTTPClientUsesTransportForMinioCompatibility(t *testing.T) {
	blocked := errors.New("blocked request")
	var calls atomic.Int32
	client := rejectingHTTPClient(blocked, &calls)

	driver, err := ParseOSURLWithHTTPClient("s3+https://access:secret@storage.googleapis.com/bucket", false, client)
	require.NoError(t, err)
	sess := driver.NewSession("stream").(*s3Session)

	_, err = sess.SaveData(context.Background(), "segment.ts", strings.NewReader("segment"), nil, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), blocked.Error())
	require.Positive(t, calls.Load())
}

func TestNewSessionWithHTTPClientUsesClientForNetworkStorage(t *testing.T) {
	blocked := errors.New("blocked request")
	var calls atomic.Int32
	client := rejectingHTTPClient(blocked, &calls)
	sess := NewSessionWithHTTPClient(&OSInfo{
		StorageType: OSInfo_S3,
		S3Info: &S3OSInfo{
			Host:   "http://example.com/bucket",
			Bucket: "bucket",
		},
	}, client).(*s3Session)
	require.Same(t, client, sess.httpClient)

	_, err := sess.SaveData(context.Background(), "segment.ts", strings.NewReader("segment"), nil, time.Second)
	require.ErrorIs(t, err, blocked)
	require.Equal(t, int32(1), calls.Load())
}

func TestLegacyObjectStoreAPIsPreserveDefaultClient(t *testing.T) {
	driver, err := ParseOSURL("s3+http://access:secret@example.com/bucket", false)
	require.NoError(t, err)
	require.Nil(t, driver.NewSession("stream").(*s3Session).httpClient)
	for _, input := range []string{"relative/path", "file:///tmp/segments"} {
		driver, err := ParseOSURL(input, false)
		require.NoError(t, err)
		require.IsType(t, &FSOS{}, driver)
	}

	sess := NewSession(&OSInfo{StorageType: OSInfo_S3, S3Info: &S3OSInfo{Host: "http://example.com"}})
	require.Nil(t, sess.(*s3Session).httpClient)
}
