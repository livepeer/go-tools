package drivers

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalOS(t *testing.T) {
	tempData1 := "dataitselftempdata1"
	tempData2 := "dataitselftempdata2"
	tempData3 := "dataitselftempdata3"
	oldDataCacheLen := dataCacheLen
	dataCacheLen = 1
	defer func() {
		dataCacheLen = oldDataCacheLen
	}()

	u, err := url.Parse("fake.com/url")
	require.NoError(t, err)

	os := NewMemoryDriver(u)
	sess := os.NewSession(("sesspath")).(*MemorySession)
	out, err := sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData1), nil, 0)
	require.NoError(t, err)
	path := out.URL
	require.Equal(t, "fake.com/url/stream/sesspath/name1/1.ts", path)

	data := sess.GetData("sesspath/name1/1.ts")
	require.Equal(t, tempData1, string(data))

	_, err = sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData2), nil, 0)
	require.NoError(t, err)

	data = sess.GetData("sesspath/name1/1.ts")
	require.Equal(t, tempData2, string(data))

	out, err = sess.SaveData(context.TODO(), "name1/2.ts", strings.NewReader(tempData3), nil, 0)
	require.NoError(t, err)
	path = out.URL

	data = sess.GetData("sesspath/name1/2.ts")
	require.Equal(t, tempData3, string(data))

	// Test trim prefix when baseURI != nil
	data = sess.GetData(path)
	require.Equal(t, tempData3, string(data))
	data = sess.GetData("sesspath/name1/1.ts")
	require.Nil(t, data)
	sess.EndSession()

	data = sess.GetData("sesspath/name1/2.ts")
	require.Nil(t, data)

	// Test trim prefix when baseURI = nil
	os = NewMemoryDriver(nil)
	sess = os.NewSession("sesspath").(*MemorySession)
	out, err = sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData1), nil, 0)
	require.NoError(t, err)
	path = out.URL
	require.Equal(t, "/stream/sesspath/name1/1.ts", path)

	data = sess.GetData(path)
	require.Equal(t, tempData1, string(data))
}
