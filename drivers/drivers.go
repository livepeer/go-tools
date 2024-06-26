// Package drivers abstracts different object storages, such as local, s3
package drivers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var ext2mime = map[string]string{
	".ts":   "video/mp2t",
	".mp4":  "video/mp4",
	".m3u8": "application/x-mpegurl",
}

var ErrFormatMime = fmt.Errorf("unknown file extension")

// ErrNoNextPage indicates that there is no next page in ListFiles
var ErrNoNextPage = fmt.Errorf("no next page")

// ErrNotSupported indicates that the functionality is not supported by the given driver
var ErrNotSupported = fmt.Errorf("not supported")

// ErrNotExist indicates that the file being fetched does not exist
var ErrNotExist = fmt.Errorf("the specified file does not exist")

// NodeStorage is current node's primary driver
var NodeStorage OSDriver

// RecordStorage is current node's "stream recording" driver
var RecordStorage OSDriver

// Testing indicates that test is running
var Testing bool

// TestMemoryStorages used for testing purposes
var TestMemoryStorages map[string]*MemoryOS
var testMemoryStoragesLock = &sync.Mutex{}

// OSDriver common interface for Object Storage
type OSDriver interface {
	NewSession(path string) OSSession
	Description() string
	UriSchemes() []string
	Publish(ctx context.Context) (string, error)
}

type FileInfo struct {
	Name         string
	ETag         string
	LastModified time.Time
	Size         *int64
}

type FileInfoReader struct {
	FileInfo
	Metadata     map[string]string
	Body         io.ReadCloser
	ContentType  string
	ContentRange string
}

type FileProperties struct {
	Metadata     map[string]string
	CacheControl string
	ContentType  string
}

type SaveDataOutput struct {
	URL                     string
	UploaderResponseHeaders http.Header
}

var AvailableDrivers = []OSDriver{
	&FSOS{},
	&GsOS{},
	&IpfsOS{},
	&MemoryOS{},
	&S3OS{},
	&W3sOS{},
}

type PageInfo interface {
	Files() []FileInfo
	Directories() []string
	HasNextPage() bool
	NextPage() (PageInfo, error)
}

type OSInfo_StorageType int32

type S3OSInfo struct {
	// Host to use to connect to S3
	Host string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	// Bucket where the object is stored
	Bucket string `json:"bucket,omitempty"`
	// Key (prefix) to use when uploading the object.
	Key string `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	// POST policy that S3 owner node creates to give write access to other node.
	Policy string `protobuf:"bytes,3,opt,name=policy,proto3" json:"policy,omitempty"`
	// Signature for POST policy.
	Signature string `protobuf:"bytes,4,opt,name=signature,proto3" json:"signature,omitempty"`
	// Needed for POST policy.
	Credential string `protobuf:"bytes,5,opt,name=credential,proto3" json:"credential,omitempty"`
	// Needed for POST policy.
	XAmzDate             string   `protobuf:"bytes,6,opt,name=xAmzDate,proto3" json:"xAmzDate,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

// OSInfo needed to negotiate storages that will be used.
// It carries info needed to write to the storage.
type OSInfo struct {
	// Storage type: direct, s3, ipfs.
	StorageType          OSInfo_StorageType `protobuf:"varint,1,opt,name=storageType,proto3,enum=net.OSInfo_StorageType" json:"storageType,omitempty"`
	S3Info               *S3OSInfo          `protobuf:"bytes,16,opt,name=s3info,proto3" json:"s3info,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

const (
	OSInfo_DIRECT OSInfo_StorageType = 0
	OSInfo_S3     OSInfo_StorageType = 1
	OSInfo_GOOGLE OSInfo_StorageType = 2
)

type OSSession interface {
	OS() OSDriver

	SaveData(ctx context.Context, name string, data io.Reader, fields *FileProperties, timeout time.Duration) (*SaveDataOutput, error)
	EndSession()

	// Info in order to have this session used via RPC
	GetInfo() *OSInfo

	// Indicates whether data may be external to this node
	IsExternal() bool

	// Indicates whether this is the correct OS for a given URL
	IsOwn(url string) bool

	// ListFiles return list of files
	ListFiles(ctx context.Context, prefix, delim string) (PageInfo, error)

	// DeleteFile deletes a single file. 'name' should be the relative filename
	DeleteFile(ctx context.Context, name string) error

	ReadData(ctx context.Context, name string) (*FileInfoReader, error)

	ReadDataRange(ctx context.Context, name, byteRange string) (*FileInfoReader, error)

	Presign(name string, expire time.Duration) (string, error)
}

type OSDriverDescr struct {
	UriSchemes  []string `json:"scheme"`
	Description string   `json:"desc"`
}

func DescribeDriversJson() []byte {
	var descrs []OSDriverDescr
	for _, h := range AvailableDrivers {
		descrs = append(descrs, OSDriverDescr{h.UriSchemes(), h.Description()})
	}
	bytes, _ := json.Marshal(struct {
		Handlers []OSDriverDescr `json:"storage_drivers"`
	}{descrs})
	return bytes
}

func TypeByExtension(ext string) (string, error) {
	if m, ok := ext2mime[ext]; ok && m != "" {
		return m, nil
	}
	m := mime.TypeByExtension(ext)
	if m == "" {
		return "", ErrFormatMime
	}
	return m, nil
}

// NewSession returns new session based on OSInfo received from the network
func NewSession(info *OSInfo) OSSession {
	if info == nil {
		return nil
	}
	switch info.StorageType {
	case OSInfo_S3:
		return newS3Session(info.S3Info)
	case OSInfo_GOOGLE:
		return newGSSession(info.S3Info)
	}
	return nil
}

// PrepareOSURL used for resolving files when necessary and turning into a URL. Don't use
// this when the URL comes from untrusted sources e.g. AuthWebhookUrl.
func PrepareOSURL(input string) (string, error) {
	u, err := url.Parse(input)
	if err != nil {
		return "", err
	}
	if u.Scheme == "gs" {
		m, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return "", err
		}
		keyfiles, ok := m["keyfile"]
		if !ok {
			return u.String(), nil
		}

		keyfile := keyfiles[0]
		content, err := ioutil.ReadFile(keyfile)
		if err != nil {
			return "", err
		}
		u.User = url.User(string(content))
	}
	return u.String(), nil
}

// ParseOSURL returns the correct OS for a given OS url
func ParseOSURL(input string, useFullAPI bool) (OSDriver, error) {
	u, err := url.Parse(input)
	if err != nil {
		return nil, err
	}
	isAws := u.Scheme == "s3"
	isS3 := u.Scheme == "s3+http" || u.Scheme == "s3+https"
	isSSL := strings.Contains(u.Scheme, "https")
	if isAws || isS3 {
		pw, ok := u.User.Password()
		if !ok {
			return nil, fmt.Errorf("password is required with s3:// OS")
		}
		// bucket immediately follows domain name, the rest is key
		splits := splitNonEmpty(u.Path, '/')
		if len(splits) == 0 {
			return nil, errors.New("S3 bucket not found in URL path")
		}
		bucket := splits[0]
		// need to get first sep position, ignoring leading sep
		sepIndex := strings.Index(u.Path[1:], "/")
		keyPrefix := ""
		if sepIndex != -1 {
			keyPrefix = u.Path[sepIndex+2:]
		}
		if isAws {
			return NewS3Driver(u.Host, bucket, u.User.Username(), pw, keyPrefix, useFullAPI)
		} else {
			return NewCustomS3Driver(u.Host, bucket, u.User.Username(), pw, keyPrefix, useFullAPI, isSSL)
		}
	}
	if u.Scheme == "ipfs" {
		// make it explicit that it's Pinata API, not IPFS node
		if u.Host == "pinata.cloud" {
			password, _ := u.User.Password()
			return NewIpfsDriver(u.User.Username(), password), nil
		} else {
			return nil, fmt.Errorf("unsupported IPFS provider: %s", u.Host)
		}
	}
	if u.Scheme == "gs" {
		file := u.User.Username()
		return NewGoogleDriver(u.Host, file, useFullAPI)
	}
	if u.Scheme == "memory" && Testing {
		testMemoryStoragesLock.Lock()
		if TestMemoryStorages == nil {
			TestMemoryStorages = make(map[string]*MemoryOS)
		}
		os, ok := TestMemoryStorages[u.Host]
		if !ok {
			os = NewMemoryDriver(nil)
			TestMemoryStorages[u.Host] = os
		}
		testMemoryStoragesLock.Unlock()
		return os, nil
	}
	if u.Scheme == "" {
		return NewFSDriver(u), nil
	}
	if u.Scheme == "file" {
		u.Scheme = ""
		return NewFSDriver(u), nil
	}
	if u.Scheme == "w3s" {
		_, present := os.LookupEnv("W3_PRINCIPAL_KEY")
		if !present {
			return nil, fmt.Errorf("env variable 'W3_PRINCIPAL_KEY' is not defined")
		}

		// W3S URL format: 'w3s://proof@pubId/path'
		// Proof is base64url-encoded
		// pubId must be a unique value used until Publish() is called
		w3sUcanProof := u.User.Username()
		pubId := u.Hostname()
		filePath := u.Path
		return NewW3sDriver(w3sUcanProof, filePath, pubId), nil
	}
	return nil, fmt.Errorf("unrecognized OS scheme: %s", u.Scheme)
}

// SaveRetried tries to SaveData specified number of times
func SaveRetried(ctx context.Context, sess OSSession, name string, data []byte, fields *FileProperties, retryCount int) (*SaveDataOutput, error) {
	if retryCount < 1 {
		return nil, fmt.Errorf("invalid retry count %d", retryCount)
	}
	var out *SaveDataOutput
	var err error
	for i := 0; i < retryCount; i++ {
		out, err = sess.SaveData(ctx, name, bytes.NewReader(data), fields, 0)
		if err == nil {
			return out, err
		}
	}
	return out, err
}

var httpc = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	Timeout:   1,
}

func splitNonEmpty(str string, sep rune) []string {
	splitFn := func(c rune) bool {
		return c == sep
	}
	return strings.FieldsFunc(str, splitFn)
}
