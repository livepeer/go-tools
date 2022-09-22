package drivers

import (
	"context"
	"github.com/livepeer/go-tools/clients"
	"io"
	"net/http"
	"path"
	"sync"
	"time"
)

type IpfsOS struct {
	key    string
	secret string
}

type IpfsSession struct {
	os       *IpfsOS
	filename string
	ended    bool
	client   clients.IPFS
	dCache   map[string]*dataCache
	dLock    sync.RWMutex
}

func NewIpfsDriver(key, secret string) *IpfsOS {
	return &IpfsOS{key: key, secret: secret}
}

func (ostore *IpfsOS) NewSession(filename string) OSSession {
	if filename != "" {
		panic("File names are not supported by Pinata IPFS driver")
	}
	var client clients.IPFS
	if ostore.key != "" {
		client = clients.NewPinataClientAPIKey(ostore.key, ostore.secret, map[string]string{})
	} else {
		client = clients.NewPinataClientJWT(ostore.secret, map[string]string{})
	}
	session := &IpfsSession{
		os:       ostore,
		filename: filename,
		dCache:   make(map[string]*dataCache),
		dLock:    sync.RWMutex{},
		client:   client,
	}
	return session
}

func (ostore *IpfsOS) UriSchemes() []string {
	return []string{"ipfs://pinata.cloud"}
}

func (ostore *IpfsOS) Description() string {
	return "Pinata cloud IPFS driver."
}

func (ostore *IpfsSession) OS() OSDriver {
	return ostore.os
}

func (ostore *IpfsSession) EndSession() {
	// no op
}

func (ostore *IpfsSession) ListFiles(ctx context.Context, dir, delim string) (PageInfo, error) {
	panic("Listing is not supported by Pinata IPFS driver")
}

func (ostore *IpfsSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	fullPath := path.Join(ostore.filename, name)
	// just get the file through Pinata HTTP gateway
	resp, err := http.Get("https://gateway.pinata.cloud/ipfs/" + fullPath)
	if err != nil {
		return nil, err
	}
	res := &FileInfoReader{
		FileInfo: FileInfo{
			Name: name,
			Size: nil,
		},
		Body: resp.Body,
	}
	return res, nil
}

func (ostore *IpfsSession) IsExternal() bool {
	return false
}

func (ostore *IpfsSession) IsOwn(url string) bool {
	return true
}

func (ostore *IpfsSession) GetInfo() *OSInfo {
	return nil
}

func (ostore *IpfsSession) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	// concatenate filename with name argument to get full filename, both may be empty
	fullPath := ostore.getAbsolutePath(name)
	if fullPath == "" {
		// pinata requires name to be set
		fullPath = "data.bin"
	}
	cid, _, err := ostore.client.PinContent(ctx, fullPath, "", data)
	return cid, err
}

func (ostore *IpfsSession) getAbsolutePath(name string) string {
	resPath := path.Clean(ostore.filename + "/" + name)
	if resPath == "/" {
		return ""
	}
	return resPath
}
