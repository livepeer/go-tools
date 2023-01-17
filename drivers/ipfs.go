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

func (session *IpfsSession) IsLocationAddressable() bool {
	return false
}

func (session *IpfsSession) OS() OSDriver {
	return session.os
}

func (session *IpfsSession) EndSession() {
	// no op
}

func (session *IpfsSession) ListFiles(ctx context.Context, cid, delim string) (PageInfo, error) {
	pinList, _, err := session.client.List(ctx, 1, 0, cid)
	pi := &singlePageInfo{
		files:       []FileInfo{},
		directories: []string{},
	}
	if err == nil && pinList.Count == 1 {
		size := pinList.Pins[0].Size
		pi.files = append(pi.files, FileInfo{Name: pinList.Pins[0].Metadata.Name, Size: &size,
			ETag: pinList.Pins[0].IPFSPinHash})
	}
	return pi, err
}

func (session *IpfsSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	fullPath := path.Join(session.filename, name)
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

func (session *IpfsSession) IsExternal() bool {
	return false
}

func (session *IpfsSession) IsOwn(url string) bool {
	return true
}

func (session *IpfsSession) GetInfo() *OSInfo {
	return nil
}

func (session *IpfsSession) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	// concatenate filename with name argument to get full filename, both may be empty
	fullPath := session.getAbsolutePath(name)
	if fullPath == "" {
		// pinata requires name to be set
		fullPath = "data.bin"
	}
	cid, _, err := session.client.PinContent(ctx, fullPath, "", data)
	return cid, err
}

func (session *IpfsSession) getAbsolutePath(name string) string {
	resPath := path.Clean(session.filename + "/" + name)
	if resPath == "/" {
		return ""
	}
	return resPath
}
