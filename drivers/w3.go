package drivers

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"time"
)

type W3OS struct {
	osURL string
	proof string
}

type W3Session struct {
	os *W3OS
}

func NewW3Driver(osURL, proof string) *W3OS {
	return &W3OS{
		osURL: osURL,
		proof: proof,
	}
}

func (ostore *W3OS) NewSession(filename string) OSSession {
	if filename != "" {
		panic("File names are not supported by Pinata IPFS driver")
	}
	session := &W3Session{
		os: ostore,
	}
	return session
}

func (ostore *W3OS) UriSchemes() []string {
	// TODO
	return []string{}
}

func (ostore *W3OS) Description() string {
	return "Web3 Storage driver."
}

func (session *W3Session) OS() OSDriver {
	return session.os
}

func (session *W3Session) EndSession() {
	// no op
}

func (session *W3Session) ListFiles(ctx context.Context, cid, delim string) (PageInfo, error) {
	// TODO
	return nil, nil
}

func (session *W3Session) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	// TODO
	return nil, nil
}

func (session *W3Session) IsExternal() bool {
	// TODO
	return false
}

func (session *W3Session) IsOwn(url string) bool {
	// TODO
	return true
}

func (session *W3Session) GetInfo() *OSInfo {
	// TODO
	return nil
}

func (session *W3Session) getAbsolutePath(name string) string {
	// TODO
	return ""
}

func (session *W3Session) IsLocationAddressable() bool {
	return false
}

func (session *W3Session) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	// Store file temporarily into filesystem
	tmpDir, _ := os.MkdirTemp("", "w3")
	filePath := path.Join(tmpDir, name)
	f, _ := os.Create(filePath)
	io.Copy(f, data)
	f.Close()

	// Upload a single file into web3.storage
	url := upload(filePath)

	// Remove the file from filesystem
	os.RemoveAll(tmpDir)

	return url, nil
}

func upload(filePath string) string {
	out, _ := exec.Command("w3", "up", filePath).Output()
	r := regexp.MustCompile(`http.*`)
	matches := r.FindAllString(string(out), -1)
	return matches[0]
}
