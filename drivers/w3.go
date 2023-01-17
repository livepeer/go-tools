package drivers

import (
	"context"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

// This represents the CAR structure, the data is removed after the CAR is uploaded / published.
// Note that this makes the driver stateful as it stores its state in memory.
var (
	carsToPublish map[string]car
	mu            sync.Mutex
)

type car struct {
	filePaths map[string]fileCar
	// tmpDir may not be needed when we switch from `w3 up` to `w3 can store add`
	tmpDir string
}
type fileCar struct {
	url string
	cid string
}

type W3OS struct {
	osURL string
	proof string
}

type W3Session struct {
	os *W3OS
}

func NewW3Driver(osURL, proof string) *W3OS {
	mu.Lock()
	defer mu.Unlock()
	if carsToPublish == nil {
		carsToPublish = make(map[string]car)
	}

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

func (session *W3Session) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	// Prepare CARs in-memory cache and its tmp directory
	// Note that the tmp directory may not be needed when we stop move to creating the CAR dir on our own
	prefix, fPath := session.os.getPrefixAndPath()
	mu.Lock()
	_, ok := carsToPublish[prefix]
	if !ok {
		carsToPublish[prefix] = car{
			filePaths: make(map[string]fileCar),
			tmpDir:    os.TempDir(),
		}
	}
	tmpDir := carsToPublish[prefix].tmpDir
	mu.Unlock()

	// Copy file into the tmp directory before uploading it to web3.storage
	dirPath := path.Join(tmpDir, prefix, fPath)
	os.MkdirAll(dirPath, os.ModePerm)
	filePath := path.Join(dirPath, name)
	f, _ := os.Create(filePath)
	io.Copy(f, data)
	f.Close()

	// Upload a single file into web3.storage
	fCar := upload(filePath)

	// Update in memory CAR structure with the uploaded file
	mu.Lock()
	carsToPublish[prefix].filePaths[fPath] = fCar
	mu.Unlock()

	return fCar.url, nil
}

func (ostore *W3OS) Publish() string {
	// Store the CAR directory in web3.storage
	// Currently uploading the whole tmp dir with `w3 up`, but we'll change it constructing the CAR dir structure on our own
	prefix, _ := ostore.getPrefixAndPath()
	mu.Lock()
	tmpDir := carsToPublish[prefix].tmpDir
	mu.Unlock()

	fCar := upload(path.Join(tmpDir, prefix))
	os.RemoveAll(tmpDir)
	mu.Lock()
	delete(carsToPublish, prefix)
	mu.Unlock()

	return fCar.url
}

func upload(filePath string) fileCar {
	// Currently we upload the file with `w3 up`, but we'll probably need to change it to encoding file as CAR and later uploading with `w3 can store add`.
	out, _ := exec.Command("w3", "up", filePath).Output()
	r := regexp.MustCompile(`http.*`)
	matches := r.FindAllString(string(out), -1)
	// When we upload the file with `w3 can store add`, then we'll be able to get the file's CID (needed later to comstruct the CAR directory)
	return fileCar{url: matches[0]}
}

func (os *W3OS) getPrefixAndPath() (string, string) {
	u, _ := url.Parse(os.osURL)
	splitPath := strings.Split(u.Path, "/")
	return splitPath[1], path.Join(splitPath[2:]...)
}
