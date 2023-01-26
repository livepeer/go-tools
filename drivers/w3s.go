package drivers

import (
	"context"
	"fmt"
	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipld/go-car"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	cidV1 = merkledag.V1CidPrefix()

	// This represents the main CAR directory structure organized by pubId.
	// Data for each pubId is removed after the CAR directory is published.
	dataToPublish   = make(map[string]*rootCar)
	dataToPublishMu sync.Mutex
)

type rootCar struct {
	root    *merkledag.ProtoNode
	dag     format.DAGService
	carCids []string
	mu      sync.Mutex
}

func newRootCar() *rootCar {
	return &rootCar{
		root: newDir(),
		dag:  merkledag.NewDAGService(bserv.New(blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())), nil)),
	}
}

type W3sOS struct {
	ucanKey   string
	ucanProof string
	dirPath   string
	pubId     string
}

type W3sSession struct {
	os *W3sOS
}

func NewW3sDriver(ucanKey, ucanProof, dirPath, pubId string) *W3sOS {
	return &W3sOS{
		ucanKey:   ucanKey,
		ucanProof: ucanProof,
		dirPath:   dirPath,
		pubId:     pubId,
	}
}

func (ostore *W3sOS) NewSession(filename string) OSSession {
	if filename != "" {
		panic("File names are not supported by W3S driver")
	}
	session := &W3sSession{
		os: ostore,
	}
	return session
}

func (ostore *W3sOS) UriSchemes() []string {
	return []string{}
}

func (ostore *W3sOS) Description() string {
	return "Web3 Storage driver."
}

func (session *W3sSession) OS() OSDriver {
	return session.os
}

func (session *W3sSession) EndSession() {
	// no op
}

func (session *W3sSession) ListFiles(ctx context.Context, cid, delim string) (PageInfo, error) {
	// not supported
	return nil, nil
}

func (session *W3sSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	// not supported
	return nil, nil
}

func (session *W3sSession) IsExternal() bool {
	return false
}

func (session *W3sSession) IsOwn(url string) bool {
	return true
}

func (session *W3sSession) GetInfo() *OSInfo {
	return nil
}

func (session *W3sSession) getAbsolutePath(name string) string {
	// not supported
	return ""
}

func (session *W3sSession) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	filePath, err := toFile(data)
	if err != nil {
		return "", err
	}
	defer deleteFile(filePath)

	carPath, fileCid, err := packCar(ctx, filePath)
	if err != nil {
		return "", err
	}
	defer deleteFile(carPath)

	carCid, err := w3StoreCar(ctx, carPath)
	if err != nil {
		return "", err
	}

	rCar := session.os.getRootCar()
	if err = rCar.addFile(ctx, session.os.dirPath, name, fileCid, carCid); err != nil {
		return "", err
	}

	return fileCid, nil
}

func toFile(data io.Reader) (string, error) {
	fRaw, err := os.CreateTemp("", "w3s-raw")
	if err != nil {
		return "", err
	}

	if _, err = io.Copy(fRaw, data); err != nil {
		deleteFile(fRaw.Name())
		return "", err
	}

	defer fRaw.Close()
	return fRaw.Name(), nil
}

func packCar(ctx context.Context, filePath string) (string, string, error) {
	fCar, err := os.CreateTemp("", "w3s-car")
	if err != nil {
		return "", "", err
	}

	out, err := exec.CommandContext(ctx, "ipfs-car", "--wrapWithDirectory", "false", "--pack", filePath, "--output", fCar.Name()).Output()
	if err != nil {
		deleteFile(fCar.Name())
		return "", "", err
	}

	r := regexp.MustCompile(`root CID: ([A-Za-z0-9]+)`)
	matches := r.FindStringSubmatch(string(out))
	if len(matches) < 2 {
		deleteFile(fCar.Name())
		return "", "", fmt.Errorf("cannot find root file CID in the output: %s", string(out))
	}
	fileCid := matches[1]

	defer fCar.Close()
	return fCar.Name(), fileCid, nil
}

func (rc *rootCar) addFile(ctx context.Context, dirPath, filename, fileCid, carCid string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.carCids = append(rc.carCids, carCid)

	// split path by "/", ignore empty strings
	dirPaths := strings.FieldsFunc(dirPath, func(c rune) bool { return c == '/' })

	// Add file into dag
	newRoot, err := rc.addFileToNode(ctx, rc.root, dirPaths, filename, fileCid)
	if err != nil {
		return err
	}
	rc.root = newRoot
	return nil
}

func (rc *rootCar) addFileToNode(ctx context.Context, n *merkledag.ProtoNode, dirPaths []string, filename, fileCid string) (*merkledag.ProtoNode, error) {
	if len(dirPaths) == 0 {
		// n is already a leaf
		fCid, err := cid.Parse(fileCid)
		if err != nil {
			return nil, err
		}
		n.AddRawLink(filename, &format.Link{Cid: fCid})
		rc.dag.Add(ctx, n)
		return n, nil
	}

	// n is not yet a leaf, recursively updating the leaf
	head, tail := dirPaths[0], dirPaths[1:]
	child, err := rc.getOrCreateChild(ctx, n, head)
	if err != nil {
		return nil, err
	}
	child, err = rc.addFileToNode(ctx, child, tail, filename, fileCid)
	if err != nil {
		return nil, err
	}

	// CIDs of n and child has changes, update links and dag
	newN, err := n.UpdateNodeLink(head, child)
	if err != nil {
		return nil, err
	}
	if err = rc.dag.Remove(ctx, n.Cid()); err != nil {
		return nil, err
	}
	if err = rc.dag.Add(ctx, newN); err != nil {
		return nil, err
	}
	return newN, nil
}

func (rc *rootCar) getOrCreateChild(ctx context.Context, n *merkledag.ProtoNode, linkName string) (*merkledag.ProtoNode, error) {
	child, err := n.GetLinkedProtoNode(ctx, rc.dag, linkName)
	if err == merkledag.ErrLinkNotFound {
		child = newDir()
		n.AddNodeLink(linkName, child)
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

func newDir() *merkledag.ProtoNode {
	n := unixfs.EmptyDirNode()
	n.SetCidBuilder(cidV1)
	return n
}

func (ostore *W3sOS) getRootCar() *rootCar {
	dataToPublishMu.Lock()
	defer dataToPublishMu.Unlock()

	if _, ok := dataToPublish[ostore.pubId]; !ok {
		dataToPublish[ostore.pubId] = newRootCar()
	}
	return dataToPublish[ostore.pubId]
}

func (ostore *W3sOS) deleteRootCar() {
	dataToPublishMu.Lock()
	defer dataToPublishMu.Unlock()

	delete(dataToPublish, ostore.pubId)
}

func deleteFile(filePath string) {
	os.RemoveAll(filePath)
}

func (ostore *W3sOS) Publish(ctx context.Context) (string, error) {
	rCar := ostore.getRootCar()
	defer ostore.deleteRootCar()

	rCar.mu.Lock()
	rootLink, err := rCar.storeDir(ctx, rCar.root, "")
	if err != nil {
		return "", nil
	}
	carCids := rCar.carCids
	rCar.mu.Unlock()

	if err := w3UploadCar(ctx, rootLink.Cid.String(), carCids); err != nil {
		return "", nil
	}

	fmt.Println("Stored at: ", rootLink.Cid.String())
	return fmt.Sprintf("https://%s.ipfs.w3s.link", rootLink.Cid), nil
}

func (rc *rootCar) storeDir(ctx context.Context, n format.Node, linkName string) (*format.Link, error) {
	// Technically it should be enough to store one CAR with the whole directory dag,
	// however car.WriteCar() fails when some links are directories and some are pure CIDs without data.
	// That is why we need to create a separate CAR for each directory in the dag.
	var nonDirLinks []*format.Link
	for _, l := range n.Links() {
		child, err := l.GetNode(ctx, rc.dag)
		if err != nil {
			// link to a file
			nonDirLinks = append(nonDirLinks, l)
		} else {
			// link to a directory, recursively convert store dir CAR and update the link
			newLink, err := rc.storeDir(ctx, child, l.Name)
			if err != nil {
				return nil, err
			}
			nonDirLinks = append(nonDirLinks, newLink)
		}
	}

	// rewrite new links, which now contains only CIDs
	newN := newDir()
	if err := newN.SetLinks(nonDirLinks); err != nil {
		return nil, err
	}
	if err := rc.dag.Remove(ctx, n.Cid()); err != nil {
		return nil, err
	}
	if err := rc.dag.Add(ctx, newN); err != nil {
		return nil, err
	}

	// Create directory CAR
	carFile, err := os.CreateTemp("", "car")
	if err != nil {
		return nil, err
	}
	defer deleteFile(carFile.Name())
	// Ignore any errors returned from car.WriteCar(), because it reports errors for links without data
	car.WriteCar(ctx, rc.dag, []cid.Cid{newN.Cid()}, carFile)
	carFile.Close()

	storedCid, err := w3StoreCar(ctx, carFile.Name())
	if err != nil {
		return nil, err
	}
	rc.carCids = append(rc.carCids, storedCid)

	return &format.Link{Name: linkName, Cid: newN.Cid()}, nil
}

func w3StoreCar(ctx context.Context, carPath string) (string, error) {
	out, err := exec.CommandContext(ctx, "w3", "can", "store", "add", carPath).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func w3UploadCar(ctx context.Context, rootCid string, carCids []string) error {
	args := []string{"can", "upload", "add"}
	args = append(args, rootCid)
	args = append(args, carCids...)
	_, err := exec.CommandContext(ctx, "w3", args...).Output()
	return err
}
