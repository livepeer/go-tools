package drivers

import (
	"context"
	"encoding/base64"
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

const w3SDefaultSaveTimeout = 5 * time.Minute

var base64Url = base64.URLEncoding.WithPadding(base64.NoPadding)

var cidV1 = merkledag.V1CidPrefix()

// This represents the main CAR directory structure organized by pubId.
// Data for each pubId is removed after the CAR directory is published.
//
// Note that if Publish() is not called for the given pubId, it can cause memory leak.
// This will be fixed as part of https://github.com/livepeer/go-tools/issues/16.
var (
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
	ucanProof string
	dirPath   string
	pubId     string
}

var _ OSSession = (*W3sSession)(nil)

type W3sSession struct {
	os *W3sOS
}

func NewW3sDriver(ucanProof, dirPath, pubId string) *W3sOS {
	return &W3sOS{
		ucanProof: ucanProof,
		dirPath:   dirPath,
		pubId:     pubId,
	}
}

func (ostore *W3sOS) NewSession(filename string) OSSession {
	if filename != "" {
		return nil
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
	return nil, ErrNotSupported
}

func (session *W3sSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	return nil, ErrNotSupported
}

func (session *W3sSession) ReadDataRange(ctx context.Context, name, byteRange string) (*FileInfoReader, error) {
	return nil, ErrNotSupported
}

func (session *W3sSession) Presign(name string, expire time.Duration) (string, error) {
	return "", ErrNotSupported
}

func (session *W3sSession) IsExternal() bool {
	return false
}

func (session *W3sSession) IsOwn(url string) bool {
	// not used anywhere
	return false
}

func (session *W3sSession) GetInfo() *OSInfo {
	return nil
}

func (session *W3sSession) getAbsolutePath(name string) string {
	// not supported
	return ""
}

func (session *W3sSession) DeleteFile(ctx context.Context, name string) error {
	return ErrNotSupported
}

func (session *W3sSession) SaveData(ctx context.Context, name string, data io.Reader, fields *FileProperties, timeout time.Duration) (*SaveDataOutput, error) {
	if timeout <= 0 {
		timeout = w3SDefaultSaveTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	filePath, err := toFile(data)
	if err != nil {
		return nil, err
	}
	defer deleteFile(filePath)

	carPath, fileCid, err := ipfsCarPack(ctx, filePath)
	if err != nil {
		return nil, err
	}
	defer deleteFile(carPath)

	carCid, err := w3StoreCar(ctx, session.os.ucanProof, carPath)
	if err != nil {
		return nil, err
	}

	rCar := session.os.getRootCar()
	if err = rCar.addFile(ctx, session.os.dirPath, name, fileCid, carCid); err != nil {
		return nil, err
	}

	return &SaveDataOutput{URL: fileCid}, nil
}

func (rc *rootCar) addFile(ctx context.Context, dirPath, filename, fileCid, carCid string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.carCids = append(rc.carCids, carCid)

	// split path by "/", ignore empty strings
	dirPaths := strings.FieldsFunc(dirPath, func(c rune) bool { return c == '/' })

	newRoot, err := rc.addFileToDagRecursive(ctx, rc.root, dirPaths, filename, fileCid)
	if err != nil {
		return err
	}
	rc.root = newRoot

	return nil
}

// addFileToDagRecursive recursively creates the nodes defined by dirPaths and adds the CID link at the end.
// This uses the DFS algorithm in which visiting each node does the following actions:
// - if no more dirPaths, create a leaf with the link to the file CID, otherwise do the following
// - create directory defined by the first element in dirPaths
// - recursively create the rest of directories defined with the remaining dirPaths
// - recalculate the CID of the current node (it changed because its children have changed)
func (rc *rootCar) addFileToDagRecursive(ctx context.Context, n *merkledag.ProtoNode, dirPaths []string, filename, fileCid string) (*merkledag.ProtoNode, error) {
	if len(dirPaths) == 0 {
		// n is a leaf
		fCid, err := cid.Parse(fileCid)
		if err != nil {
			return nil, err
		}
		n.AddRawLink(filename, &format.Link{Cid: fCid})
		rc.dag.Add(ctx, n)
		return n, nil
	}

	// n is not a leaf, recursively update until the leaf
	rootPath, childPaths := dirPaths[0], dirPaths[1:]
	child, err := rc.getOrCreateChild(ctx, n, rootPath)
	if err != nil {
		return nil, err
	}
	child, err = rc.addFileToDagRecursive(ctx, child, childPaths, filename, fileCid)
	if err != nil {
		return nil, err
	}

	// CIDs of n and child have changed, update links and dag
	newN, err := n.UpdateNodeLink(rootPath, child)
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

func (ostore *W3sOS) Publish(ctx context.Context) (string, error) {
	rCar := ostore.getRootCar()
	rootCid := rCar.root.Cid().String()

	rCar.mu.Lock()
	if err := rCar.storeDir(ctx, ostore.ucanProof); err != nil {
		return "", err
	}
	carCids := rCar.carCids
	rCar.mu.Unlock()

	if err := w3UploadCar(ctx, ostore.ucanProof, rootCid, carCids); err != nil {
		return "", err
	}

	defer ostore.deleteRootCar()
	return fmt.Sprintf("ipfs://%s", rootCid), nil
}

func (rc *rootCar) storeDir(ctx context.Context, proof string) error {
	carFile, err := os.CreateTemp("", "car")
	if err != nil {
		return err
	}
	defer deleteFile(carFile.Name())
	car.WriteCar(ctx, rc.dag, []cid.Cid{rc.root.Cid()}, carFile, merkledag.IgnoreMissing())
	carFile.Close()

	storedCid, err := w3StoreCar(ctx, proof, carFile.Name())
	if err != nil {
		return err
	}
	rc.carCids = append(rc.carCids, storedCid)

	return nil
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

func newDir() *merkledag.ProtoNode {
	n := unixfs.EmptyDirNode()
	n.SetCidBuilder(cidV1)
	return n
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

func deleteFile(filePath string) {
	os.RemoveAll(filePath)
}

// ipfsCarPack uses external binary 'ipfs-car' to convert a file into a CAR.
func ipfsCarPack(ctx context.Context, filePath string) (string, string, error) {
	fCar, err := os.CreateTemp("", "w3s-car")
	if err != nil {
		return "", "", err
	}

	out, err := exec.CommandContext(ctx, "ipfs-car", "--wrapWithDirectory", "false", "--pack", filePath, "--output", fCar.Name()).CombinedOutput()
	if err != nil {
		deleteFile(fCar.Name())
		return "", "", fmt.Errorf("executing 'ipfs-car' failed, command output: %s, err: %v", string(out), err)
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

// w3StoreCar uses external binary `w3` to store a CAR file in web3.storage.
func w3StoreCar(ctx context.Context, proof, carPath string) (string, error) {
	out, err := runWithCredentials(exec.CommandContext(ctx, "livepeer-w3", "can", "store", "add", carPath), proof)
	if err != nil {
		return "", fmt.Errorf("executing 'livepeer-w3 can store add' failed, command output: %s, err: %v", string(out), err)
	}
	return strings.TrimSpace(string(out)), nil
}

// w3StoreCar uses external binary `w3` to bind and publish multiple CARs.
func w3UploadCar(ctx context.Context, proof, rootCid string, carCids []string) error {
	args := []string{"can", "upload", "add"}
	args = append(args, rootCid)
	args = append(args, carCids...)
	out, err := runWithCredentials(exec.CommandContext(ctx, "livepeer-w3", args...), proof)
	if err != nil {
		return fmt.Errorf("executing 'livepeer-w3 can store upload' failed, command output: %s, err: %v", string(out), err)
	}
	return nil
}

func runWithCredentials(cmd *exec.Cmd, proof string) ([]byte, error) {
	if proof == "" {
		return nil, fmt.Errorf("UCAN proof not found")
	}
	base64Proof, err := base64UrlToBase64(proof)
	if err != nil {
		return nil, fmt.Errorf("invalid UCAN proof format: %s", err)
	}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("W3_DELEGATION_PROOF='%s'", base64Proof))
	return cmd.CombinedOutput()
}

func base64UrlToBase64(proof string) (string, error) {
	ucanProofByte, err := base64Url.DecodeString(proof)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ucanProofByte), nil
}
