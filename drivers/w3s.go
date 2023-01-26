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

// This represents the CAR structure, the data is removed after the CAR is uploaded / published.
// Note that this makes the driver stateful as it stores its state in memory.
var (
	cidV1 = merkledag.V1CidPrefix()

	carsToPublish map[string]*carToPublish
	mu            sync.Mutex
)

type carToPublish struct {
	root       *merkledag.ProtoNode
	dagService format.DAGService
	carCids    []string
}

func newCarToPublish() *carToPublish {
	return &carToPublish{
		root:       newDagNodeDirectory(),
		dagService: merkledag.NewDAGService(bserv.New(blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())), nil)),
	}
}

type node struct {
	fileCid  string
	carCid   string
	children map[string]*node
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
	mu.Lock()
	defer mu.Unlock()
	if carsToPublish == nil {
		carsToPublish = make(map[string]*carToPublish)
	}

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
	// TODO
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
	// TODO
	return nil, nil
}

func (session *W3sSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	// TODO
	return nil, nil
}

func (session *W3sSession) IsExternal() bool {
	// TODO
	return false
}

func (session *W3sSession) IsOwn(url string) bool {
	// TODO
	return true
}

func (session *W3sSession) GetInfo() *OSInfo {
	// TODO
	return nil
}

func (session *W3sSession) getAbsolutePath(name string) string {
	// TODO
	return ""
}

func (session *W3sSession) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	filePath, err := createFile(data)
	if err != nil {
		return "", err
	}
	defer deleteFile(filePath)

	carPath, fileCid, err := packCar(filePath)
	if err != nil {
		return "", err
	}
	defer deleteFile(carPath)
	fmt.Println("FILE CID:")
	fmt.Println(fileCid)

	carCid, err := storeCar(carPath)
	if err != nil {
		return "", err
	}
	//carCid := "bagbaieraqrx7ttm5hrcef6uvkx25z2l7bjvqfixmeo6q6cg5fqir7qa2n5vq"
	fmt.Println("CAR CID:")
	fmt.Println(carCid)

	session.addToPublish(name, fileCid, carCid)

	return fileCid, nil
}

func (session *W3sSession) addToPublish(fileName, fileCid, carCid string) {
	mu.Lock()
	defer mu.Unlock()

	dirPaths := strings.FieldsFunc(session.os.dirPath, func(c rune) bool { return c == '/' })
	//filePaths := strings.Split(strings.Trim(session.os.dirPath, "/"), "/")
	//filePaths = append(filePaths, fileName)

	c := session.os.getCarToPublish()
	c.root, _ = addFile(c, dirPaths, fileName, fileCid)
	c.carCids = append(c.carCids, carCid)
}

func addFile(c *carToPublish, paths []string, filename, fileCid string) (*merkledag.ProtoNode, error) {
	return addFileToNode(c.root, c, paths, filename, fileCid)
}

func addFileToNode(n *merkledag.ProtoNode, c *carToPublish, paths []string, filename, fileCid string) (*merkledag.ProtoNode, error) {
	if len(paths) == 0 {
		fCid, err := cid.Parse(fileCid)
		if err != nil {
			return nil, err
		}
		n.AddRawLink(filename, &format.Link{Cid: fCid})
		c.dagService.Add(context.TODO(), n)
		return n, nil
	}

	head, tail := paths[0], paths[1:]
	pn, err := n.GetLinkedProtoNode(context.TODO(), c.dagService, head)
	if err == merkledag.ErrLinkNotFound {
		pn = newDagNodeDirectory()
		n.AddNodeLink(head, pn)
	} else if err != nil {
		return nil, err
	}
	pn, err = addFileToNode(pn, c, tail, filename, fileCid)
	if err != nil {
		return nil, err
	}
	fmt.Println("Adding node CID: ", n.Cid())
	nn, err := n.UpdateNodeLink(head, pn)
	if err != nil {
		return nil, err
	}
	err = c.dagService.Add(context.TODO(), nn)
	if err != nil {
		return nil, err
	}
	return nn, nil
}

func newDagNodeDirectory() *merkledag.ProtoNode {
	n := unixfs.EmptyDirNode()
	n.SetCidBuilder(cidV1)
	return n
}

func (ostore *W3sOS) getCarToPublish() *carToPublish {
	pubId := ostore.pubId
	if _, ok := carsToPublish[pubId]; !ok {
		carsToPublish[pubId] = newCarToPublish()
	}
	root, _ := carsToPublish[pubId]
	return root
}

func makeNodes(n *node, paths []string) *node {
	cn := n
	for _, p := range paths {
		if p != "" {
			if _, ok := cn.children[p]; !ok {
				cn.children[p] = newNode()
			}
			cn = cn.children[p]
		}
	}
	return cn
}

func newNode() *node {
	return &node{children: make(map[string]*node)}
}

func createFile(data io.Reader) (string, error) {
	fRaw, err := os.CreateTemp("", "w3-raw")
	if err != nil {
		return "", err
	}
	defer fRaw.Close()

	if _, err = io.Copy(fRaw, data); err != nil {
		deleteFile(fRaw.Name())
		return "", err
	}
	return fRaw.Name(), nil
}

func packCar(filePath string) (string, string, error) {
	fCar, err := os.CreateTemp("", "w3-car")
	if err != nil {
		return "", "", err
	}
	defer fCar.Close()

	out, err := exec.Command("ipfs-car", "--wrapWithDirectory", "false", "--pack", filePath, "--output", fCar.Name()).Output()
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
	return fCar.Name(), matches[1], nil
}

func storeCar(carPath string) (string, error) {
	out, err := exec.Command("w3", "can", "store", "add", carPath).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func deleteFile(filePath string) {
	os.RemoveAll(filePath)
}

//func upload(filePath string) fileCar {
//	// Currently we upload the file with `w3 up`, but we'll probably need to change it to encoding file as CAR and later uploading with `w3 can store add`.
//	out, _ := exec.Command("w3", "up", filePath).Output()
//	r := regexp.MustCompile(`http.*`)
//	matches := r.FindAllString(string(out), -1)
//	// When we upload the file with `w3 can store add`, then we'll be able to get the file's CID (needed later to comstruct the CAR directory)
//	return fileCar{url: matches[0]}
//}

func (ostore *W3sOS) Publish() string {
	mu.Lock()
	defer mu.Unlock()

	carFile, _ := os.Create("test.car")
	defer carFile.Close()

	c := ostore.getCarToPublish()

	rootLink := storeDirRecursively(c.root, c.dagService, c, "")

	//time.Sleep(1 * time.Second)
	//uploadCmd := fmt.Sprintf(strings.Join([]string{"w3", "can", "upload", "add", dir.Cid().String(), "bagbaieraryp54uvfvxrpedxmmoms36g2hfanxuimvzk3i4l54ndaylz67voa", storedCid}, " "))
	//fmt.Println(uploadCmd)

	out, err := exec.Command("w3", "can", "upload", "add", rootLink.Cid.String(), c.carCids[0], c.carCids[1], c.carCids[2], c.carCids[3], c.carCids[4]).Output()
	if err != nil {
		fmt.Println(err)
		fmt.Println(string(out))
	}
	fmt.Println(string(out))
	fmt.Println("Stored at: ", rootLink.Cid.String())

	//var cidsToUpload []string
	//root := ostore.getRoot()
	//fileCid, carCid, err := createDirCar(root, &cidsToUpload)
	//if err != nil {
	//	// TODO: handle error
	//}
	//fmt.Printf("w3 upload %s %s cidsToUpload\n", fileCid, carCid)
	//
	//fmt.Println(cidsToUpload)
	//
	//// Store the CAR directory in web3.storage
	//// Currently uploading the whole tmp dir with `w3 up`, but we'll change it constructing the CAR dir structure on our own
	////prefix, _ := ostore.getPrefixAndPath()
	////mu.Lock()
	////tmpDir := carsToPublish[prefix].tmpDir
	////mu.Unlock()
	////
	////fCar := upload(path.Join(tmpDir, prefix))
	////os.RemoveAll(tmpDir)
	////mu.Lock()
	////delete(carsToPublish, prefix)
	////mu.Unlock()
	////
	////return fCar.url
	return fmt.Sprintf("https://%s.ipfs.w3s.link", rootLink.Cid)
}

func storeDirRecursively(n format.Node, dagService format.DAGService, c *carToPublish, name string) *format.Link {
	var nonDirLinks []*format.Link
	for _, l := range n.Links() {
		child, err := l.GetNode(context.TODO(), dagService)
		if err != nil { // link to a file
			nonDirLinks = append(nonDirLinks, l)
		} else { // link to a directory
			nonDirLinks = append(nonDirLinks, storeDirRecursively(child, dagService, c, l.Name))
		}
	}

	res := newDagNodeDirectory()
	res.SetLinks(nonDirLinks)
	dagService.Add(context.TODO(), res)

	carFile, _ := os.CreateTemp("", "car")
	defer carFile.Close()
	car.WriteCar(context.TODO(), dagService, []cid.Cid{res.Cid()}, carFile)
	storedCid, _ := storeCar(carFile.Name())
	c.carCids = append(c.carCids, storedCid)
	return &format.Link{Name: name, Cid: res.Cid()}

}

func createDirCar(n *node, cidsToUpload *[]string) (string, string, error) {
	if len(n.children) > 0 {
		var dirEntries []string

		// Create and store cars for each subdirectory
		for name, val := range n.children {
			fmt.Printf("Creating dir car for: %s\n", name)
			fileCid, carCid, err := createDirCar(val, cidsToUpload)
			if err != nil {
				return "", "", err
			}
			*cidsToUpload = append(*cidsToUpload, carCid)

			dirEntries = append(dirEntries, fmt.Sprintf("{\"name\":\"%s\",\"link\":{\"cid\":\"%s\"}}\n", name, fileCid))
		}

		carPath, dirCid, err := createCar(dirEntries)
		if err != nil {
			return "", "", err
		}
		//carCid, err := storeCar(carPath)
		//if err != nil {
		//	return "", "", err
		//}
		fmt.Println(carPath)
		carCid := "#$%^"
		return dirCid, carCid, nil
	}

	return n.fileCid, n.carCid, nil
}

func createCar(entries []string) (string, string, error) {
	// TODO: Check if we can use go-car library to create a directory from file links
	// TODO: Plan B: Use JS code from Yondon
	// https://golang.hotexamples.com/examples/github.com.ipfs.go-ipfs.unixfs/-/FilePBData/golang-filepbdata-function-examples.html#0xbb2d9e901a8466c5fab9a918fdee22e9e56885f589b08d5fd56e09b8422eddf8-449,,469,
	// https://github.com/web3-storage/go-w3s-client/blob/main/put.go#L29
	return "file-path", "car-cid", nil

}

func (ostore *W3sOS) getPrefixAndPath() (string, string) {
	return ostore.pubId, ostore.dirPath
}
