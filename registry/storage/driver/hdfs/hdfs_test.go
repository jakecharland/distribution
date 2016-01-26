package hdfs

import (
  storagedriver "github.com/docker/distribution/registry/storage/driver"
  "gopkg.in/check.v1"
  "testing"
  "io/ioutil"
  "io"
  "os"
  "fmt"
  //"crypto/sha1"
  "sync"
  "strconv"
  "bytes"
  "github.com/jakecharland/distribution/registry/storage/driver/testsuites"
  "github.com/docker/distribution/context"
)
// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
  check.TestingT(t)
}

var skipHdfs func() string

var hdfsDriverConstructor func(rootDirectory string) (storagedriver.StorageDriver, error)

func init() {
  hdfsURL := "172.17.100.236"
  //hdfsURL := "10.0.1.18"
  port := "50070"
  root, err := ioutil.TempDir("", "driver-")
  if err != nil {
		panic(err)
	}
	defer os.Remove(root)
  hdfsDriverConstructor = func(rootDirectory string) (storagedriver.StorageDriver, error) {
    parameters := DriverParameters{
      hdfsURL,
      port,
      rootDirectory,
    }
    return New(parameters), nil
  }

  skipHdfs = func() string {
    return ""
  }

  testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return hdfsDriverConstructor(root)
	}, skipHdfs)
  //TestHdfsFileStat(test)
}

func TestHdfsFileStat(t *testing.T) {
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}

	filename := "/docker"
	ctx := context.Background()
	fi, err := rootedDriver.Stat(ctx, filename)
	if err != nil {
    t.FailNow()
	}
  if !fi.IsDir() {
    t.FailNow()
  }
}

func TestGetFile(t *testing.T){
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}

	filename := "/docker/hdfsFile.txt"
	ctx := context.Background()
  file, err := rootedDriver.GetContent(ctx, filename)
  if err != nil{
    t.FailNow()
  }
  if file == nil {
    t.FailNow()
  }
}

func TestPutFile(t *testing.T){
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}

	path := "/docker/hdfsFile1.txt"
	ctx := context.Background()
  filename := "/Users/jakecharland/greetings.txt"
  fileBytes, err := ioutil.ReadFile(filename)
  if err != nil {
    t.FailNow()
  }
  err = rootedDriver.PutContent(ctx, path, fileBytes)
  if err != nil{
    t.FailNow()
  }
}

func TestMoveFile(t *testing.T){
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}
  path := "/docker/hdfsFile1.txt"
  destPath := "/docker/hdfsFile2.txt"
  ctx := context.Background()
  err = rootedDriver.Move(ctx, path, destPath)
  if err != nil {
    t.FailNow()
  }
}

func TestDeleteFile(t *testing.T){
  t.Skip("skip delete")
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}
  delPath := "/docker/hdfsFile2.txt"
  ctx := context.Background()
  err = rootedDriver.Delete(ctx, delPath)
  if err != nil {
    t.FailNow()
  }
}

func TestListDir(t *testing.T){
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}
  path := "/docker"
  ctx := context.Background()
  dir, err := rootedDriver.List(ctx, path)
  if err != nil {
    fmt.Println(err)
    t.FailNow()
  }
  fmt.Println(dir)
}

func TestWriteStream(t *testing.T){
  validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
    fmt.Println(err)
    return
	}
	defer os.Remove(validRoot)

  rootedDriver, err := hdfsDriverConstructor(validRoot)
	if err != nil {
    fmt.Println(err)
    return
	}
  //checksum := sha1.New()
	var fileSize int64 = 3 * 1024 * 1024

	//contents := newRandReader(fileSize)
  path := "/docker/testWriteLargeStream.txt"
  ctx := context.Background()
  written, err := rootedDriver.WriteStream(ctx, path, 0, bytes.NewReader(randomContents(fileSize)))
  if err != nil {
    fmt.Println(err)
    t.FailNow()
  }
  if written != fileSize {
    fmt.Println("Expected to write " + strconv.FormatInt(fileSize,10) + " bytes but wrote " + strconv.FormatInt(written,10) + " bytes.")
    t.FailNow()
  }
}

type randReader struct {
	r int64
	m sync.Mutex
}
var randomBytes = make([]byte, 128<<20)

func randomContents(length int64) []byte {
	return randomBytes[:length]
}

func (rr *randReader) Read(p []byte) (n int, err error) {
	rr.m.Lock()
	defer rr.m.Unlock()

	n = copy(p, randomContents(int64(len(p))))
	rr.r -= int64(n)

	if rr.r <= 0 {
		err = io.EOF
	}

	return
}

func newRandReader(n int64) *randReader {
	return &randReader{r: n}
}
