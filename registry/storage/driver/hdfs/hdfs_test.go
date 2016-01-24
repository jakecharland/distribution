package hdfs

import (
  storagedriver "github.com/docker/distribution/registry/storage/driver"
  "gopkg.in/check.v1"
  "testing"
  "io/ioutil"
  "os"
  "fmt"
  //"github.com/docker/distribution/registry/storage/driver/testsuites"
  "github.com/docker/distribution/context"
)
// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
  check.TestingT(t)
}

var hdfsDriverConstructor func(rootDirectory string) (storagedriver.StorageDriver, error)

func init() {
  hdfsUrl := "10.0.1.18"
  port := "50070"
  root, err := ioutil.TempDir("", "driver-")
  if err != nil {
		panic(err)
	}
	defer os.Remove(root)
  hdfsDriverConstructor = func(rootDirectory string) (storagedriver.StorageDriver, error) {
    parameters := DriverParameters{
      hdfsUrl,
      port,
      rootDirectory,
    }
    return New(parameters), nil
  }

  /*testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return hdfsDriverConstructor(root)
	}, skipHdfs)*/
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

	filename := "/jakeTest"
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
    fmt.Println("Ya broke it")
    fmt.Println(err)
  }
  fmt.Println(file)
}
