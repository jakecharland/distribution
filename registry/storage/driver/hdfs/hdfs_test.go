package hdfs

import (
  //storagedriver "github.com/docker/distribution/registry/storage/driver"
  "gopkg.in/check.v1"
  "testing"
  "io/ioutil"
  "os"
  "fmt"
  //"github.com/docker/distribution/registry/storage/driver/testsuites"
  "github.com/docker/distribution/context"
)

var test *testing.T
// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
  check.TestingT(t)
}

var hdfsDriverConstructor func(rootDirectory string) (*Driver, error)
var skipHdfs func() string

func init() {
  hdfsUrl := "10.0.1.18"
  port := "50070"
  root, err := ioutil.TempDir("", "driver-")
  if err != nil {
		panic(err)
	}
	defer os.Remove(root)
  hdfsDriverConstructor = func(rootDirectory string) (*Driver, error) {
    parameters := DriverParameters{
      hdfsUrl,
      port,
      rootDirectory,
    }
    return New(parameters), nil
  }

  // Skip S3 storage driver tests if environment variable parameters are not provided
  skipHdfs = func() string {
    return ""
  }

  /*testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return hdfsDriverConstructor(root)
	}, skipHdfs)*/
  TestHdfsFileStat(test)
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
    fmt.Println(err)
    return
	}
  fmt.Println(fi)
  if fi.IsDir() {
    fmt.Println("IsDir")
  }
}
