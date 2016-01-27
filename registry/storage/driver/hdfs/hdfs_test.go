package hdfs

import (
  "gopkg.in/check.v1"
  "testing"
  "io/ioutil"
  "os"

  "github.com/jakecharland/distribution/registry/storage/driver/testsuites"
  storagedriver "github.com/docker/distribution/registry/storage/driver"
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
  username := "jakecharland"
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
      username,
    }
    return New(parameters), nil
  }

  skipHdfs = func() string {
    return ""
  }

  testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return hdfsDriverConstructor(root)
	}, skipHdfs)
}
