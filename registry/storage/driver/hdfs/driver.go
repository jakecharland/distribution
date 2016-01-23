package hdfs

import (
  "fmt"
  "net/http"
  storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const driverName = "hdfs"

type DriverParameters struct {
	HdfsUrl       string
	Port          string
  RootDirectory string
  //TODO add user for now I will just use user docker
}



func init() {
	factory.Register(driverName, &hdfsDriverFactory{})
}

type hdfsDriverFactory struct{}

func (factory *hdfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters), nil
}

type driver struct {
	HdfsUrl       string
	Port          int32
	RootDirectory string

  pool  sync.Pool // pool []byte buffers used for WriteStream
  zeros []byte    // shared, zero-valued buffer used for WriteStream
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Amazon S3
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) *Driver {
	var rootDirectory = defaultRootDirectory
	if parameters != nil {
		rootDir, ok := parameters["rootdirectory"]
		if ok {
			rootDirectory = fmt.Sprint(rootDir)
		}
    hdfsUrl, ok := parameters["hdfsurl"]
    if ok{
      hdfsUrl = fmt.Sprintf(hdfsUrl)
    }
    portInt := 50070
    port, ok := parameters["port"]
    if ok{
      portInt = port.(int32)
    }
	}

  params := DriverParameters{
		hdfsUrl,
		portInt,
	  rootDirectory,
	}

	return New(params)
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {

  //TODO create the root directory inside of the hdfs if it doesnt already exist.
  d := &driver{
		HdfsUrl:        params.HdfsUrl,
		Port:           params.Port,
		RootDirectory:  params.RootDirectory,
	}

  return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {

}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {

}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

}

// WriteStream stores the contents of the provided io.Reader at a location
// designated by the given path.
func (d *driver) WriteStream(ctx context.Context, subPath string, offset int64, reader io.Reader) (nn int64, err error) {

}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {

}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {

}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
  //curl -i -X PUT "http://10.0.1.18:50070/webhdfs/v1/jake?op=RENAME&destination=<PATH>"
  baseURI, err := d.URLFor(ctx, sourcePath, nil)
  if err != nil {

  }
  client := &http.Client{
	   CheckRedirect: redirectPolicyFunc,
  }
  //TODO check if the file exists before calling delete
  baseURI += "?op=RENAME&destination=" + destPath
  req, err := http.NewRequest("PUT", baseURI, nil)
  resp, err := client.Do(req)
  return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
  //curl -i -X DELETE "http://10.0.1.18:50070/webhdfs/v1/jake?op=DELETE&recursive=true"
  baseURI, err := d.URLFor(ctx, subPath, nil)
  if err != nil {

  }
  client := &http.Client{
	   CheckRedirect: redirectPolicyFunc,
  }
  //TODO check if the file exists before calling delete
  baseURI += "?op=DELETE&recursive=true"
  req, err := http.NewRequest("DELETE", baseURI, nil)
  resp, err := client.Do(req)
  return err
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
  methodString := "GET"
  method, ok := options["method"]
  if ok {
    methodString, ok = method.(string)
    if !ok || (methodString != "GET" && methodString != "PUT") {
      return "", storagedriver.ErrUnsupportedMethod{}
    }
  }

  //TODO find out what path looks like most importantly does it have a leading or trailing '/'
  return "http://" + DriverParameters.HdfsUrl + ":" + DriverParameters.Port + "/webhdfs/v1/" + path, nil

}
