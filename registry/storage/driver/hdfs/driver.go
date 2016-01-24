package hdfs

import (
  "fmt"
  "net/http"
  "encoding/json"
  "sync"
  "io"
  "time"
  "strconv"

  "github.com/docker/distribution/context"
  storagedriver "github.com/docker/distribution/registry/storage/driver"
  "github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "hdfs"
const defaultRootDirectory = "/"
const minChunkSize = 5 << 20
const defaultHdfsUrl = "10.0.1.18"
const defaultPort = "50070"

const defaultChunkSize = 2 * minChunkSize

type DriverParameters struct {
	HdfsUrl       string
	Port          string
  RootDirectory string
  //TODO add user for now I will just use user docker
}

type FileStatusJson struct{
  FileStatus struct{
    AccessTime int  `json:"accessTime"`
    BlockSize  int  `json:"BlockSize"`
    Group      string `json:"group"`
    Length     int64  `json:"length"`
    ModificationTime  int  `json:"modificationTime"`
    Owner      string `json:"owner"`
    PathSuffix string `json:"pathSuffix"`
    Permission string  `json:"permission"`
    FileType   string  `json:"type"`
  }
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
	Port          string
	RootDirectory string

  pool  sync.Pool // pool []byte buffers used for WriteStream
  zeros []byte    // shared, zero-valued buffer used for WriteStream
  Client http.Client //http client for sending http requests to the hdfs
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
  hdfsUrl := defaultHdfsUrl
  portInt := defaultPort
	if parameters != nil {
		rootDir, ok := parameters["rootdirectory"]
		if ok {
			rootDirectory = fmt.Sprint(rootDir)
		}
    hdfsUrlTemp, ok := parameters["hdfsurl"]
    if ok{
      hdfsUrl = fmt.Sprint(hdfsUrlTemp)
    }
    portIntTemp, ok := parameters["port"]
    if ok{
      portInt = fmt.Sprint(portIntTemp)
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
  client := &http.Client{
	   CheckRedirect: redirectPolicyFunc,
  }
  d := &driver{
		HdfsUrl:        params.HdfsUrl,
		Port:           params.Port,
		RootDirectory:  params.RootDirectory,
    Client:         *client,
    zeros:         make([]byte, defaultChunkSize),
	}

  d.pool.New = func() interface{} {
		return make([]byte, defaultChunkSize)
	}

  return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}
}

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
  return nil, nil

}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
  return nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
  return nil, nil
}

// WriteStream stores the contents of the provided io.Reader at a location
// designated by the given path.
func (d *driver) WriteStream(ctx context.Context, subPath string, offset int64, reader io.Reader) (nn int64, err error) {
  return 0, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
  baseURI, err := d.URLFor(ctx, subPath, nil)
  if err != nil {

  }
  //TODO check if the file exists before calling delete
  baseURI += "?op=GETFILESTATUS"
  resp, err := http.Get(baseURI)
  defer resp.Body.Close()
  fileStatusJson := FileStatusJson{}
  err = getJson(resp, &fileStatusJson)

  if err != nil{
    return nil, err
  }

  fi := storagedriver.FileInfoFields{
		Path: subPath,
	}
  if fileStatusJson.FileStatus.FileType == "DIRECTORY" {
    fi.IsDir = true
  } else{
    fi.IsDir = false
    fi.Size = fileStatusJson.FileStatus.Length
  }


  timestamp, err := msToTime(strconv.Itoa(fileStatusJson.FileStatus.ModificationTime))
  if err != nil {
    return nil, err
  }
  fi.ModTime = timestamp
  return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
  return nil, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
  //curl -i -X PUT "http://10.0.1.18:50070/webhdfs/v1/jake?op=RENAME&destination=<PATH>"
  baseURI, err := d.URLFor(ctx, sourcePath, nil)
  if err != nil {

  }
  //TODO check if the file exists before calling delete
  baseURI += "?op=RENAME&destination=" + destPath
  req, err := http.NewRequest("PUT", baseURI, nil)
  resp, err := d.Client.Do(req)
  defer resp.Body.Close()
  return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
  //curl -i -X DELETE "http://10.0.1.18:50070/webhdfs/v1/jake?op=DELETE&recursive=true"
  baseURI, err := d.URLFor(ctx, subPath, nil)
  if err != nil {

  }
  //TODO check if the file exists before calling delete
  baseURI += "?op=DELETE&recursive=true"
  req, err := http.NewRequest("DELETE", baseURI, nil)
  resp, err := d.Client.Do(req)
  defer resp.Body.Close()
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
  return "http://" + d.HdfsUrl + ":" + d.Port + "/webhdfs/v1" + path, nil

}
// CheckRedirect specifies the policy for handling redirects.
// If CheckRedirect is not nil, the client calls it before
// following an HTTP redirect. The arguments req and via are
// the upcoming request and the requests made already, oldest
// first. If CheckRedirect returns an error, the Client's Get
// method returns both the previous Response and
// CheckRedirect's error (wrapped in a url.Error) instead of
// issuing the Request req.
//
// If CheckRedirect is nil, the Client uses its default policy,
// which is to stop after 10 consecutive requests.
func redirectPolicyFunc(req *http.Request, via []*http.Request) error {
  return nil
}

func getJson(r *http.Response, target interface{}) error {
    return json.NewDecoder(r.Body).Decode(target)
}

func msToTime(ms string) (time.Time, error) {
    msInt, err := strconv.ParseInt(ms, 10, 64)
    if err != nil {
        return time.Time{}, err
    }

    return time.Unix(0, msInt*int64(time.Millisecond)), nil
}
