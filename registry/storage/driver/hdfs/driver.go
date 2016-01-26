package hdfs

import (
  "fmt"
  "net/http"
  "encoding/json"
  "sync"
  "io"
  "time"
  "strconv"
  "io/ioutil"
  "errors"
  "bytes"
  "path"

  "github.com/docker/distribution/context"
  storagedriver "github.com/docker/distribution/registry/storage/driver"
  "github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "hdfs"
const defaultRootDirectory = "/"
const defaultHdfsURL = "10.0.1.18"
const defaultPort = "50070"

//Set defaultChunkSize to 5mb which is the default chunk size for hdfs
//const defaultChunkSize = 1.25e+8
const defaultChunkSize = 10 * 1024 * 1024
var skipS3 func() string

//DriverParameters contains the url and port for the namenode of your
//HDFS cluster the RootDirectory is where all data will be relative to.
type DriverParameters struct {
	HdfsURL       string
	Port          string
  RootDirectory string
  //TODO add user for now I will just use user docker
}

//FileStatusJSON contains the structure of the response from HDFS status of a
//file or directory
type FileStatusesJSON struct{
  FileStatuses struct{
    FileStatus []struct{
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
}

//FileStatusJSON contains the structure of the response from HDFS status of a
//file or directory
type FileStatusJSON struct{
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
	HdfsURL       string
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

//FromParameters parses the paremeters passed in from the config file on
//registry startup.
func FromParameters(parameters map[string]interface{}) *Driver {
	var rootDirectory = defaultRootDirectory
  HdfsURL := defaultHdfsURL
  portInt := defaultPort
	if parameters != nil {
		rootDir, ok := parameters["rootdirectory"]
		if ok {
			rootDirectory = fmt.Sprint(rootDir)
		}
    HdfsURLTemp, ok := parameters["hdfsurl"]
    if ok{
      HdfsURL = fmt.Sprint(HdfsURLTemp)
    }
    portIntTemp, ok := parameters["port"]
    if ok{
      portInt = fmt.Sprint(portIntTemp)
    }
	}

  params := DriverParameters{
		HdfsURL,
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
		HdfsURL:        params.HdfsURL,
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
  fi, err := d.Stat(ctx, path)
  if err != nil {
    return nil, storagedriver.PathNotFoundError{Path: path}
  }
  if fi.IsDir(){
    return nil, storagedriver.PathNotFoundError{Path: path}
  }
  rc, err := d.ReadStream(ctx, path, 0)
	if err != nil {
		return nil, err
	}
  defer rc.Close()
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
  requestOptions := map[string]string{
    "method": "CREATE",
    //TODO add buffersize for now using default buffersize.
  }
  requestURI, err := getHdfsURI(path, requestOptions, d)
  if err != nil {
    return err
  }
  req, err := http.NewRequest("PUT", requestURI, nil)
  if err != nil {
    return err
  }
  resp, err := d.Client.Do(req)
  defer resp.Body.Close()
  if err != nil {
    return err
  }
  requestURI = resp.Header["Location"][0]
  //TODO deal with file permissions.
  resp.Body.Close()
  req, err = http.NewRequest("PUT", requestURI, bytes.NewBuffer(contents))
  if err != nil{
    return err
  }
  resp1, err := d.Client.Do(req)

  if err != nil{
    return err
  }
  defer resp1.Body.Close()
  return nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
  //TODO check if file exists before reading from hdfs.
  fi, err := d.Stat(ctx, path)
  if err != nil {
    return nil, storagedriver.PathNotFoundError{Path: path}
  }
  if fi.IsDir(){
    return nil, storagedriver.PathNotFoundError{Path: path}
  }
  if offset >= fi.Size() {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}
  requestOptions := map[string]string{
    "method": "OPEN",
    "offset": strconv.FormatInt(offset, 10),
    //TODO add buffersize for now using default buffersize.
  }
  requestURI, err := getHdfsURI(path, requestOptions, d)
  if err != nil {
    return nil, err
  }
  resp, err := http.Get(requestURI)
  if err != nil {
    return nil, err
  }
  return resp.Body, nil
}

// WriteStream stores the contents of the provided io.Reader at a
// location designated by the given path. The driver will know it has
// received the full contents when the reader returns io.EOF. The number
// of successfully READ bytes will be returned, even if an error is
// returned. May be used to resume writing a stream by providing a nonzero
// offset. Offsets past the current size will write from the position
// beyond the end of the file.
func (d *driver) WriteStream(ctx context.Context, subPath string, offset int64, reader io.Reader) (nn int64, err error) {
  totalRead := int64(0)
  firstPass := true
  fi, _ := d.Stat(ctx, subPath)
  if fi != nil{
    if offset == fi.Size() {
        //if offset is equal to the file size we can simply append to the file.
        //by setting firstPass equal to false the write stream will simply append
        //only instead of creating the file first.
        firstPass = false
  	}

    if offset < fi.Size() {
      //In this case we must truncate the file back to the offset and then append
      //from that point on. The truncate rest api for webhdfs allows you to specify
      //the length of the new file therefore we want to truncate with
      //newLength = offset
      firstPass = false
      requestOptions := map[string]string{
        "method": "TRUNCATE",
        "newLength": strconv.FormatInt(offset, 10),
      }
      requestURI, err := getHdfsURI(subPath, requestOptions, d)
      if err != nil {
        return totalRead, err
      }
      resp, err := http.Post(requestURI, "application/octet-stream", nil)
      if err != nil {
        return totalRead, err
      }
      //resp, err := d.Client.Do(req)
      resp.Body.Close()
      //Have to call truncate twice because webHDFS refuses to work the first
      //time you call it but works fine the second time if you wait for
      //two, not one but two seconds.....WTF???????
      time.Sleep(1000*1000*1000*4)
      resp1, err := http.Post(requestURI, "application/json", nil)
      if err != nil {
        return totalRead, err
      }
      //resp, err := d.Client.Do(req)
      resp1.Body.Close()
    }
    if offset > fi.Size() {
      firstPass = false
      zeroBufSize := offset - fi.Size()
      requestOptions := map[string]string{
        "method": "APPEND",
      }

      requestURI, err := getHdfsURI(subPath, requestOptions, d)
      if err != nil {
        return totalRead, err
      }
      req, err := http.NewRequest("POST", requestURI, nil)
      if err != nil {
        return totalRead, err
      }
      resp, err := d.Client.Do(req)
      defer resp.Body.Close()
      if err != nil {
        return totalRead, err
      }

      requestURI = resp.Header["Location"][0]
      //TODO deal with file permissions.
        buf := d.getbuf()
      req, err = http.NewRequest("POST", requestURI, bytes.NewBuffer(buf[:zeroBufSize]))
      if err != nil{
        return totalRead, err
      }
      resp1, err := d.Client.Do(req)
      if err != nil{
        return totalRead, err
      }
      //totalRead += zeroBufSize
      resp1.Body.Close()
      d.putbuf(buf)
    }
  }

  for {
    buf := d.getbuf()
    //read bytes up to defaultChunkSize into buffer
		// Read from `reader` this function loops until sizeRead is equal to sizeToRead
    //or EOF it must keep reading until then because EOF only occurs when
    //exactly 0 bytes are read therefore if EOF is hit and some bytes were read
    //as well the Read function wont indicate EOF but another error entirely.
    // Align to chunk size
		sizeRead := uint64(0)
		sizeToRead := uint64(offset+totalRead) % defaultChunkSize
		if sizeToRead == 0 {
			sizeToRead = defaultChunkSize
		}

		// Read from `reader`
    EOF := false
		for sizeRead < sizeToRead {
			n, err := reader.Read(buf[sizeRead:sizeToRead])
			sizeRead += uint64(n)
			if err != nil {
				if err != io.EOF {
					return totalRead, err
				}
        EOF = true
				break
			}
		}

		// End of file and nothing was read
		if sizeRead == 0 {
			break
		}
    //open file in hdfs with append option and write the buffer onto the end of the file.
    method := "APPEND"
    httpRequestType := "POST"
    if firstPass {
      method = "CREATE"
      httpRequestType = "PUT"
      firstPass = false
    }
    requestOptions := map[string]string{
      "method": method,
    }

    requestURI, err := getHdfsURI(subPath, requestOptions, d)
    if err != nil {
      return totalRead, err
    }
    req, err := http.NewRequest(httpRequestType, requestURI, nil)
    if err != nil {
      return totalRead, err
    }
    resp, err := d.Client.Do(req)
    defer resp.Body.Close()
    if err != nil {
      return totalRead, err
    }

    requestURI = resp.Header["Location"][0]
    //TODO deal with file permissions.
    req, err = http.NewRequest(httpRequestType, requestURI, bytes.NewBuffer(buf[:sizeRead]))
    if err != nil{
      return totalRead, err
    }
    resp1, err := d.Client.Do(req)
    if err != nil{
      return totalRead, err
    }
    defer resp1.Body.Close()
    //update nn with the number of bytes written
    totalRead += int64(sizeRead)
		// End of file

		if EOF {
			break
		}
    d.putbuf(buf) // needs to be here to pick up new buf value
  }
  return totalRead, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
  requestOptions := map[string]string{
    "method": "GETFILESTATUS",
  }
  requestURI, err := getHdfsURI(subPath, requestOptions, d)
  if err != nil {
    return nil, err
  }
  //TODO check if the file exists before calling getfileStatus
  resp, err := http.Get(requestURI)
  if err != nil{
    return nil, err
  }
  defer resp.Body.Close()
  FileStatusJSON := FileStatusJSON{}
  err = getJSON(resp, &FileStatusJSON)
  if err != nil{
    return nil, err
  }

  if FileStatusJSON.FileStatus.FileType == ""{
    return nil, storagedriver.PathNotFoundError{Path: subPath}
  }

  fi := storagedriver.FileInfoFields{
		Path: subPath,
	}
  if FileStatusJSON.FileStatus.FileType == "DIRECTORY" {
    fi.IsDir = true
  } else{
    fi.IsDir = false
    fi.Size = FileStatusJSON.FileStatus.Length
  }

  timestamp, err := msToTime(strconv.Itoa(FileStatusJSON.FileStatus.ModificationTime))
  if err != nil {
    return nil, err
  }
  fi.ModTime = timestamp
  return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
  //Make sure that the file to be moved exists
  fi, err := d.Stat(ctx, subPath)
  if err != nil {
    return nil, storagedriver.PathNotFoundError{Path: subPath}
  }
  if !fi.IsDir(){
    return nil, storagedriver.PathNotFoundError{Path: subPath}
  }
  requestOptions := map[string]string{
    "method": "LISTSTATUS",
  }
  requestURI, err := getHdfsURI(subPath, requestOptions, d)
  if err != nil {
    return nil, err
  }
  resp, err := http.Get(requestURI)
  defer resp.Body.Close()

  FileStatusesJSON := FileStatusesJSON{}
  err = getJSON(resp, &FileStatusesJSON)

  if err != nil{
    return nil, err
  }

  files := []string{}
  for _, element := range FileStatusesJSON.FileStatuses.FileStatus {
  // element is the element from someSlice for where we are
  addSlash := "/"
  if subPath == "/"{
    addSlash = ""
  }
  files = append(files, subPath + addSlash + element.PathSuffix)
  }

  return files, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
  //Make sure that the file to be moved exists
  _, err := d.Stat(ctx, sourcePath)
  if err != nil {
    return storagedriver.PathNotFoundError{Path: sourcePath}
  }

  fi, _ := d.Stat(ctx, destPath)
  if fi != nil {
    fmt.Println("Destinfo")
    fmt.Println(fi)
    if !fi.IsDir(){
      _ = d.Delete(ctx, destPath)
    }
  } else {
    //Mkdir since it doesnt exist
    destDir := path.Dir(destPath)
    requestOptions := map[string]string{
      "method": "MKDIRS",
    }
    requestURI, err := getHdfsURI(destDir, requestOptions, d)
    if err != nil {
      return err
    }

    req, err := http.NewRequest("PUT", requestURI, nil)
    if err != nil {
      return err
    }
    resp, err := d.Client.Do(req)
    if err != nil {
      return err
    }
    resp.Body.Close()
  }

  requestOptions := map[string]string{
    "method": "RENAME",
    "destPath": destPath,
  }
  requestURI, err := getHdfsURI(sourcePath, requestOptions, d)
  if err != nil {
    return err
  }

  req, err := http.NewRequest("PUT", requestURI, nil)
  if err != nil {
    return err
  }
  resp, err := d.Client.Do(req)
  if err != nil {
    return err
  }

  defer resp.Body.Close()
  return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
  //curl -i -X DELETE "http://10.0.1.18:50070/webhdfs/v1/jake?op=DELETE&recursive=true
  _, err := d.Stat(ctx, subPath)
  if err != nil{
    return storagedriver.PathNotFoundError{Path: subPath}
  }
  requestOptions := map[string]string{
    "method": "DELETE",
  }
  requestURI, err := getHdfsURI(subPath, requestOptions, d)
  if err != nil {
    return err
  }
  req, err := http.NewRequest("DELETE", requestURI, nil)
  if err != nil {
    return err
  }
  resp, err := d.Client.Do(req)
  if err != nil {
    return err
  }
  defer resp.Body.Close()
  return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
  //TODO find out what path looks like most importantly does it have a leading or trailing '/'
  return "", storagedriver.ErrUnsupportedMethod{}

}

func getHdfsURI(path string, options map[string]string, d *driver)(string, error){
  baseURI := "http://" + d.HdfsURL + ":" + d.Port + "/webhdfs/v1" + path
  fullURI := baseURI
  method, ok := options["method"]
  if ok {
    switch method {
    case "GETFILESTATUS":
        fullURI = baseURI + "?op=GETFILESTATUS&user.name=jakecharland"
      case "DELETE":
        fullURI = baseURI + "?op=DELETE&recursive=true&user.name=jakecharland"
      case "RENAME":
        destPath, ok := options["destPath"]
        if ok {
          fullURI = baseURI + "?op=RENAME&destination=" + fmt.Sprint(destPath) + "&user.name=jakecharland"
        } else {
          return "", nil
        }
      case "OPEN":
        offset, ok := options["offset"]
        if ok{
          fullURI = baseURI + "?op=OPEN&offset=" + offset + "&user.name=jakecharland"
        } else {
          return "", nil
        }
      case "CREATE":
        fullURI = baseURI + "?op=CREATE&overwrite=true&user.name=jakecharland"
      case "LISTSTATUS":
        fullURI = baseURI + "?op=LISTSTATUS&user.name=jakecharland"
      case "APPEND":
        fullURI = baseURI + "?op=APPEND&buffersize=" + strconv.FormatInt(defaultChunkSize, 10) + "&user.name=jakecharland"
      case "MKDIRS":
        fullURI = baseURI + "?op=MKDIRS&user.name=jakecharland"
      case "TRUNCATE":
        newLength, ok := options["newLength"]
        if ok{
          fullURI = baseURI + "?op=TRUNCATE&newlength=" + newLength + "&user.name=jakecharland"
        } else {
          return "", nil
        }
      default:
        return "", storagedriver.ErrUnsupportedMethod{}
    }
  }
  return fullURI, nil
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
  return errors.New("cancel")
}

func (d *driver) getbuf() []byte {
	return d.pool.Get().([]byte)
}

func (d *driver) putbuf(p []byte) {
	copy(p, d.zeros)
	d.pool.Put(p)
}

func getJSON(r *http.Response, target interface{}) error {
    return json.NewDecoder(r.Body).Decode(target)
}

func msToTime(ms string) (time.Time, error) {
    msInt, err := strconv.ParseInt(ms, 10, 64)
    if err != nil {
        return time.Time{}, err
    }

    return time.Unix(0, msInt*int64(time.Millisecond)), nil
}

func printResponseBody(r *http.Response) error{
  contents, err := ioutil.ReadAll(r.Body)
  if err != nil {
      fmt.Printf("%s", err)
      return err
  }
  fmt.Printf("%s\n", string(contents))
  return nil
}
