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
const defaultHdfsURL = "127.0.0.1"
const defaultPort = "50070"
const defaultUserName = "hdfs"

//Set defaultChunkSize to 125mb which is the default block size for hdfs
const defaultChunkSize = 1.25e+8

//DriverParameters contains the url and port for the namenode of your
//HDFS cluster the RootDirectory is where all data will be relative to.
type DriverParameters struct {
	HdfsURL       string
	Port          string
  RootDirectory string
  UserName     string
  //TODO add user for now I will just use user hdfs
}

type BooleanResponseJSON struct{
  Success bool `json:"boolean"`
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
  UserName      string

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
  UserName := defaultUserName
	if parameters != nil {
		rootDir, ok := parameters["rootdirectory"]
		if ok {
			rootDirectory = fmt.Sprint(rootDir)
      fmt.Println(rootDirectory)
		}
    HdfsURLTemp, ok := parameters["hdfsurl"]
    if ok{
      HdfsURL = fmt.Sprint(HdfsURLTemp)
    }
    portIntTemp, ok := parameters["port"]
    if ok{
      portInt = fmt.Sprint(portIntTemp)
    }
    UserNameTemp, ok := parameters["username"]
    if ok {
      UserName = fmt.Sprint(UserNameTemp)
    }
	}

  params := DriverParameters{
		HdfsURL,
		portInt,
	  rootDirectory,
    UserName,
	}

	return New(params)
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {
  client := &http.Client{
	   CheckRedirect: redirectPolicyFunc,
  }
  d := &driver{
		HdfsURL:        params.HdfsURL,
		Port:           params.Port,
		RootDirectory:  params.RootDirectory,
    UserName:       params.UserName,
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
  resp, err := d.CreateFile(path, contents, uint64(len(contents)))

  if err != nil{
    return err
  }
  defer resp.Body.Close()
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
  resp, err := d.OpenFile(path, offset)
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
      //from that point on.
      firstPass = false
      resp, err := d.TruncateFile(subPath, offset)
      if err != nil {
        return totalRead, err
      }
      resp.Body.Close()
    }
    //If the offset is greater then the file size in hdfs then we fill the gap
    //with zeros and continue appening.
    if offset > fi.Size() {
      firstPass = false
      zeroBufSize := offset - fi.Size()

      resp, err := d.AppendToFile(subPath, d.zeros, uint64(zeroBufSize))
      if err != nil {
        return totalRead, err
      }
      resp.Body.Close()
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

    if firstPass {
      //Create the file since this is the first pass it does not exist yet.
      firstPass = false
      resp, err := d.CreateFile(subPath, buf, uint64(sizeRead))
      if err != nil {
        return totalRead, err
      }
      resp.Body.Close()
    } else{
      //Append to file as we recieve bytes from the buffer
      resp, err := d.AppendToFile(subPath, buf, uint64(sizeRead))
      if err != nil {
        return totalRead, err
      }
      resp.Body.Close()
    }

    //update nn with the number of bytes written
    totalRead += int64(sizeRead)

		// End of file stream
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
  resp, err := d.GetFileInfo(subPath)
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

  resp, err := d.GetDirInfo(subPath)
  if err != nil {
    return nil, err
  }
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
    if !fi.IsDir(){
      _ = d.Delete(ctx, destPath)
    }
  } else {
    //Mkdir since it doesnt exist
    destDir := path.Dir(destPath)
    resp, err := d.MkDirs(destDir)
    if err != nil {
      return err
    }
    resp.Body.Close()
  }

  resp, err := d.Rename(sourcePath, destPath)
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
  resp, err := d.DeleteFile(subPath)
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

func (d *driver) GetBaseURL(path string) string{
  rootDir := d.GetRootDir()
  return "http://" + d.HdfsURL + ":" + d.Port + "/webhdfs/v1" + rootDir + path
}


func (d *driver) AppendToFile(subPath string, buf []byte, sizeRead uint64)(*http.Response, error) {
  baseURL := d.GetBaseURL(subPath)
  requestURI := baseURL + "?op=APPEND&buffersize=" + strconv.FormatInt(defaultChunkSize, 10) + "&user.name=" + d.UserName
  req, err := http.NewRequest("POST", requestURI, nil)
  if err != nil {
    return nil, err
  }
  resp, err := d.Client.Do(req)
  defer resp.Body.Close()
  if err != nil {
    return nil, err
  }

  requestURI = resp.Header["Location"][0]

  req, err = http.NewRequest("POST", requestURI, bytes.NewBuffer(buf[:sizeRead]))
  if err != nil{
    return nil, err
  }
  return d.Client.Do(req)
}

func (d *driver) CreateFile(subPath string, buf []byte, sizeRead uint64)(*http.Response, error) {
  baseURL := d.GetBaseURL(subPath)
  requestURI := baseURL + "?op=CREATE&overwrite=true&user.name=" + d.UserName
  req, err := http.NewRequest("PUT", requestURI, nil)
  if err != nil {
    return nil, err
  }
  resp, err := d.Client.Do(req)
  defer resp.Body.Close()
  if err != nil {
    return nil, err
  }

  requestURI = resp.Header["Location"][0]

  req, err = http.NewRequest("PUT", requestURI, bytes.NewBuffer(buf[:sizeRead]))
  if err != nil{
    return nil, err
  }
  return d.Client.Do(req)
}

func (d *driver) OpenFile(path string, offset int64)(*http.Response, error){
  baseURL := d.GetBaseURL(path)
  requestURI := baseURL + "?op=OPEN&offset=" + strconv.FormatInt(offset, 10) + "&user.name=" + d.UserName
  return http.Get(requestURI)
}

func (d * driver) TruncateFile(subPath string, offset int64) (*http.Response, error){
  baseURL := d.GetBaseURL(subPath)
  requestURI := baseURL + "?op=TRUNCATE&newlength=" + strconv.FormatInt(offset, 10) + "&user.name=" + d.UserName
  //Have to call truncate in a loop because webHDFS refuses to work the first
  //time you call it usually works the second time if you wait sleep
  //for a couple seconds.....WTF???????
  for i := 0; i < 5; i++ {
    resp, err := http.Post(requestURI, "application/octet-stream", nil)
    if err != nil {
      return nil, err
    }
    BooleanResponseJSON := BooleanResponseJSON{}
    err = getJSON(resp, &BooleanResponseJSON)
    if err != nil {
      return nil, err
    }
    if BooleanResponseJSON.Success {
      return resp, err
    }
    resp.Body.Close()
    time.Sleep(1000*1000*1000*2)
  }
  return nil, fmt.Errorf("Truncate unsuccessful")
}

func (d *driver) GetFileInfo(subPath string) (*http.Response, error){
  baseURL := d.GetBaseURL(subPath)
  requestURI := baseURL + "?op=GETFILESTATUS&user.name=" + d.UserName
  return http.Get(requestURI)
}

func (d *driver) GetDirInfo(subPath string) (*http.Response, error){
  baseURL := d.GetBaseURL(subPath)
  requestURI := baseURL + "?op=LISTSTATUS&user.name=" + d.UserName
  return http.Get(requestURI)
}

func (d *driver) MkDirs(destDir string)(*http.Response, error){
  baseURL := d.GetBaseURL(destDir)
  requestURI := baseURL + "?op=MKDIRS&user.name=" + d.UserName
  req, err := http.NewRequest("PUT", requestURI, nil)
  if err != nil {
    return nil, err
  }
  return d.Client.Do(req)
}

func (d *driver) Rename(sourcePath string, destPath string)(*http.Response, error){
  baseURL := d.GetBaseURL(sourcePath)
  rootDir := d.GetRootDir()
  requestURI := baseURL + "?op=RENAME&destination=" + rootDir + fmt.Sprint(destPath) + "&user.name=" + d.UserName
  req, err := http.NewRequest("PUT", requestURI, nil)
  if err != nil {
    return nil, err
  }
  return d.Client.Do(req)
}

func (d *driver) DeleteFile(path string)(*http.Response, error){
  baseURL := d.GetBaseURL(path)
  requestURI := baseURL + "?op=DELETE&recursive=true&user.name=" + d.UserName
  req, err := http.NewRequest("DELETE", requestURI, nil)
  if err != nil {
    return nil, err
  }
  return d.Client.Do(req)
}

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

func (d *driver) GetRootDir() string{
  rootDir := ""
  if d.RootDirectory != "/"{
    rootDir = d.RootDirectory
  }
  return rootDir
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
