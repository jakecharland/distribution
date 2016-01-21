// Package hdfsweb provides a storagedriver.StorageDriver implementation to
// store blobs in Hadoop HDFS cloud storage.
//
// This package leverages the vladimirvivien/gowfs client functions for
// interfacing with Hadoop HDFS.
package hdfsweb

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"reflect"
	"strconv"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName         = "hdfsweb"
	defaultBlockSize   = int64(64 << 20)
	defaultBufferSize  = int32(4096)
	defaultReplication = int16(1)
)

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	NameNodeHost  string
	NameNodePort  string
	RootDirectory string
	UserName      string
	BlockSize     int64
	BufferSize    int32
	Replication   int16
}

func init() {
	factory.Register(driverName, &hdfsDriverFactory{})
}

// hdfsDriverFactory implements the factory.StorageDriverFactory interface
type hdfsDriverFactory struct{}

func (factory *hdfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	fs            *FileSystem
	RootDirectory string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// hdfs. All provided paths will be subpaths of the RootDirectory.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	params := DriverParameters{
		BlockSize:   defaultBlockSize,
		BufferSize:  defaultBufferSize,
		Replication: defaultReplication,
	}

	if parameters != nil {
		if v, ok := parameters["namenodehost"]; ok {
			params.NameNodeHost = fmt.Sprint(v)
		} else {
			return nil, fmt.Errorf("parameter doesn't provided")
		}

		if v, ok := parameters["namenodeport"]; ok {
			params.NameNodePort = fmt.Sprint(v)
		} else {
			return nil, fmt.Errorf("parameter doesn't provided")
		}

		if v, ok := parameters["rootdirectory"]; ok {
			params.RootDirectory = fmt.Sprint(v)
		} else {
			return nil, fmt.Errorf("parameter doesn't provided")
		}

		if v, ok := parameters["username"]; ok {
			params.UserName = fmt.Sprint(v)
		} else {
			usr, err := user.Current()
			if err != nil {
				return nil, err
			}
			params.UserName = usr.Username
		}

		if blockSizeParam, ok := parameters["blocksize"]; ok {
			switch v := blockSizeParam.(type) {
			case string:
				vv, err := strconv.ParseInt(v, 0, 64)
				if err != nil {
					return nil, err
				}
				params.BlockSize = vv
			case int64:
				params.BlockSize = v
			case int, uint, int32, uint32, uint64:
				params.BlockSize = reflect.ValueOf(v).Convert(reflect.TypeOf(params.BlockSize)).Int()
			default:
				return nil, fmt.Errorf("invalid valud %#v for blocksize", blockSizeParam)
			}
		}

		if bufferSizeParam, ok := parameters["buffersize"]; ok {
			switch v := bufferSizeParam.(type) {
			case string:
				vv, err := strconv.ParseInt(v, 0, 32)
				if err != nil {
					return nil, err
				}
				params.BufferSize = int32(vv)
			case int32:
				params.BufferSize = v
			case int, int64, uint, uint32, uint64:
				params.BufferSize = int32(reflect.ValueOf(v).Convert(reflect.TypeOf(params.BufferSize)).Int())
			default:
				return nil, fmt.Errorf("invalid valud %#v for buffersize", bufferSizeParam)
			}
		}

		if replicationParam, ok := parameters["replication"]; ok {
			switch v := replicationParam.(type) {
			case string:
				vv, err := strconv.ParseInt(v, 0, 16)
				if err != nil {
					return nil, err
				}
				params.Replication = int16(vv)
			case int16:
				params.Replication = v
			case int, int64, uint, int32, uint32, uint64:
				params.Replication = int16(reflect.ValueOf(v).Convert(reflect.TypeOf(params.Replication)).Int())
			default:
				return nil, fmt.Errorf("invalid valud %#v for replication", replicationParam)
			}
		}

	}
	return New(params)
}

// New constructs a new Driver with given parameters
func New(params DriverParameters) (*Driver, error) {
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					fs: &FileSystem{
						NameNodeHost: params.NameNodeHost,
						NameNodePort: params.NameNodePort,
						UserName:     params.UserName,
						BlockSize:    params.BlockSize,
						BufferSize:   params.BufferSize,
						Replication:  params.Replication,
					},
					RootDirectory: params.RootDirectory,
				},
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, subPath string) ([]byte, error) {
	rc, err := d.ReadStream(ctx, subPath, 0)
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
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	hdfsFullPath := d.fullHdfsPath(subPath)
	return d.fs.Create(
		bytes.NewReader(contents),
		hdfsFullPath)
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, subPath string, offset int64) (io.ReadCloser, error) {
	hdfsFullPath := d.fullHdfsPath(subPath)

	status, err := d.fs.GetFileStatus(hdfsFullPath)
	if err != nil {
		if re, ok := err.(RemoteException); ok && re.Exception == ExceptionFileNotFound {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	} else if status.Type != "FILE" {
		return nil, fmt.Errorf("not file type")
	} else if status.Length < offset {
		return nil, storagedriver.InvalidOffsetError{Path: subPath, Offset: offset}
	}

	if status.Length == offset {
		r := bytes.NewReader([]byte{})
		rc := ioutil.NopCloser(r)
		return rc, nil
	}

	reader, err := d.fs.Open(hdfsFullPath, offset, (status.Length - offset))
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// WriteStream stores the contents of the provided io.Reader at a location
// designated by the given path.
func (d *driver) WriteStream(ctx context.Context, subPath string, offset int64, reader io.Reader) (int64, error) {
	var (
		buf          = new(bytes.Buffer)
		hdfsFullPath = d.fullHdfsPath(subPath)
	)

	if offset > 0 {
		if status, err := d.fs.GetFileStatus(hdfsFullPath); err == nil && status.Type == "FILE" {
			rc, err := d.ReadStream(ctx, subPath, 0)
			if err != nil {
				return 0, fmt.Errorf("Writing Failed %s", err)
			}
			if length, err := buf.ReadFrom(rc); err != nil {
				return 0, err
			} else if offset > length {
				if _, err := buf.ReadFrom(bytes.NewReader((make([]byte, offset-length)))); err != nil {
					return 0, err
				}
			} else if offset < length {
				buf.Truncate(int(offset))
			}
		} else if re, ok := err.(RemoteException); (ok && re.Exception == ExceptionFileNotFound) ||
			status.Type != "FILE" {
			if _, err := buf.ReadFrom(bytes.NewReader((make([]byte, offset)))); err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	sum, err := buf.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	if err := d.fs.Create(buf, hdfsFullPath); err != nil {
		return 0, err
	}
	return sum, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	fi := storagedriver.FileInfoFields{
		Path: subPath,
	}
	hdfsFullPath := d.fullHdfsPath(subPath)
	status, err := d.fs.GetFileStatus(hdfsFullPath)
	if err != nil {
		if re, ok := err.(RemoteException); ok && re.Exception == ExceptionFileNotFound {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	}
	if status.Type == "FILE" {
		fi.IsDir = false
	} else if status.Type == "DIRECTORY" {
		fi.IsDir = true
	} else {
		return nil, fmt.Errorf("unknown path type")
	}
	fi.Size = status.Length
	fi.ModTime = time.Unix((status.ModificationTime / 1000), 0)
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	legalPath := subPath
	if legalPath[len(legalPath)-1] != '/' {
		legalPath += "/"
	}
	hdfsFullPath := d.fullHdfsPath(legalPath)
	list, err := d.fs.ListStatus(hdfsFullPath)
	if err != nil {
		if re, ok := err.(RemoteException); ok && re.Exception == ExceptionFileNotFound {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	}

	keys := make([]string, 0, len(list))
	for _, stat := range list {
		keys = append(keys, path.Join(legalPath, stat.PathSuffix))
	}
	return keys, nil
}

// Move moves an object stored at subSrcPath to subDstPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, subSrcPath, subDstPath string) error {
	fullSrcPath := d.fullHdfsPath(subSrcPath)
	fullDstPath := d.fullHdfsPath(subDstPath)

	// check if source path exist
	if _, err := d.fs.GetFileStatus(fullSrcPath); err != nil {
		if re, ok := err.(RemoteException); ok && re.Exception == ExceptionFileNotFound {
			return storagedriver.PathNotFoundError{Path: subSrcPath}
		}
		return err
	}
	// delete destination if exists
	// for loop is for avoiding the clients competitive
	for {
		if _, err := d.fs.GetFileStatus(fullDstPath); err == nil {
			if err := d.Delete(ctx, subDstPath); err != nil && err != ErrBoolean {
				return err
			}
		}
		if err := d.fs.MkDirs(path.Dir(fullDstPath)); err != nil {
			return err
		}
		if err := d.fs.Rename(fullSrcPath, fullDstPath); err == ErrBoolean {
			continue
		} else {
			return err
		}
	}
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
	hdfsFullPath := d.fullHdfsPath(subPath)
	// check if path exist
	if _, err := d.fs.GetFileStatus(hdfsFullPath); err != nil {
		if re, ok := err.(RemoteException); ok && re.Exception == ExceptionFileNotFound {
			return storagedriver.PathNotFoundError{Path: subPath}
		}
		return err
	}
	if err := d.fs.Delete(hdfsFullPath); err != nil {
		return err
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

func (d *driver) fullHdfsPath(subPath string) string {
	return path.Join(d.RootDirectory, subPath)
}

func checkFileExist(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	}

	return false
}

func convIntParameter(param interface{}) (int64, error) {
	var num int64
	switch v := param.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return 0, fmt.Errorf("parameter must be an integer, %v invalid", param)
		}
		num = vv
	case int64:
		num = v
	case int, uint, int32, uint32, uint64:
		num = reflect.ValueOf(v).Convert(reflect.TypeOf(num)).Int()
	default:
		return 0, fmt.Errorf("invalid valud %#v", param)
	}
	return num, nil
}
