package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/snbsfs"

	"code.suning.com/glog"
)

var (
	//for handler
	FUSEErrBadSerBuc          = errors.New("fuse:bad server(bucket)")
	FUSEErrBadSerAcc          = errors.New("fuse:bad server(account)")
	FUSEErrBadSerFiler        = errors.New("fuse:bad server(filer)")
	FUSEErrNameExist          = errors.New("fuse:user name exist")
	FUSEErrNoPermission       = errors.New("fuse:Permission denied")
	FUSEErrInvalidPath        = errors.New("fuse:Invalid path")
	FUSEErrReNameDir          = errors.New("fuse:can not rename dir")
	FUSEErrMvFile             = errors.New("fuse:can not mv file to different dir")
	FUSEErrNoFile             = errors.New("fuse:No such file or directory")
	FUSEErrBadRequest         = errors.New("fuse:Bad Request ")
	FUSEErrBadConf            = errors.New("fuse:Bad conf file ")
	FUSEErrBadConfVersion     = errors.New("fuse:Bad conf file version")
	FUSEErrOpened             = errors.New("fuse:ftp had opened ")
	FUSEErrClosed             = errors.New("fuse:ftp had closed ")
	FUSEErrUserNotExist       = errors.New("fuse:user not exist ")
	FUSEErrUserPassWdMisMatch = errors.New("fuse:user password mismatch ")
	FUSEErrNomalUserExist     = errors.New("fuse:nomal user exist ")
	FUSEErrAdminNeeded        = errors.New("fuse:admin user needed ")
	FUSEErrUserExist          = errors.New("fuse:user exist ")
	//FUSEErrAcessKeyAuthFailed = errors.New("fuse:acesskey auth failed ")
	FUSEErrBucketIsSystemDef = errors.New("fuse:bucket is sys-define")
	FUSEErrBucketIsPublicWR  = errors.New("fuse:bucket is public-read-write")
	FUSEErrBucketNotExist    = errors.New("fuse:bucket not exist")
	FUSEErrCreated           = errors.New("fuse:ftp had created ")
	FUSEErrNoCreated         = errors.New("fuse:ftp not created ")
	FUSEErrEncrypt           = errors.New("fuse:error to Encrypt ")
	FUSEErrDecrypt           = errors.New("fuse:error to Decrypt ")

	FUSEErrBadUserName   = errors.New("fuse:bad name ")
	FUSEErrBadPass       = errors.New("fuse:bad pass ")
	FUSEErrBadRootPath   = errors.New("fuse:bad root path ")
	FUSEErrRootPathInUse = errors.New("fuse:rootpath is in use ")
	FUSEErrBadMode       = errors.New("fuse:bad mode ")

	FUSEErrRead  = errors.New("fuse:read file or directory error")
	FUSEErrWrite = errors.New("fuse:write file or directory error")

	//inside
	ErrBadAuth    = errors.New("Error Authorization")
	ErrNotExist   = errors.New("Error Not Exist")
	ErrDirnotnull = errors.New("dir Not null")
	ErrRequest    = errors.New("Error Request")
	ErrUnknow     = errors.New("Error Unknow")
	ErrBaseConf   = errors.New("Error BaseConf")
	ErrUserConf   = errors.New("Error UserConf")
)

//??????4M ?????????????????????
var fusemultthreshold = int64(4 * 1024 * 1024)

//????????????4M
var fuseblocksize = int64(4 * 1024 * 1024)

var maxshowfileSum = 3000

//???????????????
type Zone struct {
	Id        string
	Total     string
	Used      string
	Free      string
	ZoneNodes []ZoneNode
}
type ZoneNode struct {
	Names  string
	Total  string
	Used   string
	Free   string
	Online string
}
type MsgBody struct {
	Filename string
	Offset   int64
	Data     []byte
	Rsp      chan int32
	Size     int64
}

//SnbsFs----???SnbsDriver
//?????????SnbsDriver???????????????
type SnbsDriver struct {
	//???????????????objectfs ????????????
	snbsfs.Driver

	//??????
	Account string
	//bucket
	Bucket string
	//id
	AccessKeyId string
	//sec
	AccessKeysecret string
	//filer?????????????????????
	Filer string
	//FilerSelecter *util.FilerNodes

	//bucketattr            *bucket.Bucket
	lastbucketrefreshtime time.Time

	Regioninfo     *RegionIPInfo
	GatewayPort    *string //add snbs gatewayport
	GatewayTcpPort *string //add snbs gatewayTcpport
	TgtMasterPort  string
	Mountpoint     *string
	MaxVolumelist  *int
	GroupIOInfo    *IOInfo

	//????????????
	Wirtestatue sync.Map
	SendMsg     chan *MsgBody
	GwClientMap sync.Map //key=filename,value=gatewayclient
	sync.RWMutex
}

func (sf *SnbsDriver) String() string {
	res := "---------------SnbsDriver---------------" + "\n"
	res += "Account:" + sf.Account + "\n"
	res += "Bucket:" + sf.Bucket + "\n"
	res += "Access-key:" + sf.AccessKeyId + "\n"
	res += "Access-Secret :" + sf.AccessKeysecret + "\n"
	res += "Snbs-endpoint:" + sf.Filer + "\n"
	res += "------------------------------------"
	return res
}

//??????????????????
func (sf *SnbsDriver) GetAttr(name string, context *fuse.Context) (*snbsfs.FileInfo, fuse.Status) {
	glog.V(0).Infoln("SnbsDriver Op GetObjAttr:", name)
	if name == "" {
		return nil, fuse.EIO
	}
	_, obj, sta := sf.getObjAttr(name)
	return obj, sta
}

type cashforattr struct {
	pAttr     *fuse.Attr
	pfileinfo *snbsfs.FileInfo
	status    fuse.Status
}

//?????????????????????????????????
//????????????????????????
func (sf *SnbsDriver) getObjAttr(path string) (*fuse.Attr, *snbsfs.FileInfo, fuse.Status) {
	starttime := time.Now()
	fileinfostr := &snbsfs.FileInfo{}
	defer func() {
		glog.V(4).Infoln("getObjAttr total spend:", time.Since(starttime).String())
	}()

	//????????????????????????
	if objattr, ok := sf.Wirtestatue.Load(path + "+objattr"); ok {
		glog.V(4).Infoln("find objattrcash", path)
		return objattr.(*cashforattr).pAttr, objattr.(*cashforattr).pfileinfo, objattr.(*cashforattr).status
	}

	var objattr *fuse.Attr
	var ok bool
	body, state := sf.httpRequest("GET", sf.GetGatewayIp(path, ""), "/region/pool/vol?volume="+path+"&pretty=y", nil)
	if state >= http.StatusBadRequest {
		return nil, fileinfostr, fuse.EIO
	}

	var Volumes map[string]interface{}
	err := json.Unmarshal(body, &Volumes)
	if err != nil {
		glog.V(0).Infoln("unmarshal respbody fail " + ": " + err.Error())
		return nil, fileinfostr, fuse.EIO
	}

	objattr = &fuse.Attr{}
	volFileName, ok := Volumes["FileName"].(string)
	if ok {
		fileinfostr.Name = volFileName
	}
	volSize, ok := Volumes["Size"].(float64)
	if ok {
		fileinfostr.Size = int64(volSize)
	}
	voltime, ok := Volumes["Creation_Time"].(string)
	if ok {
		lmtime, err := time.ParseInLocation("2006-01-02 15:04:05", voltime, time.Local)
		if err == nil {
			fileinfostr.LastModified = strconv.FormatInt(lmtime.Unix(), 10)
		}
	}
	gatewayaddr, ok := Volumes["Owner"].(string)
	if ok {
		gatewayip := strings.Split(gatewayaddr, ":")
		sf.CheckAndSetClient(volFileName, gatewayip[0])
	}
	if fileinfostr.IsDir {
		objattr.Mode = fuse.S_IFDIR | 0755
	} else {
		objattr.Size = uint64(fileinfostr.Size)
		objattr.Mode = fuse.S_IFREG | 0644
	}
	sf.Wirtestatue.Store(path+"+Size", int64(objattr.Size))
	sf.Wirtestatue.Store(path+"+objattr", &cashforattr{objattr, fileinfostr, fuse.OK})

	return objattr, fileinfostr, fuse.OK
}

//?????????????????????
func (sf *SnbsDriver) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op GetObjAttr:", name, " mode:", mode)
	return fuse.OK
}

//????????????????????????,?????????
func (sf *SnbsDriver) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Chown:", name, " uid:", uid, " gid:", gid)
	return fuse.OK
}

//?????????????????????,?????????
func (sf *SnbsDriver) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Utimens:", name, " Atime:", Atime, " Mtime", Mtime)
	return fuse.OK
}

//???????????????????????????,?????????
//??????????????????reopen??????????????????,??????????????????
func (sf *SnbsDriver) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("SnbsDriver Op Truncate:", name, " size:", size)
	//??????????????????????????????
	if size == 0 {
		return fuse.OK
	}

	_, filename, parentkey, _, patherr := path2namebase(name)
	if patherr != nil {
		glog.V(0).Infoln("error while MakeDir " + name + ": " + patherr.Error())
		return fuse.EINVAL
	}
	//???gateway???????????????
	var (
		poolname string = strings.Trim(parentkey, "/")
		volname  string = strings.Trim(filename, "/")
	)
	glog.V(4).Infoln("SnbsDriver Op Truncate:", name, " size:", size, poolname, volname)
	//?????????????????????????????????????????????????????????????????????????????????????????????
	_, obj, sta := sf.getObjAttr(volname)
	if sta == fuse.OK {
		if uint64(obj.Size) >= size {
			return fuse.OK
		} else {
			//????????????
			urlname := "/region/pool/vol?&volume=" + volname + "&size=" + strconv.FormatUint(size, 10)
			if *fuseop.ChunkSize != 0 {
				urlname += "&chunksize=" + strconv.FormatInt(int64(*fuseop.ChunkSize)*1024*1024, 10)
			}
			_, state := sf.httpRequest("POST", sf.GetGatewayIp(volname, poolname), urlname, nil)
			if state >= http.StatusBadRequest {
				glog.V(0).Infoln("rsp state fail:", state)
				return fuse.EIO
			}
			return fuse.OK

		}

	}
	urlname := "/region/pool/vol?pool=" + poolname + "&volume=" + volname + "&size=" + strconv.FormatUint(size, 10)

	if *fuseop.Repnum != -1 {
		urlname += "&repnum=" + strconv.FormatInt(int64(*fuseop.Repnum), 10)
	}

	if *fuseop.Near != -1 {
		urlname += "&near=" + strconv.FormatInt(int64(*fuseop.Near), 10)
	}

	if *fuseop.ChunkSize != 0 {
		urlname += "&chunksize=" + strconv.FormatInt(int64(*fuseop.ChunkSize)*1024*1024, 10)
	}

	_, state := sf.httpRequest("PUT", sf.GetGatewayIp(volname, poolname), urlname, nil)
	if state >= http.StatusBadRequest {
		glog.V(0).Infoln("rsp state fail:", state)
		return fuse.EIO
	}

	if *fuseop.TempFile != "" {
		os.Truncate(*fuseop.TempFile, int64(size))
	}

	if !sf.GroupIOInfo.NewWriteCron(volname, poolname, sf.GetGatewayIp(volname, poolname)) {
		glog.V(0).Infoln("NewWriteCron fail")
		return fuse.EIO
	}

	return fuse.OK
}

//?????????????????????/????????????????????????????????????
func (sf *SnbsDriver) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Access:", name, " mode:", mode)
	return fuse.OK
}

//????????????oldName??????newName
func (sf *SnbsDriver) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Link, oldName:", oldName, " newName:", newName)
	return fuse.ENOSYS
}

//???????????????
func (sf *SnbsDriver) Mkdir(path string, mode uint32, context *fuse.Context) fuse.Status {
	return fuse.EINVAL
}

//??????????????????
func (sf *SnbsDriver) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	glog.V(4).Infoln("SnbsDriver Op Mkdir:", name, "  mode :", mode, " dev:", dev)
	return fuse.ENOSYS
}

//??????????????????/??????
func (sf *SnbsDriver) Rename(fromInfo *snbsfs.FileInfo, oldName string, newName string, context *fuse.Context) (code fuse.Status) {

	glog.V(4).Infoln("SnbsDriver Op Rename ,oldName:", oldName, "  newName :", newName)
	//add snbs
	return fuse.EINVAL
}

//???????????????
func (sf *SnbsDriver) Rmdir(path string, context *fuse.Context) (code fuse.Status) {

	glog.V(4).Infoln("SnbsDriver Op Rmdir:", path)
	//add snbs
	return fuse.EINVAL
}

//????????????,????????????
func (sf *SnbsDriver) Unlink(path string, context *fuse.Context) (code fuse.Status) {

	glog.V(0).Infoln("SnbsDriver Op Unlink:", path)
	isDirbool, filename, parentkey, myKey, patherr := path2namebase(path)
	if patherr != nil || myKey == "" {
		glog.V(0).Infoln(patherr)
		return fuse.EINVAL
	}
	glog.V(4).Infoln("isDis", isDirbool, "filename", filename, "parentkey", parentkey, "mykey", myKey)

	volume := strings.Trim(filename, "/")
	//first close file
	if cli := sf.GetTcpClient(volume, ""); cli != nil {
		if lensp := cli.Delete(volume); lensp < 0 {
			glog.V(0).Infoln("Unlink and delete file fail", lensp)
		}
	}

	_, state := sf.httpRequest("DELETE", sf.GetGatewayIp(volume, ""), "/region/pool/vol?volume="+volume+"&pretty=y", nil)
	if state >= http.StatusBadRequest {
		glog.V(4).Infoln("satue", state)
		return fuse.EIO
	}

	//????????????
	sf.Wirtestatue.Delete(volume + "+objattr")
	sf.DelTcpClient(volume)

	return fuse.OK
}

//????????????????????????
//????????????dirlist
func (sf *SnbsDriver) OpenDir(path string, fixname string, context *fuse.Context) (c map[string]*snbsfs.FileInfo, code fuse.Status) {

	glog.V(0).Infoln("SnbsDriver Op OpenDirpath:", path)
	glog.V(0).Infoln("SnbsDriver Op OpenDirfixname:", fixname)
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	c = make(map[string]*snbsfs.FileInfo)
	_, filename, _, myKey, patherr := sf.path2name(path)
	if patherr != nil {
		glog.V(0).Infoln(patherr.Error())
		return nil, fuse.EINVAL
	}
	glog.V(4).Infoln("myfilename:", myKey, filename)
	//???????????????gateway
	//???????????????pool/volumn
	//?????????pool?????????volumn???
	glog.V(0).Infoln(path, *sf.Mountpoint)
	var url string
	var rootpool bool = false
	var filenamedir string
	var poolnamedir string

	if path == "/" {
		url = "/pool/info?pretty=y"
		rootpool = true
	} else { //volumn???
		url = "/region/pool?pretty=y"
		filenamedir = fixname
		poolnamedir = strings.TrimLeft(path, "/")
	}

	body, state := sf.httpRequest("GET", sf.GetGatewayIp(filenamedir, poolnamedir), url, nil)
	if state >= http.StatusBadRequest {
		glog.V(0).Infoln("rsp state fail:", state, filename)
		return nil, fuse.EIO
	}

	if rootpool { //pool??????
		var Pools []interface{}
		err := json.Unmarshal(body, &Pools)
		if err != nil {
			glog.V(0).Infoln("unmarshal respbody fail " + ": " + err.Error())
			return
		}

		for _, v := range Pools {
			objstr, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			fileinfostr := &snbsfs.FileInfo{}
			polFileName, ok := objstr["name"].(string)
			if ok {
				fileinfostr.Name = polFileName
				if poolfile := sf.GetPoolInfo(fileinfostr.Name); poolfile != nil {
					c[fileinfostr.Name] = poolfile
				}
			}
		}
	} else {
		//vol ??????
		var (
			ok         bool
			Volumes    map[string]interface{}
			VolumeRoot []interface{}
			NextMarket string
		)
		err := json.Unmarshal(body, &Volumes)
		if err != nil {
			glog.V(0).Infoln("unmarshal respbody fail " + ": " + err.Error())
			return nil, fuse.EIO
		}

		VolumeRoot, ok = Volumes["Volumes"].([]interface{})
		if !ok {
			glog.V(0).Infoln("Get Volumes fail ")
			return nil, fuse.EIO
		}

		for _, v := range VolumeRoot {
			volstr, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			//??????pool??????
			volPoolName, ok := volstr["Pool"].(string)
			if !ok || volPoolName != strings.Trim(filename, "/") {
				continue
			}
			fileinfostr := &snbsfs.FileInfo{IsDir: false}
			volFileName, ok := volstr["FileName"].(string)
			if ok {
				fileinfostr.Name = volFileName
			}
			volSize, ok := volstr["Size"].(float64)
			if ok {
				fileinfostr.Size = int64(volSize)
			}
			voltime, ok := volstr["Creation_Time"].(string)
			if ok {
				lmtime, err := time.ParseInLocation("2006-01-02 15:04:05", voltime, time.Local)
				if err == nil {
					fileinfostr.LastModified = strconv.FormatInt(lmtime.Unix(), 10)
				}
			}
			c[fileinfostr.Name] = fileinfostr
		}
		//??????????????????
		NextMarket, ok = Volumes["NextMarker"].(string)
		if ok {
			sf.GetVolumnList(filename, NextMarket, c)
		}
	}
	return c, fuse.OK
}

//??????????????????????????????????????????

func (sf *SnbsDriver) Open(name string, flags uint32, context *fuse.Context) (file *snbsfs.FileInfo, code fuse.Status) {
	glog.V(0).Infoln("SnbsDriver Op Open:", name, " with flag :", flags)
	if name == "/" {
		return nil, fuse.EINVAL
	}

	_, filename, pathkey, _, patherr := sf.path2name(name)
	if patherr != nil {
		glog.V(0).Infoln(patherr.Error())
		return nil, fuse.EINVAL
	}
	filename = strings.Trim(filename, "/")
	pathkey = strings.Trim(pathkey, "/")

	//???filer ????????????????????????????????????????????????????????????????????????????????????
	_, obj, status := sf.getObjAttr(filename)
	if status == fuse.ENOENT || status == fuse.EIO {
		//?????????
		return nil, fuse.EINVAL
	}

	sf.Wirtestatue.Store(filename, false)

	//??????gateway?????????
	if cli := sf.GetTcpClient(filename, pathkey); cli != nil {
		if _, err := cli.Open(filename); err != nil {
			glog.V(0).Infoln("Open in Read fail fileanme:" + filename + "err:" + err.Error())
			//close
			if lensp := cli.Delete(filename); lensp < 0 {
				glog.V(0).Infoln("close file fail", lensp)
			}
			if _, err := cli.Open(filename); err != nil {
				glog.V(0).Infoln("Open in Read fail fileanme again:" + filename + "err:" + err.Error())
				return nil, fuse.EIO
			}
		}
	} else {
		glog.V(0).Infoln("not find tcp client", filename)
	}

	return obj, fuse.OK
}

//?????????
func (sf *SnbsDriver) Read(name string, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Read:", name, "  off :", off, "size", size)

	_, filename, _, _, patherr := sf.path2name(name)
	if patherr != nil {
		glog.V(0).Infoln(patherr.Error())
		return nil, fuse.EINVAL
	}
	filename = strings.Trim(filename, "/")

	req := &MsgBody{filename, off, dest, make(chan int32), size}
	var lenrsp int32

	Readchan <- req
	select {
	case lenrsp = <-req.Rsp:
	case <-time.After(time.Minute + time.Second):
		lenrsp = -1
	}

	if lenrsp < 0 {
		glog.V(0).Infoln("Read fail fileanme:"+filename, off, lenrsp)
		return nil, fuse.EIO
	}

	templen := int64(len(dest))
	if templen < size || templen == 0 {
		glog.V(0).Infoln("err size")
		return nil, fuse.EIO
	}
	glog.V(4).Infoln("get length :", len(dest), "offset:", off, "size:", size, "lenrsp", lenrsp)
	res := fuse.ReadResultData(dest)
	return res, fuse.OK
}

func (sf *SnbsDriver) ReadData(name string, off int64, size int64, context *fuse.Context) ([]byte, fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op ReadData:", name, "  off :", off, "size", size)
	var data []byte
	header := make(map[string]string)
	//??????range
	if size >= 1 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off+size-1)
	} else {
		header["Range"] = fmt.Sprintf("bytes=%d-", off)
	}

	data, err := sf.urlRequest(name, "GET", nil, header)
	if err != nil {
		glog.V(0).Infoln("error while GetFile " + name + ": " + err.Error())
		return nil, fuse.EIO
	}
	if len(data) == 0 {
		return nil, fuse.EIO
	}
	templen := int64(len(data))
	if templen < size {
		return nil, fuse.EIO
	}
	return data[:size], fuse.OK
}

//??????sftp??????????????????????????????

func (sf *SnbsDriver) Write(name string, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	glog.V(4).Infoln("SnbsDriver Op Write:", name, "off :", off, "len", len(data))
	_, filename, pathkey, _, patherr := sf.path2name(name)
	if patherr != nil {
		glog.V(4).Infoln(patherr.Error())
		return 1, fuse.EINVAL
	}

	filename = strings.Trim(filename, "/")
	pathkey = strings.Trim(pathkey, "/")
	//??????????????????????????????
	if isbool, ok := sf.Wirtestatue.Load(filename); ok && isbool.(bool) {
		glog.V(0).Infoln("write wrong in last time", filename, isbool.(bool))
		return 1, fuse.EIO
	} else if !ok {
		//???????????????
		sf.Wirtestatue.Store(filename, false)
		if cli := sf.GetTcpClient(filename, pathkey); cli != nil {
			if lensp, err := cli.Open(filename); lensp < 0 && err != nil {
				glog.V(0).Infoln("Open in Read fail fileanme:" + filename + "err:" + err.Error())
			}
		}
	}

	tempdata := make([]byte, len(data), len(data))
	copy(tempdata, data)
	req := &MsgBody{filename, off, tempdata, nil, 0}
	if !sf.GroupIOInfo.InsertCron(req) {
		return 1, fuse.EIO
	}
	if *fuseop.TempFile != "" {
		Filechan <- req
	}

	return uint32(len(data)), fuse.OK
}

func path2name2(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	if !strings.HasPrefix(path, "/") {
		err = errors.New("Invalid path")
		return
	}
	if path == "/" {
		myKey = "/"
		parentKey = ""
		fileName = "/"
		isDir = true
		return
	}
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	parts := strings.Split(path, "/")

	if isDir {
		fileName = parts[len(parts)-2] + "/"
	} else {
		fileName = parts[len(parts)-1]
	}
	myKey = path[1:]
	parentKey = myKey[:len(myKey)-len(fileName)]

	fileName = url.QueryEscape(fileName)
	myKey = url.QueryEscape(myKey)
	parentKey = url.QueryEscape(parentKey)
	return
}

//path????????????
func (sf *SnbsDriver) WriteReader(path string, data io.Reader, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	return uint32(0), fuse.OK
}

//????????????????????????
//??????url????????????
func (sf *SnbsDriver) get_sign(method string, realpath string) map[string]string {
	return nil
}

func (sf *SnbsDriver) uploadnomalfile(object string, data []byte) (int64, error) {
	return 0, nil
}

func (sf *SnbsDriver) Release(file *snbsfs.FileInfo) fuse.Status {

	glog.V(0).Infoln("SnbsDriver release", file.Name)
	if file == nil {
		return fuse.EIO
	}

	if cli := sf.GetTcpClient(file.Name, ""); cli != nil {
		if lensp := cli.Delete(file.Name); lensp < 0 {
			glog.V(0).Infoln("Open in Read fail fileanme:" + file.Name)
			return fuse.EIO
		}
	} else {
		glog.V(0).Infoln("not find client", file.Name)
		return fuse.EIO
	}
	glog.V(0).Infoln("Gwtcpclient delete filename release time:", file.Name)
	sf.Wirtestatue.Delete(file.Name)
	//????????????
	sf.Wirtestatue.Delete(file.Name + "+objattr")

	return fuse.OK
}

func (sf *SnbsDriver) complete(object string, uploadid string, idx int) error {
	return nil
}

func (sf *SnbsDriver) getuploadid(object string) (uploadid string, err error) {
	return
}

func (sf *SnbsDriver) uploadblock(object string, uploadid string, idx int, data []byte) error {
	return nil
}

// ??????????????????????????????????????????
func (sf *SnbsDriver) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
	return nil, fuse.ENOSYS
}

//????????????
func (sf *SnbsDriver) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.OK
}

//??????????????????
func (sf *SnbsDriver) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.OK
}

//????????????????????????
func (sf *SnbsDriver) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	glog.V(0).Infoln(name, attribute)
	return nil, fuse.ENOSYS
}

//?????????????????????
//???????????????????????????
func (sf *SnbsDriver) OnUnmount() {
	return
}

//?????????????????????
//???????????????????????????
func (sf *SnbsDriver) OnMount(nodeFs *snbsfs.Snbsfs) {
	return
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (sf *SnbsDriver) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file *snbsfs.FileInfo, code fuse.Status) {
	//create file inode ,return
	glog.V(0).Infoln("SnbsDriver Op Create:", name)
	tempname := strings.Split(name, ":")
	//???gateway???????????????
	var (
		poolname string = strings.Trim(tempname[0], "/")
		volname  string = tempname[1]
		filesize        = "107374182400" //byte?????????
	)

	req, err := http.NewRequest("PUT", "http://"+sf.GetGatewayIp(volname, poolname)+":"+*sf.GatewayPort+"/region/pool/vol?pool="+poolname+"&volume="+volname+"&size="+filesize, nil)
	if err != nil || req == nil {
		glog.V(0).Infoln("Createfile create httpreq fail " + ": " + err.Error() + req.RequestURI)
		return nil, fuse.EIO
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp == nil {
		glog.V(0).Infoln("Createfile fail (httpclientDo(put)) " + ": " + err.Error())
		return nil, fuse.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		glog.V(0).Infoln("createfile rsp fail "+": "+err.Error(), "respstatus", resp.Status)
		return nil, fuse.EIO
	}
	fileinfostr := &snbsfs.FileInfo{}
	fileinfostr.IsDir = false
	fileinfostr.Name = volname
	fileinfostr.Size = 107374182400
	fileinfostr.LastModified = strconv.FormatInt(time.Now().Unix(), 10)

	return fileinfostr, fuse.OK
}

//?????????
func (sf *SnbsDriver) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}

//?????????????????????????????????
func (sf *SnbsDriver) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	return "", fuse.ENOSYS
}

//??????????????????
func (sf *SnbsDriver) StatFs(name string) *fuse.StatfsOut {
	glog.V(0).Infoln("SnbsDriver StatFs", name)
	temp := &fuse.StatfsOut{}
	body, state := sf.httpRequest("GET", sf.GetGatewayIp(name, ""), "/pool/info?pretty=y", nil)
	if state >= http.StatusBadRequest {
		glog.V(0).Infoln("rsp state fail:", state, name)
		return temp
	}
	var Pools []interface{}
	err := json.Unmarshal(body, &Pools)
	if err != nil {
		glog.V(0).Infoln("unmarshal respbody fail:" + err.Error())
		return temp
	}

	var totalsize, freesize, usesize float64
	for _, v := range Pools {
		objstr, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		poolname, _ := objstr["name"].(string)
		temptotal, _ := objstr["total"].(float64)
		tempused, _ := objstr["used"].(float64)
		tempfree, _ := objstr["free"].(float64)
		glog.V(4).Infoln("poolname", poolname, "totalsize", temptotal, "tempused", tempused, "tempfree", tempfree)
		totalsize += temptotal
		freesize += tempfree
		usesize += tempused
	}

	temp.Bsize = 1024 * 1024 //1M
	temp.Bfree = (uint64(freesize)) / (uint64(temp.Bsize))
	temp.Blocks = (uint64(totalsize)) / (uint64(temp.Bsize))
	temp.Frsize = temp.Bsize
	temp.Bavail = temp.Bfree
	temp.Files = temp.Blocks
	temp.Ffree = temp.Blocks
	temp.NameLen = 250
	return temp
}

func (sf *SnbsDriver) Fsync(name string, data []byte) (code fuse.Status) {
	glog.V(0).Infoln("SnbsDriver Fsync:"+name, len(data))

	sf.GroupIOInfo.StopCron(name)

	return fuse.OK
}

//???????????????????????????????????????????????????????????????????????????
func (sf *SnbsDriver) CheckPasswd() error {
	return nil
}

//???????????????????????????
//????????????url???????????????
func path2name(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	isDir, fileName, parentKey, myKey, err = path2namebase(path)

	fileName = url.QueryEscape(fileName)
	myKey = url.QueryEscape(myKey)
	parentKey = url.QueryEscape(parentKey)
	return
}
func path2namebase(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	if !strings.HasPrefix(path, "/") {
		err = FUSEErrInvalidPath
		return
	}
	if path == "/" {
		myKey = "/"
		parentKey = ""
		fileName = "/"
		isDir = true
		return
	}
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	parts := strings.Split(path, "/")

	//????????????len(parts)>2
	if isDir {
		fileName = parts[len(parts)-2] + "/"
	} else {
		fileName = parts[len(parts)-1]
	}
	myKey = path[1:]
	parentKey = myKey[:len(myKey)-len(fileName)]

	return
}
