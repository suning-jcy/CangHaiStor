package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/fuse/snbsfs"

	"code.suning.com/glog"
	"code.suning.com/util"
)

//path is fs-path
//like "/"  or "/mnt/data/weedfs-redis/src"
func (sf *SnbsDriver) path2name(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	parts := strings.Split(path, "/")
	isDir = false
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	if len(parts) == 2 {
		if parts[1] == "" {
			myKey = "/"
			parentKey = ""
			fileName = "/"
			isDir = true
		} else {
			myKey = parts[1]
			parentKey = "/"
			fileName = parts[1]
			isDir = false
		}

	} else {
		if isDir {
			fileName = parts[len(parts)-2] + "/"
		} else {
			fileName = parts[len(parts)-1]
		}
		myKey = path[1:]
		parentKey = myKey[:len(myKey)-len(fileName)]
		if parentKey == "" {
			parentKey = "/"
		}
	}
	err = nil
	return

}

//object: object_key to filer (+"/" if dir)
//mathod,PUT/DELETE/GET
//parameters,request param
func (sf *SnbsDriver) urlRequestOLD(object, method string, data []byte, parameters map[string]string) (res []byte, err error) {
	return
}

//upload file to filer
//todo 使用ftp一样的流式上传方式
func (sf *SnbsDriver) upload(object string, reader io.Reader) (uint32, error) {
	return 0, nil

}

//todo 鉴权要调整在url中
func (sf *SnbsDriver) getHeaders(bucket, object, method string) map[string]string {
	return nil
}

func (sf *SnbsDriver) httpRequest(method string, gwip string, parturl string, body io.Reader) (rspbody []byte, state int) {

	req, err := http.NewRequest(method, "http://"+GetTgtMasterIP()+parturl, body)
	if req == nil {
		glog.V(0).Infoln("Createfile create httpreq fail", gwip)
		return
	}
	if err != nil {
		glog.V(0).Infoln("Createfile create httpreq fail " + ": " + err.Error())
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp == nil {
		glog.V(0).Infoln("Createfile fail (httpclientDo(put))", method, parturl)
		if err != nil {
			glog.V(0).Infoln("Createfile fail (httpclientDo(put))", "err", err.Error())
		}
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	state = resp.StatusCode
	rspbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(0).Infoln("Read respbody fail " + ": " + err.Error())
	}
	return
}

//包装了重试，请求鉴权放到头内
func (sf *SnbsDriver) urlRequest(object, method string, parameters map[string]string, heads map[string]string) (data []byte, err error) {
	return nil, nil
}

//其他请求 列目录直接使用
func urlRequest(server string, account, bucket, object, method string, parameters map[string]string, heads map[string]string, AccessID, AccessSecret string) (res []byte, err error) {
	return nil, nil
}

func getHeaders(AccessKeyId, AccessKeysecret, bucket, object, method string) map[string]string {
	date := util.ParseUnix(strconv.FormatInt(time.Now().Unix(), 10))
	return getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date)
}

func getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date string) map[string]string {
	return nil
}

func (sf *SnbsDriver) GetPoolInfo(poolname string) (fileinfostr *snbsfs.FileInfo) {
	resp, err := http.Get("http://" + sf.GetGatewayIp("", poolname) + ":" + *sf.GatewayPort + "/pool?poolname=" + poolname)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var Pools map[string]interface{}
	var ok bool
	err = json.Unmarshal(body, &Pools)
	if err != nil {
		return
	}
	fileinfostr = &snbsfs.FileInfo{}
	fileinfostr.IsDir = true
	polFileName, ok := Pools["poolname"].(string)
	if ok {
		fileinfostr.Name = polFileName
	}
	poltime, ok := Pools["createtime"].(string)
	if ok {
		lmtime, err := time.ParseInLocation("2006-01-02 15:04:05", poltime, time.Local)
		if err == nil {
			fileinfostr.LastModified = strconv.FormatInt(lmtime.Unix(), 10)
		}

	}
	polSize, ok := Pools["total_kbyte"].(float64)
	if ok {
		fileinfostr.Size = int64(polSize)
	}
	return
}

//判断gateway是否可用
//设置超时
var testclient = http.Client{
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
	},
	Timeout: 5 * time.Second,
}

func (sf *SnbsDriver) GetVolumnList(filename string, nextmarket string, c map[string]*snbsfs.FileInfo) {

	for i := 0; i < *sf.MaxVolumelist; i++ {
		body, state := sf.httpRequest("GET", sf.GetGatewayIp("", ""), "/region/pool?marker="+nextmarket+"&pretty=y&max-keys=1000", nil)
		if state >= http.StatusBadRequest {
			glog.V(0).Infoln("rsp state fail:", state, filename)
			return
		}
		var (
			ok         bool
			Volumes    map[string]interface{}
			VolumeRoot []interface{}
		)
		err := json.Unmarshal(body, &Volumes)
		if err != nil {
			glog.V(0).Infoln("unmarshal respbody fail " + ": " + err.Error())
			return
		}

		VolumeRoot, ok = Volumes["Volumes"].([]interface{})
		if !ok {
			glog.V(0).Infoln("Get Volumes fail ")
			return
		}

		for _, v := range VolumeRoot {
			volstr, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			//选择pool目录
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
		// 判断是否还有list
		nextmarket, ok = Volumes["NextMarker"].(string)
		if !ok {
			break
		}
	}
	return
}

//设置超时
var masterclient = http.Client{
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
	},
	Timeout: 5 * time.Second,
}

type ServerInfo struct {
	IpPort      string `json:"IpPort"`
	Ip          string
	Online      bool   `json:"Online"`
	VolumeCount int    `json:"VolumeCount"`
	PoolID      string `json:"PoolID"`
	ZoneID      string `json:"ZoneID"`
}

type RegionIPInfo struct {
	IPInfo []*ServerInfo
	//key保存pool名
	PoolIPInfo map[string][]*ServerInfo
	//key保存卷名
	VolumeIPInfo map[string]*ServerInfo
	sync.RWMutex
}

func NewRegionIPInfo() *RegionIPInfo {
	return &RegionIPInfo{
		IPInfo:       make([]*ServerInfo, 0),
		PoolIPInfo:   make(map[string][]*ServerInfo),
		VolumeIPInfo: make(map[string]*ServerInfo),
	}
}

func (region *RegionIPInfo) Print() {
	region.RLock()
	defer region.RUnlock()
	for _, v := range region.IPInfo {
		glog.V(0).Infoln("the gateway ip info:", v.Ip, v.PoolID, v.Online)
	}
}

func HTTPGetTgtMasterInfo(tempmasterip string, masterport string) (bool, []byte) {
	if nil == net.ParseIP(tempmasterip) {
		return false, nil
	}
	resp, err := masterclient.Get("http://" + tempmasterip + ":" + masterport + "/tgtmaster/gatewaymap?pretty=y&all=y")
	if err != nil || resp == nil {
		glog.V(0).Infoln("create get req fail", tempmasterip)
		return false, nil
	}
	defer resp.Body.Close()
	rspbody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(0).Infoln("read resp body fail", err.Error())
		return false, nil
	}
	return true, rspbody
}

//用来检测gateway节点服务是否正常
func CheckServer(addr string, network string) bool {
	conn, err := net.DialTimeout(network, addr, time.Duration(time.Second))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

//从tgtmaster更新gateway信息
func (region *RegionIPInfo) Update(tgtmasterport string) (updateok bool) {
	for _, v := range fuseop.TotalMasterIps {
		isbool, rspbody := HTTPGetTgtMasterInfo(v, tgtmasterport)
		if !isbool {
			continue
		}

		var (
			ok             bool
			Gatewayips     map[string]interface{}
			GatewayipsRoot []interface{}
		)
		err := json.Unmarshal(rspbody, &Gatewayips)
		if err != nil {
			glog.V(0).Infoln("unmarshal fail")
			return
		}

		GatewayipsRoot, ok = Gatewayips["ServerInfos"].([]interface{})
		if !ok {
			glog.V(0).Infoln("Get ServerInfos fail")
			return
		}

		for _, v := range GatewayipsRoot {
			volstr, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			var Serverinfo ServerInfo
			Serverinfo.IpPort, ok = volstr["IpPort"].(string)
			if ok {
				Serverinfo.Ip, _, _ = net.SplitHostPort(Serverinfo.IpPort)
			} else {
				continue
			}
			Serverinfo.Online, ok = volstr["Online"].(bool)
			Serverinfo.VolumeCount, ok = volstr["VolumeCount"].(int)
			Serverinfo.PoolID, ok = volstr["PoolID"].(string)
			Serverinfo.ZoneID, ok = volstr["ZoneID"].(string)

			region.RLock()
			var okquery bool
			var info *ServerInfo
			for _, v := range region.IPInfo {
				if v.Ip == Serverinfo.Ip && v.Ip != "" {
					okquery = true
					info = v
				}
			}
			region.RUnlock()

			if okquery && info != nil {
				region.Lock()
				info.Ip = Serverinfo.Ip
				info.IpPort = Serverinfo.IpPort
				info.Online = Serverinfo.Online
				info.PoolID = Serverinfo.PoolID
				info.ZoneID = Serverinfo.ZoneID
				region.Unlock()
			} else {
				if !(Serverinfo.Online && CheckServer(Serverinfo.IpPort, "tcp")) {
					continue
				}

				region.Lock()
				region.IPInfo = append(region.IPInfo, &Serverinfo)
				if Serverinfo.PoolID != "" {
					region.PoolIPInfo[Serverinfo.PoolID] = append(region.PoolIPInfo[Serverinfo.PoolID], &Serverinfo)
				}
				region.Unlock()

			}
			updateok = true

		}
		if updateok {
			break
		}
	}
	return
}

func (region *RegionIPInfo) GetIP(filename string, poolname string) (rspip string) {
	region.Lock()
	defer region.Unlock()
	//如果卷名所在的地址已经存在，沿用以前
	if filename != "" {
		v, ok := region.VolumeIPInfo[filename]
		if ok && v.Online && CheckServer(v.IpPort, "tcp") {
			return v.Ip
		}
	}

	var rspserver *ServerInfo
	defer func() {
		//如果没有池的信息或者这次获取有池名//用来下次复用
		if rspserver != nil && (poolname != "" || len(region.PoolIPInfo) == 0) && filename != "" {
			region.VolumeIPInfo[filename] = rspserver
			glog.V(0).Infoln("matchip", filename, rspserver.Ip, rspserver.PoolID)
		}
	}()

	//如果有池信息同时有池的名称首先从池中获取
	if poolname != "" {
		serverinfoSlice, ok := region.PoolIPInfo[poolname]
		if ok {
			for _, v := range serverinfoSlice {
				if v.Online && CheckServer(v.IpPort, "tcp") {
					rspserver = v
					return v.Ip
				}
			}
		}
	}
	//优先获取pool为空的gatewayip
	for _, v := range region.IPInfo {
		if v.PoolID == "" && v.Online && CheckServer(v.IpPort, "tcp") {
			rspserver = v
			return v.Ip
		}
	}
	//最后挑选一个可用的gateway
	for _, v := range region.IPInfo {
		if v.Online && CheckServer(v.IpPort, "tcp") {
			rspserver = v
			return v.Ip
		}
	}

	glog.V(0).Infoln("not find the gateway ip poolname: ", poolname)
	return ""
}

func GetTgtMasterIP() (addr string) {
	for _, tgtmasterip := range fuseop.TotalMasterIps {
		if CheckServer(net.JoinHostPort(tgtmasterip, *fuseop.MasterPort), "tcp") {
			return net.JoinHostPort(tgtmasterip, *fuseop.MasterPort)
		}
	}
	return ""
}

//tcp must exist
func (sf *SnbsDriver) GetTcpClient(filename string, poolname string) (client *GwTcpClient) {
	//find the filegateway tcp client
	if c, ok := sf.GwClientMap.Load(filename); ok {
		if c.(*GwTcpClient).IsTimeOut(20 * time.Second) {
			if !c.(*GwTcpClient).ReConnect() {
				glog.V(0).Infoln("reconnect fail")
			}
		}
		return c.(*GwTcpClient)
	} else {
		//not find and create
		//first find the gateway
		body, state := sf.httpRequest("GET", sf.GetGatewayIp(filename, poolname), "/region/pool/vol?volume="+filename+"&pretty=y", nil)
		if state >= http.StatusBadRequest {
			glog.V(0).Infoln("rsp state fail:", state, filename)
			return nil
		}

		var Volumes map[string]interface{}
		err := json.Unmarshal(body, &Volumes)
		if err != nil {
			glog.V(0).Infoln("unmarshal respbody fail " + ": " + err.Error())
			return nil
		}
		gatewayaddr, ok := Volumes["Owner"].(string)
		if ok {
			gatwayip, _, err := net.SplitHostPort(gatewayaddr)
			//有可能会出现么有归属的gateway
			if err != nil {
				gatwayip = sf.GetGatewayIp(filename, "")
			}
			gwtcpclient, err := NewClient(gatwayip+":"+*fuseop.GatewayTcpPort, 20*time.Second)
			if err != nil {
				glog.V(0).Infoln("create tcp fail", err.Error()+gatwayip+":"+*fuseop.GatewayTcpPort)
				return nil
			} else {
				sf.GwClientMap.Store(filename, gwtcpclient)
				return gwtcpclient
			}
		} else {
			glog.V(0).Infoln("not find owner")
			return nil
		}
	}
}

func (sf *SnbsDriver) DelTcpClient(filename string) {
	if c, ok := sf.GwClientMap.Load(filename); ok {
		c.(*GwTcpClient).Close()
		sf.GwClientMap.Delete(filename)
	}
}

//用来重新更新卷的归属gateway//主要出现过更换的情况
func (sf *SnbsDriver) CheckAndSetClient(filename string, gatewayip string) {
	if c, ok := sf.GwClientMap.Load(filename); ok {
		if !strings.Contains(c.(*GwTcpClient).Addr, gatewayip) {
			glog.V(0).Infoln("gateway ip not ==", gatewayip, c.(*GwTcpClient).Addr)
			gwtcpclient, err := NewClient(gatewayip+":"+*fuseop.GatewayTcpPort, 20*time.Second)
			if err != nil {
				glog.V(0).Infoln("create tcp fail", err.Error())
			} else {
				sf.GwClientMap.Store(filename, gwtcpclient)
				c.(*GwTcpClient).Close()
			}
		} else {
			if c.(*GwTcpClient).IsTimeOut(20 * time.Second) {
				if !c.(*GwTcpClient).ReConnect() {
					glog.V(0).Infoln("reconnect fail")
				}
			}
		}
	}
}

func (sf *SnbsDriver) GetGatewayIp(filename string, poolname string) (rsp string) {

	if c, ok := sf.GwClientMap.Load(filename); ok && filename != "" {
		ip, _, _ := net.SplitHostPort(c.(*GwTcpClient).Addr)
		rsp = ip
	} else {
		rsp = sf.Regioninfo.GetIP(filename, strings.Trim(poolname, "/"))
	}

	return
}

type WriteCron struct {
	filename   string
	poolname   string
	owngateway string
	status     int32
	cancel     context.CancelFunc
	cronch     map[int]chan *MsgBody

	cronchnum int
	wmaxnum   *int32
	//fuse接受qemu 上传的次数
	RcvNum int64
	//fuse写入gateway成功的次数
	SendOkNum int64

	time.Time
	sync.RWMutex
}

const (
	FileStatusNormal = iota
	FileStatusRspErr
	FileStatusInsertFail
)

func (w *WriteCron) Handle(client *GwTcpClient, ctx context.Context, num int) {
	defer func() {
		client.Close()
		atomic.AddInt32(w.wmaxnum, 1)
		glog.V(0).Infoln("Realase client", w.filename)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.cronch[num]:
			w.Send(req, client)
		}
	}
}

func (w *WriteCron) Send(req *MsgBody, clienttep *GwTcpClient) {
	if req == nil || clienttep == nil {
		return
	}
	var lenrsp int64
	for i := 0; i < loopctx; i++ {
		lenrsp = clienttep.Write(req.Filename, int64(len(req.Data)), uint64(req.Offset), req.Data)
		if lenrsp < 0 {
			glog.V(0).Infoln("write fail fileanme:"+req.Filename, "offset:", req.Offset, "rsp", lenrsp)
			if lenrsp != gatewayrsperr {
				time.Sleep(time.Second * 10)
			} else {
				break
			}
		}
	}
	//如果写入失败，对这次写任务设置状态
	if lenrsp < 0 {
		atomic.StoreInt32(&w.status, FileStatusRspErr)
	}
	atomic.AddInt64(&w.SendOkNum, 1)
}

func (w *WriteCron) Run() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	//如果没有资源先等待
	for w.GetNum() == 0 {
		time.Sleep(time.Second * 3)
		glog.V(0).Infoln("wait the num now", w.filename, w.poolname)
	}

	n := w.GetNum()
	for i := 0; i < n; i++ {
		ch := make(chan *MsgBody, writechlen)
		w.cronch[i] = ch
	}

	w.Time = time.Now()

	//分配客户端
	for i := 0; i < n; i++ {
		cli, err := NewClient(w.owngateway+":"+*fuseop.GatewayTcpPort, 60*time.Second)
		if err != nil {
			glog.V(0).Infoln("get client fail", err.Error())
			continue
		}
		atomic.AddInt32(w.wmaxnum, -1)
		go w.Handle(cli, ctx, w.cronchnum)
		w.cronchnum++
	}
}

//用来控制速度和fuse内存占用情况
//todo
func (w *WriteCron) GetNum() int {
	num := atomic.LoadInt32(w.wmaxnum)
	switch {
	case num > MaxNumTcpClient/2:
		return 32

	case num > MaxNumTcpClient/4:
		return 16

	case num > MaxNumTcpClient/8:
		return 8

	case num > MaxNumTcpClient/16:
		return 4

	case num > MaxNumTcpClient/32:
		return 2

	default:
		return 0
	}
}

func (w *WriteCron) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

type IOInfo struct {
	wfiles  map[string]*WriteCron
	wlock   sync.RWMutex
	wmaxnum int32
	//todo 后面增加读操作
}

const (
	//最大客户端数量
	MaxNumTcpClient = 256
	loopctx         = 5
	writechlen      = 500
)

func NewIOInfo() *IOInfo {
	ioinfo := &IOInfo{
		wfiles:  make(map[string]*WriteCron),
		wmaxnum: MaxNumTcpClient,
	}
	go ioinfo.Recycle()
	return ioinfo
}

func (ioInfo *IOInfo) NewWriteCron(filename, poolname, gatewayip string) bool {
	ioInfo.wlock.RLock()
	_, ok := ioInfo.wfiles[filename]
	ioInfo.wlock.RUnlock()
	if ok {
		return false
	}

	writecron := &WriteCron{
		filename:   filename,
		poolname:   poolname,
		owngateway: gatewayip,
		wmaxnum:    &ioInfo.wmaxnum,
		cronch:     make(map[int]chan *MsgBody),
	}

	ioInfo.wlock.Lock()
	ioInfo.wfiles[filename] = writecron
	ioInfo.wlock.Unlock()

	go writecron.Run()

	return true
}

func (ioInfo *IOInfo) InsertCron(req *MsgBody) bool {
	ioInfo.wlock.RLock()
	winfo, ok := ioInfo.wfiles[req.Filename]
	ioInfo.wlock.RUnlock()
	if !ok {
		glog.V(0).Infoln("not find in IOInfo", req.Filename)
		return false
	}

	if atomic.LoadInt32(&winfo.status) != FileStatusNormal {
		glog.V(0).Infoln("cron status is wrong", atomic.LoadInt32(&winfo.status))
		return false
	}

	select {
	case winfo.cronch[int(winfo.RcvNum%int64(winfo.cronchnum))] <- req:
		atomic.AddInt64(&winfo.RcvNum, 1)
	case <-time.After(time.Minute):
		glog.V(0).Infoln("Insert Cron time out:", req.Filename, req.Offset)
		atomic.StoreInt32(&winfo.status, FileStatusInsertFail)
		return false
	}
	return true
}

func (ioInfo *IOInfo) StopCron(filename string) {
	ioInfo.wlock.RLock()
	winfo, ok := ioInfo.wfiles[filename]
	ioInfo.wlock.RUnlock()
	if !ok {
		glog.V(0).Infoln("not find in IOInfo", filename)
		return
	}

	glog.V(0).Infoln("stop cron", filename)

	if atomic.LoadInt64(&winfo.RcvNum) == 0 {
		return
	}

	t := time.Now()

	for atomic.LoadInt32(&winfo.status) == FileStatusNormal &&
		atomic.LoadInt64(&winfo.SendOkNum) != atomic.LoadInt64(&winfo.RcvNum) {

		if time.Now().Sub(t) > time.Minute*20 {
			glog.V(0).Infoln("flush time out", winfo.filename)
			break
		}

		time.Sleep(time.Second)
	}

	winfo.Stop()

	ioInfo.wlock.Lock()
	delete(ioInfo.wfiles, winfo.filename)
	ioInfo.wlock.Unlock()
}

func (ioInfo *IOInfo) Recycle() {
	t := time.NewTicker(time.Minute * 5)
	defer t.Stop()
	var temp = make(map[string]*WriteCron)
	for range t.C {
		ioInfo.wlock.RLock()
		for k, v := range ioInfo.wfiles {
			temp[k] = v
		}
		ioInfo.wlock.RUnlock()

		for k, v := range temp {
			//这边设置如果超过30min就强制释放
			if time.Now().Sub(v.Time) > time.Minute*30 {
				v.Stop()
				ioInfo.wlock.Lock()
				delete(ioInfo.wfiles, k)
				ioInfo.wlock.Unlock()
				glog.V(0).Infoln("Recycle", v.filename)
			}
		}
		temp = make(map[string]*WriteCron)
	}
}
