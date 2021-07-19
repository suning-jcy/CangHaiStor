// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A Go mirror of libfuse's hello.c

package main

import (
	"bytes"
	"container/list"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/snbsfs"

	"code.suning.com/glog"
	"code.suning.com/util"
)

var (
	fuseop FuseOptions
)

var usageTemplate = `
snbsfuse  : fuse file systems supported by snbs!

Usage:

	snbsfuse command [arguments]

The commands are:
   {{.Name | printf "%-11s"}} {{.Short}}

Use "snbsfuse help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: snbsfuse {{.UsageLine}}
{{end}}
 {{.Long}}
`
var IsDebug *bool

var Filechan = make(chan *MsgBody, 5000)
var Readchan = make(chan *MsgBody, 100)

type FuseOptions struct {
	accessKeyId     *string
	account         *string
	bucket          *string
	accessKeysecret *string
	filer           *string
	mountpoint      *string
	conffile        *string
	numtransfer     *int
	contentMD5      *bool
	diskdir         *string
	cacheclear      *bool

	cachesum          *int
	maxcachesum       *int
	cachetimeout      *int
	minsubmitinterval *int
	dirtyscaninterval *int

	maxwrite       *int
	maxbackground  *int
	rememberinodes *bool
	debug          *bool
	pprofport      *int
	StaticSwitch   *bool
	vMaxCpu        *int

	MaxInodeSum *int

	FuseIp          *string
	FusePort        *int
	MasterIp        *string
	TotalMasterIps  []string
	MasterPort      *string
	GatewayIp       *string //add snbs gatewayaddrs
	TotalGatewayIps []string
	GatewayPort     *string //add snbs gatewayport
	GatewayTcpPort  *string //add snbs gatewayTcpport
	MaxVolumelist   *int
	ChunkSize       *int
	Near            *int
	Repnum          *int
	GPort           *int
	MaxShowFileSum  *int
	MaxCachetime    *int
	AllowOther      *bool
	BlockSize       *int
	PortalAddr      *string
	MinPort         *int //snbsfuse进程使用的端口最小值
	MaxPort         *int //snbsfuse进程使用的端口最大值  如果MinPort或者MaxPort有一个没有配置，就使用默认端口
	TempFile        *string
}

func init() {
	cmdSnbsFuse.Run = runSnbsFuse

	fuseop.mountpoint = cmdSnbsFuse.Flag.String("mountpoint", "", "path to mount")
	fuseop.filer = cmdSnbsFuse.Flag.String("snbsendpoint", "", "snbs-endpoint")

	fuseop.conffile = cmdSnbsFuse.Flag.String("conffile", "", "path of accout&id&sec")
	fuseop.contentMD5 = cmdSnbsFuse.Flag.Bool("contentMD5", false, " check md5sum ")
	fuseop.diskdir = cmdSnbsFuse.Flag.String("diskdir", "", "local cache")
	fuseop.cacheclear = cmdSnbsFuse.Flag.Bool("cacheclear", false, "clean cache on exit")

	fuseop.maxcachesum = cmdSnbsFuse.Flag.Int("maxcachesum", 600, "max cache block sum")
	fuseop.cachetimeout = cmdSnbsFuse.Flag.Int("cachetimeout", 1, "timeout for cache  ,in s")
	fuseop.numtransfer = cmdSnbsFuse.Flag.Int("numtransfer", 36, "max transfersum with snbs")
	fuseop.minsubmitinterval = cmdSnbsFuse.Flag.Int("minsubmitinterval", 100, "min file submit Interval to snbs,in ms")
	fuseop.dirtyscaninterval = cmdSnbsFuse.Flag.Int("dirtyscaninterval", 10, "snbsfuse dirty block scan interval,in ms")

	fuseop.debug = cmdSnbsFuse.Flag.Bool("debug", false, "debug mode ,default false")
	fuseop.pprofport = cmdSnbsFuse.Flag.Int("pprofport", 0, "pprof port")

	fuseop.maxwrite = cmdSnbsFuse.Flag.Int("maxwrite", 1<<17, "maxwrite ,default 128K")
	fuseop.maxbackground = cmdSnbsFuse.Flag.Int("maxbackground", 12, "This numbers controls the allowed number of requests that relate to  async I/O")
	fuseop.rememberinodes = cmdSnbsFuse.Flag.Bool("rememberinodes", false, "If RememberInodes is set, we will never forget inodes. This may be useful for NFS.")
	fuseop.vMaxCpu = cmdSnbsFuse.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")

	fuseop.StaticSwitch = cmdSnbsFuse.Flag.Bool("StaticSwitch", true, " ")

	fuseop.MaxInodeSum = cmdSnbsFuse.Flag.Int("maxInodeSum", 3000000, "maximum number of Inodess")
	fuseop.MaxShowFileSum = cmdSnbsFuse.Flag.Int("maxshowfilesum", 3000, "maximum number of files while list")

	fuseop.FuseIp = cmdSnbsFuse.Flag.String("ip", "", "ip or server name")
	fuseop.FusePort = cmdSnbsFuse.Flag.Int("port", 9696, "http listen port")
	fuseop.GPort = cmdSnbsFuse.Flag.Int("gport", 7786, "")
	fuseop.AllowOther = cmdSnbsFuse.Flag.Bool("allowOther", false, "allow other users to visit fuse mount dir")
	fuseop.MaxCachetime = cmdSnbsFuse.Flag.Int("maxCacheTime", 2, "how long does a block cache keeps(second)")
	fuseop.BlockSize = cmdSnbsFuse.Flag.Int("blockSize", 4*1024*1024, "block buffer size")
	fuseop.PortalAddr = cmdSnbsFuse.Flag.String("portalAddr", "", "portal server address")
	fuseop.MinPort = cmdSnbsFuse.Flag.Int("minPort", 0, "min port for fuse and it's pprof")
	fuseop.MaxPort = cmdSnbsFuse.Flag.Int("maxPort", 0, "max port for fuse and it's pprof")

	fuseop.MasterIp = cmdSnbsFuse.Flag.String("tgtmasterIp", "", "tgtMaster Ip")
	fuseop.MasterPort = cmdSnbsFuse.Flag.String("tgtmasterPort", "", "tgtMasterPort")

	fuseop.GatewayPort = cmdSnbsFuse.Flag.String("gatewayPort", "", "Gateway Port")
	fuseop.GatewayTcpPort = cmdSnbsFuse.Flag.String("gatewayTcpPort", "", "Gateway TcpPort")
	fuseop.MaxVolumelist = cmdSnbsFuse.Flag.Int("maxvolumelist", 1, "max volume list nums")
	fuseop.ChunkSize = cmdSnbsFuse.Flag.Int("chunksize", 0, "chunksize")
	fuseop.Near = cmdSnbsFuse.Flag.Int("near", -1, "Near")
	fuseop.TempFile = cmdSnbsFuse.Flag.String("tempfile", "", "tempfile")
	fuseop.Repnum = cmdSnbsFuse.Flag.Int("repnum", -1, "Repnum")
}

var cmdSnbsFuse = &Command{
	UsageLine: "snbsfuse  -mountpoint=/tmp/snbsfs -mountpoint=/mnt/etcd/lutest/lufuse " +
		"-debug=false -tgtmasterIp=10.242.180.211 " +
		" –tgtmasterPort=8888 -gatewayPort=8686 -gatewayTcpPort=8585 ",
	Short: "snbsfuse expired objects",
	Long: `This tool is used to connect snbs with fuse

  `,
}

func runSnbsFuse(cmd *Command, args []string) bool {
	defer func() {
		time.Sleep(time.Second * 3)
	}()
	OnSighup(func() {
		glog.RotateHttpFile()
	})
	if *fuseop.vMaxCpu < 1 {
		*fuseop.vMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*fuseop.vMaxCpu)

	//必选参数判断
	if *fuseop.GatewayPort == "" || *fuseop.MasterIp == "" ||
		*fuseop.mountpoint == "" || *fuseop.GatewayTcpPort == "" {
		glog.Fatalf("MasterIp,GatewayIp,mountpoint,GatewayTcpPort can not be null!")
		return false
	}

	//chunksize大小判断
	if *fuseop.ChunkSize != 0 && *fuseop.ChunkSize != 64 &&
		*fuseop.ChunkSize != 128 && *fuseop.ChunkSize != 256 {
		glog.Fatalf("chunksize not right!", *fuseop.ChunkSize)
		return false
	}

	//获取tgtmaster 地址列表
	fuseop.TotalMasterIps = strings.Split(*fuseop.MasterIp, ",")

	glog.V(0).Infoln("tgtmasterip:", fuseop.TotalMasterIps)

	//从tgmaster  获取gateway地址
	regioninfo := NewRegionIPInfo()
	if !regioninfo.Update(*fuseop.MasterPort) {
		glog.Fatalf("Get gatewayips fail")
		return false
	}
	regioninfo.Print()

	//挂载点信息
	info, err := os.Stat(*fuseop.mountpoint)
	if err != nil || !info.IsDir() {
		glog.Fatalf("can not find mountpoint or it's not a dir")
		return false
	}

	/*------------PprofPort 调试监听---------*/
	if *fuseop.debug {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		runtime.SetCPUProfileRate(1)
		go func() {
			if *fuseop.pprofport > 0 {
				pproferr := http.ListenAndServe(":"+fmt.Sprint(*fuseop.pprofport), nil)
				if pproferr != nil {
					glog.V(0).Infof("open pprof err:%s", pproferr.Error())
				}
			}
		}()
	}

	//fuse层面配置 //需要
	fuseopt := &snbsfs.Options{}
	fuseopt.Debug = *fuseop.debug
	fuseopt.MaxWrite = *fuseop.maxwrite
	fuseopt.MaxBackground = *fuseop.maxbackground
	fuseopt.RememberInodes = *fuseop.rememberinodes
	fuseopt.MaxInodesSum = *fuseop.MaxInodeSum
	fuseopt.AllowOther = *fuseop.AllowOther
	if *fuseop.conffile == "" {
		//没有配置的时候，默认weed所在位置
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		*fuseop.conffile = path.Join(dir, "/passwd-snbsfs")
	}

	//实例化driver
	tempSdsnbsfs := &SnbsDriver{
		GatewayPort:    fuseop.GatewayPort,
		GatewayTcpPort: fuseop.GatewayTcpPort,
		Mountpoint:     fuseop.mountpoint,
		MaxVolumelist:  fuseop.MaxVolumelist,
		Wirtestatue:    sync.Map{},
		SendMsg:        make(chan *MsgBody, 5000),
		GwClientMap:    sync.Map{},
		Regioninfo:     regioninfo,
		GroupIOInfo:    NewIOInfo(),
	}
	//挂载
	ofs := snbsfs.NewSnbsfs(tempSdsnbsfs, nil)
	server, _, err := snbsfs.MountRoot(*fuseop.mountpoint, ofs.Root(), fuseopt)
	if err != nil {
		glog.Fatalf("Mount fail: %v\n", err)
	}
	OnInterrupt(func() {
		server.Unmount()
	})

	//定时任务
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for range t.C {
			if !regioninfo.Update(*fuseop.MasterPort) {
				glog.V(0).Infof("now we get the gatewayip is fail!!!")
			}
			runtime.GC()
		}
	}()

	//tempdatawrite
	glog.V(0).Infoln("tempfile", *fuseop.TempFile)
	if *fuseop.TempFile != "" {
		go WriteTempFile()
	}
	//read cache
	go ReadCacheHandle(tempSdsnbsfs)

	//启动fuse server
	server.Serve()
	return true
}

func fuseTpsHandler(server *fuse.Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		tempstr := server.TpsStats.GetStatic()
		bytes, _ := json.Marshal(tempstr)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
		w.Write(bytes)
		return
	}
}
func fuseLogHandler(w http.ResponseWriter, r *http.Request) {
	tmpV := r.FormValue("v")
	v, err := strconv.Atoi(tmpV)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	glog.SetVerbosity(v)
	glog.V(-1).Infoln("set glog verbosity to", v)
	w.WriteHeader(http.StatusOK)
	return
}
func fuseVerHandler(w http.ResponseWriter, r *http.Request) {

	m := make(map[string]interface{})
	verstr := "version " + util.VERSION + " " + runtime.GOOS + " " + runtime.GOARCH
	m["Version"] = verstr
	bytes, _ := json.Marshal(m)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
	w.Write(bytes)
	return

}

func fuseMacStatusHandler(w http.ResponseWriter, r *http.Request) {
	ioutil.ReadAll(r.Body)
	m := make(map[string]interface{})
	var mstatus runtime.MemStats
	runtime.ReadMemStats(&mstatus)
	m["Mem"] = mstatus
	bytes, _ := json.Marshal(m)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
	_, err := w.Write(bytes)
	if err != nil {
		glog.V(0).Infoln(err)
	}
	return

}

func getKey(ip string) []byte {
	strKey := ip + ":snbs_fuse_aes_key_suffix"
	arrKey := []byte(strKey)
	//取前16个字节
	return arrKey[:16]
}

//加密字符串
func Encrypt(ip, strMesg string) (string, error) {
	key := getKey(ip)
	var iv = []byte(key)[:aes.BlockSize]
	encrypted := make([]byte, len(strMesg))
	aesBlockEncrypter, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aesEncrypter := cipher.NewCFBEncrypter(aesBlockEncrypter, iv)
	aesEncrypter.XORKeyStream(encrypted, []byte(strMesg))
	return base64.URLEncoding.EncodeToString(encrypted), nil
}

//解密字符串
func Decrypt(ip, src string) (strDesc string, err error) {
	defer func() {
		//错误处理
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	key := getKey(ip)
	tempdata, err := base64.URLEncoding.DecodeString(src)
	if err != nil {
		return "", err
	}
	var iv = []byte(key)[:aes.BlockSize]
	decrypted := make([]byte, len(tempdata))
	var aesBlockDecrypter cipher.Block
	aesBlockDecrypter, err = aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	aesDecrypter := cipher.NewCFBDecrypter(aesBlockDecrypter, iv)
	aesDecrypter.XORKeyStream(decrypted, tempdata)

	return string(decrypted), nil
}

func main() {
	glog.MaxSize = 1024 * 1024 * 32
	rand.Seed(time.Now().UnixNano())
	flag.Usage = snbsFuseUsage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		snbsFuseUsage()
	}

	if args[0] == "help" {
		fusehelp(args[1:])
		fmt.Fprintf(os.Stderr, "Default Parameters:\n")
		cmdSnbsFuse.Flag.PrintDefaults()
		return
	}
	if args[0] == "version" {
		fmt.Printf("version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
		return
	}

	if cmdSnbsFuse.Run != nil {
		cmdSnbsFuse.Flag.Usage = func() {
			cmdSnbsFuse.Usage()
		}
		cmdSnbsFuse.Flag.Parse(args[1:])
		args = cmdSnbsFuse.Flag.Args()
		IsDebug = cmdSnbsFuse.IsDebug
		if !cmdSnbsFuse.Run(cmdSnbsFuse, args) {
			fmt.Fprintf(os.Stderr, "\n")
			cmdSnbsFuse.Flag.Usage()
			fmt.Fprintf(os.Stderr, "Default Parameters:\n")
			cmdSnbsFuse.Flag.PrintDefaults()
		}
		exit()
		return
	}

	fmt.Fprintf(os.Stderr, "snbsfuse: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

// help 相关----------
func fusehelp(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'weed help'.
		return
	}

	tmpl(os.Stdout, helpTemplate, cmdSnbsFuse)
	// not exit 2: succeeded at 'weed help cmd'.
	return

}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}
func snbsFuseUsage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"snbsfuse [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, cmdSnbsFuse)
}
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

// help 相关----------

// Command模板----------
//从weed文件夹下面直接拷贝过来的，这个基本不会改动和变更，至于代码结构，以后重构再说吧
type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run func(cmd *Command, args []string) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	Short string

	// Long is the long message shown in the 'go help <this-command>' output.
	Long string

	// Flag is a set of flags specific to this command.
	Flag flag.FlagSet

	IsDebug *bool

	//add by hujianfei in 20141010
	//to read the param from conf file
	//paramConf *string

	//the sectorNmae in conf file
	//sectorName string
}

func (c *Command) Name() string {
	name := c.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: weed %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

func (c *Command) Runnable() bool {
	return c.Run != nil
}

// Command模板----------

func OnInterrupt(fn func()) {
	// deal with control+c,etc
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		for _ = range signalChan {
			fn()
			os.Exit(0)
		}
	}()
}

func OnSighup(fn func()) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1)
	go func() {
		for _ = range signalChan {
			fn()
		}
	}()
}

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

var atexitFuncs []func()

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	os.Exit(exitStatus)
}

// 中断退出相关----------

type registResult struct {
	Result  bool
	Message string
}

type registContent struct {
	AccountName string `json:"accountName,omitempty"` //account
	BucketName  string `json:"bucketName,omitempty"`  //bucket
	Ip          string `json:"ip,omitempty"`          //fuse的IP
	Cachepath   string `json:"cachepath,omitempty"`   //缓存路径
	Description string `json:"description,omitempty"` //描述
	Dir         string `json:"dir,omitempty"`         //挂载点
	Port        string `json:"port,omitempty"`        //fuse的port
	SnbsUrl     string `json:"snbsUrl,omitempty"`     //snbs集群的域名
	GateName    string `json:"gateName,omitempty"`    //网关
}

//向portal注册
func Register(portalServer string, fusePara registContent) (ret registResult) {
	ret.Result = false
	ParaMeter, err := json.Marshal(fusePara)
	if err != nil {
		glog.V(0).Infoln("marshal err:", err)
		ret.Message = err.Error()
		return ret
	}

	url := portalServer + "gateway/saveSelfGateWay.htm"
	client := &http.Client{
		Timeout: 5 * time.Second, ///设定超时时间
	}
	body_buf := bytes.NewBuffer(ParaMeter)
	contentType := "application/json;charset=utf-8"
	request, _ := http.NewRequest("POST", url, body_buf)
	request.Header.Set("Content-Type", contentType)
	response, err := client.Do(request)
	if err != nil {
		ret.Message = err.Error()
		glog.V(0).Infoln(" post req:", url, " fail,err:", err)
		return ret
	}
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	if response.StatusCode == 200 {
		unmarshal_err := json.Unmarshal(body, &ret)
		if unmarshal_err != nil {
			glog.V(0).Infoln("failing to get resonse", url, " err:", unmarshal_err)
			return ret
		}
		glog.V(0).Infoln("register ok! message:", ret.Message)
	} else {
		ret.Message = response.Status
		glog.V(0).Infoln("register fail! statuscode:", response.StatusCode)
	}
	return ret
}

//snbsfuse从本地文件中读取可用端口
func PortTest(ip string, port int) (available bool) {
	address := ip + ":" + strconv.Itoa(port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		available = false
		return available
	}
	l.Close()
	available = true
	return available
}

//从可用的端口范围中找到两个端口，分配给fuse和pprof
func CheckFusePort(min, max int) (fuseport, gport int) {
	//如果未指定范围，则使用默认端口
	if min == 0 || max == 0 || min >= max {
		return *fuseop.FusePort, *fuseop.GPort
	}
	fuseport = 0
	gport = 0
	var start = min
	for {
		if fuseport == 0 {
			if PortTest(*fuseop.FuseIp, start) {
				fuseport = start
			}
			start++
		}
		if gport == 0 {
			if PortTest(*fuseop.FuseIp, start) {
				gport = start
			}
			start++
		}

		if start > max || (fuseport != 0 && gport != 0) {
			break
		}
	}
	return
}

func WriteTempFile() {
	tempfile, err := os.OpenFile(*fuseop.TempFile, os.O_CREATE|os.O_WRONLY, 0)
	if err != nil {
		glog.V(0).Infoln("open or create file fail", err.Error())
		return
	}
	defer tempfile.Close()

	for {
		select {
		case req := <-Filechan:
			_, err := tempfile.WriteAt(req.Data, req.Offset)
			if err != nil {
				glog.V(0).Infoln("write temp file fail", err.Error(), req.Filename, req.Offset)
			}
		}
	}

}

func WriteCache(tempSdsnbsfs *SnbsDriver) {
	var (
		startreqkey  int
		startreqbool bool
		Cancel       context.CancelFunc
		filename     string
		sf           = tempSdsnbsfs
		num          = 32
		timecancel   = time.NewTimer(time.Second * 60)
		reqmap       = make(map[int]chan *MsgBody)

		datalenmap = make(map[string]int64)
		datalenmux = sync.Mutex{}

		SendMsgfunc = func(req *MsgBody, clienttep *GwTcpClient) {
			if req == nil || clienttep == nil {
				return
			}
			var ctx int
			const loopctx = 8
			if syncgroup, ok := sf.Wirtestatue.Load(req.Filename + "+syncgroup"); ok {
			Loop:
				lenrsp := clienttep.Write(req.Filename, int64(len(req.Data)), uint64(req.Offset), req.Data)
				if lenrsp < 0 {
					glog.V(0).Infoln("write fail fileanme:"+req.Filename, "offset:", req.Offset, "rsp", lenrsp, "ctx", ctx)
					if lenrsp != gatewayrsperr && ctx < loopctx {
						ctx++
						time.Sleep(time.Second * 10)
						goto Loop
					}

					sf.Wirtestatue.Store(req.Filename, true)
				}

				datalenmux.Lock()
				datalenmap[req.Filename] += int64(len(req.Data))
				datalenmux.Unlock()

				syncgroup.(*sync.WaitGroup).Done()
			}
		}
		handleSendMsg = func(i int, ctx context.Context, clienttep *GwTcpClient) {
			for {
				select {
				case req := <-reqmap[i]:
					SendMsgfunc(req, clienttep)
				case <-ctx.Done():
					clienttep.Close()
					return
				}
			}
		}
		startsend = func(filename string) {
			glog.V(0).Infoln("start resource")
			var ctx context.Context
			ctx, Cancel = context.WithCancel(context.Background())
			for i := 0; i < num; i++ {
				cli, err := NewClient(sf.GetGatewayIp(filename, "")+":"+*fuseop.GatewayTcpPort, 60*time.Second)
				if err != nil {
					glog.V(0).Infoln("get client fail", err.Error())
					continue
				}
				go handleSendMsg(i, ctx, cli)
			}
		}
	)

	for i := 0; i < num; i++ {
		reqmap[i] = make(chan *MsgBody, 500)
	}
	defer timecancel.Stop()

	for {
		select {
		case req := <-sf.SendMsg:
			if !startreqbool ||
				(req.Filename != filename &&
					sf.GetGatewayIp(req.Filename, "") != sf.GetGatewayIp(filename, "")) {
				//不能同时上传不同池的数据//后面可以修改
				//如果要同时上传不同的池可以建多个fuse客户端
				if startreqbool && Cancel != nil {
					Cancel()
				}
				startsend(req.Filename)
				startreqbool = true
			}
			reqmap[startreqkey%num] <- req
			startreqkey++
			filename = req.Filename
			timecancel.Reset(time.Second * 30)

		case <-timecancel.C:
			//说明到这里没有发送，取消协程
			if _, ok := sf.Wirtestatue.Load(filename + "+syncgroup"); !ok && Cancel != nil {
				glog.V(0).Infoln("release resource")
				Cancel()
				datalenmux.Lock()
				for k, v := range datalenmap {
					glog.V(0).Infof("result:filename(%s),writedatatotal(%d)", k, v)
				}
				datalenmap = make(map[string]int64)
				datalenmux.Unlock()
				runtime.GC()
			} else {
				timecancel.Reset(time.Second * 60)
			}

			startreqbool = false
			startreqkey = 0

		}
	}

}

const readsize = 1024 * 128

func ReadCacheHandle(tempSdsnbsfs *SnbsDriver) {
	var num = 16
	var sendlist = list.New()
	var listmux = sync.RWMutex{}
	var Cancel = sync.Map{}
	var Offset int64

	var SendMsgfunc = func(req *MsgBody, clienttep *GwTcpClient) {
		if req == nil || clienttep == nil {
			return
		}
		lenrsp := clienttep.Read(req.Filename, int64(len(req.Data)), uint64(req.Offset), req.Data)
		req.Rsp <- lenrsp

	}

	var sendmap = make(map[string]int)
	var sendmapmu = sync.RWMutex{}
	var startsend = func(filename string) {
		ctx, cancel := context.WithCancel(context.Background())
		Cancel.Store(filename, cancel)
		var reqmap = make(map[int]chan *MsgBody)
		for i := 0; i < num; i++ {
			reqmap[i] = make(chan *MsgBody, 10)
		}

		//启动文件预读
		go func() {
			if size, ok := tempSdsnbsfs.Wirtestatue.Load(filename + "+Size"); ok {
				glog.V(0).Infoln("find file size", size)
				var offset int64 = 0
				var i int
				var lenght int64 = readsize
				var Min = func(x, y int64) int64 {
					if x < y {
						return x
					}
					return y
				}
				for offset+lenght <= size.(int64) && lenght > 0 {
					req := NewReq(filename, lenght, offset)
					select {
					case reqmap[i%num] <- req:
						i++
						lenght = Min(readsize, size.(int64)-offset)
						offset += lenght
					case <-ctx.Done():
						glog.V(0).Infoln("file complete cancel", filename)
						return
					case <-time.After(time.Minute):
						glog.V(0).Infoln("time out file complete cancel", filename)
						return
					}
				}
				glog.V(0).Infoln("file complete", filename)
			} else {
				glog.V(0).Infoln(" not find size", filename)
			}
		}()

		//启动发送消息到gateway读
		for i := 0; i < num; i++ {
			cli, err := NewClient(tempSdsnbsfs.GetGatewayIp(filename, "")+":"+*fuseop.GatewayTcpPort, 60*time.Second)
			if err != nil {
				glog.V(0).Infoln("get client fail", err.Error())
				continue
			}

			sendmapmu.Lock()
			if v, ok := sendmap[filename]; ok {
				sendmap[filename] = v + 1
			} else {
				sendmap[filename] = 1
			}
			sendmapmu.Unlock()

			go func(i int, ctx context.Context, clienttep *GwTcpClient) {
				defer func() {
					sendmapmu.Lock()
					if v, ok := sendmap[filename]; ok {
						sendmap[filename] = v - 1
					}
					sendmapmu.Unlock()
				}()

				for {
					select {
					case req := <-reqmap[i]:
						SendMsgfunc(req, clienttep)
						listmux.Lock()
						sendlist.PushBack(req)
						listmux.Unlock()

						if atomic.LoadInt64(&Offset)+100*readsize < req.Offset {
							time.Sleep(time.Millisecond)
						}

					case <-ctx.Done():
						clienttep.Close()
						glog.V(4).Infoln("cancel and release client", i)
						return

					case <-time.After(time.Minute):
						clienttep.Close()
						glog.V(4).Infoln("time out cancel and release client", i)
						return
					}
				}
			}(i, ctx, cli)
		}

	}

	var timecancel = time.NewTimer(time.Second * 20)
	defer timecancel.Stop()
	var startcache = make(map[string]bool)
	var find bool

	for {
		select {
		case req := <-Readchan:
			if b, _ := startcache[req.Filename]; !b {
				glog.V(0).Infoln("startcache", req.Filename)
				startsend(req.Filename)
				startcache[req.Filename] = true
				time.Sleep(time.Second)
			}

			find = false
			atomic.StoreInt64(&Offset, req.Offset)

			t := time.Now()
		Loop:
			listmux.Lock()
			for e := sendlist.Front(); e != nil; e = e.Next() {
				msgbody := e.Value.(*MsgBody)
				if msgbody.Filename == req.Filename &&
					msgbody.Offset == req.Offset &&
					msgbody.Size == req.Size {
					copy(req.Data, msgbody.Data)
					req.Rsp <- <-msgbody.Rsp
					sendlist.Remove(e)
					msgbody.Release()
					find = true
					break
					//防止阻塞
				} else if time.Now().After(t.Add(time.Minute)) {
					select {
					case req.Rsp <- -1:
					default:
					}

					glog.V(4).Infoln("time out  minute", req.Filename, req.Offset)
					sendlist.Remove(e)
					msgbody.Release()
					find = true
					break
				}
			}
			listmux.Unlock()
			//说明没找到
			if !find {
				time.Sleep(time.Millisecond * 5)
				goto Loop
			}

			timecancel.Reset(time.Second * 3)

		case <-timecancel.C:
			glog.V(0).Infoln("time out and release resource")
			Cancel.Range(func(key, value interface{}) bool {
				value.(context.CancelFunc)()
				glog.V(0).Infoln("release cancel")
				return true
			})
			startcache = make(map[string]bool)
			time.Sleep(time.Second * 3)
			//防止释放过程中有新的加入
			for sendlist.Len() != 0 {
				listmux.Lock()
				for e := sendlist.Front(); e != nil; e = e.Next() {
					msgbody := e.Value.(*MsgBody)
					sendlist.Remove(e)
					msgbody.Release()
				}
				listmux.Unlock()
				if sendlist.Len() != 0 {
					continue
				}
				time.Sleep(time.Second * 2)
				//如果到这里说明有异常退出，重新再次释放
				timecancel.Reset(time.Second * 3)
			}

			sendmapmu.Lock()
			for _, v := range sendmap {
				if v > 0 {
					timecancel.Reset(time.Second * 3)
					break
				}
			}
			sendmapmu.Unlock()

			glog.V(0).Infoln("len(send list)", sendlist.Len())
			runtime.GC()

		}
	}

}

var ReqData = sync.Pool{
	New: func() interface{} {
		var req = &MsgBody{}
		req.Data = make([]byte, readsize)
		return req
	},
}

func NewReq(filename string, size int64, offset int64) *MsgBody {

	var req *MsgBody
	if size == readsize {
		req = ReqData.Get().(*MsgBody)
	} else {
		req = &MsgBody{}
		req.Data = make([]byte, size)
	}

	req.Filename = filename
	req.Size = size
	req.Rsp = make(chan int32, 1)
	req.Offset = offset
	return req
}

func (req *MsgBody) Release() {
	if req.Size == readsize {
		ReqData.Put(req)
	}
}
