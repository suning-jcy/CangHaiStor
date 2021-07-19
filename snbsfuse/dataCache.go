package main

import (
	"container/list"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/fuse/snbsfs"

	"code.suning.com/glog"
)

//缓存块最多多少个
var MAXCapacity = 600

//脏数据保留的最大时间，30S
var MaxDirtytime = int64(1)

//缓存保存时间
var MaxCachetime = int64(2)

//每次读取的大小   分块阈值  分块大小,不变更
var BlockSize = int64(4 * 1024 * 1024)

var FlushThreadSum = 36

//可以刷新重试几次
var MaxRetryTime = 3

//上传的最高频率，单位毫秒
var MinRefreshInterval = int64(100)

//脏缓存块的扫描周期
var DirtyScanInterval = int64(100)

//单个文件是否允许并发读写
var MultyThreadRW = true
var debug = false

var (
	NewDirtyDataErr   = errors.New("new dirty data")
	DisorderDataErr   = errors.New("Disorder data")
	EmptyFileErr      = errors.New("Empty file err")
	GetUploadIdErr    = errors.New("get upload id failed")
	UploadIdIsNullErr = errors.New("upload id is null while commit")
	ReadOutOfSize     = errors.New("Read Out Of Size")
	WriteOutOfSize    = errors.New("Write Out Of Size")
	RequestError      = errors.New("Request Error")
)

type CacheOption struct {
	//最大缓存块数量，0表示使用默认值600
	MAXCapacity int
	//脏页最长保存时间 ，0表示使用默认值30s
	MaxDirtytime int64
	//cache->SNBS 之间的刷新工作队列长度，0表示默认值12
	FlushThreadSum int
	//单位毫秒，0表示默认自500ms
	MinRefreshInterval int64
	//单位毫秒,0表示默认自100ms
	DirtyScanInterval int64
	//缓存最大保持时间
	MaxCachetime int64

	Debug bool

	BlockSize int
}

type cachedata struct {
	//4MB 大小的缓存空间，跟块是一个大小，除了最后一块，都是4M
	data []byte

	off   int64
	users int32
	blkid uint64 //blockid 作为全局索引号
	//读流程不变化，写流程只能递增。删除流程可以直接置0
	dirtysize int
	//脏数据的最早引入时间
	earliestdirtytime int64
	//上次刷新的时间
	lastrefreshtime int64

	lastaccesstime int64
	//在lur链表中的节点
	locelem *list.Element

	//不可逆，只能被设置为true，指的是这个缓存被移除
	isdeleted bool

	of *opfile
	//提交时候要做的
	Onflush     *list.List
	Onflushlock sync.RWMutex
	isInFlush   bool  //是否已经在flush线程中
	flushErr    error //本脏块是否最终刷失败
	sync.RWMutex
}

func (c *cachedata) isDirty() bool {
	c.RLock()
	defer c.RUnlock()
	return c.dirtysize != 0
}

func (c *cachedata) insertOnflush(onflush func(dirtysize int, err error)) {
	c.Onflushlock.Lock()
	c.Onflush.PushBack(onflush)
	c.Onflushlock.Unlock()
}

func (c *cachedata) getOnflushs() (onflushs []func(dirtysize int, err error)) {
	res := []func(dirtysize int, err error){}
	c.Onflushlock.Lock()
	f := c.Onflush.Back()
	for f != nil {
		if temp, ok := f.Value.(func(dirtysize int, err error)); ok {
			res = append(res, temp)
		}
		newf := f.Prev()
		c.Onflush.Remove(f)
		f = newf
	}
	c.Onflushlock.Unlock()
	return res
}

//用于记录SNBS操作文件的中间状态的
type opfile struct {
	oplock sync.RWMutex

	fileType   int
	isopend    bool
	cacheOnly  bool
	isclosed   bool
	uploadid   string
	snbsSize   int64
	reopenlock sync.Mutex

	fullpath          string
	nodehandle        uint64
	basefile          *snbsfs.FileInfo
	blocks            map[int64]*cachedata
	cachedatalock     sync.RWMutex
	blockpushsize     map[int64]int //每个block已经完成推送的size
	blockneedpushsize map[int64]int //每个block已经完成推送的size
	blockpushlock     sync.RWMutex
	maxoff            int64 //当前已推送block的偏移量最大值
	err               error
	blkchan           []chan struct{}
}

func Newopfile(nodehandle uint64, fullpath string, basefile *snbsfs.FileInfo) *opfile {
	of := &opfile{
		nodehandle:        nodehandle,
		basefile:          basefile,
		blocks:            make(map[int64]*cachedata),
		blockpushsize:     make(map[int64]int),
		blkchan:           make([]chan struct{}, 1),
		blockneedpushsize: make(map[int64]int),
		fullpath:          fullpath,
		snbsSize:          basefile.Size,
		isclosed:          false,
	}
	if basefile.Size < BlockSize {
		of.fileType = 0
	} else {
		of.fileType = 1
	}
	return of
}
func (of *opfile) cachesum() int {
	of.cachedatalock.RLock()
	defer of.cachedatalock.RUnlock()
	return len(of.blocks)
}
func (of *opfile) dirtysize() (size int64) {
	of.cachedatalock.Lock()
	defer of.cachedatalock.Unlock()
	for _, v := range of.blocks {
		if v.dirtysize != 0 {
			glog.V(4).Infoln("size:", size, " dirtysize:", v.dirtysize, " off", v.off, " inflush:", v.isInFlush)
		}
		size += int64(v.dirtysize)
	}
	return
}
func (of *opfile) GetBlock(off int64) (block *cachedata) {
	of.cachedatalock.Lock()
	defer of.cachedatalock.Unlock()
	if v, ok := of.blocks[off]; !ok {
		return nil
	} else {
		atomic.AddInt32(&(v.users), 1)
		return v
	}
}

func (of *opfile) GetAllBlock() (blocks map[int64]*cachedata) {
	of.cachedatalock.Lock()
	defer of.cachedatalock.Unlock()
	blocks = make(map[int64]*cachedata)
	for k, v := range of.blocks {
		blocks[k] = v
	}
	return blocks
}

func (of *opfile) SetBlock(block *cachedata) bool {
	of.cachedatalock.Lock()
	defer of.cachedatalock.Unlock()
	if _, ok := of.blocks[block.off]; !ok {
		of.blocks[block.off] = block
		block.of = of
		return true
	}
	return false
}

func (of *opfile) removecachedata(data *cachedata) (hasdel bool) {
	of.cachedatalock.Lock()
	off := data.off
	users := data.users
	filename := data.of.fullpath
	datalen := len(data.data)
	defer func() {
		defer of.cachedatalock.Unlock()
		glog.V(1).Infoln(" remove cached block?", hasdel, " file:", filename, " off:", off, " dirtysize:", data.dirtysize, " users:", users, " datalen:", datalen)

	}()
	hasdel = false
	if atomic.LoadInt32(&(data.users)) != 0 {
		return hasdel
	}
	hasdel = true
	if data, ok := of.blocks[data.off]; ok {
		delete(of.blocks, data.off)
		<-cachesolt
	}
	return hasdel
}

//PS：9-10 还没有把write和read 的缓存合并到一块，这两理论上应该合并。SNBS需要新增一个读取正在进行分块上传动作的文件的某一个块内容才可以
type CacheDataMana struct {
	//缓存数据,可以读，可以写。
	lrulist *list.List
	lrulock sync.RWMutex

	workers []*flushWorker

	isstop bool

	driver      *SnbsDriver
	filemap     map[*snbsfs.FileInfo]*opfile
	filemaplock sync.RWMutex
	cacheSeqId  uint64 //为每个block配置唯一idx
}

var cachesolt chan struct{}

func NewCacheDataMana(driver *SnbsDriver, opt *CacheOption) *CacheDataMana {
	cdm := &CacheDataMana{
		lrulist: list.New(),
		isstop:  false,
		driver:  driver,
		filemap: make(map[*snbsfs.FileInfo]*opfile),
	}
	if opt != nil {
		if opt.MAXCapacity != 0 {
			MAXCapacity = opt.MAXCapacity
		}
		if opt.MaxDirtytime != 0 {
			MaxDirtytime = opt.MaxDirtytime
		}
		if opt.FlushThreadSum != 0 {
			FlushThreadSum = opt.FlushThreadSum
		}
		if opt.MinRefreshInterval != 0 {
			MinRefreshInterval = opt.MinRefreshInterval
		}
		if opt.DirtyScanInterval != 0 {
			DirtyScanInterval = opt.DirtyScanInterval
		}
		if opt.MaxCachetime != 0 {
			MaxCachetime = opt.MaxCachetime
		}
		if opt.BlockSize != 0 {
			BlockSize = int64(opt.BlockSize)
		}
		debug = opt.Debug
	}
	cachesolt = make(chan struct{}, MAXCapacity)
	cdm.workers = make([]*flushWorker, FlushThreadSum)
	for i := 0; i < FlushThreadSum; i++ {
		cdm.workers[i] = newFlushWorker(cdm)
		go cdm.workers[i].start(i)
	}
	cdm.Start()
	glog.V(0).Infoln("MaxBlockNum:", MAXCapacity, " BlockSize:", BlockSize, " MaxDirtytime:", MaxDirtytime, "s FlushThreadSum:", FlushThreadSum, " MinRefreshInterval:", MinRefreshInterval, "ms")
	return cdm
}

func (c *CacheDataMana) getopfile(finfo *snbsfs.FileInfo) *opfile {
	c.filemaplock.RLock()
	defer c.filemaplock.RUnlock()
	if v, ok := c.filemap[finfo]; ok {
		return v
	}
	return nil
}

func (c *CacheDataMana) printallopfile() *opfile {
	c.filemaplock.RLock()
	defer c.filemaplock.RUnlock()
	for k, _ := range c.filemap {
		glog.V(4).Infoln(k.Name)
	}
	return nil
}

//每次移除缓存块的时候尝试一下
//需要of先被锁
func (c *CacheDataMana) tryremoveopfile(finfo *snbsfs.FileInfo) bool {
	of := c.getopfile(finfo)
	if of == nil {
		return true
	}
	//把of整个锁住
	if of.cachesum() == 0 && !of.cacheOnly {
		c.filemaplock.Lock()
		glog.V(1).Infoln("remove file:", of.fullpath)
		delete(c.filemap, finfo)
		c.filemaplock.Unlock()
		return true
	} else {
		return false
	}
}

//开始刷新循环
func (c *CacheDataMana) Start() {
	go c.refreshColdLoop()
	go c.refreshDirtyLoop()
}

//完全释放，程序退出时候进行
//释放所有的内存（释放之前提交）
func (c *CacheDataMana) Stop() {
	c.isstop = true

	c.flushAll()
	var wg sync.WaitGroup
	for i := 0; i < FlushThreadSum; i++ {
		wg.Add(1)
		go func() {
			c.workers[i].stop()
			wg.Done()
		}()
	}
	wg.Wait()
	return
}

//添加一个块到缓存中。
func (c *CacheDataMana) SetBlock(block *cachedata, nodehandle uint64, info *snbsfs.FileInfo) bool {
	if c.isstop {
		return false
	}

	cachesolt <- struct{}{}
	if debug {
		glog.V(0).Infoln("now cache has blocks:", len(cachesolt))
	}

	c.filemaplock.Lock()
	defer c.filemaplock.Unlock()
	if _, ok := c.filemap[info]; !ok {
		<-cachesolt
		return false
	}
	c.refreshLRU(block)
	if !c.filemap[info].SetBlock(block) {
		<-cachesolt
		return true
	}
	glog.V(4).Infoln(" SetBlock ,", info.Name, block.off, " now cachedata total sum:", c.lrulist.Len())
	return true
}
func (c *CacheDataMana) Addopfile(nodehandle uint64, fullpath string, info *snbsfs.FileInfo) *opfile {
	if c.isstop {
		return nil
	}
	c.filemaplock.Lock()
	defer c.filemaplock.Unlock()
	if v, ok := c.filemap[info]; !ok {
		if strings.HasPrefix(fullpath, "/") {
			fullpath = fullpath[1:]
		}
		newof := Newopfile(nodehandle, fullpath, info)
		c.filemap[info] = newof
		if debug {
			glog.V(0).Infoln(fullpath, " Add new opfile,now sum:", len(c.filemap))
		}
		return newof
	} else {
		return v
	}
}

//触发某个文件的刷新
func (c *CacheDataMana) FlushFile(fileinfo *snbsfs.FileInfo) error {
	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(fileinfo.Name, " FlushFile time:", time.Since(start))
		}
	}()
	//把所有数据都提交
	of := c.getopfile(fileinfo)
	if of == nil {
		return nil
	}
	glog.V(4).Infoln("FlushFile:", of.fullpath)
	//文件锁住不让改了
	of.oplock.Lock()
	defer of.oplock.Unlock()

	_, err := c.flushFile(of)
	if err != nil {
		glog.V(0).Infoln("FlushFile err while flushFile:", err)
		return err
	}

	return nil
}
func waitClean(of *opfile, wg *sync.WaitGroup, where string) (err error) {
	var i = 0
	var size int64 = 0
	err = nil
	for {
		size = of.dirtysize()
		if size == 0 {
			break
		} else {
			time.Sleep(time.Microsecond * time.Duration(10))
			i++
		}
		if i%100 == 0 {
			glog.V(2).Infoln("wait file:", of.fullpath, " clean.dirtysize:", size, " where:", where)
		}
	}
	wg.Done()
	glog.V(2).Infoln("wait file:", of.fullpath, " clean DONE where:", where)
	return
}

//触发某个文件的刷新
//文件的所有块都要触发一次刷新
func (c *CacheDataMana) flushFile(of *opfile) (int, error) {

	//锁住缓存map准备删除
	totaldirtysize := 0
	var templock sync.Mutex
	of.cachedatalock.Lock()
	res := []*cachedata{}
	for _, v := range of.blocks {
		res = append(res, v)
	}
	of.cachedatalock.Unlock()

	if debug {
		glog.V(0).Infoln(" flushAll  ", "push ", of.fullpath, " need upload block:", len(res))
	}

	var wg sync.WaitGroup
	var flusherr error
	for _, v := range res {
		if !needFlush(v) {
			continue
		}
		wg.Add(1)
		v.insertOnflush(func(dirtysize int, err error) {
			if err != nil {
				flusherr = err
			}
			templock.Lock()
			totaldirtysize += dirtysize
			templock.Unlock()
		})
		idx := getAIdleThread(v.of, FlushThreadSum)
		isadd := c.workers[idx].add(v, 0)
		glog.V(1).Infoln(" flushFile  ", "push ", v.of.fullpath, " off:", v.off, " to work:", idx, " ok?", isadd, " inflush:", v.isInFlush)
		wg.Done()
	}
	wg.Add(1)
	go waitClean(of, &wg, "flushfile")
	wg.Wait()
	flusherr = of.err
	glog.V(1).Infoln("flushFile:", of.fullpath, " err:", of.err)
	return totaldirtysize, flusherr
}

//确定刷新
//以实时为准，
func (c *CacheDataMana) SyncFile(fileinfo *snbsfs.FileInfo, anywrite bool) (err error) {
	if !anywrite {
		return nil
	}

	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln("SyncFile:", fileinfo.Name, " time:", time.Since(start), " error:", err)
		}
	}()

	of := c.getopfile(fileinfo)
	if of == nil {
		return nil
	}

	//文件锁住不让改了
	of.oplock.Lock()
	defer of.oplock.Unlock()
	glog.V(1).Infoln("SyncFile:", of.fullpath, " SIZE:", fileinfo.Size)

	dirtysize, err := c.flushFile(of)
	if err != nil {
		glog.V(0).Infoln("SyncFile:", of.fullpath, " err while FlushFile:", err)
		return err
	}
	//是分块上传的，commit
	if fileinfo.Size >= BlockSize {
		if debug {
			glog.V(0).Infoln(of.fullpath, "is block file ,uploadid:", of.uploadid)
		}
		if of.uploadid == "" {
			if dirtysize == 0 {
				return nil
			}
			glog.V(0).Infoln("SyncFile:", of.fullpath, " err while commit: null uploadid ")
			return UploadIdIsNullErr
		}
		idx := fileinfo.Size / BlockSize

		if fileinfo.Size%BlockSize > 0 {
			idx += 1
		}

		of.reopenlock.Lock()
		err = c.driver.complete(of.fullpath, of.uploadid, int(idx))
		if err != nil {
			glog.V(0).Infoln("SyncFile:", of.fullpath, " push complete:", "error:", err)
			of.reopenlock.Unlock()
			return err
		}
		of.cacheOnly = false
		of.uploadid = ""
		of.isopend = false
		of.reopenlock.Unlock()
	} else {
		//普通文件，不需要做什么
	}
	return of.err
}

//只有写打开的文件才会有这个关闭动作
func (c *CacheDataMana) CloseFile(fileinfo *snbsfs.FileInfo, anywrite bool) (err error) {
	of := c.getopfile(fileinfo)
	if of == nil {
		return nil
	}
	if !anywrite {
		of.oplock.Lock()
		of.isclosed = true
		of.oplock.Unlock()
		return nil
	}
	var filename string
	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln("CloseFile:", filename, " time:", time.Since(start), " error:", err)
		}
	}()

	filename = of.fullpath
	glog.V(4).Infoln("CloseFile:", of.fullpath, fileinfo.Size)
	//文件锁住不让改了
	of.oplock.Lock()
	defer func() {
		of.cacheOnly = false // 文件可以从map中移除了
		of.isclosed = true
		of.oplock.Unlock()
	}()

	dirtysize, err := c.flushFile(of)
	if err != nil {
		glog.V(0).Infoln("CloseFile err while FlushFile:", err)
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go waitClean(of, &wg, "closefile")
	wg.Wait()
	//是分块上传的，commit
	if fileinfo.Size >= BlockSize {
		if debug {
			glog.V(0).Infoln(of.fullpath, "is block file ,uploadid:", of.uploadid)
		}
		if of.uploadid == "" {
			glog.V(4).Infoln("CloseFile , while commit: null uploadid: ", of.fullpath, fileinfo.Size, BlockSize)
			if dirtysize == 0 {
				return nil
			}
			return UploadIdIsNullErr
		}
		idx := fileinfo.Size / BlockSize

		if fileinfo.Size%BlockSize > 0 {
			idx += 1
		}
		glog.V(4).Infoln(of.fullpath, "complete:", fileinfo.Size, idx)

		of.reopenlock.Lock()
		err = c.driver.complete(of.fullpath, of.uploadid, int(idx))
		if err != nil {
			glog.V(0).Infoln(of.fullpath, "complete:", "error:", err)
			of.reopenlock.Unlock()
			return err
		}
		of.uploadid = ""
		of.isopend = false
		of.reopenlock.Unlock()
	} else {
		//普通文件，不需要做什么
	}
	err = of.err
	return of.err
}

//直接移除
func (c *CacheDataMana) Truncate(fileinfo *snbsfs.FileInfo, size uint64) {
	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(fileinfo.Name, " Truncate time:", time.Since(start))
		}
	}()

	of := c.getopfile(fileinfo)
	if of == nil {
		fileinfo.Size = 0
		glog.V(4).Infoln("can not get opfile:", fileinfo.Name)
		if glog.V(4) {
			c.printallopfile()
		}
		return
	}
	glog.V(4).Infoln("CacheDataMana Truncate:", of.fullpath, fileinfo.Size)
	//文件锁住不让改了
	of.oplock.Lock()
	defer of.oplock.Unlock()

	for i := 0; i < 3; i++ {
		blocks := of.GetAllBlock()
		if len(blocks) == 0 {
			break
		}
		for _, v := range of.blocks {
			err := c.removeBlock(v, true)
			if err != nil && err != NewDirtyDataErr {
				glog.V(0).Infoln("filename:", v.of.fullpath, " off:", v.off, "remove err:", err)
				continue
			}

		}
	}

	if len(of.blocks) != 0 {
		for _, v := range of.blocks {
			glog.V(0).Infoln("filename:", v.of.fullpath, "nodeid:", v.of.nodehandle, " off:", v.off, "has not flush yet !!!!! dirty data in cache!")
		}
	}
	if size == 0 {
		of.cacheOnly = true
		fileinfo.Size = 0
		of.snbsSize = 0
		of.fileType = 0
		of.isclosed = false
		of.blocks = make(map[int64]*cachedata)
		of.blockpushsize = make(map[int64]int)
		of.blkchan = make([]chan struct{}, 1)
		of.blockneedpushsize = make(map[int64]int)
	}

	return
}

//删除文件时候使用
//提交+移除
func (c *CacheDataMana) RemoveFile(fileinfo *snbsfs.FileInfo) {
	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(fileinfo.Name, " RemoveFile time:", time.Since(start))
		}
	}()

	of := c.getopfile(fileinfo)
	if of == nil {
		return
	}
	//文件锁住不让改了
	of.oplock.Lock()
	defer of.oplock.Unlock()

	for i := 0; i < 3; i++ {
		blocks := of.GetAllBlock()
		if len(blocks) == 0 {
			break
		}
		for _, v := range blocks {
			err := c.removeBlock(v, true)
			if err != nil {
				glog.V(0).Infoln("filename:", v.of.fullpath, " off:", v.off, "remove err:", err)
				continue
			}
		}
	}
	if len(of.blocks) != 0 {
		for _, v := range of.blocks {
			glog.V(0).Infoln("filename:", v.of.fullpath, "nodeid:", v.of.nodehandle, " off:", v.off, "has not flush yet !!!!! dirty data in cache!")
		}
	}
	of.snbsSize = 0
	of.fileType = 0
	return
}

func (c *CacheDataMana) RenameOpFile(fileinfo *snbsfs.FileInfo, oldfullpath string, newfullpath string) (err error) {
	c.filemaplock.Lock()
	defer c.filemaplock.Unlock()
	glog.V(2).Infoln("rename opfile.oldfilename:", oldfullpath, " newfilename:", newfullpath)
	if v, ok := c.filemap[fileinfo]; ok {
		if "/"+v.fullpath == oldfullpath {
			v.fullpath = newfullpath[1:]
		} else {
			glog.V(0).Infoln("rename err! find no opfile.oldfilename:", oldfullpath, " newfilename:", newfullpath, " fullpath:", v.fullpath)
			return errors.New("No Opfile Matched")
		}
	}
	return nil
}

//获取一个新的块，从snbs取
//假如分块上传了但是没有提交，也应该能够取到
func (c *CacheDataMana) GetNewBlockFromSnbs(of *opfile, off int64) (*cachedata, error) {

	header := make(map[string]string)
	parms := make(map[string]string)
	if of.basefile.Size <= off {
		return nil, ReadOutOfSize
	}
	endoff := off + BlockSize - 1
	if endoff >= of.basefile.Size {
		endoff = of.basefile.Size - 1
	}
	wantsize := endoff - off + 1
	if of.uploadid != "" {
		parms["upload-id"] = of.uploadid
		parms["getdata"] = "true"
		idx := off / BlockSize
		if off%BlockSize > 0 {
			idx += 1
		}
		parms["partnum"] = fmt.Sprint(idx)
	} else {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, endoff)
	}
	data, err := c.driver.urlRequest(of.fullpath, "GET", parms, header)
	if err != nil {
		glog.V(0).Infoln("error while GetFile " + of.fullpath + ": " + err.Error())
		return nil, RequestError
	}
	getdatasize := len(data)
	if getdatasize == 0 || getdatasize != int(wantsize) {
		data, err = c.driver.urlRequest(of.fullpath, "GET", parms, header)
		if err != nil {
			glog.V(0).Infoln("error while GetFile " + of.fullpath + ": " + err.Error())
			return nil, RequestError
		}
		getdatasize = len(data)
		if len(data) == 0 || getdatasize != int(wantsize) {
			glog.V(0).Infoln("error while GetFile "+of.fullpath+": want :", wantsize, " but get:", getdatasize)
			return nil, RequestError
		}
	}

	res := &cachedata{
		data:           data,
		off:            off,
		of:             of,
		Onflush:        list.New(),
		lastaccesstime: time.Now().UnixNano(),
	}
	return res, nil
}

//创建一个完全的新的块，还没有放到SNBS中的
func (c *CacheDataMana) GetNewBlock(of *opfile, off int64) (*cachedata, error) {
	res := &cachedata{
		data:           []byte{},
		off:            off,
		of:             of,
		Onflush:        list.New(),
		lastaccesstime: time.Now().UnixNano(),
	}
	return res, nil
}

func (c *CacheDataMana) WriteAt(handle uint64, fullpath string, fileinfo *snbsfs.FileInfo, data []byte, off int64) (err error) {
	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(fileinfo.Name, " WriteAt time:", time.Since(start), "off:", off, " err:", err)
		}
	}()

	if off > fileinfo.Size {
		//centos7.3 上实验出现问题，调整填充空字符
		nullsize := off - fileinfo.Size
		tempnull := make([]byte, 128*1024)
		for nullsize > 0 {
			tlen := int64(128 * 1024)
			if nullsize < 128*1024 {
				tlen = nullsize
			}
			nullsize -= tlen
			err = c.WriteAt(handle, fullpath, fileinfo, tempnull[:tlen], fileinfo.Size)
			if err != nil {
				return err
			}
		}

	}
	of := c.getopfile(fileinfo)
	if of == nil {
		glog.V(4).Infoln("Addopfile", fileinfo.Name)
		//创建一个opfile
		of = c.Addopfile(handle, fullpath, fileinfo)
	}
	of.oplock.Lock()
	defer of.oplock.Unlock()
	of.isclosed = false
	//fileinfo.Size 是0 表示是新的文件，第一次来写的时候
	if fileinfo.Size == 0 {
		of.cacheOnly = true
	}
	size := int64(len(data))
	startidx := off / BlockSize
	endidx := (off + size - 1) / BlockSize
	if (off+size)%BlockSize > 0 {
		endidx += 1
	}
	newoff := off
	left := size
	glog.V(4).Infoln("WriteAt: fullpath ", fullpath, startidx, endidx, fileinfo.Size, time.Since(start))
	for i := startidx; i <= endidx; i++ {
		if left == 0 {
			break
		}
		blockoff := int64(i) * BlockSize
		tempblock := of.GetBlock(blockoff)
		if tempblock == nil {
			glog.V(4).Infoln(" can not get block:", fullpath, " blockid:", i, " filesize:", fileinfo.Size, of.snbsSize)
			if blockoff == of.snbsSize {
				if debug {
					glog.V(0).Infoln(fullpath, "WriteAt: filesize is offset!,create new block. blockoff:", blockoff, " snbsSize:", of.snbsSize)
				}
				tempblock, err = c.GetNewBlock(of, blockoff)
				if err != nil {
					glog.V(0).Infoln("WriteAt:GetNewBlock error:", err, fileinfo.Size, of.snbsSize, fullpath)
					return err
				}
			} else if blockoff < of.snbsSize {
				if debug {
					glog.V(0).Infoln(fullpath, "WriteAt: get new block from snbs. blockoff:", blockoff, " snbsSize:", of.snbsSize)
				}
				tempblock, err = c.GetNewBlockFromSnbs(of, blockoff)
				if err != nil {
					glog.V(0).Infoln("WriteAt:GetNewBlockFromSnbs error:", err, fileinfo.Size, of.snbsSize, fullpath)
					return err
				}
			} else {
				if debug {
					glog.V(0).Infoln(fullpath, "WriteAt: ", blockoff, " snbsSize:", of.snbsSize)
				}
				lastblockoff := int64(i-1) * BlockSize
				lastblock := of.GetBlock(lastblockoff)
				if lastblock != nil && int64(len(lastblock.data)) == BlockSize {
					c.refreshLRU(lastblock)
					atomic.AddInt32(&(lastblock.users), -1)
					tempblock, err = c.GetNewBlock(of, blockoff)
					if err != nil {
						glog.V(0).Infoln("WriteAt :GetNewBlockFromSnbs error:", err, fileinfo.Size, of.snbsSize, fullpath)
						return err
					}
				} else {
					if lastblock != nil {
						atomic.AddInt32(&(lastblock.users), -1)
					}
					glog.V(0).Infoln("WriteAt: WriteOutOfSize", fileinfo.Size, of.snbsSize, fullpath)
					return WriteOutOfSize
				}
			}
			atomic.AddInt32(&(tempblock.users), 1)
			c.SetBlock(tempblock, handle, fileinfo)
		} else {
			c.refreshLRU(tempblock)
		}

		tempblock.Lock()
		if of.maxoff < tempblock.off {
			atomic.SwapInt64(&of.maxoff, tempblock.off)
		}
		//从tempblock块的tempoff 这个位置开始输入
		tempoff := newoff - blockoff
		//从data的dataoff位置开始输出
		dataoff := size - left

		//可以输入到tempblock上一共datawantsize 大小的数据
		datawantsize := BlockSize - tempoff
		if left < datawantsize {
			datawantsize = left
		}
		//tempblock中可以覆盖的大小
		blockleft := int64(len(tempblock.data)) - tempoff
		adddatasize := datawantsize - blockleft

		glog.V(4).Infoln("file:", fullpath, " maxoff:", of.maxoff, " WriteAt:  off:", off, " size:", size, "block off:", tempblock.off, " blocksize:", len(tempblock.data), " block off2:", tempoff, " addsize:", adddatasize)

		if blockleft < datawantsize {
			tempblock.data = append(tempblock.data, make([]byte, adddatasize)...)
		}
		copy(tempblock.data[tempoff:], data[dataoff:])
		if tempblock.dirtysize == 0 {
			tempblock.earliestdirtytime = time.Now().Unix()
		}
		tempblock.dirtysize += int(datawantsize)
		newoff += datawantsize
		left -= datawantsize
		if adddatasize > 0 {
			fileinfo.Size += adddatasize
			glog.V(4).Infoln("after WriteAt ,file:", of.fullpath, " size is :", fileinfo.Size)
		}
		atomic.AddInt32(&(tempblock.users), -1)
		tempblock.Unlock()
	}
	glog.V(4).Infoln("WriteAt: fullpath ", fullpath, "after:", fileinfo.Size)
	return
}

func (c *CacheDataMana) ReadAt(handle uint64, fullpath string, fileinfo *snbsfs.FileInfo, size, off int64, data []byte) (ressize int64, err error) {

	start := time.Now()
	defer func() {
		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(fileinfo.Name, " ReadAt time:", time.Since(start), "off:", off, " size:", size, " err:", err)
		}
	}()

	glog.V(4).Infoln("ReadAt:", fullpath, size, off)
	if off+size > fileinfo.Size {
		return 0, ReadOutOfSize
	}
	of := c.getopfile(fileinfo)
	if of == nil {
		glog.V(4).Infoln("ReadAt:create opfile", fullpath)
		//创建一个opfile
		of = c.Addopfile(handle, fullpath, fileinfo)
	}

	of.oplock.RLock()
	defer of.oplock.RUnlock()
	of.isclosed = false
	startidx := off / BlockSize

	endidx := (off + size - 1) / BlockSize

	newoff := off
	left := size
	glog.V(1).Infoln("ReadAt: fullpath ", fullpath, " of.fullpath:", of.fullpath, " startidx:", startidx, " endidx:", endidx, " size:", size, " off:", off, " filesize:", of.basefile.Size)
	for i := startidx; i <= BlockSize; i++ {
		if left == 0 {
			break
		}
		blockoff := int64(i) * BlockSize
		tempblock := of.GetBlock(blockoff)
		if tempblock == nil {
			of.cachedatalock.Lock()
			blkchanlength := int64(len(of.blkchan))
			if blkchanlength < (endidx + 1) {
				of.blkchan = append(of.blkchan, make([]chan struct{}, endidx+1-blkchanlength)...)
			}
			if cap(of.blkchan[i]) < 1 {
				of.blkchan[i] = make(chan struct{}, 1)
			}
			of.cachedatalock.Unlock()
			of.blkchan[i] <- struct{}{}
			tempblock = of.GetBlock(blockoff)
			if tempblock == nil {
				glog.V(1).Infoln(" can not get block:", fullpath, " blockid:", i, " blockoff:", blockoff, " size:", of.snbsSize)
				if blockoff < of.snbsSize {
					tempblock, err = c.GetNewBlockFromSnbs(of, blockoff)
					if err != nil {
						<-of.blkchan[i]
						glog.V(0).Infoln("ReadAt:GetNewBlockFromSnbs error ", fullpath, " blockoff:", blockoff, " err:", err)
						return 0, err
					}
					atomic.AddInt32(&(tempblock.users), 1)
					c.SetBlock(tempblock, handle, fileinfo)
				} else {
					<-of.blkchan[i]
					return 0, ReadOutOfSize
				}
				<-of.blkchan[i]
			} else {
				<-of.blkchan[i]
				c.refreshLRU(tempblock)
			}
		} else {
			c.refreshLRU(tempblock)
		}
		glog.V(4).Infoln("datasize:", len(tempblock.data), " size:", size, " left:", left, " newoff:", newoff, " blockoff:", blockoff)
		if size < left {
			atomic.AddInt32(&(tempblock.users), -1)
			glog.V(0).Infoln("ReadAt: error ", fullpath, " blockoff:", blockoff, " err:", ReadOutOfSize)
			return 0, ReadOutOfSize
		}
		if newoff < blockoff {
			atomic.AddInt32(&(tempblock.users), -1)
			glog.V(0).Infoln("ReadAt: error ", fullpath, " blockoff:", blockoff, " err:", ReadOutOfSize)
			return 0, ReadOutOfSize
		}
		if int64(len(data)) < (size - left) {
			atomic.AddInt32(&(tempblock.users), -1)
			glog.V(0).Infoln("ReadAt: error ", fullpath, " blockoff:", blockoff, " err:", ReadOutOfSize)
			return 0, ReadOutOfSize
		}
		if int64(len(tempblock.data)) < (newoff - blockoff) {
			atomic.AddInt32(&(tempblock.users), -1)
			glog.V(0).Infoln("ReadAt: error ", fullpath, " blockoff:", blockoff, " err:data get from snbs less than info'size")
			return 0, ReadOutOfSize
		}

		copy(data[size-left:], tempblock.data[newoff-blockoff:])
		n := (int64(len(tempblock.data)) - (newoff - blockoff))
		if left < n {
			n = left
		}
		left -= n
		newoff += n
		ressize += n
		atomic.AddInt32(&(tempblock.users), -1)
	}

	return
}

//根据实际情况把数据刷到snbs,实际的上传文件
//需要知道1 文件路径  2 上传的偏移 3 上传的数据 4 文件现在的类型 5 文件现在是否已经被重新打开
//原来就存在，是分块上传，reopen（首次） + replace  块的大小必须保证跟以前的一样
//            是直接上传，reopen（首次），现在超过分块阈值了，采用分块上传方式上传
//                                        没超过阈值，直接覆盖上传
//原来不存在，原来超过阈值了，采用分块上传方式上传
//            原来没超过阈值，直接上传
//close的动作会把最后没提交的一次性都提交了。对于是分块上传的，要进行commit操作
func (c *CacheDataMana) flushBlock(offset int64, data []byte, of *opfile) error {
	if debug {
		glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset)
	}
	start := time.Now()
	if of.basefile.Size == 0 {
		return EmptyFileErr
	}
	//小于4M的直接上传
	if of.basefile.Size < BlockSize {

		//直接上传，覆盖上传
		_, err := c.driver.uploadnomalfile(of.fullpath, data)
		if err != nil {
			of.err = err
			glog.V(0).Infoln("file:", of.fullpath, " flushBlock err:", err)
			return err
		}
		of.blockpushlock.Lock()
		of.blockpushsize[offset] = len(data)
		of.blockpushlock.Unlock()
		glog.V(1).Infoln("file:", of.fullpath, " flushBlock off:", offset, " dirtysize:", len(data), " uploadnomalfile mode,", time.Since(start))
		of.cacheOnly = false
	} else {
		of.reopenlock.Lock()
		if of.uploadid == "" {
			//CacheOnly 是 true 表示是新文件
			// 不需要reopen，直接进行分块上传
			if of.cacheOnly {
				if of.err != nil {
					of.reopenlock.Unlock()
					glog.V(0).Infoln("file:", of.fullpath, " has writed failed.no more try.err:", of.err)
					return of.err
				}
				//获取uploadid
				uploadid, err := c.driver.getuploadid(of.fullpath)
				if err != nil {
					of.err = GetUploadIdErr
					of.reopenlock.Unlock()
					glog.V(0).Infoln("file:", of.fullpath, "flushBlock getuploadid err:", err)
					return GetUploadIdErr
				}
				if debug || time.Since(start) > 2*time.Second {
					glog.V(0).Infoln("file:", of.fullpath, " flushBlock off:", offset, " getuploadid timecost: ", time.Since(start), " dirtysize:", len(data))
				}
				of.uploadid = uploadid
			} else { //表示是旧文件需要reopen
				if !of.isopend {
					if debug || time.Since(start) > 2*time.Second {
						glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset, " reopen getuploadid timecost:", time.Since(start))
					}

					of.isopend = true
					if of.uploadid == "" {
						if of.err != nil {
							of.reopenlock.Unlock()
							glog.V(0).Infoln("file:", of.fullpath, " has writed failed.no more try.err:", of.err)
							return of.err
						}
						uploadid, err := c.driver.getuploadid(of.fullpath)
						if err != nil {
							of.err = GetUploadIdErr
							of.reopenlock.Unlock()
							glog.V(0).Infoln("flushBlock getuploadid err:", err)
							return GetUploadIdErr
						}
						if debug || time.Since(start) > 2*time.Second {
							glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset, " is reopen op,but also need getuploadid timecost:", time.Since(start))
						}
						of.uploadid = uploadid
					}
				}
			}
		}
		of.reopenlock.Unlock()
		glog.V(1).Infoln("START flushBlock  ", of.fullpath, " off:", offset, " dirtysize:", len(data), "getuploadid timecost:", time.Since(start))
		if of.uploadid == "" {
			glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset, "uploadid is nil ")
			return UploadIdIsNullErr
		}

		idx := int(offset / BlockSize)
		if offset%BlockSize > 0 {
			idx += 1
		}
		start = time.Now()
		err := c.driver.uploadblock(of.fullpath, of.uploadid, idx, data)
		if err != nil {
			glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset, " uploadblock err ", err)
			return err
		}
		of.blockpushlock.Lock()
		of.blockpushsize[offset] = len(data)
		if of.blockpushsize[offset] > of.blockneedpushsize[offset] {
			of.blockneedpushsize[offset] = of.blockpushsize[offset]
		}
		of.blockpushlock.Unlock()
		glog.V(1).Infoln("END flushBlock  ", of.fullpath, " off:", offset, " dirtysize:", len(data), " maxoff", of.maxoff, "getuploadid timecost:", time.Since(start))

		if debug || time.Since(start) > 2*time.Second {
			glog.V(0).Infoln(" flushBlock  ", of.fullpath, " off:", offset, " uploadblock ", time.Since(start))
		}
	}
	return nil
}

//获取需要移除的缓存
//从lrulist 中取的
func (c *CacheDataMana) getColdBlock() map[string]*cachedata {
	c.lrulock.RLock()
	defer c.lrulock.RUnlock()
	count := c.lrulist.Len()
	res := make(map[string]*cachedata)
	last := c.lrulist.Back()
	for i := count; i > 0 && last != nil; i-- {
		temp, ok := last.Value.(*cachedata)
		if !ok {
			last = last.Prev()
			continue
		}
		if temp.isInFlush { //在刷的不能被置换出来,不用锁，宁可多等一会
			last = last.Prev()
			continue
		}

		if !needDelete(temp) {
			last = last.Prev()
			continue
		}
		if temp.of.isclosed == true { //文件关闭后，可以立即释放缓存块
		} else {
			if temp.lastaccesstime+MaxCachetime*1e9 > time.Now().UnixNano() {
				last = last.Prev()
				continue
			}
		}
		key := fmt.Sprintf("%d_%d", temp.of.nodehandle, temp.off)
		res[key] = temp
		last = last.Prev()
	}
	if debug {
		glog.V(0).Infoln(" getColdBlock  total:", c.lrulist.Len(), " cold sum:", len(res))
	}
	return res
}

func needFlush(block *cachedata) bool {
	var datalen int
	of := block.of
	defer of.blockpushlock.Unlock()
	of.blockpushlock.Lock()
	datalen = len(block.data)
	for k, v := range of.blockneedpushsize {
		if k == atomic.LoadInt64(&of.maxoff) || k == block.off {
			continue
		}
		if v != int(BlockSize) {
			return false
		}
	}
	for k1, v1 := range of.blockpushsize {
		if k1 == atomic.LoadInt64(&of.maxoff) || k1 == block.off {
			continue
		}
		if v1 != int(BlockSize) {
			return false
		}
	}
	glog.V(2).Infoln("needflush file:", of.fullpath, " block[", block.off, "]=", datalen, " maxoff:", of.maxoff)
	return true
}

func startFlush(block *cachedata) bool {
	var datalen int
	of := block.of
	defer of.blockpushlock.Unlock()
	of.blockpushlock.Lock()
	if of.err != nil {
		return true
	}
	datalen = len(block.data)
	for k, v := range of.blockneedpushsize {
		if k == atomic.LoadInt64(&of.maxoff) || k == block.off {
			continue
		}
		if v != int(BlockSize) {
			of.blockneedpushsize[block.off] = datalen
			return false
		}
	}
	for k1, v1 := range of.blockpushsize {
		if k1 == atomic.LoadInt64(&of.maxoff) || k1 == block.off {
			continue
		}
		if v1 != int(BlockSize) {
			of.blockneedpushsize[block.off] = datalen
			return false
		}
	}
	glog.V(1).Infoln("startflush file:", of.fullpath, " block[", block.off, "]=", datalen, " maxoff:", of.maxoff, " inflush:", block.isInFlush)
	of.blockneedpushsize[block.off] = datalen
	return true
}

func needDelete(block *cachedata) bool {
	var datalen int
	of := block.of
	of.blockpushlock.RLock()
	datalen = len(block.data)
	if of.blockneedpushsize[block.off] != 0 && of.blockpushsize[block.off] != datalen {
		of.blockpushlock.RUnlock()
		return false
	}
	of.blockpushlock.RUnlock()

	block.RLock()
	defer block.RUnlock()
	if block.dirtysize != 0 {
		return false
	}
	if atomic.LoadInt32(&(block.users)) != 0 {
		return false
	}
	return true
}

//获取脏的 且已经很久没刷新的文件
func (c *CacheDataMana) getDirtyBlock() map[string]*cachedata {
	c.lrulock.RLock()
	defer c.lrulock.RUnlock()
	res := make(map[string]*cachedata)
	last := c.lrulist.Back()
	for last != nil {
		temp, ok := last.Value.(*cachedata)
		if ok {
			//脏，且最早写入时间超过MaxDirtytime
			if temp.dirtysize > 0 && (temp.lastaccesstime+MaxDirtytime*1e9) < time.Now().UnixNano() && needFlush(temp) {
				glog.V(4).Infoln(temp.of.fullpath, " dirtysize:", temp.dirtysize, " off:", temp.off, " inflush:", temp.isInFlush)
				key := fmt.Sprintf("%d_%d", temp.of.nodehandle, temp.off)
				res[key] = temp
			}
		}
		last = last.Prev()
	}
	if debug {
		glog.V(1).Infoln(" getDirtyBlock  total:", c.lrulist.Len(), " dirty sum:", len(res))
	}
	return res
}

var queIdx uint64

//根据队列长度，获取一个队列短的线程
func getAIdleThread(newfile *opfile, threadsum int) (threadIdx uint64) {
	threadIdx = atomic.LoadUint64(&queIdx) % uint64(threadsum)
	atomic.AddUint64(&queIdx, 1)
	return
}

//退出之前，把存在的脏数据下刷
func (c *CacheDataMana) flushAll() {
	if debug {
		glog.V(0).Infoln("want to  flushAll")
	}
	c.lrulock.RLock()
	last := c.lrulist.Back()
	for last != nil {
		temp, ok := last.Value.(*cachedata)
		if ok {
			//脏，且最早写入时间超过MaxDirtytime
			if temp.isDirty() {
				idx := getAIdleThread(temp.of, FlushThreadSum)
				isadd := c.workers[idx].add(temp, 0)
				if debug && isadd {
					glog.V(0).Infoln(" flushAll  ", "push ", temp.of.fullpath, " off:", temp.off, " to work:", idx)
				}

			}
		}
		last = last.Prev()
	}
	c.lrulock.RUnlock()
	return
}

//把一个块从缓存中移除（请确认已经被刷过了）
//注意注意！！！！block 的读写锁
//移除block是在读锁的情况下移除，避免影响有可能的读操作（到这儿的时候，就不管读操作引起的置先了）。但是这个读操作不会影响流程
func (c *CacheDataMana) removeBlock(block *cachedata, f bool) error {
	//1 没脏数据直接移除，脏数据先提交（copy提交，可以判定能不能提交），
	//2 又变化了？就放弃移除.没变化了就移除

	defer c.tryremoveopfile(block.of.basefile)
	block.Lock()
	defer block.Unlock()
	//被别人移除了（）
	if block.isdeleted {
		return nil
	}
	//没人修改,这时候也没人有写锁，可以在读锁的情况下修改isdeleted，直接就删除了
	if block.dirtysize == 0 || f {
		//真正的移除
		if c.removecachedata(block) {
			block.isdeleted = true
		}
	} else {
		return NewDirtyDataErr
	}
	return nil

}

//cachedata 被使用之后调用，刷新他的位置
func (c *CacheDataMana) refreshLRU(data *cachedata) {
	c.lrulock.Lock()
	defer c.lrulock.Unlock()
	data.lastaccesstime = time.Now().UnixNano()
	if data.locelem == nil {
		if c.cacheSeqId == 0 {
			c.cacheSeqId++
		}
		if data.blkid == 0 {
			data.blkid = c.cacheSeqId
			c.cacheSeqId++
		}
		locelem := c.lrulist.PushFront(data)
		data.locelem = locelem
		return
	}
	c.lrulist.MoveToFront(data.locelem)
}

//of.cachedatalock 要先被锁
func (c *CacheDataMana) removecachedata(data *cachedata) (hasdel bool) {
	hasdel = data.of.removecachedata(data)
	if hasdel && data.locelem != nil {
		c.lrulock.Lock()
		c.lrulist.Remove(data.locelem)
		c.lrulock.Unlock()
		return hasdel
	}
	hasdel = false
	return hasdel
}

//不停的刷新缓存，close之后退出循环
// 包括把脏数据写入到SNBS，
//     把超过4块的缓存移除
func (c *CacheDataMana) refreshColdLoop() {
	//每0.2S一次
	timetick := time.Tick(200 * time.Millisecond)
	for _ = range timetick {
		datas := c.getColdBlock()
		for _, v := range datas {
			err := c.removeBlock(v, false)
			if err != nil {
				glog.V(0).Infoln(" refreshColdLoop  : want to removeBlock:", v.of.fullpath, " off:", v.off, " err:", err)
			}
		}
		if c.isstop {
			break
		}
	}
	return
}

//不停的刷新缓存，close之后退出循环
// 包括把脏数据写入到SNBS，
//     把超过4块的缓存移除
func (c *CacheDataMana) refreshDirtyLoop() {
	//每1S一次
	timetick := time.Tick(time.Duration(DirtyScanInterval) * time.Millisecond)
	for _ = range timetick {
		datas := c.getDirtyBlock()
		if debug {
			glog.V(1).Infoln(" refreshDirtyLoop  :", "getDirtyBlock sum:", len(datas))
		}
		for _, v := range datas {
			//根据nodeid把数据发送到flushworker中进行刷盘
			//把v添加入
			idx := getAIdleThread(v.of, FlushThreadSum)
			isadd := c.workers[idx].add(v, 0)
			glog.V(4).Infoln(" refreshDirtyLoop  ", "push ", v.of.fullpath, " off:", v.off, " to work:", idx, " ok?", isadd, " inFlush:", v.isInFlush)
		}
		if c.isstop {
			break
		}
	}
	return
}

//上传指定块
func (c *CacheDataMana) upload(block *cachedata, idx int) (err error) {
	dirtysize := 0
	defer func() {
		onflushs := block.getOnflushs()
		for _, f := range onflushs {
			f(dirtysize, err)
		}
	}()
	block.RLock()
	olddirtysize := block.dirtysize
	if olddirtysize == 0 {
		block.RUnlock()
		if debug {
			glog.V(0).Infoln(" flushWorker  upload ", block.of.fullpath, " dirtysize is 0. off:", block.off)
		}
		return nil
	}
	if len(block.data) == 0 {
		block.RUnlock()
		if debug {
			glog.V(0).Infoln(" flushWorker  upload ", block.of.fullpath, " data size is 0. off:", block.off)
		}
		return nil
	}
	dirtysize = len(block.data)
	data := make([]byte, dirtysize)
	copy(data, block.data)
	oldsize := block.off + int64(dirtysize)
	block.RUnlock()

	if err = c.flushBlock(block.off, data, block.of); err != nil {
		glog.V(0).Infoln("flushBlock:", block.of.fullpath, " len: ", len(data), " off:", block.off, "err:", err)
		if err == EmptyFileErr {
			return nil
		}
		return err
	}
	glog.V(4).Infoln("upload data :", block.of.fullpath, " nodehandle:", block.of.nodehandle, " off:", block.off, " dirtysize:", dirtysize)

	block.Lock()
	defer block.Unlock()

	if oldsize > block.of.snbsSize {
		block.of.snbsSize = oldsize
	}
	block.lastrefreshtime = time.Now().UnixNano()
	block.earliestdirtytime = time.Now().Unix()
	if block.dirtysize != olddirtysize {
		block.dirtysize -= olddirtysize
		return NewDirtyDataErr
	}
	//标记已经没有脏数据了
	block.dirtysize = 0
	return nil
}

type flushWorker struct {
	ma *CacheDataMana
	//等待队列
	waitqueue *list.List
	queuelock sync.RWMutex
	//重试次数 key=nodeid_offset
	//超过3次的丢弃+报错  这个很严重
	retrysum map[string]int

	//单向
	isstop bool

	running bool

	//宁多别少，插入时候添加，需要重试时候添加
	trigger chan struct{}
}

func newFlushWorker(ma *CacheDataMana) *flushWorker {
	f := &flushWorker{
		ma:        ma,
		waitqueue: list.New(),
		retrysum:  make(map[string]int),
		trigger:   make(chan struct{}, MAXCapacity),
	}
	return f
}

func (f *flushWorker) add(block *cachedata, hastrytime int) (isadd bool) {
	isadd = false
	block.Lock()
	if block.isInFlush == true || block.dirtysize == 0 {
		block.Unlock()
		return
	}
	block.isInFlush = true
	block.Unlock()
	key := fmt.Sprintf("%d", block.blkid)
	f.queuelock.Lock()
	if v, ok := f.retrysum[key]; ok {
		glog.V(0).Infoln("BUG? file:", block.of.fullpath, " off:", block.off, " hastrytime:", hastrytime, " key:", key, " v:", v)
		f.retrysum[key] = v + hastrytime
		f.queuelock.Unlock()
		return
	}
	f.retrysum[key] = hastrytime
	f.waitqueue.PushBack(block)
	f.queuelock.Unlock()
	f.touchTrigger()
	isadd = true
	return
}

func (f *flushWorker) touchTrigger() {
	select {
	case f.trigger <- struct{}{}:
		break
	case <-time.After(time.Duration(DirtyScanInterval) * time.Millisecond):
		break
	}
}
func cleanBlockFlag(block *cachedata, idx int, cleanDirty bool, err error) {
	block.Lock()
	block.isInFlush = false
	if cleanDirty == true {
		block.dirtysize = 0
		block.flushErr = err
		if block.of.err == nil {
			block.of.err = err
		}
	}
	block.Unlock()
	if debug {
		glog.V(1).Infoln("clean block flag:", block.off, " thr idx:", idx, " cleanDirty:", cleanDirty, " err:", err)
	}
}
func (f *flushWorker) start(idx int) {
	f.running = true
	clean := false
	for {
		select {
		case <-f.trigger:
			f.queuelock.Lock()
			//有东西要
			h := f.waitqueue.Front()
			if h == nil {
				f.queuelock.Unlock()
				continue
			}

			block, ok := h.Value.(*cachedata)
			//脏数据？？？
			if !ok {
				f.waitqueue.Remove(h)
				glog.V(0).Infoln("dirty data,not cachedata in FlushWorker ")
				f.queuelock.Unlock()
				continue
			}

			if !startFlush(block) {
				//放到队尾
				f.waitqueue.MoveToBack(h)
				f.queuelock.Unlock()
				f.touchTrigger()
				continue
			}

			key := fmt.Sprintf("%d", block.blkid)
			//先移除，再上传，失败了重新添加
			block.RLock()
			dsize := block.dirtysize
			block.RUnlock()

			if dsize > 0 && block.lastrefreshtime+MinRefreshInterval*1e6 > time.Now().UnixNano() {
				if debug {
					glog.V(0).Infoln(" flushWorker  ", key, " flush later please,now queue len:", f.waitqueue.Len())
				}
				time.Sleep(20 * time.Millisecond)
				if f.waitqueue.Len() == 1 {
					f.queuelock.Unlock()
					f.touchTrigger()
					continue
				}
				//放到队尾
				f.waitqueue.MoveToBack(h)
				f.queuelock.Unlock()
				f.touchTrigger()
				continue
			}

			oldtrytime := f.retrysum[key]
			delete(f.retrysum, key)
			f.waitqueue.Remove(h)
			f.queuelock.Unlock()

			if oldtrytime >= MaxRetryTime {
				glog.V(0).Infoln(" flush file:", block.of.fullpath, " off:", block.off, " dirty data retry times ,but always err,give it up", " oldtrytime:", oldtrytime, " MaxRetryTime:", MaxRetryTime)
				block.of.blockpushlock.Lock()
				delete(block.of.blockpushsize, block.off)
				delete(block.of.blockneedpushsize, block.off)
				block.of.blockpushlock.Unlock()
				cleanBlockFlag(block, idx, true, errors.New("write failed after try many times"))
				continue
			}

			clean = false
			err := f.ma.upload(block, idx)
			if err == nil || err == NewDirtyDataErr {
				if debug {
					glog.V(1).Infoln("flushWorker thr idx:", idx, " upload file:", block.of.fullpath, block.of.nodehandle, block.off, " ok.now queue len:", f.waitqueue.Len())
				}

				if err == nil {
					clean = true
				}
				cleanBlockFlag(block, idx, clean, err)
				//没报错或者有新的脏数据了，都算作这次上传的成功
				continue
			} else {
				if block.of.err != nil {
					oldtrytime = MaxRetryTime //其他块写失败了，本块就立即失败 jjj
				}
				cleanBlockFlag(block, idx, clean, nil)
			}
			isadd := f.add(block, oldtrytime+1)
			glog.V(0).Infoln("idx:", idx, " retry add ok?", isadd, " file:", block.of.fullpath, " off:", block.off, "upload err:", err, " key:", key, " oldtrytime:", oldtrytime, " MaxRetryTime:", MaxRetryTime, " dirtysize:", block.dirtysize, " inflush:", block.isInFlush)
			f.touchTrigger()
		case <-time.After(time.Duration(DirtyScanInterval) * time.Millisecond):
			if f.isstop {
				glog.V(0).Infoln("stop FlushWorker")
				return
			}
			f.touchTrigger()
			continue
		}
	}
	f.running = false
}

func (f *flushWorker) stop() {
	f.isstop = true

	for {
		if f.running {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}
