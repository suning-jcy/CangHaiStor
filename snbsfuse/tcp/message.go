package tcp

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"unsafe"

	"code.suning.com/glog"
)

type MsgHead = messageHeader

var ErrBadMessType = errors.New("unknow message type")
var ErrUnDefindMetaFunc = errors.New("UnDefind Meta GetFunc")
var messageHeaderStructSize = int(unsafe.Sizeof(messageHeader{}))

type NewMessMetaFunc func() interface{}

var AllNewMessMetaFuncMap = make(map[MessType]NewMessMetaFunc)

type messageHeader struct {
	MessSeq      uint64
	SendTime     int64
	MessType     uint64
	MetaLen      int64
	CheckSum     uint64
	Version      uint32
	Flag         [4]byte
	Name         [64]byte
	Offset       uint64
	WriteDataLen int64
	ReadDataLen  int64
	Result       int64
}

func newMessageHeader() *messageHeader {
	var m = &messageHeader{}
	return m
}

func GwMetaPro(meta messageMetaData, head *MsgHead, r io.Reader, opType MessType) error {

	meta.MemberSet(head)
	if opType == GwWriteReq {
		return meta.Unmarshal(r)
	} else if opType == GwReadReq {
		return meta.Unmarshal(r)
	}
	return nil
}

func (m *messageHeader) getData() []byte {
	var x reflect.SliceHeader
	x.Len = messageHeaderStructSize
	x.Cap = messageHeaderStructSize
	x.Data = uintptr(unsafe.Pointer(m))
	return *(*[]byte)(unsafe.Pointer(&x))
}

//填充头
func (m *messageHeader) pull(r io.Reader) (err error) {
	tempbuf := m.getData()
	_, err = io.ReadFull(r, tempbuf)
	return err
}

//发出头
func (m *messageHeader) push(w io.Writer) (err error) {
	tempbuf := m.getData()
	_, err = w.Write(tempbuf)
	return err
}

func (m *messageHeader) getOpMethod() MessType {
	return MessType(m.MessType)
}

func (m *messageHeader) Reset() {
	m.MessSeq = 0
	m.SendTime = 0
	m.MessType = 0
	m.MetaLen = 0
	m.CheckSum = 0
	for i := 0; i < 8; i++ {
		m.Flag[i] = 0
	}
}

// message a socket message data.
type Message struct {
	Header *messageHeader
	Meta   messageMetaData
	Body   *readOnlyBody
}

var messagePool = sync.Pool{
	New: func() interface{} {
		return newMessage()
	},
}

func CaptureMessage() *Message {
	m := messagePool.Get().(*Message)
	return m
}

func ReleaseMessage(m *Message) {
	if m == nil {
		return
	}
	m.reset()
	messagePool.Put(m)
}

func ReleaseMessage2(m *Message) {
	if m == nil {
		return
	}
	if m.Meta != nil {
		m.Meta.Reset()
	}
	m.reset()
	messagePool.Put(m)
}

func newMessage() *Message {
	var m = &Message{
		Header: newMessageHeader(),
	}
	return m
}

//重置Message
func (m *Message) reset() {
	m.Header.Reset()
	m.Meta = nil
	m.Body = nil
}

func (m *Message) GetOpMethod() MessType {
	return m.Header.getOpMethod()
}

func (m *Message) pull(r io.Reader) error {
	//读到一个消息头
	err := m.Header.pull(r)
	if err != nil {
		return err
	}
	//获取一个meta信息
	m.Meta, err = GetMessMeta(m.GetOpMethod())
	if err != nil {
		glog.V(0).Infoln(err.Error(), m.GetOpMethod())
		return err
	}
	opType := m.GetOpMethod()
	if (opType == GwReadReq) || (opType == GwWriteReq) || (opType == GwRemoveReq) || (opType == GwQueryReq) {
		m.Body = nil
		err = GwMetaPro(m.Meta, m.Header, r, opType)
		if err != nil {
			return err
		}
		return nil
	}
	if m.Meta == nil {
		glog.V(0).Infoln(ErrUnDefindMetaFunc.Error(), m.GetOpMethod())
		return ErrUnDefindMetaFunc
	}

	//填充mate信息
	err = m.Meta.Unmarshal(r)
	if err != nil {
		return err
	}

	if m.Meta == nil {
		return nil
	}
	size := m.Meta.GetBodyLen()
	if size == 0 {
		return nil
	}
	//现在只有写的时候会从r中读取数据
	m.Body = newReadOnlyBody(r, size)
	m.Meta.SetBody(m.Body)
	return err
}

func (m *Message) push(w io.Writer) (err error) {

	err = m.Header.push(w)
	if err != nil {
		glog.V(0).Infoln("fatal error Header push:", err)
		return err
	}
	err = m.Meta.Marshal(w)
	if err != nil {
		glog.V(0).Infoln("fatal error Meta push:", err)
		return err
	}
	if m.Body == nil {
		return
	}
	if m.Body != nil {
		n := int64(0)
		n, err = io.Copy(w, m.Body)
		if err != nil {
			glog.V(0).Infoln("message push body:", n, err, m.Meta.GetBodyLen())
		}
	}
	return err
}

type MessType uint64

const (
	RemoveReq              = 1001
	CreateReq              = 1002
	CloneChunkReq          = 1003
	ChunkStatReq           = 1004
	ReadBlockReq           = 1005
	ReadDataVecPackageReq  = 1006
	WriteBlockPackageReq   = 1007
	ReadDataVecPackage2Req = 1008
	LinkCloneReq           = 1009
	FullCloneReq           = 1010
	RollToSnapReq          = 1011
	UnLinkCloneReq         = 1012
	MakeSnapReq            = 1013

	RemoveRes              = 2001
	CreateRes              = 2002
	CloneChunkRes          = 2003
	ChunkStatRes           = 2004
	ReadBlockRes           = 2005
	ReadDataVecPackageRes  = 2006
	WriteBlockPackageRes   = 2007
	ReadDataVecPackage2Res = 2008
	LinkCloneRes           = 2009
	FullCloneRes           = 2010
	RollToSnapRes          = 2011
	UnLinkCloneRes         = 2012
	MakeSnapRes            = 2013

	GwReadReq   = 3001
	GwWriteReq  = 3002
	GwRemoveReq = 3003
	GwQueryReq  = 3004
)

//没用锁保护，请启动时候服务开启前就调用完毕
func AddMetaFunc(mType MessType, factory NewMessMetaFunc) {
	AllNewMessMetaFuncMap[mType] = factory
}

//拎出来而不是集成到handler里，是为了强制写一个构造方法
func GetMessMeta(mType MessType) (meta messageMetaData, err error) {
	factory, ok := AllNewMessMetaFuncMap[mType]
	if !ok {
		glog.V(0).Infof("get bad MessType:%d", mType)
		return nil, ErrBadMessType
	}
	meta = factory().(messageMetaData)
	return
}
