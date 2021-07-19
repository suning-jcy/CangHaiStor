package tcp

import (
	"io"

	"code.suning.com/glog"
)

type Request messageMetaData

type Response messageMetaData

type messageMetaData interface {
	//请保证速度，控制内存
	//序列化之后直接输出到w
	Marshal(w io.Writer) error
	//从r中读取数据，直接反序列化
	Unmarshal(r io.Reader) error

	//请求类型，请一定规定下
	GetType() uint64

	//这个结构体本身序列化后所占的实际字节
	GetLen() int64
	//body体的长度，设置这个是为了以后内存池的应用。body的长度提早固定，请一定要返回有效的实际数值
	GetBodyLen() int32

	//要发送的body
	GetBody() io.Reader
	//实际上虽然是reader但是不希望读到一个新的内存中。
	SetBody(io.Reader)
	//包括了释放到池
	Reset()

	MemberSet(*MsgHead)

	GetWriteDataLen() int64

	GetReadDataLen() int64

	GetResult() int64
}

type readOnlyBody struct {
	r               io.Reader
	off             int32
	maxsize         int32
	readAllCallBack func()
}

func newReadOnlyBody(r io.Reader, maxsize int32) *readOnlyBody {
	rob := &readOnlyBody{
		r:       r,
		maxsize: maxsize,
	}
	return rob
}

func (rob *readOnlyBody) Read(p []byte) (n int, err error) {

	if rob.off >= rob.maxsize {
		return 0, io.EOF
	}
	leftszie := rob.maxsize - rob.off
	if int32(len(p)) > leftszie {
		n, err = io.ReadFull(rob.r, p[:leftszie])
	} else {
		n, err = io.ReadFull(rob.r, p)
	}
	rob.off += int32(n)
	if rob.off == rob.maxsize && rob.readAllCallBack != nil {
		rob.readAllCallBack()
	}
	//希望读取maxsize，但是没读到，EOF了也不停止。（一般r是网络）
	if rob.off < rob.maxsize && err == io.EOF {
		err = nil
	}
	if err != nil && err != io.EOF {
		if rob.readAllCallBack != nil {
			rob.readAllCallBack()
		}
		glog.V(0).Infoln("readOnlyBody:read:", n, err)
	}

	return
}

func (rob *readOnlyBody) GetSize() int32 {
	return rob.maxsize
}

func (rob *readOnlyBody) setReadAllCallBack(callback func()) {
	rob.readAllCallBack = callback
}
