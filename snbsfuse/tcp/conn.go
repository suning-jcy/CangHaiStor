package tcp

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"code.suning.com/glog"
)

//todo
//1 接收时候的[]byte 可以从pool中取

//RedialTimes 重连次数
var RedialTimes = 3

//RedialInterval 重连等待时间1S
var RedialInterval = 1000

type MetaProtoFunc func(m *Message) error

type Conn struct {
	Conn          net.Conn
	ControlReader *bufio.Reader
	ControlWriter *bufio.Writer
	sessionID     string

	//for server
	Server *Server

	closed         bool
	lastAccessTime time.Time

	sync.Mutex
}

func NewConn(tcpConn net.Conn) *Conn {
	c := new(Conn)
	c.Conn = tcpConn
	c.ControlReader = bufio.NewReader(tcpConn)
	c.ControlWriter = bufio.NewWriter(tcpConn)
	c.sessionID = newSessionID()
	c.closed = false

	return c
}

//使用时候用
func (conn *Conn) SetOpTimeOut(opTimeout time.Duration) {
	if opTimeout == 0 {
		conn.Conn.SetDeadline(time.Unix(0, 0))
	} else {
		conn.Conn.SetDeadline(time.Now().Add(opTimeout))
	}

}

// 入/出 空闲队列了用，freeTimeout！=0 是出队列
func (conn *Conn) SetWaitTimeOut(waitTimeout time.Duration) {
	if waitTimeout == 0 {
		//出队列
		conn.Conn.SetDeadline(time.Unix(0, 0))
	} else {
		conn.lastAccessTime = time.Now()
		conn.Conn.SetDeadline(conn.lastAccessTime.Add(waitTimeout))
	}
}

func (conn *Conn) IsTimeOut(waitTimeOut time.Duration) bool {
	//50ms的重叠时间
	if conn.lastAccessTime.Add(waitTimeOut + 50*time.Millisecond).Before(time.Now()) {
		return true
	}
	return false
}

func (conn *Conn) PublicIP() string {
	return conn.Server.publicIP
}

// returns a random 20 char string that can be used as a unique session ID
func newSessionID() string {
	hash := sha256.New()
	_, err := io.CopyN(hash, rand.Reader, 50)
	if err != nil {
		return "????????????????????"
	}
	md := hash.Sum(nil)
	mdStr := hex.EncodeToString(md)
	return mdStr[0:20]
}

//供服务端调用，每个链路一个协程处理。
//消息处理是同步
func (conn *Conn) Serve() {
	for {
		//接收到请求头了
		reqMess, err := conn.ReceiveMess()
		if err != nil {
			//接收时候出现错误的话 直接打印错误，断开这个客户端链接（也包括正常退出）
			if err != io.EOF {
				glog.V(0).Infof("receive message err:%s", err.Error())
			} else {
				glog.V(0).Infof("receive close fd message from:%s ", conn.Conn.RemoteAddr())
			}
			break
		}
		//根据消息头中的消息类型获得相应的处理方法
		if handler, ok := conn.Server.Handlers[reqMess.GetOpMethod()]; ok {
			//处理结束，得到一个结果meta
			respMeta, err := handler(reqMess.Meta)

			if err != nil {
				ReleaseMessage2(reqMess)
				glog.V(0).Infof("handler mess from %s err:%s", reqMess.GetOpMethod(), conn.Conn.RemoteAddr(), err.Error())
				break
			} else {
				//根据meta构建出mess
				sendMess := GetMess(respMeta)
				//mess发送给客户端
				err = conn.WriteMessage(sendMess)
				if respMeta == reqMess.Meta {
					ReleaseMessage(sendMess)
					ReleaseMessage(reqMess)
				} else {
					ReleaseMessage2(sendMess)
					ReleaseMessage2(reqMess)
				}

				if err != nil {
					glog.V(0).Infof("send  message err:", err)
					break
				}
			}
		} else {
			glog.V(0).Infof("get unknow op:%d ,pass it", reqMess.GetOpMethod())
			ReleaseMessage2(reqMess)
		}
	}
	if conn.Conn != nil {
		conn.Conn.Close()
	}
}

//get mess from client and deal it
func (conn *Conn) Serve2() {
	defer func() {
		if err := recover(); err != nil {
			glog.V(0).Info("get panic err!!!", err)
		}
	}()
	glog.V(4).Info("new conn-server start")

	stopChan := make(chan error, 3)
	readChan := make(chan *Message, 1)
	writeChan := make(chan *Message, 1)
	stopflag := false
	go conn.readConn(readChan, stopChan)
	go conn.writeConn(writeChan, stopChan)
	for {
		select {
		case mess := <-readChan:
			if mess == nil {
				glog.V(0).Infof("get nil mess from %s", conn.Conn.RemoteAddr())
				continue
			}

			if handler, ok := conn.Server.Handlers[mess.GetOpMethod()]; ok {

				respMeta, err := handler(mess.Meta)
				ReleaseMessage(mess)
				if err != nil {
					glog.V(0).Infof("handler mess from %s err:%s", conn.Conn.RemoteAddr(), err.Error())
					stopflag = true
					stopChan <- err
					break
				} else {
					resp := GetMess(respMeta)
					writeChan <- resp
				}
			} else {
				glog.V(0).Infof("get unknow op:%d ,pass it", mess.GetOpMethod())
				ReleaseMessage(mess)
			}
		case stoperr := <-stopChan:
			if stoperr != nil && stoperr != io.EOF {
				glog.V(0).Info(fmt.Sprintln("read error:", stoperr))
			}
			stopChan <- stoperr
			stopflag = true
			break
		}
		if stopflag {
			break
		}
	}
	conn.Close()
	glog.V(4).Info("Connection Terminated")
}

func (conn *Conn) readConn(readChan chan<- *Message, stopChan chan error) {
	var err error
	for {
		var resMess *Message
		resMess, err = conn.ReceiveMess()
		if err != nil {
			if err != io.EOF {
				glog.V(0).Infof("receive message err:%s", err.Error())
			}
			break
		}
		readChan <- resMess
	}
	stopChan <- err
}

func (conn *Conn) writeConn(writeChan <-chan *Message, stopChan chan error) {
	var err error
	for {
		var sendMess *Message
		select {
		case sendMess = <-writeChan:
		case err = <-stopChan:
		}
		if err != nil {
			break
		}
		if sendMess == nil {
			glog.V(0).Infof("send want send nil message ")
			break
		}
		err = conn.WriteMessage(sendMess)

		ReleaseMessage(sendMess)
		if err != nil {
			glog.V(0).Infof("send want send nil message ")
			break
		}

	}
	stopChan <- err
}

// Close will manually close this connection, even if the client isn't ready.
func (conn *Conn) Close() {
	conn.Lock()
	defer conn.Unlock()

	conn.Conn.Close()
	conn.closed = true
}

// Pack writes the Message into the connection.
// NOTE: Make sure to write only once or there will be package contamination!
func (conn *Conn) WriteMessage(m *Message) (err error) {
	err = m.push(conn.ControlWriter)
	if err != nil {
		return
	}
	conn.ControlWriter.Flush()
	return err
}

var errDataCheck = errors.New("check failed")

func getMd5(src []byte) ([]byte, error) {
	newMd5 := md5.New()
	_, err := newMd5.Write(src)
	if err != nil {
		return nil, err
	}

	return newMd5.Sum(nil), nil
}

// Unpack reads bytes from the connection to the Message.
// NOTE: Concurrent unsafe!
func (conn *Conn) ReceiveMess() (*Message, error) {
	m := CaptureMessage()
	err := m.pull(conn.ControlReader)
	if err != nil {
		ReleaseMessage(m)
		if err != io.EOF {
			glog.V(0).Infof(conn.sessionID, "binary.Read from controlReader conn:%s,err:%s", conn.Conn.RemoteAddr().String(), err.Error())
		}
		return nil, err
	}
	return m, nil
}

func (conn *Conn) GetSessionID() string {
	return conn.sessionID
}

func (conn *Conn) IsClosed() bool {
	return conn.closed
}
func (conn *Conn) getConn() net.Conn {
	return conn.Conn
}

//重连动作
func (conn *Conn) RedialForClient(oldConn net.Conn, dialFunc func() (net.Conn, error)) bool {
	conn.Lock()
	defer conn.Unlock()
	//重连
	if oldConn != conn.getConn() {
		return true
	}
	var err error
	var newConn net.Conn
	for i := RedialTimes; i > 0; i-- {
		time.Sleep(time.Duration(RedialInterval) * time.Millisecond)
		newConn, err = dialFunc()
		if err == nil {
			conn.Conn = newConn
			conn.ControlReader = bufio.NewReader(newConn)
			conn.ControlWriter = bufio.NewWriter(newConn)
			conn.closed = false
			return true
		}
	}
	conn.closed = true
	glog.V(0).Infof("redial with%s fail err:%s", oldConn.RemoteAddr(), err.Error())
	return false
}

func GetMess(meta messageMetaData) *Message {
	mess := CaptureMessage()
	mess.Header.MessType = meta.GetType()
	mess.Header.MetaLen = meta.GetLen()
	mess.Header.SendTime = time.Now().UnixNano()

	mess.Header.ReadDataLen = meta.GetReadDataLen()
	mess.Header.WriteDataLen = meta.GetWriteDataLen()
	mess.Header.Result = meta.GetResult()
	//todo 加上crc校验

	mess.Meta = meta
	if meta.GetBodyLen() > 0 {
		mess.Body = newReadOnlyBody(meta.GetBody(), meta.GetBodyLen())
	} else {
		mess.Body = nil
	}
	return mess
}
