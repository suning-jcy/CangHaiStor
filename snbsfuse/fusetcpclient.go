package main

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"code.suning.com/glog"
	"code.suning.com/snbsfuse/tcp"
)

type GwTcpClient struct {
	tcp.Client
}

var HeaderStructSize = int(unsafe.Sizeof(tcp.MsgHead{}))

const gatewayrsperr = -12

var flagfuse = [4]byte{byte(1), 0, 0, 0}

func getData(m *tcp.MsgHead) []byte {
	var x reflect.SliceHeader
	x.Len = HeaderStructSize
	x.Cap = HeaderStructSize
	x.Data = uintptr(unsafe.Pointer(m))
	return *(*[]byte)(unsafe.Pointer(&x))
}

func StringToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{sh.Data, sh.Len, 0}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func NewClient(addr string, optimeout time.Duration) (cli *GwTcpClient, err error) {
	tempcli, err := tcp.NewClient(addr, optimeout)
	if err != nil {
		return nil, err
	}
	cli = &GwTcpClient{}
	cli.Client = *tempcli
	glog.V(0).Infoln("get newclinet", addr)
	return cli, nil
}

func (cli *GwTcpClient) ReceiveMsgHead() (*tcp.MsgHead, error) {
	head := &tcp.MsgHead{}
	tempbuf := getData(head)
	_, err := io.ReadFull(cli.Clientconn.ControlReader, tempbuf)
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (cli *GwTcpClient) ReceiveMsgBody(data []byte) error {
	_, err := io.ReadFull(cli.Clientconn.ControlReader, data)
	if err != nil {
		return err
	}
	return nil
}

func (cli *GwTcpClient) MgSend(req *tcp.MsgHead) error {
	var err error
	tempbuf := getData(req)
	_, err = cli.Clientconn.ControlWriter.Write(tempbuf)
	cli.Clientconn.ControlWriter.Flush()
	return err
}

func (cli *GwTcpClient) ReadSend(req *tcp.MsgHead) error {
	var err error

	tempbuf := getData(req)
	_, err = cli.Clientconn.ControlWriter.Write(tempbuf)
	cli.Clientconn.ControlWriter.Flush()
	return err
}

func (cli *GwTcpClient) WriteSend(req *tcp.MsgHead, data []byte) error {
	var err error

	tempbuf := getData(req)
	_, err = cli.Clientconn.ControlWriter.Write(tempbuf)
	if err != nil {
		glog.V(0).Infoln("ControlWriter error push body:", err)
		return err
	}
	_, err = cli.Clientconn.ControlWriter.Write(data)
	if err != nil {
		glog.V(0).Infoln("WriteSend error push body:", err, len(data))
	}
	cli.Clientconn.ControlWriter.Flush()
	return err
}
func (cli *GwTcpClient) Read(devName string, length int64, offset uint64, data []byte) int32 {
	cli.Lock()
	defer cli.Unlock()

	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	defer cli.Clientconn.SetOpTimeOut(0)

	req := &tcp.MsgHead{
		MessType:     tcp.GwReadReq,
		Offset:       offset,
		WriteDataLen: 0,
		ReadDataLen:  length,
		Flag:         flagfuse,
	}
	req.Flag[0] = 1
	copy(req.Name[:len(devName)], StringToBytes(devName))
	err := cli.ReadSend(req)
	if err != nil {
		glog.V(4).Infof("ReadSend err:%s", err.Error())
		return -1
	}

	head, err1 := cli.ReceiveMsgHead()
	if err1 != nil {
		glog.V(4).Infof("ReceiveMsgHead err:%s,devName: %s,length:%d,offset:%d  ", err1.Error(), devName, length, offset)
		if strings.Contains(err1.Error(), "broken pipe") {
			if cli.ReConnect() {
				head, err1 = cli.ReceiveMsgHead()
				if err1 != nil {
					glog.V(4).Infof("ReceiveMsgBody again err:%s", err.Error())
					return -1
				}
			} else {
				return -1
			}
		} else {
			return -1
		}
	}
	if head == nil || head.Result != 0 {
		glog.V(4).Infof("Result devName: %s,length:%d,offset:%d  ", devName, length, offset)
		return -1
	}
	err = cli.ReceiveMsgBody(data)
	if err != nil {
		glog.V(0).Infof("ReceiveMsgBody err:%s,devName: %s,length:%d,offset:%d  ", err1.Error(), devName, length, offset)
		//if client bloken and try again once
		if strings.Contains(err.Error(), "broken pipe") {
			if cli.ReConnect() {
				err := cli.ReceiveMsgBody(data)
				if err != nil {
					glog.V(4).Infof("ReceiveMsgBody again err:%s", err.Error())
					return -1
				}
			} else {
				return -1
			}
		} else {
			return -1
		}
	}
	return 0
}

func (cli *GwTcpClient) Write(devName string, length int64, offset uint64, data []byte) int64 {
	cli.Lock()
	defer cli.Unlock()
	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	defer cli.Clientconn.SetOpTimeOut(0)
	glog.V(4).Infof("WriteSend start")
	req := &tcp.MsgHead{
		MessType:     tcp.GwWriteReq,
		Offset:       offset,
		WriteDataLen: length,
		ReadDataLen:  0,
		Flag:         flagfuse,
	}
	req.Flag[0] = 1
	copy(req.Name[:len(devName)], StringToBytes(devName))
	err := cli.WriteSend(req, data)
	if err != nil {
		glog.V(4).Infof("WriteSend err:%s", err.Error())
		//if client bloken and try again once
		if strings.Contains(err.Error(), "broken pipe") {
			if cli.ReConnect() {
				err := cli.WriteSend(req, data)
				if err != nil {
					glog.V(4).Infof("WriteSend again err:%s", err.Error())
					return -1
				}
			} else {
				return -1
			}
		} else {
			return -1
		}
	}

	head, _ := cli.ReceiveMsgHead()

	if head == nil {
		glog.V(4).Infof("head == nil")
		return gatewayrsperr
	}
	return head.Result
}

func (cli *GwTcpClient) Open(devName string) (int64, error) {
	cli.Lock()
	defer cli.Unlock()
	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	defer cli.Clientconn.SetOpTimeOut(0)
	glog.V(0).Infof("start open:%s", devName)
	req := &tcp.MsgHead{
		MessType: tcp.GwQueryReq,
		Flag:     flagfuse,
	}
	req.Flag[0] = 1
	copy(req.Name[:len(devName)], StringToBytes(devName))
	err := cli.MgSend(req)
	if err != nil {
		glog.V(0).Infof("MgSend err:%s", err.Error())
		return -1, errors.New("Open error")
	}

	head, err1 := cli.ReceiveMsgHead()
	if err1 != nil {
		glog.V(0).Infof("ReceiveMsgHead err:%s", err1.Error())
		return -1, errors.New("Open error")
	}
	if head == nil {
		glog.V(4).Infof("head == nil")
		return -1, errors.New("Open error")
	}

	return head.Result, nil
}

func (cli *GwTcpClient) Query(devName string) (int64, error) {
	cli.Lock()
	defer cli.Unlock()
	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	defer cli.Clientconn.SetOpTimeOut(0)
	req := &tcp.MsgHead{
		MessType: tcp.GwQueryReq,
		Flag:     flagfuse,
	}
	req.Flag[0] = 1
	copy(req.Name[:len(devName)], StringToBytes(devName))
	err := cli.MgSend(req)
	if err != nil {
		glog.V(0).Infof("MgSend err:%s", err.Error())
		return -1, errors.New("Open error")
	}

	head, err1 := cli.ReceiveMsgHead()
	if err1 != nil {
		glog.V(0).Infof("ReceiveMsgHead err:%s", err1.Error())
		return -1, errors.New("Open error")
	}
	return head.Result, nil
}

func (cli *GwTcpClient) Delete(devName string) int64 {
	cli.Lock()
	defer cli.Unlock()
	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	defer cli.Clientconn.SetOpTimeOut(0)
	glog.V(0).Infof("start delete:%s", devName)
	req := &tcp.MsgHead{
		MessType: tcp.GwRemoveReq,
		Flag:     flagfuse,
	}
	req.Flag[0] = 1
	copy(req.Name[:len(devName)], StringToBytes(devName))
	err := cli.MgSend(req)
	if err != nil {
		glog.V(0).Infof("MgSend err:%s", err.Error())
		return -1
	}

	head, err1 := cli.ReceiveMsgHead()
	if err1 != nil {
		glog.V(0).Infof("ReceiveMsgHead err:%s ", err1.Error())
		return -1
	}
	if head == nil {
		glog.V(4).Infof("head == nil")
		return -1
	}
	return head.Result
}
