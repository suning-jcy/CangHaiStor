package tcp

import (
	"errors"
	"net"
	"sync"
	"time"

	"code.suning.com/glog"
)

var ErrClientIsNotFree = errors.New("client is not free")

var networktype = "tcp"

type Client struct {
	Addr           string
	Optimeout      time.Duration
	Clientconn     *Conn
	LastAccessTime time.Time
	sync.RWMutex
}

func NewClient(addr string, optimeout time.Duration) (cli *Client, err error) {
	if optimeout <= 0 {
		return nil, errors.New("bad op-timeout")
	}

	localAddr, err := net.ResolveTCPAddr(networktype, net.JoinHostPort(addr, "0"))
	d := &net.Dialer{
		LocalAddr: localAddr,
		Timeout:   optimeout,
	}
	var conn net.Conn
	conn, err = d.Dial(networktype, addr)
	if err != nil {
		glog.V(0).Infof("%s", err.Error())
		return nil, err
	}
	cli = &Client{
		Addr:      addr,
		Optimeout: optimeout,
	}
	c := NewConn(conn)
	cli.Clientconn = c

	return
}

//Send send data and get respone
//可以补充一些读写超时
func (cli *Client) sendAndRec(mess *Message) (*Message, error) {

	var err error
	err = cli.Clientconn.WriteMessage(mess)
	if err != nil {
		glog.V(0).Infoln("sendAndRec err:%s", err.Error())
		return nil, err
	}
	return cli.Clientconn.ReceiveMess()

}

func (cli *Client) ReConnect() bool {
	cli.Lock()
	defer cli.Unlock()

	tempfunc := func() (net.Conn, error) {
		localAddr, _ := net.ResolveTCPAddr(networktype, net.JoinHostPort(cli.Addr, "0"))
		d := &net.Dialer{
			LocalAddr: localAddr,
			Timeout:   cli.Optimeout,
		}
		return d.Dial(networktype, cli.Addr)
	}

	return cli.Clientconn.RedialForClient(cli.Clientconn.Conn, tempfunc)
}

func (cli *Client) Close() {
	cli.Lock()
	defer cli.Unlock()
	glog.V(0).Infoln("close client", cli.Addr)
	cli.Clientconn.Close()
}

func (cli *Client) IsClosed() bool {
	return cli.Clientconn.IsClosed()
}

func (cli *Client) IsTimeOut(waitTimeOut time.Duration) bool {
	return cli.Clientconn.IsTimeOut(waitTimeOut)
}

//注意！！！！！，操作完body里面是有东西的，请把res.body读完之后再进行下一次操作!!!!!
//可以补充一些读写超时
func (cli *Client) Do(req Request) (res Response, err error) {
	cli.Clientconn.SetOpTimeOut(cli.Optimeout)
	mess := GetMess(req)
	defer ReleaseMessage(mess)
	resMess, err := cli.sendAndRec(mess)

	defer ReleaseMessage(resMess)
	if err != nil {
		cli.Clientconn.SetOpTimeOut(0)
		return nil, err
	}
	res = resMess.Meta
	if resMess.Meta.GetBodyLen() == 0 {
		cli.Clientconn.SetOpTimeOut(0)
	} else {
		resMess.Body.setReadAllCallBack(func() {
			cli.Clientconn.SetOpTimeOut(0)
		})
		res.SetBody(resMess.Body)
	}

	return
}
