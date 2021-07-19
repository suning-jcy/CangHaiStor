package tcp

import (
	"fmt"
	"net"

	"code.suning.com/glog"
)

/*
弄这套DT的server/client主要目的：减少不必要的malloc，减少gc。使用tcp这种方式不是主要目的。
使用方式：
1 请定义自己的request和response，实现 Request 和 Response 接口（必要或者不必要实现的方法 里面注释写了）
2 请完善下GetMessMeta方法，把你的req和resp的构造方法加一下
3 启动server前请RegisterHandler处理方法
4 client请使用Do方法  ，其他的外层假如需要自己封装
5 假如看不惯，随意修改，修改之后麻烦go test -v code.suning.com/message/proto/tcp/chunk/
*/

type HandlerFunc func(mess Request) (Response, error)

type HandlerMap map[MessType]HandlerFunc

// Server is the root of your application. You should instantiate one
// Always use the NewServer() method to create a new Server.
type Server struct {
	port     int
	publicIP string
	ListenTo string
	Listener net.Listener
	stopFlag bool

	Handlers HandlerMap
}

// NewConn constructs a new object that will handle the  protocol over
// an active net.TCPConn. The TCP connection should already be open before
// it is handed to this functions.
func (server *Server) newConn(tcpConn net.Conn) *Conn {
	c := NewConn(tcpConn)
	c.Server = server
	return c
}

// If the server fails to start for any reason, an error will be returned. Common
// errors are trying to bind to a privileged port or something else is already
// listening on the same port.
func (server *Server) Serve(lis net.Listener) error {
	var servererr error
	glog.V(4).Info(fmt.Sprintf("%s listening on %d", server.publicIP, server.port))
	server.Listener = lis
	for {
		if server.stopFlag {
			break
		}
		tcpConn, err := server.Listener.Accept()
		if err != nil {
			glog.V(4).Info(fmt.Sprintf("listening error: %v", err))
			servererr = err
			break
		}
		glog.V(4).Info(fmt.Sprintf("get new conn from %s ", tcpConn.RemoteAddr()))

		newConn := server.newConn(tcpConn)
		go newConn.Serve()
	}
	return servererr
}

//Shutdown  stops a server. Already connected clients will retain their connections
func (server *Server) Shutdown() error {
	server.stopFlag = true
	if server.Listener != nil {
		return server.Listener.Close()
	}
	// server wasnt even started
	return nil
}

//ShutdownGracefully Gracefully stop
func (server *Server) GracefulStop() {
	server.stopFlag = true
	//todo  stop cient-conn
	if server.Listener != nil {
		server.Listener.Close()
	}
	// server wasnt even started
	return
}

//需要处理那些消息
//mType 指定的消息类型
//dealFunc 处理方法
//请在Serve 动作之前做
func (server *Server) RegisterHandler(mType MessType, dealFunc HandlerFunc) {
	server.Handlers[mType] = dealFunc
}
