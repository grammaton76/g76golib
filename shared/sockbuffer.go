package shared // import "github.com/grammaton76/g76golib/shared"

import (
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type SocketFunction struct {
	SockPath  string
	Buffer    string
	ServeFunc func() string
	Updated   time.Time
}

func (Sock *SocketFunction) process(c net.Conn) {
	log.Printf("Local socket reading %s [%s]", Sock.SockPath, c.RemoteAddr().Network())
	var Return string
	if Sock.ServeFunc != nil {
		Return += Sock.ServeFunc()
	}
	Return += Sock.Buffer
	r := strings.NewReader(Return)
	io.Copy(c, r)
	c.Close()
}

func (Sock *SocketFunction) SetBuffer(Buffer string) {
	Sock.Buffer = Buffer
}

func (Sock *SocketFunction) SetHandler(Func func() string) {
	Sock.ServeFunc = Func
}

func (Sock *SocketFunction) serveforever() {
	listener, err := net.Listen("unix", Sock.SockPath)
	if err != nil {
		log.Fatalf("listen error on '%s':", Sock.SockPath, err)
	}
	defer listener.Close()
	for {
		// Accept new connections, dispatching them to RelayServer
		// in a goroutine.
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("accept error on %s: %s", Sock.SockPath, err)
		}
		go Sock.process(conn)
	}
}

func NewSocketFunction(Path string) *SocketFunction {
	var Sock SocketFunction
	if err := os.RemoveAll(Path); err != nil {
		log.Fatalf("couldn't clear path for socket '%s': %s\n", Path, err)
	}
	Sock.SockPath = Path
	go Sock.serveforever()
	return &Sock
}

func (Sock SocketFunction) ServeAsSocket(Path string) {
	if err := os.RemoveAll(Path); err != nil {
		log.Fatalf("couldn't clear path for socket '%s': %s\n", Path, err)
	}
	Sock.SockPath = Path
	listener, err := net.Listen("unix", Path)
	if err != nil {
		log.Fatalf("listen error on '%s':", Path, err)
	}
	defer listener.Close()
	for {
		// Accept new connections, dispatching them to RelayServer
		// in a goroutine.
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("accept error:", err)
		}
		go Sock.process(conn)
	}
}
