package tls

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
)

// TLSServer TLS服务器
type TLSServer struct {
	addr     string
	config   *tls.Config
	listener net.Listener
	running  bool
	mutex    sync.RWMutex
}

// NewTLSServer 创建TLS服务器
func NewTLSServer(addr string, certFile, keyFile string) (*TLSServer, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %v", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	return &TLSServer{
		addr:   addr,
		config: config,
	}, nil
}

// Start 启动TLS服务器
func (s *TLSServer) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.running {
		return fmt.Errorf("server is already running")
	}

	listener, err := tls.Listen("tcp", s.addr, s.config)
	if err != nil {
		return fmt.Errorf("failed to start TLS listener: %v", err)
	}

	s.listener = listener
	s.running = true

	// 启动接受连接的goroutine
	go s.acceptConnections()

	return nil
}

// Stop 停止TLS服务器
func (s *TLSServer) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return fmt.Errorf("server is not running")
	}

	s.running = false
	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

// acceptConnections 接受连接
func (s *TLSServer) acceptConnections() {
	for {
		s.mutex.RLock()
		running := s.running
		s.mutex.RUnlock()

		if !running {
			break
		}

		conn, err := s.listener.Accept()
		if err != nil {
			s.mutex.RLock()
			running := s.running
			s.mutex.RUnlock()

			if !running {
				break
			}
			continue
		}

		// 处理连接
		go s.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (s *TLSServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// 这里可以实现具体的业务逻辑
	// 例如读取数据、处理请求等
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}

		// 回显数据
		_, err = conn.Write(buf[:n])
		if err != nil {
			break
		}
	}
}

// IsRunning 检查服务器是否正在运行
func (s *TLSServer) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.running
}
