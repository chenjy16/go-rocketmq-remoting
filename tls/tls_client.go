package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
)

// TLSClient TLS客户端
type TLSClient struct {
	config *tls.Config
}

// NewTLSClient 创建TLS客户端
func NewTLSClient(certFile, keyFile, caFile string, skipVerify bool) (*TLSClient, error) {
	config := &tls.Config{
		InsecureSkipVerify: skipVerify,
	}

	// 如果提供了证书文件，加载证书
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate: %v", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	// 如果提供了CA文件，加载CA证书
	if caFile != "" {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.RootCAs = caCertPool
	}

	return &TLSClient{
		config: config,
	}, nil
}

// Connect 连接到TLS服务器
func (c *TLSClient) Connect(addr string) (net.Conn, error) {
	// 创建TCP连接
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP connection: %v", err)
	}

	// 创建TLS连接
	tlsConn := tls.Client(tcpConn, c.config)

	// 执行握手
	err = tlsConn.Handshake()
	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf("TLS handshake failed: %v", err)
	}

	return tlsConn, nil
}

// SendAndReceive 发送数据并接收响应
func (c *TLSClient) SendAndReceive(conn net.Conn, data []byte) ([]byte, error) {
	// 发送数据
	_, err := conn.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to send data: %v", err)
	}

	// 接收响应
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to receive data: %v", err)
	}

	return buf[:n], nil
}
