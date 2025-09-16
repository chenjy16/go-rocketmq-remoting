package tls

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"
)

// generateTestCertificate 生成测试证书
func generateTestCertificate() (certFile, keyFile string, err error) {
	// 生成私钥
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// 创建证书模板
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// 生成自签名证书
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return "", "", err
	}

	// 写入证书文件
	certFile = "test_cert.pem"
	certOut, err := os.Create(certFile)
	if err != nil {
		return "", "", err
	}
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		return "", "", err
	}

	// 写入私钥文件
	keyFile = "test_key.pem"
	keyOut, err := os.Create(keyFile)
	if err != nil {
		return "", "", err
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", err
	}

	err = pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	if err != nil {
		return "", "", err
	}

	return certFile, keyFile, nil
}

// TestTLSServerAndClient 测试TLS服务器和客户端
func TestTLSServerAndClient(t *testing.T) {
	// 生成测试证书
	certFile, keyFile, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate test certificate: %v", err)
	}
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// 创建TLS服务器
	server, err := NewTLSServer("127.0.0.1:0", certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create TLS server: %v", err)
	}

	// 启动服务器
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start TLS server: %v", err)
	}
	defer server.Stop()

	// 获取服务器地址
	addr := server.listener.Addr().String()

	// 创建TLS客户端
	client, err := NewTLSClient(certFile, keyFile, "", true) // 跳过验证用于测试
	if err != nil {
		t.Fatalf("Failed to create TLS client: %v", err)
	}

	// 连接到服务器
	conn, err := client.Connect(addr)
	if err != nil {
		t.Fatalf("Failed to connect to TLS server: %v", err)
	}
	defer conn.Close()

	// 发送测试数据
	testData := []byte("Hello, TLS!")
	response, err := client.SendAndReceive(conn, testData)
	if err != nil {
		t.Fatalf("Failed to send and receive data: %v", err)
	}

	// 验证响应
	if !bytes.Equal(response, testData) {
		t.Errorf("Expected response %s, got %s", testData, response)
	}

	// 验证服务器正在运行
	if !server.IsRunning() {
		t.Error("Server should be running")
	}
}

// TestNewTLSServerWithInvalidCertificate 测试使用无效证书创建TLS服务器
func TestNewTLSServerWithInvalidCertificate(t *testing.T) {
	// 尝试使用不存在的证书文件创建服务器
	_, err := NewTLSServer("127.0.0.1:8443", "nonexistent_cert.pem", "nonexistent_key.pem")
	if err == nil {
		t.Error("Expected error when creating server with invalid certificate")
	}
}

// TestNewTLSClientWithInvalidCertificate 测试使用无效证书创建TLS客户端
func TestNewTLSClientWithInvalidCertificate(t *testing.T) {
	// 尝试使用不存在的证书文件创建客户端
	_, err := NewTLSClient("nonexistent_cert.pem", "nonexistent_key.pem", "", false)
	if err == nil {
		t.Error("Expected error when creating client with invalid certificate")
	}
}
