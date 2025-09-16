package connection

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/command"
)

// ConnectionPoolError 连接池错误
type ConnectionPoolError struct {
	Code    int
	Message string
	Err     error
}

// Error 实现error接口
func (e *ConnectionPoolError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ConnectionPoolError[code=%d, message=%s]: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("ConnectionPoolError[code=%d, message=%s]", e.Code, e.Message)
}

// Unwrap 返回底层错误
func (e *ConnectionPoolError) Unwrap() error {
	return e.Err
}

// Connection pool error codes
const (
	ErrCodePoolClosed        = 3001
	ErrCodePoolFull          = 3002
	ErrCodeConnectionFailed  = 3003
	ErrCodeInvalidAddress    = 3004
	ErrCodeConnectionTimeout = 3005
	ErrCodePoolConfigInvalid = 3006
	ErrCodeInvalidResponse   = 3007
	ErrCodeEncodeFailed      = 3008
	ErrCodeDecodeFailed      = 3009
)

// NewConnectionPoolError 创建连接池错误
func NewConnectionPoolError(code int, message string, err error) *ConnectionPoolError {
	return &ConnectionPoolError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// ConnectionPool 连接池
type ConnectionPool struct {
	connections    sync.Map // map[string]*command.Connection
	maxConnections int32
	currentCount   int32
	maxIdleTime    time.Duration
	connectTimeout time.Duration
	requestTimeout time.Duration
	closed         int32
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup

	// 错误处理配置
	errorConfig *ConnectionPoolErrorConfig

	// 指标收集
	metrics *ConnectionPoolMetrics

	// 熔断器映射
	circuitBreakers sync.Map // map[string]*CircuitBreaker

	// 连接使用计数
	connectionUseCount sync.Map // map[string]*int64

	// 配置
	config *ConnectionPoolConfig
}

// ConnectionPoolErrorConfig 连接池错误处理配置
type ConnectionPoolErrorConfig struct {
	MaxRetryAttempts        int           // 最大重试次数
	RetryInterval           time.Duration // 重试间隔
	EnableCircuitBreaker    bool          // 是否启用熔断器
	CircuitBreakerThreshold int           // 熔断器阈值
	CircuitBreakerTimeout   time.Duration // 熔断器超时时间
}

// DefaultConnectionPoolErrorConfig 默认连接池错误处理配置
var DefaultConnectionPoolErrorConfig = &ConnectionPoolErrorConfig{
	MaxRetryAttempts:        3,
	RetryInterval:           1 * time.Second,
	EnableCircuitBreaker:    true,
	CircuitBreakerThreshold: 5,
	CircuitBreakerTimeout:   30 * time.Second,
}

// ConnectionPoolMetrics 连接池指标
type ConnectionPoolMetrics struct {
	TotalConnections   int64         // 总连接数
	ActiveConnections  int64         // 活跃连接数
	FailedConnections  int64         // 失败连接数
	TimeoutConnections int64         // 超时连接数
	RetryConnections   int64         // 重试连接数
	AvgConnectionTime  time.Duration // 平均连接时间
	ErrorCountByCode   map[int]int64 // 按错误代码统计
	mutex              sync.RWMutex
}

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	MaxConnections int32         // 最大连接数
	MaxIdleTime    time.Duration // 最大空闲时间
	ConnectTimeout time.Duration // 连接超时
	RequestTimeout time.Duration // 请求超时
	// TLS配置
	TLSConfig *TLSConfig // TLS配置
}

// TLSConfig TLS配置
type TLSConfig struct {
	EnableTLS  bool   // 是否启用TLS
	CertFile   string // 证书文件路径
	KeyFile    string // 私钥文件路径
	CAFile     string // CA证书文件路径
	ServerName string // 服务器名称
	SkipVerify bool   // 是否跳过证书验证
}

// DefaultConnectionPoolConfig 默认连接池配置
func DefaultConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		MaxConnections: 100,
		MaxIdleTime:    5 * time.Minute,
		ConnectTimeout: 3 * time.Second,
		RequestTimeout: 30 * time.Second,
		TLSConfig:      &TLSConfig{EnableTLS: false}, // 默认不启用TLS
	}
}

// NewConnectionPool 创建连接池
func NewConnectionPool(config *ConnectionPoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}

	// 验证配置
	if config.MaxConnections <= 0 {
		// 使用默认值而不是返回错误，因为这是构造函数
		config.MaxConnections = 100
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 3 * time.Second
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnectionPool{
		// client is managed by the connection pool
		maxConnections: config.MaxConnections,
		maxIdleTime:    config.MaxIdleTime,
		connectTimeout: config.ConnectTimeout,
		requestTimeout: config.RequestTimeout,
		ctx:            ctx,
		cancel:         cancel,
		// 初始化错误处理配置
		errorConfig: DefaultConnectionPoolErrorConfig,
		// 初始化指标
		metrics: &ConnectionPoolMetrics{
			ErrorCountByCode: make(map[int]int64),
		},
		// 配置
		config: config,
	}

	// 启动清理goroutine
	pool.wg.Add(1)
	go pool.cleanupRoutine()

	return pool
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(addr string) (*command.Connection, error) {
	if atomic.LoadInt32(&cp.closed) == 1 {
		return nil, NewConnectionPoolError(ErrCodePoolClosed, "connection pool is closed", nil)
	}

	// 验证地址
	if addr == "" {
		return nil, NewConnectionPoolError(ErrCodeInvalidAddress, "invalid address", nil)
	}

	startTime := time.Now()
	defer func() {
		cp.metrics.updateConnectionTime(time.Since(startTime))
		cp.metrics.incrementTotalConnections()
	}()

	// 检查熔断器
	if cp.errorConfig.EnableCircuitBreaker {
		cb := cp.getCircuitBreaker(addr)
		if !cb.AllowRequest() {
			cp.metrics.incrementFailedConnections()
			cp.metrics.incrementError(ErrCodeConnectionFailed)
			return nil, NewConnectionPoolError(ErrCodeConnectionFailed, fmt.Sprintf("circuit breaker is open for %s", addr), nil)
		}

		// 重试机制
		var lastErr error
		for attempt := 0; attempt <= cp.errorConfig.MaxRetryAttempts; attempt++ {
			if attempt > 0 {
				// 记录重试
				cp.metrics.incrementRetryConnections()
				time.Sleep(cp.errorConfig.RetryInterval)
			}

			conn, err := cp.getConnectionWithoutCircuitBreaker(addr)
			if err == nil {
				// 请求成功，更新熔断器
				cb.OnSuccess()
				return conn, nil
			}

			lastErr = err
			// 请求失败，更新熔断器
			cb.OnFailure()

			// 检查是否应该重试
			if !cp.shouldRetry(err, attempt) {
				break
			}
		}

		cp.metrics.incrementFailedConnections()
		return nil, lastErr
	} else {
		// 不启用熔断器的重试机制
		var lastErr error
		for attempt := 0; attempt <= cp.errorConfig.MaxRetryAttempts; attempt++ {
			if attempt > 0 {
				// 记录重试
				cp.metrics.incrementRetryConnections()
				time.Sleep(cp.errorConfig.RetryInterval)
			}

			conn, err := cp.getConnectionWithoutCircuitBreaker(addr)
			if err == nil {
				return conn, nil
			}

			lastErr = err

			// 检查是否应该重试
			if !cp.shouldRetry(err, attempt) {
				break
			}
		}

		cp.metrics.incrementFailedConnections()
		return nil, lastErr
	}
}

// getConnectionWithoutCircuitBreaker 不带熔断器的获取连接
func (cp *ConnectionPool) getConnectionWithoutCircuitBreaker(addr string) (*command.Connection, error) {
	// 尝试获取现有连接
	if connValue, exists := cp.connections.Load(addr); exists {
		conn := connValue.(*command.Connection)
		conn.Mutex.Lock()
		defer conn.Mutex.Unlock()

		if !conn.Closed {
			conn.LastUsed = time.Now()
			// 增加使用计数
			if countValue, ok := cp.connectionUseCount.Load(addr); ok {
				count := countValue.(*int64)
				atomic.AddInt64(count, 1)
			} else {
				count := int64(1)
				cp.connectionUseCount.Store(addr, &count)
			}
			return conn, nil
		}
	}

	// 检查连接数限制
	if atomic.LoadInt32(&cp.currentCount) >= cp.config.MaxConnections {
		return nil, NewConnectionPoolError(ErrCodePoolFull, "connection pool is full", nil)
	}

	// 创建新连接
	return cp.createConnection(addr)
}

// createConnection 创建新连接
func (cp *ConnectionPool) createConnection(addr string) (*command.Connection, error) {
	var conn net.Conn
	var err error

	// Check if TLS is enabled
	if cp.config.TLSConfig != nil && cp.config.TLSConfig.EnableTLS {
		// Create TLS connection
		conn, err = cp.createTLSConnection(addr)
	} else {
		// Create regular TCP connection
		conn, err = net.DialTimeout("tcp", addr, cp.config.ConnectTimeout)
	}

	if err != nil {
		cp.metrics.incrementFailedConnections()
		cp.metrics.incrementError(ErrCodeConnectionFailed)
		return nil, NewConnectionPoolError(ErrCodeConnectionFailed, fmt.Sprintf("failed to connect to %s", addr), err)
	}

	// Create connection wrapper
	connection := &command.Connection{
		Addr:       addr,
		Conn:       conn,
		Reader:     bufio.NewReader(conn),
		Writer:     bufio.NewWriter(conn),
		LastUsed:   time.Now(),
		RemoteAddr: conn.RemoteAddr().String(),
	}

	// Store connection
	cp.connections.Store(addr, connection)
	atomic.AddInt32(&cp.currentCount, 1)

	// 初始化使用计数
	count := int64(1)
	cp.connectionUseCount.Store(addr, &count)

	return connection, nil
}

// createTLSConnection 创建TLS连接
func (cp *ConnectionPool) createTLSConnection(addr string) (net.Conn, error) {
	// 解析地址
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	// 创建TCP连接
	tcpConn, err := net.DialTimeout("tcp", addr, cp.config.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	// 配置TLS
	tlsConfig := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: cp.config.TLSConfig.SkipVerify,
	}

	// 如果提供了证书文件，加载证书
	if cp.config.TLSConfig.CertFile != "" && cp.config.TLSConfig.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cp.config.TLSConfig.CertFile, cp.config.TLSConfig.KeyFile)
		if err != nil {
			tcpConn.Close()
			return nil, fmt.Errorf("failed to load TLS certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// 如果提供了CA文件，加载CA证书
	if cp.config.TLSConfig.CAFile != "" {
		caCert, err := ioutil.ReadFile(cp.config.TLSConfig.CAFile)
		if err != nil {
			tcpConn.Close()
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	// 如果指定了服务器名称，使用它
	if cp.config.TLSConfig.ServerName != "" {
		tlsConfig.ServerName = cp.config.TLSConfig.ServerName
	}

	// 创建TLS连接
	tlsConn := tls.Client(tcpConn, tlsConfig)

	// 设置握手超时
	errChan := make(chan error, 1)
	go func() {
		errChan <- tlsConn.Handshake()
	}()

	select {
	case err := <-errChan:
		if err != nil {
			tcpConn.Close()
			return nil, fmt.Errorf("TLS handshake failed: %v", err)
		}
	case <-time.After(cp.config.ConnectTimeout):
		tcpConn.Close()
		return nil, fmt.Errorf("TLS handshake timeout")
	}

	return tlsConn, nil
}

// ReturnConnection 归还连接
func (cp *ConnectionPool) ReturnConnection(conn *command.Connection) {
	// Implementation for returning connections to the pool
	// For now, we'll just update the last used time
	if conn != nil {
		conn.Mutex.Lock()
		conn.LastUsed = time.Now()
		conn.Mutex.Unlock()
	}
}

// Close 关闭连接池
func (cp *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&cp.closed, 0, 1) {
		return NewConnectionPoolError(ErrCodePoolClosed, "connection pool is already closed", nil)
	}

	cp.cancel()

	// Close all connections
	cp.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*command.Connection); ok {
			conn.Mutex.Lock()
			if !conn.Closed {
				conn.Closed = true
				if conn.Conn != nil {
					conn.Conn.Close()
				}
			}
			conn.Mutex.Unlock()
		}
		cp.connections.Delete(key)
		return true
	})

	// 等待所有goroutine结束
	cp.wg.Wait()

	return nil
}

// cleanupRoutine 清理例程
func (cp *ConnectionPool) cleanupRoutine() {
	defer cp.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.cleanupConnections()
		case <-cp.ctx.Done():
			return
		}
	}
}

// cleanupConnections 清理过期连接
func (cp *ConnectionPool) cleanupConnections() {
	now := time.Now()
	cp.connections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		conn := value.(*command.Connection)
		conn.Mutex.RLock()
		lastUsed := conn.LastUsed
		closed := conn.Closed
		conn.Mutex.RUnlock()

		if closed || now.Sub(lastUsed) > cp.maxIdleTime {
			cp.connections.Delete(key)
			// Close the connection
			conn.Mutex.Lock()
			if !conn.Closed {
				conn.Closed = true
				if conn.Conn != nil {
					conn.Conn.Close()
				}
			}
			conn.Mutex.Unlock()
			atomic.AddInt32(&cp.currentCount, -1)
			// Remove use count
			cp.connectionUseCount.Delete(addr)
		}
		return true
	})
}

// getCircuitBreaker 获取熔断器
func (cp *ConnectionPool) getCircuitBreaker(addr string) *CircuitBreaker {
	if cbValue, exists := cp.circuitBreakers.Load(addr); exists {
		return cbValue.(*CircuitBreaker)
	}

	cb := NewCircuitBreaker(cp.errorConfig.CircuitBreakerThreshold, cp.errorConfig.CircuitBreakerTimeout)
	cp.circuitBreakers.Store(addr, cb)
	return cb
}

// shouldRetry 检查是否应该重试
func (cp *ConnectionPool) shouldRetry(err error, attempt int) bool {
	if attempt >= cp.errorConfig.MaxRetryAttempts {
		return false
	}

	// 检查错误类型
	if connErr, ok := err.(*ConnectionPoolError); ok {
		// 某些错误不应该重试
		switch connErr.Code {
		case ErrCodeInvalidAddress, ErrCodePoolClosed, ErrCodePoolFull:
			return false
		case ErrCodeConnectionTimeout:
			return true
		}
	}

	// 对于其他错误，默认重试
	return true
}

// IsConnected 检查是否连接到指定地址
func (cp *ConnectionPool) IsConnected(addr string) bool {
	if connValue, exists := cp.connections.Load(addr); exists {
		conn := connValue.(*command.Connection)
		conn.Mutex.RLock()
		closed := conn.Closed
		conn.Mutex.RUnlock()
		return !closed
	}
	return false
}

// Connection pool metrics methods
func (cm *ConnectionPoolMetrics) incrementTotalConnections() {
	cm.mutex.Lock()
	cm.TotalConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementActiveConnections() {
	cm.mutex.Lock()
	cm.ActiveConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) decrementActiveConnections() {
	cm.mutex.Lock()
	cm.ActiveConnections--
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementFailedConnections() {
	cm.mutex.Lock()
	cm.FailedConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementTimeoutConnections() {
	cm.mutex.Lock()
	cm.TimeoutConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementRetryConnections() {
	cm.mutex.Lock()
	cm.RetryConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) updateConnectionTime(latency time.Duration) {
	cm.mutex.Lock()
	if cm.AvgConnectionTime == 0 {
		cm.AvgConnectionTime = latency
	} else {
		cm.AvgConnectionTime = (cm.AvgConnectionTime + latency) / 2
	}
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementError(code int) {
	cm.mutex.Lock()
	cm.ErrorCountByCode[code]++
	cm.mutex.Unlock()
}

// GetStats 获取连接池统计信息
func (cp *ConnectionPool) GetStats() map[string]interface{} {
	cp.metrics.mutex.RLock()
	defer cp.metrics.mutex.RUnlock()

	return map[string]interface{}{
		"total_connections":   cp.metrics.TotalConnections,
		"active_connections":  cp.metrics.ActiveConnections,
		"failed_connections":  cp.metrics.FailedConnections,
		"timeout_connections": cp.metrics.TimeoutConnections,
		"retry_connections":   cp.metrics.RetryConnections,
		"avg_connection_time": cp.metrics.AvgConnectionTime.Milliseconds(),
		"error_count_by_code": cp.metrics.ErrorCountByCode,
	}
}

// CircuitBreaker 熔断器
type CircuitBreaker struct {
	failures    int32
	lastFailure time.Time
	open        bool
	mutex       sync.RWMutex
	threshold   int
	timeout     time.Duration
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// AllowRequest 检查是否允许请求
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	if !cb.open {
		return true
	}

	// 检查是否应该半开
	if time.Since(cb.lastFailure) > cb.timeout {
		cb.mutex.Lock()
		cb.open = false
		cb.mutex.Unlock()
		return true
	}

	return false
}

// OnSuccess 记录成功
func (cb *CircuitBreaker) OnSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures = 0
	cb.open = false
}

// OnFailure 记录失败
func (cb *CircuitBreaker) OnFailure() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if int(cb.failures) >= cb.threshold {
		cb.open = true
	}
}
