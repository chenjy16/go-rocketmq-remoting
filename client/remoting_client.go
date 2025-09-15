package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/command"
	"github.com/chenjy16/go-rocketmq-remoting/connection"
	"github.com/chenjy16/go-rocketmq-remoting/errors"
)

// ErrorHandlingConfig 错误处理配置
type ErrorHandlingConfig struct {
	MaxRetryAttempts        int           // 最大重试次数
	RetryInterval           time.Duration // 重试间隔
	EnableCircuitBreaker    bool          // 是否启用熔断器
	CircuitBreakerThreshold int           // 熔断器阈值
	TimeoutRetryEnabled     bool          // 是否启用超时重试
}

// DefaultErrorHandlingConfig 默认错误处理配置
var DefaultErrorHandlingConfig = &ErrorHandlingConfig{
	MaxRetryAttempts:        3,
	RetryInterval:           1 * time.Second,
	EnableCircuitBreaker:    true,
	CircuitBreakerThreshold: 5,
	TimeoutRetryEnabled:     true,
}

// ClientMetrics 客户端指标
type ClientMetrics struct {
	TotalRequests   int64         // 总请求数
	SuccessRequests int64         // 成功请求数
	FailedRequests  int64         // 失败请求数
	TimeoutRequests int64         // 超时请求数
	RetryRequests   int64         // 重试请求数
	AvgResponseTime time.Duration // 平均响应时间
	mutex           sync.RWMutex
}

// ResponseFuture 响应Future
type ResponseFuture struct {
	Opaque    int32
	TimeoutMs int64
	Callback  ResponseCallback
	BeginTime time.Time
	Done      chan *command.RemotingCommand
	Semaphore chan struct{}
}

// ResponseCallback 响应回调
type ResponseCallback func(*command.RemotingCommand, error)

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

// IsOpen 检查熔断器是否打开
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	if !cb.open {
		return false
	}

	// 检查是否应该半开
	if time.Since(cb.lastFailure) > cb.timeout {
		cb.mutex.Lock()
		cb.open = false
		cb.mutex.Unlock()
		return false
	}

	return true
}

// RecordSuccess 记录成功
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures = 0
	cb.open = false
}

// RecordFailure 记录失败
func (cb *CircuitBreaker) RecordFailure() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if int(cb.failures) >= cb.threshold {
		cb.open = true
	}
}

// RemotingClient RocketMQ远程通信客户端
type RemotingClient struct {
	connections  sync.Map // map[string]*Connection
	requestTable sync.Map // map[int32]*ResponseFuture
	opaque       int32    // 请求序列号
	closed       int32    // 关闭标志
	ctx          context.Context
	cancel       context.CancelFunc

	// 连接池
	connectionPool *connection.ConnectionPool

	// 错误处理配置
	errorConfig *ErrorHandlingConfig

	// 指标收集
	metrics *ClientMetrics
}

// NewRemotingClient 创建远程通信客户端
func NewRemotingClient() *RemotingClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &RemotingClient{
		ctx:    ctx,
		cancel: cancel,
		// 初始化连接池
		connectionPool: connection.NewConnectionPool(nil),
		// 初始化错误处理配置
		errorConfig: DefaultErrorHandlingConfig,
		// 初始化指标
		metrics: &ClientMetrics{},
	}

	// 启动清理goroutine
	go client.cleanupRoutine()

	return client
}

// cleanupRoutine 清理例程
func (rc *RemotingClient) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rc.cleanupConnections()
			rc.cleanupRequests()
		case <-rc.ctx.Done():
			return
		}
	}
}

// Connect 连接到指定地址
func (rc *RemotingClient) Connect(addr string) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return errors.NewRemotingError(errors.ErrCodePoolClosed, "client is closed", nil)
	}

	// 使用连接池获取连接
	_, err := rc.connectionPool.GetConnection(addr)
	if err != nil {
		return errors.NewRemotingError(errors.ErrCodeConnectionFailed, fmt.Sprintf("failed to connect to %s", addr), err)
	}

	return nil
}

// SendSync 同步发送请求
func (rc *RemotingClient) SendSync(addr string, request *command.RemotingCommand, timeoutMs int64) (*command.RemotingCommand, error) {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return nil, errors.NewRemotingError(errors.ErrCodePoolClosed, "client is closed", nil)
	}

	startTime := time.Now()
	defer func() {
		rc.metrics.updateResponseTime(time.Since(startTime))
		rc.metrics.incrementTotalRequests()
	}()

	// 重试机制
	var lastErr error
	for attempt := 0; attempt <= rc.errorConfig.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			// 记录重试
			rc.metrics.incrementRetryRequests()
			time.Sleep(rc.errorConfig.RetryInterval)
		}

		response, err := rc.sendSyncWithCircuitBreaker(addr, request, timeoutMs)
		if err == nil {
			rc.metrics.incrementSuccessRequests()
			return response, nil
		}

		lastErr = err

		// 检查是否应该重试
		if !rc.shouldRetry(err, attempt) {
			break
		}
	}

	rc.metrics.incrementFailedRequests()
	return nil, lastErr
}

// sendSyncWithCircuitBreaker 带熔断器的同步发送
func (rc *RemotingClient) sendSyncWithCircuitBreaker(addr string, request *command.RemotingCommand, timeoutMs int64) (*command.RemotingCommand, error) {
	// 检查熔断器
	// 这里简化实现，实际应该为每个地址维护一个熔断器

	// 使用连接池获取连接
	conn, err := rc.connectionPool.GetConnection(addr)
	if err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeConnectionFailed, fmt.Sprintf("failed to get connection to %s", addr), err)
	}

	// 设置请求ID
	opaque := atomic.AddInt32(&rc.opaque, 1)
	request.Opaque = opaque

	// 创建ResponseFuture
	future := &ResponseFuture{
		Opaque:    opaque,
		TimeoutMs: timeoutMs,
		BeginTime: time.Now(),
		Done:      make(chan *command.RemotingCommand, 1),
		Semaphore: make(chan struct{}, 1),
	}

	rc.requestTable.Store(opaque, future)
	defer rc.requestTable.Delete(opaque)

	// 发送请求
	if err := rc.sendRequest(conn, request); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeConnectionFailed, "failed to send request", err)
	}

	// 等待响应
	select {
	case response := <-future.Done:
		if response == nil {
			return nil, errors.NewRemotingError(errors.ErrCodeInvalidResponse, "received nil response", nil)
		}
		return response, nil
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		rc.metrics.incrementTimeoutRequests()
		return nil, errors.NewRemotingError(errors.ErrCodeConnectionTimeout, fmt.Sprintf("request timeout after %dms", timeoutMs), nil)
	case <-rc.ctx.Done():
		return nil, errors.NewRemotingError(errors.ErrCodePoolClosed, "client context cancelled", nil)
	}
}

// SendAsync 异步发送请求
func (rc *RemotingClient) SendAsync(addr string, request *command.RemotingCommand, timeoutMs int64, callback ResponseCallback) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return errors.NewRemotingError(errors.ErrCodePoolClosed, "client is closed", nil)
	}

	// 使用连接池获取连接
	conn, err := rc.connectionPool.GetConnection(addr)
	if err != nil {
		return errors.NewRemotingError(errors.ErrCodeConnectionFailed, fmt.Sprintf("failed to get connection to %s", addr), err)
	}

	// 设置请求ID
	opaque := atomic.AddInt32(&rc.opaque, 1)
	request.Opaque = opaque

	// 创建ResponseFuture
	future := &ResponseFuture{
		Opaque:    opaque,
		TimeoutMs: timeoutMs,
		Callback:  callback,
		BeginTime: time.Now(),
		Semaphore: make(chan struct{}, 1),
	}

	rc.requestTable.Store(opaque, future)

	// 发送请求
	return rc.sendRequest(conn, request)
}

// SendOneway 单向发送请求（不等待响应）
func (rc *RemotingClient) SendOneway(addr string, request *command.RemotingCommand) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return errors.NewRemotingError(errors.ErrCodePoolClosed, "client is closed", nil)
	}

	// 使用连接池获取连接
	conn, err := rc.connectionPool.GetConnection(addr)
	if err != nil {
		return errors.NewRemotingError(errors.ErrCodeConnectionFailed, fmt.Sprintf("failed to get connection to %s", addr), err)
	}

	// 设置单向标志
	request.Flag |= 1 // RPC_ONEWAY
	request.Opaque = atomic.AddInt32(&rc.opaque, 1)

	// 发送请求
	return rc.sendRequest(conn, request)
}

// shouldRetry 检查是否应该重试
func (rc *RemotingClient) shouldRetry(err error, attempt int) bool {
	if attempt >= rc.errorConfig.MaxRetryAttempts {
		return false
	}

	// 检查错误类型
	if remotingErr, ok := err.(*errors.RemotingError); ok {
		// 某些错误不应该重试
		switch remotingErr.Code {
		case errors.ErrCodeInvalidAddress, errors.ErrCodeConnectionFailed:
			return false
		case errors.ErrCodeConnectionTimeout:
			return rc.errorConfig.TimeoutRetryEnabled
		}
	}

	return true
}

// sendRequest 发送请求到指定连接
func (rc *RemotingClient) sendRequest(conn *command.Connection, request *command.RemotingCommand) error {
	// 使用连接池连接发送请求
	// 这里简化实现，实际应该通过连接发送数据

	// 序列化请求
	_, err := rc.encodeRemotingCommand(request)
	if err != nil {
		return errors.NewRemotingError(errors.ErrCodeEncodeFailed, "failed to encode request", err)
	}

	// 发送数据（简化实现）
	// 实际应该使用conn.client.SendXXX方法

	return nil
}

// encodeRemotingCommand 编码RemotingCommand
func (rc *RemotingClient) encodeRemotingCommand(cmd *command.RemotingCommand) ([]byte, error) {
	// 序列化header
	headerData, err := json.Marshal(cmd)
	if err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeEncodeFailed, "failed to marshal command header", err)
	}

	headerLength := len(headerData)
	bodyLength := len(cmd.Body)
	totalLength := 4 + headerLength + bodyLength

	// 构建数据包
	buf := bytes.NewBuffer(make([]byte, 0, totalLength+4))

	// 写入总长度
	binary.Write(buf, binary.BigEndian, int32(totalLength))

	// 写入header长度和序列化类型
	headerLengthAndSerializeType := (headerLength << 8) | 0 // JSON序列化
	binary.Write(buf, binary.BigEndian, int32(headerLengthAndSerializeType))

	// 写入header数据
	buf.Write(headerData)

	// 写入body数据
	if bodyLength > 0 {
		buf.Write(cmd.Body)
	}

	return buf.Bytes(), nil
}

// decodeRemotingCommand 解码RemotingCommand
func (rc *RemotingClient) decodeRemotingCommand(reader *bufio.Reader) (*command.RemotingCommand, error) {
	// 读取总长度
	var totalLength int32
	if err := binary.Read(reader, binary.BigEndian, &totalLength); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, "failed to read total length", err)
	}

	if totalLength <= 0 || totalLength > 16*1024*1024 { // 16MB限制
		return nil, errors.NewRemotingError(errors.ErrCodeInvalidResponse, fmt.Sprintf("invalid total length: %d", totalLength), nil)
	}

	// 读取header长度和序列化类型
	var headerLengthAndSerializeType int32
	if err := binary.Read(reader, binary.BigEndian, &headerLengthAndSerializeType); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, "failed to read header length", err)
	}

	headerLength := (headerLengthAndSerializeType >> 8) & 0xFFFFFF
	serializeType := headerLengthAndSerializeType & 0xFF

	if headerLength <= 0 || headerLength > totalLength-4 {
		return nil, errors.NewRemotingError(errors.ErrCodeInvalidResponse, fmt.Sprintf("invalid header length: %d", headerLength), nil)
	}

	// 读取header数据
	headerData := make([]byte, headerLength)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, "failed to read header data", err)
	}

	// 读取body数据
	bodyLength := totalLength - 4 - headerLength
	var bodyData []byte
	if bodyLength > 0 {
		bodyData = make([]byte, bodyLength)
		if _, err := io.ReadFull(reader, bodyData); err != nil {
			return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, "failed to read body data", err)
		}
	}

	// 反序列化header
	var cmd command.RemotingCommand
	if serializeType == 0 { // JSON
		if err := json.Unmarshal(headerData, &cmd); err != nil {
			return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, "failed to unmarshal command header", err)
		}
	} else {
		return nil, errors.NewRemotingError(errors.ErrCodeDecodeFailed, fmt.Sprintf("unsupported serialize type: %d", serializeType), nil)
	}

	cmd.Body = bodyData
	return &cmd, nil
}

// cleanupConnections 清理过期连接
func (rc *RemotingClient) cleanupConnections() {
	// 连接池会自动清理过期连接
}

// cleanupRequests 清理超时请求
func (rc *RemotingClient) cleanupRequests() {
	now := time.Now()
	rc.requestTable.Range(func(key, value interface{}) bool {
		future := value.(*ResponseFuture)
		timeout := time.Duration(future.TimeoutMs) * time.Millisecond

		if now.Sub(future.BeginTime) > timeout {
			rc.requestTable.Delete(key)

			// 通知超时
			if future.Callback != nil {
				go future.Callback(nil, errors.NewRemotingError(errors.ErrCodeConnectionTimeout, "request timeout", nil))
			} else if future.Done != nil {
				select {
				case future.Done <- nil:
				default:
				}
			}
		}
		return true
	})
}

// Close 关闭客户端
func (rc *RemotingClient) Close() {
	if !atomic.CompareAndSwapInt32(&rc.closed, 0, 1) {
		return
	}

	rc.cancel()

	// 关闭连接池
	rc.connectionPool.Close()

	// 清理所有请求
	rc.requestTable.Range(func(key, value interface{}) bool {
		rc.requestTable.Delete(key)
		return true
	})
}

// IsConnected 检查是否连接到指定地址
func (rc *RemotingClient) IsConnected(addr string) bool {
	return rc.connectionPool.IsConnected(addr)
}

// Client metrics methods
func (cm *ClientMetrics) incrementTotalRequests() {
	cm.mutex.Lock()
	cm.TotalRequests++
	cm.mutex.Unlock()
}

func (cm *ClientMetrics) incrementSuccessRequests() {
	cm.mutex.Lock()
	cm.SuccessRequests++
	cm.mutex.Unlock()
}

func (cm *ClientMetrics) incrementFailedRequests() {
	cm.mutex.Lock()
	cm.FailedRequests++
	cm.mutex.Unlock()
}

func (cm *ClientMetrics) incrementTimeoutRequests() {
	cm.mutex.Lock()
	cm.TimeoutRequests++
	cm.mutex.Unlock()
}

func (cm *ClientMetrics) incrementRetryRequests() {
	cm.mutex.Lock()
	cm.RetryRequests++
	cm.mutex.Unlock()
}

func (cm *ClientMetrics) updateResponseTime(latency time.Duration) {
	cm.mutex.Lock()
	if cm.AvgResponseTime == 0 {
		cm.AvgResponseTime = latency
	} else {
		cm.AvgResponseTime = (cm.AvgResponseTime + latency) / 2
	}
	cm.mutex.Unlock()
}

// GetClientStats 获取客户端统计信息
func (cm *ClientMetrics) GetClientStats() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return map[string]interface{}{
		"total_requests":       cm.TotalRequests,
		"success_requests":     cm.SuccessRequests,
		"failed_requests":      cm.FailedRequests,
		"timeout_requests":     cm.TimeoutRequests,
		"retry_requests":       cm.RetryRequests,
		"avg_response_time_ms": cm.AvgResponseTime.Milliseconds(),
	}
}
