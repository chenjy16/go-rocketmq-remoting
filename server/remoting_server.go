package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/command"
	"github.com/chenjy16/go-rocketmq-remoting/errors"
)

// ServerErrorHandlingConfig 服务端错误处理配置
type ServerErrorHandlingConfig struct {
	MaxConcurrentConnections int           // 最大并发连接数
	ConnectionTimeout        time.Duration // 连接超时
	RequestTimeout           time.Duration // 请求超时
	EnableRequestLogging     bool          // 是否启用请求日志
}

// DefaultServerErrorHandlingConfig 默认服务端错误处理配置
var DefaultServerErrorHandlingConfig = &ServerErrorHandlingConfig{
	MaxConcurrentConnections: 10000,
	ConnectionTimeout:        60 * time.Second,
	RequestTimeout:           30 * time.Second,
	EnableRequestLogging:     true,
}

// ServerMetrics 服务端指标
type ServerMetrics struct {
	TotalConnections  int64         // 总连接数
	ActiveConnections int64         // 活跃连接数
	TotalRequests     int64         // 总请求数
	SuccessRequests   int64         // 成功请求数
	FailedRequests    int64         // 失败请求数
	AvgRequestTime    time.Duration // 平均请求时间
	AvgResponseSize   int64         // 平均响应大小
	ErrorCountByCode  map[int]int64 // 按错误代码统计
	mutex             sync.RWMutex
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code command.ResponseCode, remark string) *command.RemotingCommand {
	return command.CreateResponseCommand(code, remark)
}

// ServerConnection 服务端连接
type ServerConnection = command.Connection

// RequestProcessor 请求处理器接口
type RequestProcessor interface {
	ProcessRequest(ctx context.Context, request *command.RemotingCommand, conn *ServerConnection) (*command.RemotingCommand, error)
}

// RequestProcessorFunc 请求处理器函数类型
type RequestProcessorFunc func(ctx context.Context, request *command.RemotingCommand, conn *ServerConnection) (*command.RemotingCommand, error)

// ProcessRequest 实现RequestProcessor接口
func (f RequestProcessorFunc) ProcessRequest(ctx context.Context, request *command.RemotingCommand, conn *ServerConnection) (*command.RemotingCommand, error) {
	return f(ctx, request, conn)
}

// RemotingServer RocketMQ远程通信服务端
type RemotingServer struct {
	listenPort  int
	listener    net.Listener
	processors  sync.Map // map[command.RequestCode]RequestProcessor
	connections sync.Map // map[string]*command.Connection
	closed      int32
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	// 错误处理配置
	errorConfig *ServerErrorHandlingConfig

	// 指标收集
	metrics *ServerMetrics
}

// NewRemotingServer 创建远程通信服务端
func NewRemotingServer(listenPort int) *RemotingServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &RemotingServer{
		listenPort: listenPort,
		ctx:        ctx,
		cancel:     cancel,
		// 初始化错误处理配置
		errorConfig: DefaultServerErrorHandlingConfig,
		// 初始化指标
		metrics: &ServerMetrics{
			ErrorCountByCode: make(map[int]int64),
		},
	}
}

// RegisterProcessor 注册请求处理器
func (rs *RemotingServer) RegisterProcessor(code command.RequestCode, processor RequestProcessor) {
	rs.processors.Store(code, processor)
}

// RegisterProcessorFunc 注册请求处理器函数
func (rs *RemotingServer) RegisterProcessorFunc(code command.RequestCode, processorFunc RequestProcessorFunc) {
	rs.processors.Store(code, processorFunc)
}

// Start 启动服务端
func (rs *RemotingServer) Start() error {
	if atomic.LoadInt32(&rs.closed) == 1 {
		return errors.NewRemotingError(errors.ErrCodeServerStartFailed, "server is closed", nil)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.listenPort))
	if err != nil {
		return errors.NewRemotingError(errors.ErrCodeServerListenFailed, fmt.Sprintf("failed to listen on port %d", rs.listenPort), err)
	}

	rs.listener = listener

	// 启动清理goroutine
	rs.wg.Add(1)
	go rs.cleanupRoutine()

	// 启动接受连接的goroutine
	rs.wg.Add(1)
	go rs.acceptConnections()

	return nil
}

// acceptConnections 接受客户端连接
func (rs *RemotingServer) acceptConnections() {
	defer rs.wg.Done()

	for {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}

		// 检查连接数限制
		if int(rs.getConnectionCount()) >= rs.errorConfig.MaxConcurrentConnections {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		conn, err := rs.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&rs.closed) == 1 {
				return
			}
			// 记录错误但继续接受连接
			rs.metrics.incrementError(errors.ErrCodeServerNetworkError)
			continue
		}

		// 处理新连接
		rs.wg.Add(1)
		go rs.handleConnection(conn)
	}
}

// handleConnection 处理客户端连接
func (rs *RemotingServer) handleConnection(conn net.Conn) {
	defer rs.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	serverConn := &command.Connection{
		Addr:       remoteAddr,
		Conn:       conn,
		Reader:     bufio.NewReader(conn),
		Writer:     bufio.NewWriter(conn),
		LastUsed:   time.Now(),
		Closed:     false,
		RemoteAddr: remoteAddr,
	}

	rs.connections.Store(remoteAddr, serverConn)
	defer func() {
		serverConn.Mutex.Lock()
		serverConn.Closed = true
		serverConn.Mutex.Unlock()
		rs.connections.Delete(remoteAddr)
	}()

	for {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}

		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(rs.errorConfig.ConnectionTimeout))

		// 读取请求
		serverConn.Mutex.RLock()
		reader := serverConn.Reader
		serverConn.Mutex.RUnlock()
		request, err := rs.decodeRemotingCommand(reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 连接超时，继续等待
				continue
			}
			// 记录错误并断开连接
			rs.metrics.incrementError(errors.ErrCodeServerDecodeFailed)
			return
		}

		serverConn.LastUsed = time.Now()

		// 处理请求
		rs.wg.Add(1)
		go rs.processRequest(request, serverConn)
	}
}

// processRequest 处理请求
func (rs *RemotingServer) processRequest(request *command.RemotingCommand, conn *ServerConnection) {
	defer rs.wg.Done()

	startTime := time.Now()
	defer func() {
		rs.metrics.updateRequestTime(time.Since(startTime))
	}()

	// 查找处理器
	processorValue, exists := rs.processors.Load(command.RequestCode(request.Code))
	if !exists {
		rs.metrics.incrementError(errors.ErrCodeServerInvalidRequest)
		response := CreateResponseCommand(command.RequestCodeNotSupported, "request code not supported")
		rs.sendResponse(response, conn)
		return
	}

	// 处理请求
	processor := processorValue.(RequestProcessor)
	response, err := processor.ProcessRequest(rs.ctx, request, conn)
	if err != nil {
		rs.metrics.incrementError(errors.ErrCodeServerProcessFailed)
		response = CreateResponseCommand(command.SystemError, err.Error())
	}

	// 发送响应
	if response != nil {
		rs.sendResponse(response, conn)
	}

	// 更新指标
	rs.metrics.incrementSuccessRequests()
	rs.metrics.updateResponseSize(response)
}

// sendResponse 发送响应
func (rs *RemotingServer) sendResponse(response *command.RemotingCommand, conn *ServerConnection) {
	conn.Mutex.Lock()
	defer conn.Mutex.Unlock()

	// 序列化响应
	data, err := rs.encodeRemotingCommand(response)
	if err != nil {
		rs.metrics.incrementError(errors.ErrCodeServerEncodeFailed)
		return
	}

	// 发送响应
	if _, err := conn.Writer.Write(data); err != nil {
		rs.metrics.incrementError(errors.ErrCodeServerNetworkError)
		return
	}

	if err := conn.Writer.Flush(); err != nil {
		rs.metrics.incrementError(errors.ErrCodeServerNetworkError)
		return
	}
}

// encodeRemotingCommand 编码RemotingCommand
func (rs *RemotingServer) encodeRemotingCommand(cmd *command.RemotingCommand) ([]byte, error) {
	// 序列化header
	headerData, err := json.Marshal(cmd)
	if err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeServerEncodeFailed, "failed to marshal command header", err)
	}

	headerLength := len(headerData)
	bodyLength := len(cmd.Body)
	totalLength := 4 + headerLength + bodyLength

	// 构建数据包
	buf := make([]byte, 4+4+headerLength+bodyLength)

	// 写入总长度
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLength))

	// 写入header长度和序列化类型
	headerLengthAndSerializeType := (headerLength << 8) | 0 // JSON序列化
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLengthAndSerializeType))

	// 写入header数据
	copy(buf[8:8+headerLength], headerData)

	// 写入body数据
	if bodyLength > 0 {
		copy(buf[8+headerLength:8+headerLength+bodyLength], cmd.Body)
	}

	return buf, nil
}

// decodeRemotingCommand 解码RemotingCommand
func (rs *RemotingServer) decodeRemotingCommand(reader *bufio.Reader) (*command.RemotingCommand, error) {
	// 读取总长度
	var totalLength int32
	if err := binary.Read(reader, binary.BigEndian, &totalLength); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, "failed to read total length", err)
	}

	if totalLength <= 0 || totalLength > 16*1024*1024 { // 16MB限制
		return nil, errors.NewRemotingError(errors.ErrCodeServerInvalidRequest, fmt.Sprintf("invalid total length: %d", totalLength), nil)
	}

	// 读取header长度和序列化类型
	var headerLengthAndSerializeType int32
	if err := binary.Read(reader, binary.BigEndian, &headerLengthAndSerializeType); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, "failed to read header length", err)
	}

	headerLength := (headerLengthAndSerializeType >> 8) & 0xFFFFFF
	serializeType := headerLengthAndSerializeType & 0xFF

	if headerLength <= 0 || headerLength > totalLength-4 {
		return nil, errors.NewRemotingError(errors.ErrCodeServerInvalidRequest, fmt.Sprintf("invalid header length: %d", headerLength), nil)
	}

	// 读取header数据
	headerData := make([]byte, headerLength)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, "failed to read header data", err)
	}

	// 读取body数据
	bodyLength := totalLength - 4 - headerLength
	var bodyData []byte
	if bodyLength > 0 {
		bodyData = make([]byte, bodyLength)
		if _, err := io.ReadFull(reader, bodyData); err != nil {
			return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, "failed to read body data", err)
		}
	}

	// 反序列化header
	var cmd command.RemotingCommand
	if serializeType == 0 { // JSON
		if err := json.Unmarshal(headerData, &cmd); err != nil {
			return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, "failed to unmarshal command header", err)
		}
	} else {
		return nil, errors.NewRemotingError(errors.ErrCodeServerDecodeFailed, fmt.Sprintf("unsupported serialize type: %d", serializeType), nil)
	}

	cmd.Body = bodyData
	return &cmd, nil
}

// Stop 停止服务端
func (rs *RemotingServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&rs.closed, 0, 1) {
		return errors.NewRemotingError(errors.ErrCodeServerStopFailed, "server is already closed", nil)
	}

	rs.cancel()

	if rs.listener != nil {
		rs.listener.Close()
	}

	// 等待所有goroutine结束
	rs.wg.Wait()

	return nil
}

// cleanupRoutine 清理例程
func (rs *RemotingServer) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rs.cleanupConnections()
		case <-rs.ctx.Done():
			return
		}
	}
}

// cleanupConnections 清理过期连接
func (rs *RemotingServer) cleanupConnections() {
	now := time.Now()
	rs.connections.Range(func(key, value interface{}) bool {
		conn := value.(*command.Connection)
		conn.Mutex.RLock()
		lastUsed := conn.LastUsed
		closed := conn.Closed
		conn.Mutex.RUnlock()

		if closed || now.Sub(lastUsed) > rs.errorConfig.ConnectionTimeout {
			rs.connections.Delete(key)
		}
		return true
	})
}

// getConnectionCount 获取连接数
func (rs *RemotingServer) getConnectionCount() int32 {
	var count int32
	rs.connections.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// GetStats 获取服务端统计信息
func (rs *RemotingServer) GetStats() map[string]interface{} {
	rs.metrics.mutex.RLock()
	defer rs.metrics.mutex.RUnlock()

	return map[string]interface{}{
		"total_connections":   rs.metrics.TotalConnections,
		"active_connections":  rs.metrics.ActiveConnections,
		"total_requests":      rs.metrics.TotalRequests,
		"success_requests":    rs.metrics.SuccessRequests,
		"failed_requests":     rs.metrics.FailedRequests,
		"avg_request_time_ms": rs.metrics.AvgRequestTime.Milliseconds(),
		"avg_response_size":   rs.metrics.AvgResponseSize,
		"error_count_by_code": rs.metrics.ErrorCountByCode,
	}
}

// Server metrics methods
func (sm *ServerMetrics) incrementSuccessRequests() {
	sm.mutex.Lock()
	sm.SuccessRequests++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) incrementError(code int) {
	sm.mutex.Lock()
	sm.ErrorCountByCode[code]++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) updateRequestTime(latency time.Duration) {
	sm.mutex.Lock()
	if sm.AvgRequestTime == 0 {
		sm.AvgRequestTime = latency
	} else {
		sm.AvgRequestTime = (sm.AvgRequestTime + latency) / 2
	}
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) updateResponseSize(response *command.RemotingCommand) {
	if response == nil {
		return
	}

	size := 4 + 4 + len(response.Body) // 简化计算
	sm.mutex.Lock()
	if sm.AvgResponseSize == 0 {
		sm.AvgResponseSize = int64(size)
	} else {
		sm.AvgResponseSize = (sm.AvgResponseSize + int64(size)) / 2
	}
	sm.mutex.Unlock()
}
