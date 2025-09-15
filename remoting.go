package remoting

import (
	"context"
	"sync"
	"time"

	client "github.com/chenjy16/go-rocketmq-remoting/client"
	"github.com/chenjy16/go-rocketmq-remoting/command"
	"github.com/chenjy16/go-rocketmq-remoting/errors"
	server "github.com/chenjy16/go-rocketmq-remoting/server"
)

// ========== REQUEST CODES ==========

// RequestCode 请求码
type RequestCode = command.RequestCode

// ResponseCode 响应码
type ResponseCode = command.ResponseCode

const (
	// Request Codes
	SendMessage              = command.SendMessage
	PullMessage              = command.PullMessage
	QueryMessage             = command.QueryMessage
	QueryMessageByKey        = command.QueryMessageByKey
	UpdateAndCreateTopic     = command.UpdateAndCreateTopic
	GetRouteInfoByTopic      = command.GetRouteInfoByTopic
	GetBrokerClusterInfo     = command.GetBrokerClusterInfo
	RegisterBroker           = command.RegisterBroker
	UnregisterBroker         = command.UnregisterBroker
	SendMessageV2            = command.SendMessageV2
	SendBatchMessage         = command.SendBatchMessage
	CheckTransactionState    = command.CheckTransactionState
	NotifyConsumerIdsChanged = command.NotifyConsumerIdsChanged
	LockBatchMQ              = command.LockBatchMQ
	UnlockBatchMQ            = command.UnlockBatchMQ
	GetAllTopicConfig        = command.GetAllTopicConfig
	UpdateBrokerConfig       = command.UpdateBrokerConfig
	GetBrokerConfig          = command.GetBrokerConfig
	SearchOffsetByTimestamp  = command.SearchOffsetByTimestamp
	GetMaxOffset             = command.GetMaxOffset
	GetMinOffset             = command.GetMinOffset
	HeartBeat                = command.HeartBeat
	UnregisterClient         = command.UnregisterClient
	ConsumerSendMsgBack      = command.ConsumerSendMsgBack
	UpdateConsumerOffset     = command.UpdateConsumerOffset
	EndTransaction           = command.EndTransaction
	GetConsumerListByGroup   = command.GetConsumerListByGroup
	CheckClientConfig        = command.CheckClientConfig

	Success                   = command.Success
	SystemError               = command.SystemError
	SystemBusy                = command.SystemBusy
	RequestCodeNotSupported   = command.RequestCodeNotSupported
	TransactionFailed         = command.TransactionFailed
	FlushDiskTimeout          = command.FlushDiskTimeout
	SlaveNotAvailable         = command.SlaveNotAvailable
	FlushSlaveTimeout         = command.FlushSlaveTimeout
	MessageIllegal            = command.MessageIllegal
	ServiceNotAvailable       = command.ServiceNotAvailable
	VersionNotSupported       = command.VersionNotSupported
	NoPermission              = command.NoPermission
	TopicNotExist             = command.TopicNotExist
	TopicExistAlready         = command.TopicExistAlready
	PullNotFound              = command.PullNotFound
	PullRetryImmediately      = command.PullRetryImmediately
	PullOffsetMoved           = command.PullOffsetMoved
	QueryNotFound             = command.QueryNotFound
	SubscriptionParseFailed   = command.SubscriptionParseFailed
	SubscriptionNotExist      = command.SubscriptionNotExist
	SubscriptionNotLatest     = command.SubscriptionNotLatest
	SubscriptionGroupNotExist = command.SubscriptionGroupNotExist
)

// ========== DATA STRUCTURES ==========

// TopicRouteData Topic路由数据
type TopicRouteData struct {
	OrderTopicConf    string              `json:"orderTopicConf"`
	QueueDatas        []*QueueData        `json:"queueDatas"`
	BrokerDatas       []*BrokerData       `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
}

// QueueData 队列数据
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int32  `json:"readQueueNums"`
	WriteQueueNums int32  `json:"writeQueueNums"`
	Perm           int32  `json:"perm"`
	TopicSysFlag   int32  `json:"topicSysFlag"`
}

// BrokerData Broker数据
type BrokerData struct {
	Cluster     string           `json:"cluster"`
	BrokerName  string           `json:"brokerName"`
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
}

// DataVersion 数据版本
type DataVersion struct {
	Timestamp int64 `json:"timestamp"`
	Counter   int64 `json:"counter"`
}

// SubscriptionData 订阅数据
type SubscriptionData struct {
	Topic          string   `json:"topic"`
	SubString      string   `json:"subString"`
	TagsSet        []string `json:"tagsSet"`
	CodeSet        []int32  `json:"codeSet"`
	SubVersion     int64    `json:"subVersion"`
	ExpressionType string   `json:"expressionType"`
}

// TopicConfig Topic配置
type TopicConfig struct {
	TopicName       string            `json:"topicName"`
	ReadQueueNums   int32             `json:"readQueueNums"`
	WriteQueueNums  int32             `json:"writeQueueNums"`
	Perm            int32             `json:"perm"`
	TopicFilterType string            `json:"topicFilterType"`
	TopicSysFlag    int32             `json:"topicSysFlag"`
	Order           bool              `json:"order"`
	Attributes      map[string]string `json:"attributes"`
}

// TopicConfigSerializeWrapper Topic配置序列化包装器
type TopicConfigSerializeWrapper struct {
	TopicConfigTable map[string]*TopicConfig `json:"topicConfigTable"`
	DataVersion      *DataVersion            `json:"dataVersion"`
	MixAllSubscribed bool                    `json:"mixAllSubscribed"`
}

// RegisterBrokerResult 注册Broker结果
type RegisterBrokerResult struct {
	HaServerAddr string `json:"haServerAddr"`
	MasterAddr   string `json:"masterAddr"`
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	BrokerAddrTable  map[string]map[int64]string `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string         `json:"clusterAddrTable"`
}

// RemotingCommand 远程调用命令
type RemotingCommand = command.RemotingCommand

// SendMessageRequestHeader 发送消息请求头
type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int32  `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int32  `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int32  `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	Batch                 bool   `json:"batch"`

	// ACL认证字段
	AccessKey string `json:"accessKey,omitempty"`
	Signature string `json:"signature,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

// SendMessageResponseHeader 发送消息响应头
type SendMessageResponseHeader struct {
	MsgId         string `json:"msgId"`
	QueueId       int32  `json:"queueId"`
	QueueOffset   int64  `json:"queueOffset"`
	TransactionId string `json:"transactionId"`
	BatchUniqId   string `json:"batchUniqId"`
}

// PullMessageRequestHeader 拉取消息请求头
type PullMessageRequestHeader struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
	ExpressionType       string `json:"expressionType"`
}

// PullMessageResponseHeader 拉取消息响应头
type PullMessageResponseHeader struct {
	SuggestWhichBrokerId int64 `json:"suggestWhichBrokerId"`
	NextBeginOffset      int64 `json:"nextBeginOffset"`
	MinOffset            int64 `json:"minOffset"`
	MaxOffset            int64 `json:"maxOffset"`
}

// UpdateConsumerOffsetRequestHeader 更新消费者偏移量请求头
type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

// GetConsumerListByGroupRequestHeader 根据消费者组获取消费者列表请求头
type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

// GetConsumerListByGroupResponseBody 根据消费者组获取消费者列表响应体
type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string `json:"consumerIdList"`
}

// LockBatchMQRequestHeader 批量锁定消息队列请求头
type LockBatchMQRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
}

// LockBatchMQRequestBody 批量锁定消息队列请求体
type LockBatchMQRequestBody struct {
	MqSet []*MessageQueue `json:"mqSet"`
}

// MessageQueue 消息队列信息
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int32  `json:"queueId"`
}

// UnlockBatchMQRequestHeader 批量解锁消息队列请求头
type UnlockBatchMQRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
}

// UnlockBatchMQRequestBody 批量解锁消息队列请求体
type UnlockBatchMQRequestBody struct {
	MqSet []*MessageQueue `json:"mqSet"`
}

// NewDataVersion 创建新的数据版本
func NewDataVersion() *DataVersion {
	return &DataVersion{
		Timestamp: time.Now().UnixMilli(),
		Counter:   0,
	}
}

// NextVersion 获取下一个版本
func (dv *DataVersion) NextVersion() *DataVersion {
	return &DataVersion{
		Timestamp: time.Now().UnixMilli(),
		Counter:   dv.Counter + 1,
	}
}

// CreateRemotingCommand 创建远程调用命令
func CreateRemotingCommand(code RequestCode) *RemotingCommand {
	return command.CreateRemotingCommand(code)
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code ResponseCode, remark string) *RemotingCommand {
	return command.CreateResponseCommand(code, remark)
}

// ========== REMOTING DEFINITIONS ==========

// RemotingClient RocketMQ远程通信客户端接口
type RemotingClient struct {
	connections  sync.Map // map[string]*Connection
	requestTable sync.Map // map[int32]*ResponseFuture
	opaque       int32    // 请求序列号
	closed       int32    // 关闭标志
	ctx          context.Context
	cancel       context.CancelFunc
}

// ResponseCallback 响应回调
type ResponseCallback func(*RemotingCommand, error)

// Connection TCP连接封装
type Connection = command.Connection

// RemotingError 远程调用错误
type RemotingError = errors.RemotingError

// ServerConnection 服务端连接类型别名
type ServerConnection = server.ServerConnection

// RequestProcessor 请求处理器接口类型别名
type RequestProcessor = server.RequestProcessor

// RequestProcessorFunc 请求处理器函数类型别名
type RequestProcessorFunc = server.RequestProcessorFunc

// RemotingClientWrapper 包装器，用于暴露client.RemotingClient的方法
type RemotingClientWrapper struct {
	client *client.RemotingClient
}

// NewRemotingClient 创建远程通信客户端
func NewRemotingClient() *RemotingClientWrapper {
	return &RemotingClientWrapper{
		client: client.NewRemotingClient(),
	}
}

// convertToClientCommand converts remoting.RemotingCommand to client.RemotingCommand
func convertToClientCommand(cmd *RemotingCommand) *command.RemotingCommand {
	if cmd == nil {
		return nil
	}
	return &command.RemotingCommand{
		Code:      command.RequestCode(cmd.Code),
		Language:  cmd.Language,
		Version:   cmd.Version,
		Opaque:    cmd.Opaque,
		Flag:      cmd.Flag,
		Remark:    cmd.Remark,
		ExtFields: cmd.ExtFields,
		Body:      cmd.Body,
	}
}

// convertFromClientCommand converts client.RemotingCommand to remoting.RemotingCommand
func convertFromClientCommand(cmd *command.RemotingCommand) *RemotingCommand {
	if cmd == nil {
		return nil
	}
	return &RemotingCommand{
		Code:      RequestCode(cmd.Code),
		Language:  cmd.Language,
		Version:   cmd.Version,
		Opaque:    cmd.Opaque,
		Flag:      cmd.Flag,
		Remark:    cmd.Remark,
		ExtFields: cmd.ExtFields,
		Body:      cmd.Body,
	}
}

// SendSync 同步发送请求
func (rcw *RemotingClientWrapper) SendSync(addr string, request *RemotingCommand, timeoutMs int64) (*RemotingCommand, error) {
	// Convert remoting command to client command
	clientRequest := convertToClientCommand(request)

	// Send request
	clientResponse, err := rcw.client.SendSync(addr, clientRequest, timeoutMs)
	if err != nil {
		return nil, err
	}

	// Convert response back to remoting command
	return convertFromClientCommand(clientResponse), nil
}

// Close 关闭客户端
func (rcw *RemotingClientWrapper) Close() {
	rcw.client.Close()
}

// RemotingServer 服务端类型别名
type RemotingServer = server.RemotingServer

// NewRemotingServer 创建远程通信服务端
func NewRemotingServer(listenPort int) *RemotingServer {
	return server.NewRemotingServer(listenPort)
}

// ProtocolProcessor 协议处理器接口
type ProtocolProcessor interface {
	RegisterProcessor(code RequestCode, processor interface{})
	UnregisterProcessor(requestCode RequestCode)
	ProcessRequest(ctx context.Context, conn *ServerConnection, request *RemotingCommand) (*RemotingCommand, error)
}

// NewProtocolProcessor 创建协议处理器
func NewProtocolProcessor() ProtocolProcessor {
	// 由于循环依赖，这里返回nil，实际使用时应该从processor包创建
	return nil
}
