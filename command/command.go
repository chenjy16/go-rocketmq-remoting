package command

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// RequestCode 请求码
type RequestCode int32

// ResponseCode 响应码
type ResponseCode int32

const (
	// Common Request Codes
	SendMessage              RequestCode = 10
	PullMessage              RequestCode = 11
	QueryMessage             RequestCode = 12
	QueryMessageByKey        RequestCode = 13
	UpdateAndCreateTopic     RequestCode = 14
	GetRouteInfoByTopic      RequestCode = 15
	GetBrokerClusterInfo     RequestCode = 16
	RegisterBroker           RequestCode = 17
	UnregisterBroker         RequestCode = 18
	SendMessageV2            RequestCode = 19
	SendBatchMessage         RequestCode = 20
	CheckTransactionState    RequestCode = 21
	NotifyConsumerIdsChanged RequestCode = 22
	LockBatchMQ              RequestCode = 23
	UnlockBatchMQ            RequestCode = 24
	GetAllTopicConfig        RequestCode = 25
	UpdateBrokerConfig       RequestCode = 26
	GetBrokerConfig          RequestCode = 27
	SearchOffsetByTimestamp  RequestCode = 28
	GetMaxOffset             RequestCode = 29
	GetMinOffset             RequestCode = 30
	HeartBeat                RequestCode = 31
	UnregisterClient         RequestCode = 32
	ConsumerSendMsgBack      RequestCode = 33
	UpdateConsumerOffset     RequestCode = 34
	EndTransaction           RequestCode = 36
	GetConsumerListByGroup   RequestCode = 38
	CheckClientConfig        RequestCode = 39
)

const (
	Success                   ResponseCode = 0
	SystemError               ResponseCode = 1
	SystemBusy                ResponseCode = 2
	RequestCodeNotSupported   ResponseCode = 3
	TransactionFailed         ResponseCode = 4
	FlushDiskTimeout          ResponseCode = 10
	SlaveNotAvailable         ResponseCode = 11
	FlushSlaveTimeout         ResponseCode = 12
	MessageIllegal            ResponseCode = 13
	ServiceNotAvailable       ResponseCode = 14
	VersionNotSupported       ResponseCode = 15
	NoPermission              ResponseCode = 16
	TopicNotExist             ResponseCode = 17
	TopicExistAlready         ResponseCode = 18
	PullNotFound              ResponseCode = 19
	PullRetryImmediately      ResponseCode = 20
	PullOffsetMoved           ResponseCode = 21
	QueryNotFound             ResponseCode = 22
	SubscriptionParseFailed   ResponseCode = 23
	SubscriptionNotExist      ResponseCode = 24
	SubscriptionNotLatest     ResponseCode = 25
	SubscriptionGroupNotExist ResponseCode = 26
)

// RemotingCommand 远程调用命令
type RemotingCommand struct {
	Code      RequestCode       `json:"code"`
	Language  string            `json:"language"`
	Version   int32             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int32             `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"body"`
}

// CreateRemotingCommand 创建远程调用命令
func CreateRemotingCommand(code RequestCode) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		Language:  "GO",
		Version:   1,
		Flag:      0,
		ExtFields: make(map[string]string),
	}
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code ResponseCode, remark string) *RemotingCommand {
	return &RemotingCommand{
		Code:      RequestCode(code),
		Language:  "GO",
		Version:   1,
		Flag:      1, // 响应标志
		Remark:    remark,
		ExtFields: make(map[string]string),
	}
}

// Connection TCP连接封装
type Connection struct {
	Addr       string
	Conn       net.Conn
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	Mutex      sync.RWMutex
	LastUsed   time.Time
	Closed     bool
	RemoteAddr string
}

// GetRemoteAddr 获取远程地址
func (c *Connection) GetRemoteAddr() string {
	return c.RemoteAddr
}
