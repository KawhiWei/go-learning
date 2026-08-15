// Package consumer 定义消息消费进程的公共契约和 Topic 路由。
// 具体业务 Handler 位于 user、audit 等子包，Kafka 客户端实现位于 data/kafka。
package consumer

import (
	"context"
	"time"
)

// Message 是与 Kafka 客户端无关的消息 DTO。业务 Handler 不依赖 franz-go
// Record，因此更换消息客户端不会迫使业务包同步修改。
type Message struct {
	// Topic 是消息所属 Topic，Router 据此选择业务 Handler。
	Topic string
	// Partition 是 Kafka 分区编号；同一分区内的 offset 保证顺序。
	Partition int32
	// Offset 是消息在 Partition 中的单调递增位置，用于提交消费进度。
	Offset int64
	// Key 是 Producer 设置的分区键，Handler 可用于关联业务实体。
	Key []byte
	// Value 是未解析的原始消息正文，由业务 Handler 按自己的 schema 解码。
	Value []byte
	// Headers 保存 Kafka 消息头；不存在的 header 不会出现在 map 中。
	Headers map[string][]byte
	// Timestamp 是 Producer 写入消息的时间戳。
	Timestamp time.Time
}

// Handler 是单条业务消息的处理边界。返回 nil 表示消息处理成功，Consumer
// 才能推进对应 partition 的 offset。
type Handler interface {
	Handle(context.Context, Message) error
}

type HandlerFunc func(context.Context, Message) error

func (f HandlerFunc) Handle(ctx context.Context, message Message) error {
	return f(ctx, message)
}

// TopicRouter 是 Kafka Consumer 所需的最小路由能力。data/kafka 只依赖这个
// 接口，不需要知道业务 Handler 的目录或具体类型。
type TopicRouter interface {
	Handler
	Has(topic string) bool
}
