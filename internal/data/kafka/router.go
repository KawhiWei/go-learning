// Package kafka 封装 API 使用的 Producer，以及独立 work 进程使用的
// Consumer 和 Topic 路由。
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrHandlerNotFound = errors.New("kafka topic handler not found")

// Message 是与 Kafka 客户端实现无关的消息 DTO。业务 Handler 不需要依赖
// franz-go 的 Record 类型，后续更换客户端不会影响业务处理函数。
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

type Handler interface {
	Handle(context.Context, Message) error
}

type HandlerFunc func(context.Context, Message) error

func (f HandlerFunc) Handle(ctx context.Context, message Message) error {
	return f(ctx, message)
}

// Router 按 Topic 把消息分发给独立 Handler。注册发生在 Consumer 启动前，
// 因此无需在每条消息处理时引入锁。
type Router struct {
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{handlers: make(map[string]Handler)}
}

func (r *Router) Register(topic string, handler Handler) error {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return errors.New("kafka topic must not be empty")
	}
	if handler == nil {
		return fmt.Errorf("kafka handler for topic %q must not be nil", topic)
	}
	if _, exists := r.handlers[topic]; exists {
		return fmt.Errorf("kafka handler for topic %q already registered", topic)
	}
	r.handlers[topic] = handler
	return nil
}

func (r *Router) Has(topic string) bool {
	_, exists := r.handlers[topic]
	return exists
}

func (r *Router) Handle(ctx context.Context, message Message) error {
	handler, exists := r.handlers[message.Topic]
	if !exists {
		return fmt.Errorf("%w: %s", ErrHandlerNotFound, message.Topic)
	}
	return handler.Handle(ctx, message)
}
