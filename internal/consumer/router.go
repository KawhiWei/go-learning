package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrHandlerNotFound = errors.New("kafka topic handler not found")

// Router 按 Topic 把消息分发给独立 Handler。
// 注册发生在 Consumer 启动前，因此无需在每条消息处理时引入锁。
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
