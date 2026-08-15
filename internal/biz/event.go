package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var ErrEventTopicNotAllowed = errors.New("event topic is not allowed")

// Event 是发布到消息系统的业务 DTO。
// 业务层不依赖具体 Kafka 客户端，因而单元测试和未来替换消息基础设施时都无需构造 franz-go 类型。
type Event struct {
	// Topic 是 Kafka Topic 名称；发布前必须属于当前进程声明的 Topic 白名单。
	Topic string
	// Key 是 Kafka 分区键；同一 key 通常会落在同一 partition 以维持业务顺序。
	Key []byte
	// Payload 是已编码的消息正文；MessagePublisher 会将业务 DTO 编码为 JSON。
	Payload []byte
}

// EventPublisher 定义业务层真正需要的消息发布能力。
// 具体 Kafka Producer 在 data 层实现该接口，依赖方向仍然是 infrastructure -> business contract。
type EventPublisher interface {
	Publish(context.Context, Event) error
}

// PublishAccepted 是统一的异步消息发布结果。
// ResourceID 是调用方可立即返回给客户端的业务 ID；它不表示 Consumer 已完成处理。
type PublishAccepted struct {
	// MessageID 是 Kafka 消息的唯一标识，用于追踪发布和消费过程。
	MessageID string `json:"message_id"`
	// ResourceID 是已接受异步处理的业务资源标识，例如待创建用户的 ID。
	ResourceID string `json:"resource_id"`
	// Status 描述接受状态，不表示 Consumer 已完成数据库写入。
	Status string `json:"status"`
}

// MessagePublisher 是所有业务共用的消息发布器。
// body 可以是任意可被 JSON 编码的值，不要求实现统一接口；Topic 和 key 是路由元数据，由调用方传入。
type MessagePublisher struct {
	publisher     EventPublisher
	allowedTopics map[string]struct{}
}

func NewMessagePublisher(publisher EventPublisher, allowedTopics []string) *MessagePublisher {
	allowed := make(map[string]struct{}, len(allowedTopics))
	for _, topic := range allowedTopics {
		if topic = strings.TrimSpace(topic); topic != "" {
			allowed[topic] = struct{}{}
		}
	}
	return &MessagePublisher{publisher: publisher, allowedTopics: allowed}
}

func (p *MessagePublisher) Publish(ctx context.Context, topic string, key []byte, body any) error {
	if p == nil || p.publisher == nil {
		return errors.New("message publisher is not configured")
	}
	topic = strings.TrimSpace(topic)
	if _, ok := p.allowedTopics[topic]; !ok {
		return fmt.Errorf("%w: %s", ErrEventTopicNotAllowed, topic)
	}
	if len(key) == 0 {
		return fmt.Errorf("%w: message key is required", ErrInvalidArgument)
	}
	if body == nil {
		return fmt.Errorf("%w: message body is required", ErrInvalidArgument)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal message body: %w", err)
	}
	return p.publisher.Publish(ctx, Event{
		Topic:   topic,
		Key:     append([]byte(nil), key...),
		Payload: payload,
	})
}

// EventService 校验调用方选择的 Topic，并协调可靠发布。
// allowedTopics 是明确白名单，避免调用方借助公共 API 向任意内部 Topic 写入消息。
type EventService struct {
	publisher     EventPublisher
	allowedTopics map[string]struct{}
}

func NewEventService(publisher EventPublisher, allowedTopics []string) *EventService {
	allowed := make(map[string]struct{}, len(allowedTopics))
	for _, topic := range allowedTopics {
		if topic = strings.TrimSpace(topic); topic != "" {
			allowed[topic] = struct{}{}
		}
	}
	return &EventService{publisher: publisher, allowedTopics: allowed}
}

func (s *EventService) Publish(ctx context.Context, event Event) error {
	event.Topic = strings.TrimSpace(event.Topic)
	if event.Topic == "" {
		return fmt.Errorf("%w: topic is required", ErrInvalidArgument)
	}
	if _, ok := s.allowedTopics[event.Topic]; !ok {
		return fmt.Errorf("%w: %s", ErrEventTopicNotAllowed, event.Topic)
	}
	if len(event.Payload) == 0 {
		return fmt.Errorf("%w: payload is required", ErrInvalidArgument)
	}
	if s.publisher == nil {
		return errors.New("event publisher is not configured")
	}
	return s.publisher.Publish(ctx, event)
}
