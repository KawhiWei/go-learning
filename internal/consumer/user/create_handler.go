package userconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/consumer"
)

// UserCreator 是 user-events Handler 所需的最小业务接口。
// Kafka 数据层只依赖此能力，不知道 Repository、SQL 或连接池的存在。
type UserCreator interface {
	CreateUserWithID(context.Context, uuid.UUID, string, string) (*biz.User, error)
}

// NewCreateHandler 把 user.create.v1 事件转换成 UserService 调用。
// 只有 Service 成功（包括相同 user_id 的幂等重投）才返回 nil，Consumer 才提交 offset。
// 数据库临时故障会返回 error，并触发当前 partition 重试。
func NewCreateHandler(service UserCreator, log *slog.Logger) consumer.Handler {
	if log == nil {
		log = slog.Default()
	}
	return consumer.HandlerFunc(func(ctx context.Context, message consumer.Message) error {
		if service == nil {
			return errors.New("user creator must not be nil")
		}
		var event biz.UserCreateMessage
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("decode user create event: %w", err)
		}
		if event.Type != biz.UserCreateMessageType {
			return fmt.Errorf("unsupported user event type %q", event.Type)
		}
		if _, err := uuid.Parse(event.MessageID); err != nil {
			return fmt.Errorf("parse message ID: %w", err)
		}
		userID, err := uuid.Parse(event.UserID)
		if err != nil {
			return fmt.Errorf("parse user ID: %w", err)
		}
		if string(message.Key) != event.UserID {
			return fmt.Errorf("kafka key %q does not match user ID %q", message.Key, event.UserID)
		}
		if _, err := service.CreateUserWithID(ctx, userID, event.Name, event.Email); err != nil {
			return fmt.Errorf("create user from message %s: %w", event.MessageID, err)
		}
		log.Info("user created from kafka event",
			"message_id", event.MessageID,
			"user_id", event.UserID,
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
		)
		return nil
	})
}
