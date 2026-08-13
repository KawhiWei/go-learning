package biz

import (
	"github.com/google/uuid"
)

const (
	// UserCreateTopic 是创建用户消息固定使用的 Kafka Topic。HTTP Handler 不接受
	// 外部传入 Topic，避免调用者绕过业务边界向其他 Topic 发布消息。
	UserCreateTopic = "user-events"

	// UserCreateMessageType 是创建用户消息的稳定版本标识。消费者可以据此在
	// 同一个 Topic 中区分事件类型，并在协议升级时继续兼容旧版本。
	UserCreateMessageType = "user.create.v1"
)

// UserCreateMessage 是 user.create.v1 的稳定 JSON 消息体。它是普通 DTO，
// 不需要实现 Publisher 接口；所有业务 DTO 都能共用 MessagePublisher。
//
// UUID 在消息协议中显式使用字符串，而不是依赖 uuid.UUID 的内部编码或
// encoding.TextMarshaler 行为。这样事件在不同语言的消费者之间仍有清晰、
// 可读且长期稳定的 JSON 表示。
type UserCreateMessage struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Email     string `json:"email"`
}

// NewUserCreateMessage 只负责本业务校验和 ID 生成。返回的普通 struct 可直接
// 作为 MessagePublisher.Publish 的 body 参数。
func NewUserCreateMessage(name, email string) (UserCreateMessage, error) {
	name, email, err := validateUser(name, email)
	if err != nil {
		return UserCreateMessage{}, err
	}

	messageID := uuid.New()
	userID := uuid.New()
	return UserCreateMessage{
		MessageID: messageID.String(), UserID: userID.String(),
		Type: UserCreateMessageType, Name: name, Email: email,
	}, nil
}
