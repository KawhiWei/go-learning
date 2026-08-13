package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
)

const maxJSONRequestBodySize = 1 << 20

type createUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserHTTPHandler 负责 user 模块的 HTTP 协议细节。
// 它把 HTTP 请求转换成 business service 调用，再把结果转换回 HTTP 响应；属于用例的校验仍由 UserService 负责。
type UserHTTPHandler struct {
	Service   UserService
	Publisher MessagePublisher
}

// Create 处理 POST /v1/users。
// API 不再直接调用 UserService.CreateUser 写库，而是把创建消息发布到 user-events；数据库写入发生在独立 work Consumer。
// decoder 会拒绝未知字段和第二个 JSON 值，避免只完成部分解析的请求入队。
func (h UserHTTPHandler) Create(ctx context.Context, c *app.RequestContext) {
	body, err := c.Body()
	if err != nil || len(body) > maxJSONRequestBodySize {
		writeError(c, consts.StatusBadRequest, "invalid_argument", "request body must be valid JSON")
		return
	}

	var req createUserRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(c, consts.StatusBadRequest, "invalid_argument", "request body must be valid JSON")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(c, consts.StatusBadRequest, "invalid_argument", "request body must contain one JSON object")
		return
	}

	if h.Publisher == nil {
		writeError(c, consts.StatusServiceUnavailable, "publish_unavailable", "message publisher is unavailable")
		return
	}
	// UserCreateMessage 只是生产者与消费者约定的消息体，不需要实现公共接口。
	// 统一 Publisher 负责 JSON 编码、Topic 白名单和底层发送。
	message, err := biz.NewUserCreateMessage(req.Name, req.Email)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	// Topic 决定消息去往哪个业务队列。
	// 相同 user_id 使用相同 key，可以让 Kafka 把同一用户的消息稳定路由到同一 partition 并保持处理顺序。
	if err := h.Publisher.Publish(ctx, biz.UserCreateTopic, []byte(message.UserID), message); err != nil {
		writeError(c, consts.StatusServiceUnavailable, "publish_unavailable", "user creation could not be queued")
		return
	}
	writeJSON(c, consts.StatusAccepted, biz.PublishAccepted{
		MessageID:  message.MessageID,
		ResourceID: message.UserID,
		Status:     "accepted",
	})
}

// Get 处理 GET /v1/users/:id。路由匹配已经提取参数，因此格式错误的
// identifier 会直接作为客户端输入错误返回，不会进入业务 service。
func (h UserHTTPHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, consts.StatusBadRequest, "invalid_argument", "id must be a valid UUID")
		return
	}

	user, err := h.Service.GetUser(ctx, id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	if user == nil {
		writeError(c, consts.StatusInternalServerError, "internal", "internal server error")
		return
	}
	writeJSON(c, consts.StatusOK, toUserResponse(user))
}
