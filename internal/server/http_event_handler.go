package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/luck/go-learning/internal/biz"
)

type publishEventRequest struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

type publishEventResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
}

// EventHTTPHandler 相当于 Event Controller：解析 HTTP DTO 后调用业务 Service，
// 不直接持有或操作 Kafka Producer。
type EventHTTPHandler struct {
	Service EventService
}

// Publish 在 broker 确认消息后返回 202。202 表示消息已经可靠交给 Kafka，
// 不表示下游 Worker 已经完成业务处理。
func (h EventHTTPHandler) Publish(ctx context.Context, c *app.RequestContext) {
	body, err := c.Body()
	if err != nil || len(body) > maxJSONRequestBodySize {
		writeError(c, consts.StatusBadRequest, "invalid_argument", "request body must be valid JSON")
		return
	}
	var req publishEventRequest
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
	if err := h.Service.Publish(ctx, biz.Event{
		Topic: c.Param("topic"), Key: []byte(req.Key), Payload: req.Payload,
	}); err != nil {
		if errors.Is(err, biz.ErrInvalidArgument) || errors.Is(err, biz.ErrEventTopicNotAllowed) {
			writeError(c, consts.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
		writeError(c, consts.StatusServiceUnavailable, "publish_unavailable", "event could not be published")
		return
	}
	writeJSON(c, consts.StatusAccepted, publishEventResponse{Status: "accepted", Topic: c.Param("topic")})
}
