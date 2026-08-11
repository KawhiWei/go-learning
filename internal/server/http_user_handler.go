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
)

const maxUserRequestBodySize = 1 << 20

type createUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserHTTPHandler 负责 user 模块的 HTTP 协议细节：把 HTTP 请求转换成
// business service 调用，再把结果转换回 HTTP 响应；属于用例的校验仍由
// UserService 负责。
type UserHTTPHandler struct {
	Service UserService
}

// Create 处理 POST /v1/users。decoder 会拒绝未知字段和第二个 JSON 值，避免
// 客户端误把只完成一部分解析的 body 当作一次有效请求。
func (h UserHTTPHandler) Create(ctx context.Context, c *app.RequestContext) {
	body, err := c.Body()
	if err != nil || len(body) > maxUserRequestBodySize {
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

	user, err := h.Service.CreateUser(ctx, req.Name, req.Email)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	if user == nil {
		writeError(c, consts.StatusInternalServerError, "internal", "internal server error")
		return
	}
	writeJSON(c, consts.StatusCreated, toUserResponse(user))
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
