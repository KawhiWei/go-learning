package server

import (
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/luck/go-learning/internal/biz"
)

type userResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func toUserResponse(user *biz.User) userResponse {
	return userResponse{
		ID:        user.ID.String(),
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: user.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
}

// writeServiceError 集中维护业务错误到 HTTP 语义的映射。它与响应 helper
// 放在一起，确保所有模块路由输出统一的公开 error envelope。
func writeServiceError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, biz.ErrInvalidArgument):
		writeError(c, consts.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, biz.ErrAlreadyExists):
		writeError(c, consts.StatusConflict, "already_exists", "user already exists")
	case errors.Is(err, biz.ErrNotFound):
		writeError(c, consts.StatusNotFound, "not_found", "user not found")
	default:
		writeError(c, consts.StatusInternalServerError, "internal", "internal server error")
	}
}

func writeError(c *app.RequestContext, status int, code, message string) {
	writeJSON(c, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(c *app.RequestContext, status int, value interface{}) {
	c.JSON(status, value)
}
