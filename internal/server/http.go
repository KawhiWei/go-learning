package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
)

// UserService 是 HTTP 和 RPC transport 共同使用的用例边界。handler 不关心
// user 如何持久化，这一职责由业务层和 repository 层隐藏在接口之后。
type UserService interface {
	CreateUser(context.Context, string, string) (*biz.User, error)
	GetUser(context.Context, uuid.UUID) (*biz.User, error)
}

// HTTPServices 是 HTTP 进程向外暴露的业务 service 集合。总路由接收这个
// struct，未来增加模块时可以扩展字段和模块路由，不必让模块彼此耦合或
// 反复改变构造函数形状。
type HTTPServices struct {
	User UserService
}

// NewHTTPServer 创建 Hertz server 和 transport 级 fallback 路由。
//
// 业务路由由调用方在 composition root 中通过 RegisterHTTPRoutes 显式注册，
// 这样 server 构造不会隐式持有或创建任何业务 service。
func NewHTTPServer(opts ...config.Option) *hertz.Hertz {
	opts = append(opts, hertz.WithHandleMethodNotAllowed(true))
	h := hertz.Default(opts...)
	h.NoMethod(func(_ context.Context, c *app.RequestContext) {
		writeError(c, consts.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})
	h.NoRoute(func(_ context.Context, c *app.RequestContext) {
		writeError(c, consts.StatusNotFound, "not_found", "resource not found")
	})
	return h
}
