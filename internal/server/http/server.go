package httpserver

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
)

// UserService 是 HTTP 查询用户所需的最小业务边界。创建用户接口通过消息
// Publisher 异步处理，因此 HTTP transport 不依赖同步 CreateUser 方法。
type UserService interface {
	GetUser(context.Context, uuid.UUID) (*biz.User, error)
}

// MessagePublisher 是所有 HTTP 业务共用的消息发布边界。业务 Handler 传入
// Topic、Kafka key 和实际消息体；消息体不需要实现任何公共接口。
type MessagePublisher interface {
	Publish(context.Context, string, []byte, any) error
}

// EventService 是 HTTP 发布消息时依赖的业务边界。Handler 只负责协议解析，
// Topic 白名单和消息发布由业务层处理。
type EventService interface {
	Publish(context.Context, biz.Event) error
}

// HTTPServices 是 HTTP 进程向外暴露的业务 service 集合。总路由接收这个
// struct，未来增加模块时可以扩展字段和模块路由，不必让模块彼此耦合或
// 反复改变构造函数形状。
type HTTPServices struct {
	User      UserService
	Publisher MessagePublisher
	Event     EventService
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
