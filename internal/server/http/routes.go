package httpserver

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/luck/go-learning/api/openapi"
)

// RegisterHTTPRoutes 是 HTTP 总路由入口，负责 health、文档等公共路由，
// 再把各业务区域交给模块 registrar；总路由不承载具体用例逻辑。
func RegisterHTTPRoutes(server *hertz.Hertz, services HTTPServices) {
	server.GET("/healthz", func(_ context.Context, c *app.RequestContext) {
		c.String(consts.StatusOK, "ok\n")
	})

	registerUserRoutes(server, services.User, services.Publisher)
	registerEventRoutes(server, services.Event)

	server.GET("/openapi.yaml", func(_ context.Context, c *app.RequestContext) {
		c.Data(consts.StatusOK, "application/yaml; charset=utf-8", openapi.Spec)
	})
	server.GET("/swagger", serveSwaggerUI)
	server.GET("/swagger/", serveSwaggerUI)
}

func serveSwaggerUI(_ context.Context, c *app.RequestContext) {
	c.Data(consts.StatusOK, "text/html; charset=utf-8", openapi.SwaggerUI)
}
