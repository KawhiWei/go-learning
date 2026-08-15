package httpserver

import hertz "github.com/cloudwego/hertz/pkg/app/server"

// registerUserRoutes 集中维护 user 模块的 URL surface。handler 仍只接收
// service contract，因此注册路由不会绕过 Handler -> Service -> Repository
// 的调用边界。
func registerUserRoutes(server *hertz.Hertz, service UserService, publisher MessagePublisher) {
	handler := UserHTTPHandler{Service: service, Publisher: publisher}
	users := server.Group("/v1/users")
	users.POST("", handler.Create)
	users.GET("/:id", handler.Get)
}
