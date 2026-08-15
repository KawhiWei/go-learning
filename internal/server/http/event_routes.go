package httpserver

import hertz "github.com/cloudwego/hertz/pkg/app/server"

func registerEventRoutes(server *hertz.Hertz, service EventService) {
	if service == nil {
		return
	}
	handler := EventHTTPHandler{Service: service}
	server.POST("/v1/events/:topic", handler.Publish)
}
