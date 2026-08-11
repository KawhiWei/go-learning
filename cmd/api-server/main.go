package main

import (
	"context"
	"os"
	"time"

	hertz "github.com/cloudwego/hertz/pkg/app/server"

	composition "github.com/luck/go-learning/internal/app"
	"github.com/luck/go-learning/internal/config"
	appserver "github.com/luck/go-learning/internal/server"
	"github.com/luck/go-learning/pkg/logger"
)

func main() {
	log := logger.New(os.Getenv("NINO_LOG_LEVEL"))
	configPath := os.Getenv("NINO_CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/app.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	log = logger.New(cfg.Logger.Level)

	// App.New 负责本进程唯一一次的 database pool 装配；Hertz Spin 负责
	// HTTP server 的信号监听和优雅关闭。
	application, err := composition.New(context.Background(), cfg)
	if err != nil {
		log.Error("initialize application", "error", err)
		os.Exit(1)
	}
	defer application.Close()

	httpServer := appserver.NewHTTPServer(
		hertz.WithHostPorts(cfg.HTTP.Addr),
		hertz.WithMaxRequestBodySize(1<<20),
		hertz.WithExitWaitTime(10*time.Second),
	)
	appserver.RegisterHTTPRoutes(httpServer, appserver.HTTPServices{User: application.Services.UserService})

	log.Info("hertz http server listening", "addr", cfg.HTTP.Addr)
	httpServer.Spin()
}
