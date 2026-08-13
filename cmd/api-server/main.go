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

	// NewAPI 只创建 Kafka Producer；独立 work 进程负责消费消息，因此 API
	// 不加入 Consumer Group，也不会运行后台消费 goroutine。
	application, err := composition.NewAPI(context.Background(), cfg, log)
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
	appserver.RegisterHTTPRoutes(httpServer, appserver.HTTPServices{
		User:      application.Services.UserService,
		Publisher: application.Services.MessagePublisher,
		Event:     application.Services.EventService,
	})

	log.Info("hertz http server listening", "addr", cfg.HTTP.Addr)
	httpServer.Spin()
}
