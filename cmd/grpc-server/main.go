package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	// 通过 init hook 注册 Kitex gzip protobuf codec。blank import 让压缩的
	// gRPC payload 可用，同时不让 composition root 依赖 codec 实现细节。
	_ "github.com/cloudwego/kitex/pkg/remote/codec/protobuf/encoding/gzip"
	kitexserver "github.com/cloudwego/kitex/server"

	"github.com/luck/go-learning/api/gen/userservice"
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
	addr, err := net.ResolveTCPAddr("tcp", cfg.GRPC.Addr)
	if err != nil {
		log.Error("resolve kitex address", "error", err)
		os.Exit(1)
	}

	// 两个入口使用同一套装配规则，但每个进程各自创建并持有自己的 pool 和
	// UserService。本入口只围绕这些依赖协调 Kitex 的启动与优雅关闭。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	application, err := composition.New(ctx, cfg)
	if err != nil {
		log.Error("initialize application", "error", err)
		os.Exit(1)
	}
	defer application.Close()

	rpcServer := userservice.NewServer(
		appserver.NewKitexUserServer(application.Services.User),
		kitexserver.WithServiceAddr(addr),
		kitexserver.WithExitWaitTime(10*time.Second),
	)
	serverErr := make(chan error, 1)
	go func() {
		log.Info("kitex grpc server listening", "addr", cfg.GRPC.Addr)
		serverErr <- rpcServer.Run()
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			log.Error("kitex grpc server stopped", "error", err)
			application.Close()
			os.Exit(1)
		}
	case <-ctx.Done():
		// Stop 让 Kitex 排空进行中的请求；之后才执行 App.Close，避免 handler
		// 仍在工作时观察到已关闭的 pool。
		if err := rpcServer.Stop(); err != nil {
			log.Error("shutdown kitex grpc server", "error", err)
		}
	}
}
