package main

import (
	"context"
	"net"
	"os"
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
	if err := run(); err != nil {
		os.Exit(1)
	}
}

// run 负责完整的进程生命周期。把实际启动逻辑放在独立函数中，可以保证
// rpcServer.Run 返回错误时先执行 defer 释放数据库连接池，之后 main 再用
// 非零状态码退出进程。
func run() error {
	log := logger.New(os.Getenv("NINO_LOG_LEVEL"))
	configPath := os.Getenv("NINO_CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/app.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Error("load config", "error", err)
		return err
	}
	log = logger.New(cfg.Logger.Level)
	addr, err := net.ResolveTCPAddr("tcp", cfg.GRPC.Addr)
	if err != nil {
		log.Error("resolve kitex address", "error", err)
		return err
	}

	// 两个入口使用同一套装配规则，但每个进程各自创建并持有自己的 pool 和
	// UserService。Run 返回后再关闭 application，避免进行中的 RPC 观察到已
	// 关闭的数据库连接池。
	application, err := composition.New(context.Background(), cfg)
	if err != nil {
		log.Error("initialize application", "error", err)
		return err
	}
	defer application.Close()

	rpcServer := userservice.NewServer(
		appserver.NewKitexUserServer(application.Services.UserService),
		kitexserver.WithServiceAddr(addr),
		kitexserver.WithExitWaitTime(10*time.Second),
	)
	log.Info("kitex grpc server listening", "addr", cfg.GRPC.Addr)
	// Kitex Run 是阻塞调用，内部负责监听退出信号、停止接收新请求并执行
	// graceful Stop；入口不再重复维护 signal channel 或手动调用 Stop。
	if err := rpcServer.Run(); err != nil {
		log.Error("run kitex grpc server", "error", err)
		return err
	}
	return nil
}
