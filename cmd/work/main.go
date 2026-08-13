package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	composition "github.com/luck/go-learning/internal/app"
	"github.com/luck/go-learning/internal/config"
	"github.com/luck/go-learning/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

// run 是 Worker 进程唯一的生命周期入口：加载配置、装配 Consumer、监听
// SIGINT/SIGTERM，并在退出前等待 Consumer 停止在途任务和离开消费组。
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

	worker, err := composition.NewWorker(context.Background(), cfg, log)
	if err != nil {
		log.Error("initialize kafka worker", "error", err)
		return err
	}
	defer worker.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("kafka worker started",
		"group_id", cfg.Kafka.GroupID,
		"topics", cfg.Kafka.Topics,
		"concurrency", cfg.Kafka.WorkerConcurrency,
	)
	if err := worker.RunConsumers(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("run kafka worker", "error", err)
		return err
	}
	log.Info("kafka worker stopped")
	return nil
}
