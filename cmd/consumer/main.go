package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	composition "github.com/luck/go-learning/internal/app"
	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
	"github.com/luck/go-learning/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

// run 是 Kafka Consumer 进程唯一的生命周期入口。
// 它负责加载配置、装配 Consumer、监听 SIGINT/SIGTERM，并在退出前等待
// Consumer 停止在途任务和离开消费组。
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

	consumer, err := composition.New(
		context.Background(), cfg,
		composition.RequireKafkaTopics(biz.UserCreateTopic),
		composition.WithKafkaConsumer(log),
	)
	if err != nil {
		log.Error("initialize kafka consumer", "error", err)
		return err
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("kafka consumer started",
		"group_id", cfg.Kafka.GroupID,
		"topics", cfg.Kafka.Topics,
		"concurrency", cfg.Kafka.ConsumerConcurrency,
	)
	if err := consumer.RunConsumer(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("run kafka consumer", "error", err)
		return err
	}
	log.Info("kafka consumer stopped")
	return nil
}
