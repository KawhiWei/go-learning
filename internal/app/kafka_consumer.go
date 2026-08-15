package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
	consumerroot "github.com/luck/go-learning/internal/consumer"
	auditconsumer "github.com/luck/go-learning/internal/consumer/audit"
	userconsumer "github.com/luck/go-learning/internal/consumer/user"
	kafkadata "github.com/luck/go-learning/internal/data/kafka"
)

// WithKafkaConsumer 为进程附加 Kafka 消费能力。user-events 通过已经由 New
// 装配好的 UserService -> UserRepository 写入 PostgreSQL。
// 每个业务的 Handler 位于对应的 internal/consumer 子包；尚未接入业务的
// Topic 使用 audit 包提供的元数据日志 Handler。HTTP/gRPC server 不会在
// Consumer 进程中创建。

func WithKafkaConsumer(log *slog.Logger) Option {
	return func(_ context.Context, cfg config.Config, application *App) error {
		if !cfg.Kafka.Enabled {
			return fmt.Errorf("kafka must be enabled for consumer process")
		}
		if application.kafkaConsumer != nil {
			return fmt.Errorf("kafka consumer is already configured")
		}

		router := consumerroot.NewRouter()
		for _, topic := range cfg.Kafka.Topics {
			var handler consumerroot.Handler = auditconsumer.NewMetadataLoggingHandler(log)
			if topic == biz.UserCreateTopic {
				handler = userconsumer.NewCreateHandler(application.Services.UserService, log)
			}
			if err := router.Register(topic, handler); err != nil {
				return fmt.Errorf("register kafka topic %q: %w", topic, err)
			}
		}
		consumer, err := kafkadata.NewConsumer(cfg.Kafka, router, log)
		if err != nil {
			return err
		}
		application.kafkaConsumer = consumer
		return nil
	}
}

// RunConsumer 阻塞运行本进程配置的 Consumer。Kafka 未启用时直接返回。
func (a *App) RunConsumer(ctx context.Context) error {
	if a == nil || a.kafkaConsumer == nil {
		return nil
	}
	return a.kafkaConsumer.Run(ctx)
}
