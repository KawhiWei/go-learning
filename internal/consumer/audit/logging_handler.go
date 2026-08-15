package auditconsumer

import (
	"context"
	"log/slog"

	"github.com/luck/go-learning/internal/consumer"
)

// NewMetadataLoggingHandler 返回一个基础设施占位 Handler，便于尚未接入真实业务逻辑时验证多 Topic 路由和 offset 提交。
// 它只记录消息元数据，不读取或输出 Value，避免业务内容和敏感数据进入日志。
func NewMetadataLoggingHandler(log *slog.Logger) consumer.Handler {
	if log == nil {
		log = slog.Default()
	}
	return consumer.HandlerFunc(func(_ context.Context, message consumer.Message) error {
		log.Info("consume kafka message",
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"key_size", len(message.Key),
			"value_size", len(message.Value),
		)
		return nil
	})
}
