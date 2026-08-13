package kafka

import (
	"context"
	"log/slog"
)

// NewMetadataLoggingHandler 返回一个基础设施占位 Handler，便于尚未接入真实
// 业务逻辑时验证多 Topic 路由和 offset 提交。它只记录消息元数据，不读取或
// 输出 Value，避免业务内容和敏感数据进入日志。
func NewMetadataLoggingHandler(log *slog.Logger) Handler {
	if log == nil {
		log = slog.Default()
	}
	return HandlerFunc(func(_ context.Context, message Message) error {
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
