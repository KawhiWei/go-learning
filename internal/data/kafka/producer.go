package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
)

type producerClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Producer 等待 Kafka 对每条消息的确认。franz-go 默认启用幂等 Producer，
// 并自动处理 broker 连接恢复、metadata 更新和可重试请求。
type Producer struct {
	client  producerClient
	timeout time.Duration
	log     *slog.Logger
	close   sync.Once
}

func NewProducer(cfg config.KafkaConfig, log *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(10),
		kgo.ProduceRequestTimeout(10*time.Second),
		kgo.RecordDeliveryTimeout(time.Duration(cfg.PublishTimeoutSecs)*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Producer{client: client, timeout: time.Duration(cfg.PublishTimeoutSecs) * time.Second, log: log}, nil
}

func (p *Producer) Publish(ctx context.Context, event biz.Event) error {
	if p == nil || p.client == nil {
		return errors.New("kafka producer is not initialized")
	}
	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	results := p.client.ProduceSync(publishCtx, &kgo.Record{
		Topic: event.Topic, Key: append([]byte(nil), event.Key...), Value: append([]byte(nil), event.Payload...),
		Timestamp: time.Now().UTC(),
	})
	if err := results.FirstErr(); err != nil {
		p.log.Error("publish kafka message", "topic", event.Topic, "key_size", len(event.Key),
			"value_size", len(event.Payload), "error", err)
		return fmt.Errorf("publish kafka event: %w", err)
	}
	return nil
}

func (p *Producer) Close() {
	if p == nil || p.client == nil {
		return
	}
	p.close.Do(p.client.Close)
}
