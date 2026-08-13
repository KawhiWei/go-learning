package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/luck/go-learning/internal/config"
)

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
}

// Consumer 以一次有界 poll 为处理批次。批次内不同 partition 可以并发，同一
// partition 的 records 始终由一个 worker 按 offset 顺序处理。
type Consumer struct {
	client          consumerClient
	router          *Router
	retryInterval   time.Duration
	shutdownTimeout time.Duration
	workerCount     int
	pollMaxRecords  int
	log             *slog.Logger
	closeOnce       sync.Once
}

func NewConsumer(cfg config.KafkaConfig, router *Router, log *slog.Logger) (*Consumer, error) {
	if router == nil {
		return nil, errors.New("kafka router must not be nil")
	}
	for _, topic := range cfg.Topics {
		if !router.Has(topic) {
			return nil, fmt.Errorf("kafka topic %q has no registered handler", topic)
		}
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ClientID(cfg.ClientID),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return newConsumer(client, cfg, router, log), nil
}

func newConsumer(client consumerClient, cfg config.KafkaConfig, router *Router, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		client: client, router: router, log: log,
		retryInterval:   time.Duration(cfg.RetryIntervalSecs) * time.Second,
		shutdownTimeout: time.Duration(cfg.ShutdownTimeoutSecs) * time.Second,
		workerCount:     cfg.WorkerConcurrency, pollMaxRecords: cfg.PollMaxRecords,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollRecords(ctx, c.pollMaxRecords)
		if ctx.Err() != nil {
			// PollRecords 之后必须解除 BlockRebalanceOnPoll，否则 Close 会等待。
			c.client.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			// franz-go 会自动刷新 metadata、重建连接并重试可恢复的 broker 错误。
			c.log.Error("poll kafka partition", "topic", topic, "partition", partition, "error", err)
		})
		records := fetches.Records()
		if len(records) == 0 {
			c.client.AllowRebalance()
			continue
		}

		// signal 到达后给当前批次一个独立的收尾窗口，而不是立刻取消 Handler。
		batchCtx, finishBatch := batchContext(ctx, c.shutdownTimeout)
		committable, err := c.processBatch(batchCtx, records)
		if len(committable) > 0 {
			if commitErr := c.commitWithRetry(batchCtx, committable); commitErr != nil && err == nil {
				err = commitErr
			}
		}
		finishBatch()
		c.client.AllowRebalance()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

type partitionKey struct {
	topic     string
	partition int32
}

type partitionJob struct {
	records []*kgo.Record
}

type partitionResult struct {
	last *kgo.Record
	err  error
}

func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record) ([]*kgo.Record, error) {
	// 并发池按 Topic+Partition 分 job，而不是按单条消息分 job。Kafka 只保证
	// partition 内顺序；若把同一 partition 的消息交给多个 goroutine，后到
	// 的 offset 可能先完成并被提交，进程崩溃后中间失败消息将无法重投。
	grouped := make(map[partitionKey][]*kgo.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		grouped[key] = append(grouped[key], record)
	}
	workers := c.workerCount
	if workers < 1 {
		workers = 1
	}
	if workers > len(grouped) {
		workers = len(grouped)
	}
	// 实际 goroutine 数取 min(worker_concurrency, 当前批次 partition 数)。
	// 因此把配置调到远高于 partition 数不会提高吞吐，只会提高潜在资源上限。
	// Handler 会访问数据库时，还应满足 worker_concurrency <= DB max_conns，
	// 并给 HTTP/gRPC 查询预留连接，否则 Worker 会把连接池占满并放大延迟。
	jobs := make(chan partitionJob)
	results := make(chan partitionResult, len(grouped))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			// Worker goroutine 只在当前 poll 批次存活。批次结束后 WaitGroup
			// 确认全部退出，避免每次 PollRecords 都累积后台 goroutine。
			defer wg.Done()
			for job := range jobs {
				var err error
				for _, record := range job.records {
					if err = c.handleWithRetry(ctx, record); err != nil {
						break
					}
				}
				result := partitionResult{err: err}
				if err == nil {
					result.last = job.records[len(job.records)-1]
				}
				results <- result
			}
		}()
	}
	go func() {
		// 单独的 dispatcher 负责关闭 jobs、等待 worker，再关闭 results。
		// results 使用 partition 数作为 buffer，worker 即使先完成也不会因
		// 主 goroutine 尚未开始收集而互相阻塞，降低尾延迟。
		for _, partitionRecords := range grouped {
			jobs <- partitionJob{records: partitionRecords}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	committable := make([]*kgo.Record, 0, len(grouped))
	var firstErr error
	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		if result.last != nil {
			committable = append(committable, result.last)
		}
	}
	return committable, firstErr
}

func (c *Consumer) handleWithRetry(ctx context.Context, record *kgo.Record) error {
	message := toMessage(record)
	for {
		if err := c.router.Handle(ctx, message); err != nil {
			c.log.Error("handle kafka message", "topic", record.Topic, "partition", record.Partition,
				"offset", record.Offset, "key_size", len(record.Key), "value_size", len(record.Value), "error", err)
			if err := waitForRetry(ctx, c.retryInterval); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (c *Consumer) commitWithRetry(ctx context.Context, records []*kgo.Record) error {
	for {
		if err := c.client.CommitRecords(ctx, records...); err != nil {
			c.log.Error("commit kafka offsets", "partitions", len(records), "error", err)
			if err := waitForRetry(ctx, c.retryInterval); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (c *Consumer) Close() {
	if c == nil || c.client == nil {
		return
	}
	c.closeOnce.Do(c.client.CloseAllowingRebalance)
}

func batchContext(signalCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-signalCtx.Done():
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancel()
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func toMessage(record *kgo.Record) Message {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = append([]byte(nil), header.Value...)
	}
	return Message{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...),
		Headers: headers, Timestamp: record.Timestamp}
}

func waitForRetry(ctx context.Context, interval time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
