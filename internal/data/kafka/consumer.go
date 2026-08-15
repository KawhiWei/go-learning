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
	"github.com/luck/go-learning/internal/consumer"
)

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
}

// Consumer 以一次有界 poll 为处理批次。
// 不同 partition 可以并发处理；同一 partition 的 records 始终由一个处理 goroutine 按 offset 顺序处理。
//
// 每个批次包含三类 goroutine：
//   - 调用 processBatch 的主 goroutine：收集处理结果，并决定哪些 offset 可以提交。
//   - 固定数量的处理 goroutine：从 jobs channel 领取一个 partition 的全部 records，并顺序调用 Handler。
//   - 一个 dispatcher goroutine：发送 partition job，关闭 jobs，等待全部处理 goroutine 退出，最后关闭 results。
//
// 这些 goroutine 只在当前批次内存活，不会跨 PollRecords 调用复用或累积。
type Consumer struct {
	client          consumerClient
	router          consumer.TopicRouter
	retryInterval   time.Duration
	shutdownTimeout time.Duration
	concurrency     int
	pollMaxRecords  int
	log             *slog.Logger
	closeOnce       sync.Once
}

func NewConsumer(cfg config.KafkaConfig, router consumer.TopicRouter, log *slog.Logger) (*Consumer, error) {
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

func newConsumer(client consumerClient, cfg config.KafkaConfig, router consumer.TopicRouter, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		client: client, router: router, log: log,
		retryInterval:   time.Duration(cfg.RetryIntervalSecs) * time.Second,
		shutdownTimeout: time.Duration(cfg.ShutdownTimeoutSecs) * time.Second,
		concurrency:     cfg.ConsumerConcurrency, pollMaxRecords: cfg.PollMaxRecords,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		// PollRecords 在当前 goroutine 中阻塞，不会为每次轮询额外创建应用层 goroutine。
		// franz-go 在客户端内部维护网络 I/O goroutine；调用方通过 ctx 结束本次阻塞轮询。
		fetches := c.client.PollRecords(ctx, c.pollMaxRecords)
		if ctx.Err() != nil {
			// 启用 BlockRebalanceOnPoll 后，每次 PollRecords 返回都必须调用 AllowRebalance。
			// 即使 ctx 已取消也不能省略，否则 CloseAllowingRebalance 可能一直等待本批次释放分区。
			c.client.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			// franz-go 会自动刷新 metadata、重建连接，并重试可以恢复的 broker 错误。
			c.log.Error("poll kafka partition", "topic", topic, "partition", partition, "error", err)
		})
		records := fetches.Records()
		if len(records) == 0 {
			c.client.AllowRebalance()
			continue
		}

		// batchCtx 不会在进程信号到达时立刻取消，而是给当前批次一个独立的收尾窗口。
		// 这样正在执行的 Handler 有机会完成，成功结果也有机会在进程退出前提交 offset。
		batchCtx, finishBatch := batchContext(ctx, c.shutdownTimeout)
		committable, err := c.processBatch(batchCtx, records)
		if len(committable) > 0 {
			if commitErr := c.commitWithRetry(batchCtx, committable); commitErr != nil && err == nil {
				err = commitErr
			}
		}
		// processBatch 和 commitWithRetry 返回后，批次内处理 goroutine 已全部退出，此时取消监督 goroutine 是安全的。
		finishBatch()
		// offset 处理结束后再允许 rebalance，避免分区在 Handler 运行期间被转交给其他 Consumer。
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
	// 并发池以 Topic+Partition 为 job 粒度，而不是以单条消息为 job 粒度。
	// Kafka 只保证 partition 内顺序；如果同一 partition 由多个 goroutine 同时处理，较大的 offset 可能先成功并被提交。
	// 一旦较大的 offset 被提交，进程崩溃后位于它之前的失败消息将不会再次投递，因此这里必须先按 partition 分组。
	grouped := make(map[partitionKey][]*kgo.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		grouped[key] = append(grouped[key], record)
	}
	concurrency := c.concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(grouped) {
		concurrency = len(grouped)
	}
	// 实际处理 goroutine 数取 min(consumer_concurrency, 当前批次包含的 partition 数)。
	// consumer_concurrency 高于 partition 数不会增加并行度，因为同一 partition 不能拆给多个 goroutine。
	// Handler 访问数据库时，consumer_concurrency 通常不应高于 DB max_conns，并且还要为其他数据库操作预留连接。
	// jobs 不设置缓冲，使 dispatcher 只在某个处理 goroutine 准备好接收时才继续发送，避免预先堆积重复的任务引用。
	jobs := make(chan partitionJob)
	// 每个 partition 最多产生一个结果。缓冲区容量等于 partition 数，可以保证处理 goroutine 上报结果时不会等待主 goroutine 开始读取。
	results := make(chan partitionResult, len(grouped))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			// 每个处理 goroutine 只在当前 poll 批次存活，并在 jobs 关闭且队列耗尽后退出。
			// Done 放在 defer 中，确保处理 goroutine 无论从哪个正常路径返回，dispatcher 都能观察到它已经结束。
			defer wg.Done()
			for job := range jobs {
				// 一个 job 只包含同一 Topic+Partition 的消息，因此这个循环天然保持 offset 顺序。
				// 当前消息持续失败时，handleWithRetry 会阻塞这个 partition，但不会阻止其他处理 goroutine 处理其他 partition。
				var err error
				for _, record := range job.records {
					if err = c.handleWithRetry(ctx, record); err != nil {
						break
					}
				}
				result := partitionResult{err: err}
				if err == nil {
					// 只有整个 partition job 全部成功时才返回最后一条 record，防止越过中间失败的 offset。
					result.last = job.records[len(job.records)-1]
				}
				// results 有足够容量容纳所有 partition 的结果，因此处理 goroutine 不会在此处与结果收集形成循环等待。
				results <- result
			}
		}()
	}
	go func() {
		// dispatcher goroutine 是 jobs 的唯一发送方和关闭方，因此不会发生重复关闭 channel 的竞争。
		for _, partitionRecords := range grouped {
			jobs <- partitionJob{records: partitionRecords}
		}
		// 关闭 jobs 告诉处理 goroutine 不会再有新任务；已经发送的任务仍会处理完。
		close(jobs)
		// 必须等待所有处理 goroutine 停止写 results，才能关闭 results，否则可能向已关闭的 channel 发送并触发 panic。
		wg.Wait()
		// 主 goroutine 使用 range 读取 results；关闭 channel 是通知它“所有 partition 都已返回结果”的唯一结束信号。
		close(results)
	}()

	committable := make([]*kgo.Record, 0, len(grouped))
	var firstErr error
	// 当前 goroutine 持续读取 results，直到 dispatcher 在所有处理 goroutine 退出后关闭 channel。
	// 成功 partition 的最后一条 record 可以提交；失败 partition 不加入 committable，因此不会推进它的 offset。
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
		// 重试发生在负责该 partition 的处理 goroutine 内，所以重试期间仍然保持分区顺序。
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
		// CommitRecords 失败时只重试提交，不重新执行已经成功的 Handler。
		// records 中每个 partition 只有最后一条成功 record，franz-go 会提交它之后的下一个 offset。
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
	// sync.Once 允许多个退出路径安全调用 Close，并确保底层客户端只关闭一次。
	c.closeOnce.Do(c.client.CloseAllowingRebalance)
}

func batchContext(signalCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// 使用 Background 派生批次上下文，避免 signalCtx 取消时立即中断在途 Handler。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// 这个监督 goroutine 只等待两个事件：进程退出信号，或者批次主动完成。
		// 批次主动完成时，外层调用 finishBatch（即 cancel），监督 goroutine 会立即退出，不会泄漏到下一次 poll。
		select {
		case <-signalCtx.Done():
			// 收到退出信号后开始计时；超时前允许 Handler 和 offset 提交继续执行。
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				// 超过关闭窗口后取消批次，唤醒 Handler 重试等待和 offset 提交重试。
				cancel()
			case <-ctx.Done():
				// 批次在超时前完成，直接退出监督 goroutine。
			}
		case <-ctx.Done():
			// 正常处理完成时由 finishBatch 触发，无需启动关闭计时器。
		}
	}()
	return ctx, cancel
}

func toMessage(record *kgo.Record) consumer.Message {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = append([]byte(nil), header.Value...)
	}
	return consumer.Message{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
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
