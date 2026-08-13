package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/luck/go-learning/internal/config"
)

type fakeConsumerClient struct {
	mu             sync.Mutex
	commits        int
	commitFailures int
	committed      []*kgo.Record
	closed         int
	allowed        int
}

func (*fakeConsumerClient) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	<-ctx.Done()
	return nil
}
func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	if f.commitFailures > 0 {
		f.commitFailures--
		return errors.New("temporary commit failure")
	}
	f.committed = append(f.committed, records...)
	return nil
}
func (f *fakeConsumerClient) AllowRebalance()         { f.allowed++ }
func (f *fakeConsumerClient) CloseAllowingRebalance() { f.closed++ }

func testConsumer(client consumerClient, router *Router, workers int) *Consumer {
	cfg := config.Default().Kafka
	cfg.WorkerConcurrency = workers
	cfg.RetryIntervalSecs = 0
	cfg.ShutdownTimeoutSecs = 1
	return newConsumer(client, cfg, router, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestConsumerPreservesPartitionOrderAndCommitsLastOffsets(t *testing.T) {
	client := &fakeConsumerClient{}
	router := NewRouter()
	var mu sync.Mutex
	got := map[int32][]int64{}
	if err := router.Register("events", HandlerFunc(func(_ context.Context, message Message) error {
		mu.Lock()
		got[message.Partition] = append(got[message.Partition], message.Offset)
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	consumer := testConsumer(client, router, 2)
	records := []*kgo.Record{
		{Topic: "events", Partition: 0, Offset: 1}, {Topic: "events", Partition: 1, Offset: 5},
		{Topic: "events", Partition: 0, Offset: 2}, {Topic: "events", Partition: 1, Offset: 6},
	}
	committable, err := consumer.processBatch(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if len(committable) != 2 {
		t.Fatalf("committable = %d, want 2", len(committable))
	}
	if got[0][0] != 1 || got[0][1] != 2 || got[1][0] != 5 || got[1][1] != 6 {
		t.Fatalf("partition order = %#v", got)
	}
}

func TestConsumerProcessesPartitionsConcurrently(t *testing.T) {
	client := &fakeConsumerClient{}
	router := NewRouter()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	if err := router.Register("events", HandlerFunc(func(context.Context, Message) error {
		started <- struct{}{}
		<-release
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	consumer := testConsumer(client, router, 2)
	done := make(chan error, 1)
	go func() {
		_, err := consumer.processBatch(context.Background(), []*kgo.Record{
			{Topic: "events", Partition: 0}, {Topic: "events", Partition: 1},
		})
		done <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("partitions did not run concurrently")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCommitRetryDoesNotRunHandlerAgain(t *testing.T) {
	client := &fakeConsumerClient{commitFailures: 1}
	router := NewRouter()
	handled := 0
	if err := router.Register("events", HandlerFunc(func(context.Context, Message) error { handled++; return nil })); err != nil {
		t.Fatal(err)
	}
	consumer := testConsumer(client, router, 1)
	committable, err := consumer.processBatch(context.Background(), []*kgo.Record{{Topic: "events"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.commitWithRetry(context.Background(), committable); err != nil {
		t.Fatal(err)
	}
	if handled != 1 || client.commits != 2 {
		t.Fatalf("handled=%d commits=%d", handled, client.commits)
	}
}

func TestHandlerRetryCanBeCancelled(t *testing.T) {
	router := NewRouter()
	if err := router.Register("events", HandlerFunc(func(context.Context, Message) error { return errors.New("fail") })); err != nil {
		t.Fatal(err)
	}
	consumer := testConsumer(&fakeConsumerClient{}, router, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.handleWithRetry(ctx, &kgo.Record{Topic: "events"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestConsumerCloseIsIdempotent(t *testing.T) {
	client := &fakeConsumerClient{}
	consumer := testConsumer(client, NewRouter(), 1)
	consumer.Close()
	consumer.Close()
	if client.closed != 1 {
		t.Fatalf("close calls = %d", client.closed)
	}
}

func TestBatchContextGivesInFlightWorkGracePeriod(t *testing.T) {
	signalCtx, stop := context.WithCancel(context.Background())
	batchCtx, finish := batchContext(signalCtx, 30*time.Millisecond)
	defer finish()
	stop()
	select {
	case <-batchCtx.Done():
		t.Fatal("batch context was canceled immediately")
	case <-time.After(5 * time.Millisecond):
	}
	select {
	case <-batchCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("batch context was not canceled after shutdown timeout")
	}
}
