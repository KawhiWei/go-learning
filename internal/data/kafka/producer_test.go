package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/luck/go-learning/internal/biz"
)

type fakeProducerClient struct {
	record *kgo.Record
	err    error
	closed int
}

func (f *fakeProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.record = records[0]
	return kgo.ProduceResults{{Record: records[0], Err: f.err}}
}
func (f *fakeProducerClient) Close() { f.closed++ }

func TestProducerPublishesAndPropagatesAckError(t *testing.T) {
	client := &fakeProducerClient{}
	producer := &Producer{client: client, timeout: time.Second, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	event := biz.Event{Topic: "events", Key: []byte("1"), Payload: []byte(`{"id":1}`)}
	if err := producer.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if client.record.Topic != "events" || string(client.record.Key) != "1" {
		t.Fatalf("record = %#v", client.record)
	}

	client.err = errors.New("broker unavailable")
	if err := producer.Publish(context.Background(), event); !errors.Is(err, client.err) {
		t.Fatalf("error = %v", err)
	}
}

func TestProducerCloseIsIdempotent(t *testing.T) {
	client := &fakeProducerClient{}
	producer := &Producer{client: client}
	producer.Close()
	producer.Close()
	if client.closed != 1 {
		t.Fatalf("close calls = %d", client.closed)
	}
}
