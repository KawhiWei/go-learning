package biz

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fakeEventPublisher struct {
	event Event
	err   error
}

type testMessage struct {
	ID string `json:"id"`
}

func TestMessagePublisherSerializesBody(t *testing.T) {
	transport := &fakeEventPublisher{}
	publisher := NewMessagePublisher(transport, []string{"commands"})
	if err := publisher.Publish(context.Background(), "commands", []byte("42"), testMessage{ID: "42"}); err != nil {
		t.Fatal(err)
	}
	if transport.event.Topic != "commands" || string(transport.event.Key) != "42" {
		t.Fatalf("event = %#v", transport.event)
	}
	var payload testMessage
	if err := json.Unmarshal(transport.event.Payload, &payload); err != nil || payload.ID != "42" {
		t.Fatalf("payload=%s error=%v", transport.event.Payload, err)
	}
}

func TestMessagePublisherRejectsUnknownTopicAndPropagatesError(t *testing.T) {
	transport := &fakeEventPublisher{}
	publisher := NewMessagePublisher(transport, []string{"other"})
	if err := publisher.Publish(context.Background(), "commands", []byte("42"), testMessage{ID: "42"}); !errors.Is(err, ErrEventTopicNotAllowed) {
		t.Fatalf("topic error = %v", err)
	}
	want := errors.New("broker unavailable")
	transport.err = want
	publisher = NewMessagePublisher(transport, []string{"commands"})
	if err := publisher.Publish(context.Background(), "commands", []byte("42"), testMessage{ID: "42"}); !errors.Is(err, want) {
		t.Fatalf("publish error = %v", err)
	}
}

func TestMessagePublisherRejectsNilAndEmptyKey(t *testing.T) {
	publisher := NewMessagePublisher(&fakeEventPublisher{}, []string{"commands"})
	if err := publisher.Publish(context.Background(), "commands", []byte("42"), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil message error = %v", err)
	}
	if err := publisher.Publish(context.Background(), "commands", nil, testMessage{ID: "42"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key error = %v", err)
	}
}

func (f *fakeEventPublisher) Publish(_ context.Context, event Event) error {
	f.event = event
	return f.err
}

func TestEventServicePublishesAllowedTopic(t *testing.T) {
	publisher := &fakeEventPublisher{}
	service := NewEventService(publisher, []string{"user-events"})
	event := Event{Topic: " user-events ", Key: []byte("1"), Payload: []byte(`{"id":1}`)}
	if err := service.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if publisher.event.Topic != "user-events" {
		t.Fatalf("topic = %q", publisher.event.Topic)
	}
}

func TestEventServiceRejectsInvalidEvent(t *testing.T) {
	service := NewEventService(&fakeEventPublisher{}, []string{"user-events"})
	if err := service.Publish(context.Background(), Event{Topic: "other", Payload: []byte("x")}); !errors.Is(err, ErrEventTopicNotAllowed) {
		t.Fatalf("topic error = %v", err)
	}
	if err := service.Publish(context.Background(), Event{Topic: "user-events"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("payload error = %v", err)
	}
}
