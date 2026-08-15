package consumer

import (
	"context"
	"errors"
	"testing"
)

func TestRouterDispatchesByTopic(t *testing.T) {
	router := NewRouter()
	var got string
	if err := router.Register("users", HandlerFunc(func(_ context.Context, message Message) error {
		got = message.Topic
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := router.Handle(context.Background(), Message{Topic: "users"}); err != nil {
		t.Fatal(err)
	}
	if got != "users" {
		t.Fatalf("got topic %q", got)
	}
}

func TestRouterRejectsDuplicateAndUnknownTopic(t *testing.T) {
	router := NewRouter()
	handler := HandlerFunc(func(context.Context, Message) error { return nil })
	if err := router.Register("users", handler); err != nil {
		t.Fatal(err)
	}
	if err := router.Register("users", handler); err == nil {
		t.Fatal("duplicate topic should fail")
	}
	if err := router.Handle(context.Background(), Message{Topic: "audit"}); !errors.Is(err, ErrHandlerNotFound) {
		t.Fatalf("unknown topic error = %v", err)
	}
}
