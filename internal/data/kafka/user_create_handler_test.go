package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
)

type fakeUserCreator struct {
	id    uuid.UUID
	name  string
	email string
	err   error
}

func (f *fakeUserCreator) CreateUserWithID(_ context.Context, id uuid.UUID, name, email string) (*biz.User, error) {
	f.id, f.name, f.email = id, name, email
	if f.err != nil {
		return nil, f.err
	}
	return &biz.User{ID: id, Name: name, Email: email}, nil
}

func TestUserCreateHandlerWritesThroughService(t *testing.T) {
	id := uuid.New()
	service := &fakeUserCreator{}
	handler := NewUserCreateHandler(service, nil)
	messageID := uuid.New().String()
	payload := `{"message_id":"` + messageID + `","user_id":"` + id.String() + `","type":"user.create.v1","name":"Alice","email":"alice@example.com"}`
	if err := handler.Handle(context.Background(), Message{Topic: "user-events", Key: []byte(id.String()), Value: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
	if service.id != id || service.name != "Alice" || service.email != "alice@example.com" {
		t.Fatalf("service args = %s %q %q", service.id, service.name, service.email)
	}
}

func TestUserCreateHandlerRejectsBadEventAndPropagatesServiceError(t *testing.T) {
	handler := NewUserCreateHandler(&fakeUserCreator{}, nil)
	if err := handler.Handle(context.Background(), Message{Value: []byte(`{`)}); err == nil {
		t.Fatal("invalid JSON should fail")
	}
	id := uuid.New()
	messageID := uuid.New().String()
	payload := `{"message_id":"` + messageID + `","user_id":"` + id.String() + `","type":"unknown","name":"Alice","email":"alice@example.com"}`
	if err := handler.Handle(context.Background(), Message{Value: []byte(payload)}); err == nil {
		t.Fatal("unknown type should fail")
	}

	want := errors.New("database unavailable")
	handler = NewUserCreateHandler(&fakeUserCreator{err: want}, nil)
	payload = `{"message_id":"` + messageID + `","user_id":"` + id.String() + `","type":"user.create.v1","name":"Alice","email":"alice@example.com"}`
	if err := handler.Handle(context.Background(), Message{Key: []byte(id.String()), Value: []byte(payload)}); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestUserCreateHandlerRejectsMismatchedKafkaKey(t *testing.T) {
	id := uuid.New()
	payload := `{"message_id":"` + uuid.New().String() + `","user_id":"` + id.String() + `","type":"user.create.v1","name":"Alice","email":"alice@example.com"}`
	handler := NewUserCreateHandler(&fakeUserCreator{}, nil)
	if err := handler.Handle(context.Background(), Message{Key: []byte("different"), Value: []byte(payload)}); err == nil {
		t.Fatal("mismatched Kafka key should fail")
	}
}
