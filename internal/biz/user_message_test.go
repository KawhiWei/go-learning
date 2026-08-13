package biz

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewUserCreateMessageNormalizesAndBuildsMetadata(t *testing.T) {
	message, err := NewUserCreateMessage("  Alice  ", "alice@example.com ")
	if err != nil {
		t.Fatal(err)
	}
	if message.Name != "Alice" || message.Email != "alice@example.com" {
		t.Fatalf("message = %#v", message)
	}
	if message.Type != UserCreateMessageType {
		t.Fatalf("type = %q", message.Type)
	}
	if _, err := uuid.Parse(message.MessageID); err != nil {
		t.Fatalf("message ID: %v", err)
	}
	if _, err := uuid.Parse(message.UserID); err != nil {
		t.Fatalf("user ID: %v", err)
	}
}

func TestNewUserCreateMessageRejectsInvalidInput(t *testing.T) {
	if _, err := NewUserCreateMessage("", "alice@example.com"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v", err)
	}
}
