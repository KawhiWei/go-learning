package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeUserRepository struct {
	created   *User
	got       *User
	createErr error
	getErr    error
}

func (f *fakeUserRepository) Create(_ context.Context, user *User) (*User, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	copy := *user
	if copy.ID == uuid.Nil {
		copy.ID = uuid.New()
	}
	copy.CreatedAt = time.Unix(1, 0).UTC()
	f.created = &copy
	return &copy, nil
}

func TestCreateUserWithIDPreservesEventIdentity(t *testing.T) {
	repo := &fakeUserRepository{}
	service := NewUserService(repo)
	id := uuid.New()
	user, err := service.CreateUserWithID(context.Background(), id, "Alice", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != id || repo.created.ID != id {
		t.Fatalf("user ID = %s, repository ID = %s, want %s", user.ID, repo.created.ID, id)
	}
}

func (f *fakeUserRepository) Get(_ context.Context, _ uuid.UUID) (*User, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.got, nil
}

func TestCreateUserValidatesAndNormalizesInput(t *testing.T) {
	repo := &fakeUserRepository{}
	service := NewUserService(repo)

	user, err := service.CreateUser(context.Background(), "  Alice  ", "alice@example.com ")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if user.Name != "Alice" || user.Email != "alice@example.com" {
		t.Fatalf("CreateUser() = %#v, want trimmed fields", user)
	}
	if repo.created == nil {
		t.Fatal("repository was not called")
	}
}

func TestCreateUserRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name  string
		email string
	}{
		{name: "", email: "a@example.com"},
		{name: "Alice", email: ""},
		{name: "Alice", email: "not-an-email"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/"+tc.email, func(t *testing.T) {
			service := NewUserService(&fakeUserRepository{})
			_, err := service.CreateUser(context.Background(), tc.name, tc.email)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestGetUserRejectsNilIDAndPropagatesRepositoryErrors(t *testing.T) {
	service := NewUserService(&fakeUserRepository{})
	if _, err := service.GetUser(context.Background(), uuid.Nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil ID error = %v, want ErrInvalidArgument", err)
	}

	repoErr := errors.New("repository unavailable")
	service = NewUserService(&fakeUserRepository{getErr: repoErr})
	if _, err := service.GetUser(context.Background(), uuid.New()); !errors.Is(err, repoErr) {
		t.Fatalf("repository error = %v, want %v", err, repoErr)
	}
}
