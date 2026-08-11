package biz

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("user not found")
	ErrAlreadyExists   = errors.New("user already exists")
)

// User is the application representation of a user.
type User struct {
	ID        uuid.UUID
	Name      string
	Email     string
	CreatedAt time.Time
}

// UserRepository is the persistence contract required by the service.
type UserRepository interface {
	Create(ctx context.Context, user *User) (*User, error)
	Get(ctx context.Context, id uuid.UUID) (*User, error)
}

// UserService owns user validation and use-case orchestration.
type UserService struct {
	repo UserRepository
}

func NewUserService(repo UserRepository) *UserService {
	return &UserService{repo: repo}
}

func (s *UserService) CreateUser(ctx context.Context, name, email string) (*User, error) {
	name, email, err := validateUser(name, email)
	if err != nil {
		return nil, err
	}
	return s.repo.Create(ctx, &User{Name: name, Email: email})
}

func (s *UserService) GetUser(ctx context.Context, id uuid.UUID) (*User, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidArgument)
	}
	return s.repo.Get(ctx, id)
}

func validateUser(name, email string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", fmt.Errorf("%w: name is required", ErrInvalidArgument)
	}
	if len([]rune(name)) > 200 {
		return "", "", fmt.Errorf("%w: name must be at most 200 characters", ErrInvalidArgument)
	}

	email = strings.TrimSpace(email)
	if email == "" {
		return "", "", fmt.Errorf("%w: email is required", ErrInvalidArgument)
	}
	parsed, err := mail.ParseAddress(email)
	if err != nil || parsed.Address != email || !strings.Contains(email, "@") {
		return "", "", fmt.Errorf("%w: email is invalid", ErrInvalidArgument)
	}
	if len(email) > 320 {
		return "", "", fmt.Errorf("%w: email is too long", ErrInvalidArgument)
	}
	return name, email, nil
}
