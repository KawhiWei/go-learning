package grpcserver

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
	"github.com/google/uuid"

	gen "github.com/luck/go-learning/api/gen"
	"github.com/luck/go-learning/internal/biz"
)

type fakeGRPCService struct {
	createErr error
	getErr    error
	got       *biz.User
}

func (f *fakeGRPCService) CreateUser(context.Context, string, string) (*biz.User, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.got != nil {
		return f.got, nil
	}
	return &biz.User{
		ID:        uuid.New(),
		Name:      "Alice",
		Email:     "alice@example.com",
		CreatedAt: time.Unix(1, 0).UTC(),
	}, nil
}

func (f *fakeGRPCService) GetUser(context.Context, uuid.UUID) (*biz.User, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.got, nil
}

func TestKitexCreateUser(t *testing.T) {
	user, err := RegisterKitexUserServer(&fakeGRPCService{}).CreateUser(
		context.Background(),
		&gen.CreateUserRequest{Name: "Alice", Email: "alice@example.com"},
	)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if user.GetId() == "" || user.GetName() != "Alice" || user.GetEmail() != "alice@example.com" {
		t.Fatalf("CreateUser() = %#v", user)
	}
	if user.GetCreatedAt() == nil || !user.GetCreatedAt().AsTime().Equal(time.Unix(1, 0).UTC()) {
		t.Fatalf("created_at = %v", user.GetCreatedAt())
	}
}

func TestKitexMapsGRPCErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "invalid", err: biz.ErrInvalidArgument, want: codes.InvalidArgument},
		{name: "exists", err: biz.ErrAlreadyExists, want: codes.AlreadyExists},
		{name: "missing", err: biz.ErrNotFound, want: codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RegisterKitexUserServer(&fakeGRPCService{createErr: tc.err}).CreateUser(
				context.Background(),
				&gen.CreateUserRequest{},
			)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("status code = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestKitexGetUserValidatesID(t *testing.T) {
	kitexServer := RegisterKitexUserServer(&fakeGRPCService{})
	for _, req := range []*gen.GetUserRequest{nil, {}, {Id: "not-a-uuid"}} {
		if _, err := kitexServer.GetUser(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("GetUser(%v) error = %v, want InvalidArgument", req, err)
		}
	}

	id := uuid.New()
	service := &fakeGRPCService{got: &biz.User{
		ID: id, Name: "Alice", Email: "alice@example.com", CreatedAt: time.Unix(1, 0).UTC(),
	}}
	user, err := RegisterKitexUserServer(service).GetUser(
		context.Background(),
		&gen.GetUserRequest{Id: id.String()},
	)
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if user.GetId() != id.String() {
		t.Fatalf("id = %q, want %q", user.GetId(), id)
	}
}
