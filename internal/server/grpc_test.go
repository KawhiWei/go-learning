package server

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

func TestKitexCreateUser(t *testing.T) {
	user, err := NewKitexUserServer(&fakeHTTPService{}).CreateUser(
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
			_, err := NewKitexUserServer(&fakeHTTPService{createErr: tc.err}).CreateUser(
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
	kitexServer := NewKitexUserServer(&fakeHTTPService{})
	for _, req := range []*gen.GetUserRequest{nil, {}, {Id: "not-a-uuid"}} {
		if _, err := kitexServer.GetUser(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("GetUser(%v) error = %v, want InvalidArgument", req, err)
		}
	}

	id := uuid.New()
	service := &fakeHTTPService{got: &biz.User{
		ID: id, Name: "Alice", Email: "alice@example.com", CreatedAt: time.Unix(1, 0).UTC(),
	}}
	user, err := NewKitexUserServer(service).GetUser(
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
