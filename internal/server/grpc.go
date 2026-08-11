package server

import (
	"context"
	"errors"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	gen "github.com/luck/go-learning/api/gen"
	"github.com/luck/go-learning/internal/biz"
)

type KitexUserServer struct {
	service UserService
}

func NewKitexUserServer(service UserService) *KitexUserServer {
	return &KitexUserServer{service: service}
}

func (s *KitexUserServer) CreateUser(ctx context.Context, req *gen.CreateUserRequest) (*gen.User, error) {
	if req == nil {
		return nil, status.Err(codes.InvalidArgument, "request is required")
	}
	user, err := s.service.CreateUser(ctx, req.GetName(), req.GetEmail())
	if err != nil {
		return nil, toKitexError(err)
	}
	if user == nil {
		return nil, status.Err(codes.Internal, "internal server error")
	}
	return toProtoUser(user), nil
}

func (s *KitexUserServer) GetUser(ctx context.Context, req *gen.GetUserRequest) (*gen.User, error) {
	if req == nil || req.GetId() == "" {
		return nil, status.Err(codes.InvalidArgument, "id must be a valid UUID")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Err(codes.InvalidArgument, "id must be a valid UUID")
	}
	user, err := s.service.GetUser(ctx, id)
	if err != nil {
		return nil, toKitexError(err)
	}
	if user == nil {
		return nil, status.Err(codes.Internal, "internal server error")
	}
	return toProtoUser(user), nil
}

func toProtoUser(user *biz.User) *gen.User {
	return &gen.User{
		Id:        user.ID.String(),
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: timestamppb.New(user.CreatedAt),
	}
}

func toKitexError(err error) error {
	switch {
	case errors.Is(err, biz.ErrInvalidArgument):
		return status.Err(codes.InvalidArgument, err.Error())
	case errors.Is(err, biz.ErrAlreadyExists):
		return status.Err(codes.AlreadyExists, "user already exists")
	case errors.Is(err, biz.ErrNotFound):
		return status.Err(codes.NotFound, "user not found")
	default:
		return status.Err(codes.Internal, "internal server error")
	}
}
