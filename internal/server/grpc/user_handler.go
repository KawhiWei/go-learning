package grpcserver

import (
	"context"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	gen "github.com/luck/go-learning/api/gen"
	"github.com/luck/go-learning/internal/biz"
)

// UserService 是 gRPC transport 实际需要的最小业务边界。它只描述用例能力，
// 不暴露 Repository、数据库连接或任何 Kitex 实现细节。
type UserService interface {
	CreateUser(context.Context, string, string) (*biz.User, error)
	GetUser(context.Context, uuid.UUID) (*biz.User, error)
}

type KitexUserServer struct {
	service UserService
}

// RegisterKitexUserServer 构造 Proto UserService 的具体实现。真正的 Kitex
// 服务注册由 api/gen/userservice.NewServer 内部的 RegisterService 完成。
func RegisterKitexUserServer(service UserService) *KitexUserServer {
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
