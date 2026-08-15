package grpcserver

import (
	"errors"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"

	"github.com/luck/go-learning/internal/biz"
)

// toKitexError 集中维护业务错误到公开 gRPC status code 的映射，避免 Handler
// 泄漏数据库错误或在每个 RPC 方法中重复协议转换逻辑。
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
