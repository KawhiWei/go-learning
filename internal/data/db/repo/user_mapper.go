package repo

import (
	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/data/db/model"
)

// toUserModel 把业务实体转换为持久化模型。数据库专属字段应在这里补齐，
// 而不是添加到 HTTP DTO 或业务实体中。
func toUserModel(user *biz.User) *model.User {
	if user == nil {
		return nil
	}
	return &model.User{
		ID:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: user.CreatedAt,
	}
}

// toBizUser 把数据库记录转换回业务实体，阻止 data 层模型越过 Repository
// 边界泄漏到 Service 和 Handler。
func toBizUser(user *model.User) *biz.User {
	if user == nil {
		return nil
	}
	return &biz.User{
		ID:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: user.CreatedAt,
	}
}
