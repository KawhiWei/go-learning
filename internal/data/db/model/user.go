// Package model 定义与数据库记录结构对应的持久化模型。
//
// 这里的类型只服务于 data 层，不应直接作为 HTTP 响应或业务 Service 的
// 入参。即使当前字段与 biz.User 相同，保留转换边界也能避免数据库字段变化
// 直接污染业务层和对外 API。
package model

import (
	"time"

	"github.com/google/uuid"
)

// User 对应 users 表的一行记录。
type User struct {
	ID        uuid.UUID
	Name      string
	Email     string
	CreatedAt time.Time
}
