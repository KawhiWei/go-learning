// Package app 负责组装一个进程内共享的应用依赖。
//
// 命令入口只处理启动、信号和关闭流程，不直接创建 repository。这样
// transport 始终从同一个装配规则取得 service；新增业务模块时只需扩展
// Services 以及 New 中的 wiring。
package app

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
	"github.com/luck/go-learning/internal/data/db"
	"github.com/luck/go-learning/internal/data/db/repo"
)

// Services 保存暴露给各 transport 的应用用例。
//
// 面向 transport 的依赖应该停留在 service 边界：HTTP 和 RPC handler 调用
// 这些用例，repository 仍是 composition root 的实现细节。未来模块完成
// 业务层后再加入具体 service 字段，不为尚不存在的模块添加空占位。
type Services struct {
	UserService *biz.UserService
}

// App 持有一个进程内各服务器共享的资源。
//
// 每个进程只创建一个供本进程 service 使用的 PostgreSQL pool。由
// App.Close 统一关闭，可以让入口拥有清晰的生命周期边界，避免 transport
// 关闭并非由它创建的资源。
type App struct {
	pool     *pgxpool.Pool
	Services Services
}

// New 是 composition root：按 database pool -> repository -> business service
// 的依赖顺序完成装配。ctx 用于初次连接数据库，也可以被进程的 signal
// handler 取消。
func New(ctx context.Context, cfg config.Config) (*App, error) {
	p, err := db.NewPool(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}

	userRepo := repo.NewUserRepository(p)
	return &App{
		pool: p,
		Services: Services{
			UserService: biz.NewUserService(userRepo),
		},
	}, nil
}

// Close 释放 New 创建的资源。对 nil App 或重复调用都是安全的，便于入口
// 使用 defer 组织关闭路径。
func (a *App) Close() {
	if a == nil || a.pool == nil {
		return
	}
	a.pool.Close()
	a.pool = nil
}
