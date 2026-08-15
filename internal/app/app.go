// Package app 负责组装一个进程内共享的应用依赖。
//
// 命令入口只处理启动、信号和关闭流程，不直接创建 repository。这样
// transport 始终从同一个装配规则取得 service；新增业务模块时只需扩展
// Services，并在对应进程的装配文件中完成 wiring。
package app

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
	"github.com/luck/go-learning/internal/data/db"
	"github.com/luck/go-learning/internal/data/db/repo"
	kafkadata "github.com/luck/go-learning/internal/data/kafka"
)

// Services 保存暴露给各 transport 的应用用例。
//
// 面向 transport 的依赖应该停留在 service 边界：HTTP 和 RPC handler 调用
// 这些用例，repository 仍是 composition root 的实现细节。未来模块完成
// 业务层后再加入具体 service 字段，不为尚不存在的模块添加空占位。
type Services struct {
	// UserService 提供 HTTP、gRPC 和 Kafka Consumer Handler 共用的用户用例。
	UserService *biz.UserService
	// MessagePublisher 将业务消息编码并发布到配置允许的 Kafka Topic。
	MessagePublisher *biz.MessagePublisher
	// EventService 提供通用事件 Topic 的发布能力，不包含内部用户消息 Topic。
	EventService *biz.EventService
}

// App 持有一个进程内各服务器共享的资源。
//
// 每个进程只创建一个供本进程 service 使用的 PostgreSQL pool。由
// App.Close 统一关闭，可以让入口拥有清晰的生命周期边界，避免 transport
// 关闭并非由它创建的资源。
type App struct {
	pool          *pgxpool.Pool
	kafkaProducer *kafkadata.Producer
	kafkaConsumer *kafkadata.Consumer
	Services      Services
}

// Option 为进程附加一项可选基础设施能力。每个入口按实际职责组合选项，
// 因此 Kafka Producer 可以被 HTTP、gRPC 或 Consumer 进程复用，而不必为
// 每种组合创建新的 NewXXX 构造函数。
type Option func(context.Context, config.Config, *App) error

// New 是 composition root 的公共依赖装配入口。它先完成 database pool ->
// repository -> business service 的基础装配，再按 options 附加 Producer 或
// Consumer 等可选能力。任一选项失败时会关闭已创建资源。
func New(ctx context.Context, cfg config.Config, options ...Option) (*App, error) {
	application, err := newApp(ctx, cfg)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(ctx, cfg, application); err != nil {
			application.Close()
			return nil, err
		}
	}
	return application, nil
}

func containsTopic(topics []string, want string) bool {
	for _, topic := range topics {
		if topic == want {
			return true
		}
	}
	return false
}

func topicsExcept(topics []string, excluded string) []string {
	result := make([]string, 0, len(topics))
	for _, topic := range topics {
		if topic != excluded {
			result = append(result, topic)
		}
	}
	return result
}

func newApp(ctx context.Context, cfg config.Config) (*App, error) {
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
	if a == nil {
		return
	}
	if a.kafkaConsumer != nil {
		a.kafkaConsumer.Close()
		a.kafkaConsumer = nil
	}
	if a.kafkaProducer != nil {
		a.kafkaProducer.Close()
		a.kafkaProducer = nil
	}
	if a.pool == nil {
		return
	}
	a.pool.Close()
	a.pool = nil
}
