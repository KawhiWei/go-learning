// Package app 负责组装一个进程内共享的应用依赖。
//
// 命令入口只处理启动、信号和关闭流程，不直接创建 repository。这样
// transport 始终从同一个装配规则取得 service；新增业务模块时只需扩展
// Services 以及 New 中的 wiring。
package app

import (
	"context"
	"fmt"
	"log/slog"

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
	UserService      *biz.UserService
	MessagePublisher *biz.MessagePublisher
	EventService     *biz.EventService
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

// New 是 composition root：按 database pool -> repository -> business service
// 的依赖顺序完成装配。ctx 用于初次连接数据库，也可以被进程的 signal
// handler 取消。
func New(ctx context.Context, cfg config.Config) (*App, error) {
	return newApp(ctx, cfg)
}

// NewAPI 在公共依赖之外创建 Kafka Producer。
// API 只发布事件，不加入 Consumer Group；消息消费由独立 work 进程承担。
func NewAPI(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	application, err := newApp(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if !cfg.Kafka.Enabled {
		return application, nil
	}
	if !containsTopic(cfg.Kafka.Topics, biz.UserCreateTopic) {
		application.Close()
		return nil, fmt.Errorf("kafka topics must include %q for HTTP user creation", biz.UserCreateTopic)
	}

	producer, err := kafkadata.NewProducer(cfg.Kafka, log)
	if err != nil {
		application.Close()
		return nil, err
	}
	application.kafkaProducer = producer
	// user-events 是内部业务 Topic，由固定 Handler 按稳定消息 schema 发布。
	// 将它从通用事件 API 白名单移除，可以防止任意 JSON 阻塞 Consumer。
	application.Services.EventService = biz.NewEventService(producer, topicsExcept(cfg.Kafka.Topics, biz.UserCreateTopic))
	application.Services.MessagePublisher = biz.NewMessagePublisher(producer, cfg.Kafka.Topics)
	return application, nil
}

// NewWorker 创建 Kafka Consumer 及其真正需要的数据库依赖。
// user-events 通过 UserService -> UserRepository 写入 PostgreSQL。
// 尚未接入业务的 Topic 使用元数据日志 Handler；HTTP/gRPC server 不会在 Worker 进程中创建。
func NewWorker(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	if !cfg.Kafka.Enabled {
		return nil, fmt.Errorf("kafka must be enabled for work process")
	}
	if !containsTopic(cfg.Kafka.Topics, biz.UserCreateTopic) {
		return nil, fmt.Errorf("kafka topics must include %q for user worker", biz.UserCreateTopic)
	}
	p, err := db.NewPool(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}
	userService := biz.NewUserService(repo.NewUserRepository(p))
	application := &App{pool: p, Services: Services{UserService: userService}}

	router := kafkadata.NewRouter()
	for _, topic := range cfg.Kafka.Topics {
		handler := kafkadata.NewMetadataLoggingHandler(log)
		if topic == biz.UserCreateTopic {
			handler = kafkadata.NewUserCreateHandler(userService, log)
		}
		if err := router.Register(topic, handler); err != nil {
			application.Close()
			return nil, fmt.Errorf("register kafka topic %q: %w", topic, err)
		}
	}
	consumer, err := kafkadata.NewConsumer(cfg.Kafka, router, log)
	if err != nil {
		application.Close()
		return nil, err
	}
	application.kafkaConsumer = consumer
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

// RunConsumers 阻塞运行本进程配置的 Consumer。Kafka 未启用时直接返回。
func (a *App) RunConsumers(ctx context.Context) error {
	if a == nil || a.kafkaConsumer == nil {
		return nil
	}
	return a.kafkaConsumer.Run(ctx)
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
