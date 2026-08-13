# nino-data-postgres

一个使用 PostgreSQL 持久化用户数据的 Go 服务示例。HTTP API 基于 CloudWeGo Hertz，gRPC API 基于 CloudWeGo Kitex 的标准 HTTP/2 transport；两个服务共享同一个业务层和仓储实现。

## 这是标准 Go 项目结构吗

Go 官方没有规定 Web 服务必须采用某一种目录结构。本项目使用的是生产项目中常见的组合方式：

- `cmd`、`internal` 是 Go 社区广泛使用的目录约定，其中 `internal` 的导入限制由 Go 工具链强制执行。
- Handler -> Service -> Repository 是应用分层方式，不是 Go 语言规范，但适合接口较多、需要同时支持 HTTP 和 gRPC 的服务。
- `api`、`configs`、`migrations`、`scripts` 和 `pkg` 是常见的工程目录，是否保留应由项目规模决定。

因此，这是一套合理且可扩展的 Go 服务结构，但不是唯一的“官方标准”。对于只有几个接口的小程序可以适当合并目录；当业务模块增多时，当前结构可以避免所有初始化、路由和数据库代码堆积在 `main.go` 中。

## 目录结构与分层职责

```text
.
├── api/                         # 对外协议及根据协议生成的代码
│   ├── proto/                   # gRPC Proto 源文件，是 RPC 接口契约
│   ├── gen/                     # Kitex/Protobuf 生成代码，不应手工修改
│   └── openapi/                 # OpenAPI 描述、Swagger UI 及嵌入资源
├── cmd/                         # 可执行程序入口，一个服务一个子目录
│   ├── api-server/              # Hertz HTTP 服务入口
│   │   └── main.go
│   ├── work/                    # Kafka Consumer Worker 独立进程入口
│   │   └── main.go
│   └── grpc-server/             # Kitex gRPC 服务入口
│       └── main.go
├── internal/                    # 本项目私有实现，仓库外部不能直接导入
│   ├── app/                     # Composition Root，统一组装全部依赖
│   ├── biz/                     # 业务实体、业务规则、Service 和仓储接口
│   ├── config/                  # 配置结构、文件读取和环境变量覆盖
│   ├── data/                    # 基础设施和数据访问实现
│   │   ├── db/                  # PostgreSQL 连接池及数据库相关代码
│   │   │   ├── model/           # 与数据库记录对应的持久化模型
│   │   │   └── repo/            # Repository 实现、SQL 和模型转换
│   │   └── kafka/               # Kafka Producer、Consumer 和 Topic 路由
│   │       ├── producer.go      # API 事件发布及 broker ack 等待
│   │       ├── consumer.go      # Worker Consumer Group、轮询、并发和 offset 提交
│   │       ├── router.go        # 按 Topic 分发到对应 Handler
│   │       └── logging_handler.go # 当前用于验证链路的元数据占位 Handler
│   └── server/                  # HTTP/gRPC 协议适配、路由和 Handler
├── pkg/                         # 确实允许其他项目导入的通用包
│   └── logger/                  # 与业务无关的日志封装
├── configs/                     # 可部署的默认配置文件
├── migrations/                  # 按顺序执行的数据库结构迁移
├── scripts/                     # 代码生成、迁移等开发和运维脚本
├── Dockerfile                   # 构建 HTTP、gRPC 和 Worker 镜像
├── docker-compose.yml           # 本地编排 PostgreSQL、Kafka、API、Worker 和 gRPC
├── go.mod                       # Go module 名称及直接依赖声明
└── go.sum                       # 依赖版本校验信息
```

### `api`：对外协议层

`api/proto/user.proto` 定义 gRPC 服务、方法和消息，是客户端与服务端共同遵守的契约。`api/gen` 由 Kitex 和 Protobuf 工具生成，业务修改应先改 Proto 再重新生成，不能直接编辑生成文件。`api/openapi` 描述 HTTP API，并将 Swagger 页面和 YAML 嵌入可执行文件。

这一层只描述“外部如何调用”，不实现用户校验、数据库操作等业务逻辑。

### `cmd`：程序入口层

每个 `cmd/<程序名>/main.go` 对应一个独立可执行文件。入口只负责：

- 加载配置和初始化日志；
- 创建应用依赖；
- 启动 Hertz、Kitex 或 Kafka Worker；
- HTTP 入口调用 `h.Spin()`，由 Hertz 监听退出信号并执行优雅关闭。
- gRPC 入口调用 `rpcServer.Run()`，由 Kitex 监听退出信号并执行优雅 `Stop()`。
- Worker 入口调用 Consumer 的运行循环，监听 `SIGINT`/`SIGTERM` 后停止拉取、排空在途任务并关闭 Consumer。

`main.go` 调用 `internal/app.New` 创建完整应用是正常的启动流程，但不应该自己执行 SQL、创建 Repository 或处理业务请求。运行期间的请求仍然由路由进入 Handler。

`cmd/api-server` 的启动形态借鉴 CloudWeGo Hertz 官方 `hertz-examples/bizdemo/hertz_gorm`：构造 Hertz 实例、注册路由，再调用 `h.Spin()`。Spin 默认处理 `SIGINT`、`SIGHUP` 和 `SIGTERM`，并在退出时等待进行中的请求完成；本项目仍保留 `internal/app` composition root 和 Handler -> Service -> Repository 的依赖注入分层。具体来说，`NewHTTPServer` 只创建 Hertz 与全局 404/405 fallback，业务 service 通过显式的 `RegisterHTTPRoutes` 注入，不把官方示例中的全局 DB 或 Handler 直连方式带入本项目。

`cmd/grpc-server` 同样把 RPC 生命周期交给 Kitex：入口直接调用阻塞式 `rpcServer.Run()`，不再另外创建 signal channel、goroutine 或手动调用 `rpcServer.Stop()`。Kitex 完成优雅关闭并返回后，`defer application.Close()` 再释放 PostgreSQL 连接池。

`cmd/work/main.go` 是独立的 Kafka Consumer 进程，由自身生命周期管理，不由 HTTP API 进程启动。它调用 `internal/app.NewWorker` 创建数据库连接池、User Repository/Service、Consumer Group 和 Topic Router。它把 `user-events` 交给真实的用户创建 Handler，把尚未接入业务的 `audit-events` 交给元数据占位 Handler；收到 `SIGTERM` 后停止拉取，等待在途任务和 offset 处理完成再退出。API 只发布消息，不加入 Consumer Group。

### `internal/app`：依赖组装层

这是应用的 Composition Root。API 进程按 PostgreSQL Pool -> Repository -> Service，再创建 Kafka Producer -> EventService/MessagePublisher 的顺序装配；Worker 装配自己的 PostgreSQL Pool -> UserRepository -> UserService -> user-events Handler，以及 Kafka Consumer/Router，但不创建 HTTP 或 gRPC server。每个进程只关闭自己创建的连接池和 Kafka 客户端。

依赖组装只在程序启动时执行一次，不属于某个 HTTP/gRPC 请求的调用链。以后增加 `OrderService` 时，应在这里增加对应 Repository 和 Service 的组装，而不是继续向 `main.go` 填充初始化细节。

### `internal/biz`：业务层

业务层保存业务实体、业务错误、Repository 接口和 Service 实现。例如 `UserService` 负责用户名、邮箱校验以及创建、查询用户的用例编排。

Repository 接口定义在业务层，是因为业务层决定自己需要哪些持久化能力；数据层只负责实现它。业务层不应依赖 Hertz、Kitex、PostgreSQL 驱动或 HTTP 状态码，因此相同 Service 可以同时被 HTTP 和 gRPC Handler 使用。

异步消息采用统一的 `MessagePublisher`。每个业务只定义自己的稳定消息 DTO 和构造函数，例如 `UserCreateMessage`/`NewUserCreateMessage`：构造函数负责该业务的输入校验、默认值和 ID 生成。消息 DTO 不需要实现 `Topic()`、`Key()` 或任何公共 `Command` 接口；不同业务的 DTO 可以共用同一个 Publisher。

调用方把消息体、Topic 和 Kafka key 显式传给统一 Publisher。Publisher 负责 JSON marshal、Topic 白名单、非空 key 校验以及底层 Kafka publish/ack。`body` 可以是任意可 JSON 编码的 struct、map 或 slice，但生产环境建议使用稳定的强类型 struct，因为它更容易做 schema review、版本兼容和消费者校验。Topic/key 应由业务代码或配置决定，不应直接透传未经校验的 HTTP 参数。

未来的订单消息可以复用同一个 Publisher：

```go
type OrderCreateMessage struct {
	MessageID string `json:"message_id"`
	OrderID   string `json:"order_id"`
	Type      string `json:"type"`
	// ...订单 schema 的强类型字段...
}

message := OrderCreateMessage{MessageID: id, OrderID: orderID, Type: "order.create.v1"}
err := publisher.Publish(ctx, "order-events", []byte(message.OrderID), message)
```

因此，新增业务通常只需要新增自己的消息 DTO/构造函数和消费者 Handler，不需要再定义一个 `OrderCreateCommandService` 或让 DTO 实现公共接口。`MessagePublisher` 仍会拒绝未登记的 Topic，确保消息只能进入应用明确允许的 Topic。

### `internal/data`：数据与基础设施层

`data/db` 创建和配置 PostgreSQL 连接池；`data/db/model` 定义与数据库记录对应的持久化模型；`data/db/repo` 实现业务层声明的 Repository 接口，负责 SQL、唯一键冲突以及 DB Model 与业务实体之间的转换。DB Model 不应直接传给 Handler，数据库字段变化也不应直接改变 HTTP DTO。

这一层只处理数据存取，不负责邮箱是否合法、用户是否允许创建等业务规则。Kafka 的基础设施实现位于 `internal/data/kafka`：`producer.go` 封装事件发布，`consumer.go` 封装 Worker 的 Consumer Group、轮询、并发、重试和 offset 提交，`router.go` 保存 Topic 到 Handler 的注册表并负责分发。

### Kafka 事件发布与消费

HTTP 创建用户已经改为异步链路，API 不再直接调用 Repository 写数据库：

```text
POST /v1/users
    -> UserHTTPHandler（解析 HTTP DTO）
    -> NewUserCreateMessage（校验并生成 message_id/user_id）
    -> MessagePublisher.Publish（JSON marshal、Topic/key 校验）
    -> Kafka Producer（Topic=user-events，key=user_id）
    -> Kafka broker ack
    -> HTTP 202 Accepted

nino-data-work
    -> user-events / user.create.v1
    -> UserCreateHandler
    -> UserService.CreateUserWithID
    -> UserRepository
    -> PostgreSQL
    -> Handler 成功后提交 offset
```

`202` 只表示 Kafka 已确认消息，数据库可能还没有完成写入。响应中的 `resource_id` 可用于随后调用 `GET /v1/users/:id`；短时间返回 `404` 属于最终一致性窗口，客户端应采用有上限的退避重试。相同事件重投会携带相同 `user_id`：数据库中 ID 与内容完全一致时视为幂等成功；同 ID 不同内容或 email 已被其他 ID 使用仍是冲突，不会被静默忽略。gRPC 的 CreateUser 暂时仍保持原同步写库语义。

API 的事件发布链路只有发布职责，不消费 Kafka：

```text
POST /v1/events/:topic
    -> EventHTTPHandler
    -> EventService
    -> EventPublisher
    -> internal/data/kafka.Producer (franz-go)
    -> Kafka broker ack
    -> HTTP 202 Accepted
```

`EventService` 从配置的 `kafka.topics` 生成 Topic 白名单，并排除系统消息 Topic。请求的 `:topic` 不在白名单时直接返回参数错误，不会向任意内部 Topic 发布。Producer 等待 broker 确认后，Handler 才返回 `202 Accepted`；这个状态只表示消息已交给 Kafka，不表示 Worker 已经取到消息、Handler 已完成或数据库事务已经提交。Producer 超时或 broker 不可用时返回发布失败，API 不伪造成功响应。

`user-events` 是内部系统消息 Topic，已从通用 `/v1/events/:topic` 白名单排除，避免任意 JSON 绕过用户校验并形成毒消息。创建用户必须调用 `/v1/users`；通用事件接口目前可用于 `audit-events` 等非系统命令 Topic。

Worker 由 `cmd/work/main.go` 独立运行：一个 Consumer Group（`group_id`）同时订阅 `topics` 中的全部 Topic，Kafka 负责把各 Topic 的 partition 分配给该 Group 的 Worker 实例。Router 按 Topic 找到业务 Handler，消息 DTO 不把 franz-go 类型泄露给业务层。

Worker 的处理模型如下：

```text
PollRecords（最多 poll_max_records 条）
    -> 有界并发池（worker_concurrency）
    -> 同一 Topic/partition 按 offset 顺序执行 Handler
    -> 只提交每个 partition 已连续成功的 offset（可批量提交）
    -> 继续拉取下一批消息
```

并发上限跨所有 Topic/partition 共享；不同 partition 可以并行，同一 partition 始终保序。Handler 返回错误时按 `retry_interval_seconds` 重试且不推进该 partition 的提交点；提交失败也按同一间隔重试，但已成功的 Handler 不因提交重试再次执行。应用退出或 rebalance 时停止接收新任务，等待在途任务结束，提交已经连续完成的 offset，再安全释放 partition；无法在 `shutdown_timeout_seconds` 内完成的任务会随进程退出而由 Kafka 重新投递。franz-go 负责 broker 连接维护和断线重连，Worker 对轮询、Handler、提交错误做日志记录和重试。

`user-events` 已注册 `UserCreateHandler` 并写 PostgreSQL；`audit-events` 当前仍使用 `NewMetadataLoggingHandler` 占位，只记录 Topic、partition、offset 和 key/value 字节数，不输出消息正文。Handler 成功但 offset 尚未提交时仍可能重投，所以所有真实 Handler 都必须幂等。当前失败消息会无限重试：毒消息会阻塞自己的 partition，并让本次 poll 批次等待收尾；其他 partition 的 Handler 可以并发完成，但 offset 也要等批次进入提交阶段。生产环境还应增加重试上限、DLQ、告警和 lag 监控；当前实现尚未提供 DLQ。

### Goroutine 与并发池调优

Consumer 的并发单位是 `Topic + Partition`，不是单条消息。同一 partition 的消息由同一个 goroutine 按 offset 顺序处理，避免后面的 offset 先提交导致中间消息丢失。每次 poll 实际创建的 worker 数为：

```text
min(worker_concurrency, 当前批次包含的 partition 数)
```

调优时遵循这些约束：

- `worker_concurrency` 高于订阅 Topic 的可用 partition 总数不会提升吞吐；要继续扩容，先增加 partition，再增加 Worker 实例或并发数。
- 用户 Handler 会访问 PostgreSQL，单个 Worker 的 `worker_concurrency` 通常不应高于 `database.max_conns`，还要为重试、健康检查以及同进程的其他任务预留连接。Compose 当前二者分别为 `8` 和 `10`。
- `poll_max_records` 越大，单批吞吐可能越高，但内存占用、批次尾延迟、rebalance 等待时间以及毒消息影响范围也越大；应结合单条消息大小和 P99 Handler 时延逐步调整。
- 扩容前监控 consumer lag、单条/单批处理时延、DB pool acquire wait、活跃连接数、CPU、内存、重试次数和 DLQ 数量。只有 CPU/DB 仍有余量且 lag 持续增长时，增加并发才有意义。
- 每批 worker goroutine 在 `WaitGroup` 完成后退出，不会跨 poll 无限累积；`shutdown_timeout_seconds` 必须大于正常批次 P99 处理时间，并小于容器 `stop_grace_period`。

如果一次业务操作同时要求“数据库写入 + Kafka 发布”原子可靠，应该采用 Transactional Outbox：在同一个数据库事务中写业务数据和 outbox 记录，再由独立 relay 发布并记录发送状态。本接口是独立事件发布接口，不提供数据库与 Kafka 的双写事务，也不应宣称具备该原子性。

### `internal/server`：传输与接口适配层

这一层负责把 HTTP 或 gRPC 请求转换为 Service 参数，再把 Service 返回值和错误转换为 JSON、Protobuf、HTTP 状态码或 gRPC 状态码，主要包括：

- `http_routes.go`：公共路由和各业务模块的总注册入口；
- `http_user_routes.go`：User 模块的 URL 与 Handler 绑定；
- `http_user_handler.go`：解析参数并调用 `UserService`；
- `http_response.go`：统一 HTTP 响应和错误映射；
- `grpc.go`：Kitex Handler 和 gRPC 错误映射。

Handler 不直接访问数据库。新增接口时，应把路由注册和 Handler 放在这一层，把实际业务规则放在 `internal/biz`。

### Hertz Handler 与 Controller

`UserHTTPHandler` 相当于 Java/Spring 或 ASP.NET 项目中的 Controller。Go 和 Hertz 社区通常使用 `Handler` 这个名称，但它承担的仍是协议入口职责：解析 HTTP 请求、调用 Service，并把结果转换成 HTTP 响应。

以创建用户方法为例：

```go
func (h UserHTTPHandler) Create(ctx context.Context, c *app.RequestContext) {
    // 从 c 读取 HTTP 请求，通过 h.Service 调用业务层，再把响应写入 c。
}
```

各部分含义如下：

| 部分 | 类型 | 含义 |
|---|---|---|
| `h` | `UserHTTPHandler` | 方法接收者，表示 `Create` 属于哪个 Handler，并通过 `h.Service` 访问注入的业务服务 |
| `ctx` | `context.Context` | Hertz 传入的请求生命周期上下文，用于向 Service、Repository 和数据库传递取消、超时、追踪等信号 |
| `c` | `*app.RequestContext` | Hertz HTTP 请求/响应上下文，用于读取 Path、Query、Header、Body，以及写入状态码和 JSON 响应 |

从函数调用角度看，`h`、`ctx`、`c` 都是传入函数的数据，其中 `h` 使用 Go 的方法接收者语法写在函数名前。这个 Handler 没有 Go 返回值，因为参数列表后直接是函数体：

```go
func (...) {
}
```

如果存在返回值，签名会写成 `func (...) error` 或 `func (...) (*User, error)`。Hertz Handler 不通过 `return response` 返回 HTTP 内容，而是把响应写入 `c`：

```go
c.JSON(consts.StatusCreated, response)
```

Handler 中单独出现的 `return` 只表示提前结束当前方法，不携带返回数据：

```go
if err != nil {
    c.JSON(consts.StatusBadRequest, errorResponse)
    return
}
```

`ctx` 和 `c` 不能混用。`ctx` 应继续向下传递：

```text
Handler(ctx) -> Service(ctx) -> Repository(ctx) -> PostgreSQL
```

`c` 只属于 HTTP 层，不应传入 Service 或 Repository。Handler 应先把 HTTP 参数转换为普通 DTO 或 Go 值，再调用业务层，这样 `biz` 不依赖 Hertz，同一个 Service 才能同时供 HTTP 和 gRPC 使用。

Handler 可以负责参数解析、协议格式校验、Service 调用及 HTTP 错误映射，但不应直接执行 SQL、创建数据库连接、管理 Repository 或实现核心业务规则。

### 其他工程目录

`internal/config` 负责把 YAML 和环境变量转换为强类型配置；`pkg` 只放真正准备被其他 module 使用的通用代码，不要把所有工具函数都放进去；`configs` 保存默认配置；`migrations` 保存可追踪的数据库结构变更；`scripts` 保存可重复执行的生成和运维命令。

## 依赖方向

请求期间的调用方向为：

```text
HTTP Route -> HTTP Handler ┐
                            ├-> Business Service -> Repository Interface
gRPC Method -> Kitex Handler┘                         |
                                                       v
                                             PostgreSQL Repository -> PostgreSQL
```

代码依赖应遵循以下规则：

- `server` 可以依赖 `biz` 的 Service 契约，但不能依赖 `data/db/repo`；
- `biz` 定义业务和仓储抽象，不能反向依赖 `server` 或具体数据库实现；
- `data/db/repo` 实现 `biz` 的仓储接口；
- `app` 是唯一知道具体 Service 和 Repository 如何连接的地方；
- `cmd` 只调用 `app` 和对应 Server 构造函数，不展开底层组装细节。

## 本地启动

要求 Go 1.23+、PostgreSQL 14+。先创建数据库并执行迁移：

```sh
createdb nino
DATABASE_URL='postgres://postgres:postgres@localhost:5432/nino?sslmode=disable' ./scripts/migrate.sh
go run ./cmd/api-server
go run ./cmd/grpc-server
# 启用 Kafka 后另起一个终端运行独立 Worker（brokers/topics 按本地环境调整）。
NINO_KAFKA_ENABLED=true NINO_KAFKA_BROKERS=localhost:9092 \
NINO_KAFKA_TOPICS=user-events,audit-events go run ./cmd/work
```

默认 HTTP 监听 `:8080`，gRPC 监听 `:9090`。配置文件为 `configs/app.yaml`，也可以使用 `NINO_HTTP_ADDR`、`NINO_GRPC_ADDR`、`NINO_DATABASE_URL`、`NINO_DB_MAX_CONNS`、`NINO_DB_MIN_CONNS` 和 `NINO_LOG_LEVEL` 覆盖配置。配置文件路径可由 `NINO_CONFIG_PATH` 指定。

## Kafka 配置

Kafka 配置位于 `configs/app.yaml` 的 `kafka` 节点。下面是完整结构；`brokers` 和 `topics` 都是列表，环境变量形式使用逗号分隔：

```yaml
kafka:
  enabled: false
  brokers: []
  group_id: "nino-data-work"
  topics: []
  client_id: "nino-data"
  retry_interval_seconds: 5
  publish_timeout_seconds: 10
  worker_concurrency: 8
  poll_max_records: 100
  shutdown_timeout_seconds: 30
```

启用 Kafka 时，`brokers`、`group_id`、`topics`、`client_id` 必须非空；所有时间和并发/批次参数都必须为正数。`brokers` 与 `topics` 是列表，环境变量形式使用逗号分隔。API 使用 `enabled`、`brokers`、`topics`、`client_id` 和 `publish_timeout_seconds` 创建 Producer；Worker 还使用 `group_id`、`retry_interval_seconds`、`worker_concurrency`、`poll_max_records` 和 `shutdown_timeout_seconds`。配置文件值可以用以下环境变量覆盖：

| 环境变量 | 含义与示例 |
|---|---|
| `NINO_KAFKA_ENABLED` | 是否启用 Kafka Producer/Worker；`true` 或 `false` |
| `NINO_KAFKA_BROKERS` | Broker 地址列表，逗号分隔，例如 `kafka:9092,kafka-2:9092` |
| `NINO_KAFKA_GROUP_ID` | Worker Consumer Group 名称，例如 `nino-data-work` |
| `NINO_KAFKA_TOPICS` | Producer 白名单及 Worker 订阅 Topic，逗号分隔，例如 `user-events,audit-events` |
| `NINO_KAFKA_CLIENT_ID` | franz-go 客户端标识，例如 `nino-data-api` 或 `nino-data-work` |
| `NINO_KAFKA_RETRY_INTERVAL_SECS` | Worker Handler/offset 提交失败后的重试间隔（秒），例如 `2` |
| `NINO_KAFKA_PUBLISH_TIMEOUT_SECS` | API 等待 broker ack 的超时时间（秒），例如 `10` |
| `NINO_KAFKA_WORKER_CONCURRENCY` | Worker 跨 partition 的有界并发任务数，例如 `8` |
| `NINO_KAFKA_POLL_MAX_RECORDS` | Worker 每次轮询最多接收的消息数，例如 `100` |
| `NINO_KAFKA_SHUTDOWN_TIMEOUT_SECS` | Worker 收到 SIGTERM 后等待在途任务/提交完成的最长时间，例如 `30` |

在本地 Compose 中，`api` 和 `work` 容器通过 `kafka:9092` 连接 Kafka；宿主机执行 Kafka CLI 时也通过 `docker compose exec kafka` 进入 Kafka 容器，不要把容器内地址改成 `localhost:9092`。`NINO_CONFIG_PATH` 仍可指定 YAML 路径。

## Docker Compose

Compose 会启动 PostgreSQL、单节点 Kafka 和 Kafbat Kafka UI，运行一次数据库迁移，创建 `user-events`、`audit-events` 两个 Topic，再启动 API、独立 Worker 和 gRPC 服务。后台启动：

PostgreSQL 容器的 `5432` 映射到宿主机 `51432`；宿主机数据库客户端使用 `localhost:51432`，Compose 内部服务仍使用 `db:5432`。

```sh
docker compose up --build -d
```

Kafka 可视化面板地址为 `http://localhost:8081`。面板中的 `local` 集群通过 Compose 内部地址 `kafka:9092` 连接 Kafka，可以查看 Broker、Topic、消息和 Consumer Group。

HTTP 健康检查地址为 `http://localhost:8080/healthz`。查看当前 Topic：

```sh
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

推荐通过 API 发布事件；API 只在 broker ack 后返回 `202`：

```sh
curl -i -X POST http://localhost:8080/v1/users \
  -H 'content-type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com"}'

# 使用上一步响应中的 user_id 查询；刚返回 202 时短暂 404 是最终一致性窗口。
curl -i http://localhost:8080/v1/users/<user_id>

curl -i -X POST http://localhost:8080/v1/events/audit-events \
  -H 'content-type: application/json' \
  -d '{"key":"demo-user","payload":{"event":"login","actor":"demo-user"}}'
```

也可以使用 Kafka CLI 向仍采用占位 Handler 的 `audit-events` 写入测试消息。不要手工向 `user-events` 写任意 JSON；它要求严格的 `user.create.v1` schema、UUID 和与 `user_id` 一致的 Kafka key，应始终通过 `/v1/users` 生成：

```sh
printf '%s\n' '{"event":"login","actor":"demo-user"}' | \
  docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic audit-events
```

查看独立 Worker 日志：

```sh
docker compose logs -f work
```

`audit-events` 日志中的 `topic`、`partition`、`offset`、`key_size` 和 `value_size` 是占位 Handler 记录的元数据，不包含正文；`user-events` 成功日志会包含事件和用户 ID。查看 Worker Consumer Group `nino-data-work` 的 partition offset、lag 等信息：

```sh
docker compose exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe --group nino-data-work
```

`work` 服务的 `container_name` 是 `nino-data-work`，并配置了 `restart: unless-stopped` 与约 35 秒的停止宽限期。Worker 进程重启后会以同一个 Group 继续消费 Kafka 中尚未提交的 offset；已提交 offset 不会回退，未提交消息可能至少一次投递。查看所有服务日志、单独重启 Worker 或停止并删除本次 Compose 容器（命名卷仍保留）：

```sh
docker compose logs -f
docker compose restart work
docker compose down
```

Swagger API 文档：

- Swagger UI：`http://localhost:8080/swagger/`
- OpenAPI YAML：`http://localhost:8080/openapi.yaml`

## 新增业务模块

新增业务模块时，先在 `internal/biz` 定义服务和仓储边界，再在 `internal/data/db` 实现仓储；随后把具体服务加入 `internal/app.Services` 并在 `app.New` 中完成 wiring，最后在 `internal/server` 为 HTTP 注册模块路由、为 Kitex 注册对应服务。总路由只负责模块编排，模块路由和 handler 负责本模块的协议转换。

## API 示例

创建用户：

```sh
curl -i -X POST http://localhost:8080/v1/users \
  -H 'content-type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com"}'
```

按 ID 查询：

```sh
curl -i http://localhost:8080/v1/users/<id>
```

Kitex gRPC 调用（需要 `grpcurl`；Kitex 服务不提供 gRPC reflection，因此显式传入 proto）：

```sh
grpcurl -plaintext -import-path api/proto -proto user.proto \
  -d '{"name":"Bob","email":"bob@example.com"}' \
  localhost:9090 nino.user.v1.UserService/CreateUser
grpcurl -plaintext -import-path api/proto -proto user.proto \
  -d '{"id":"<id>"}' \
  localhost:9090 nino.user.v1.UserService/GetUser
```

协议源文件在 `api/proto/user.proto`，Kitex 生成代码已提交到 `api/gen`。安装 `protoc` 和 Kitex v0.16.3 后可重新生成：

```sh
go install github.com/cloudwego/kitex/tool/cmd/kitex@v0.16.3
./scripts/gen-proto.sh
```

如果 protoc 的 well-known types 不在默认前缀，可设置 `PROTOC_INCLUDE` 指向包含 `google/protobuf/timestamp.proto` 的目录。OpenAPI 描述位于 `api/openapi/user.swagger.yaml`。

## 验证

```sh
gofmt -w $(find . -name '*.go' -type f)
go test ./...
go build ./...
docker compose config
```
