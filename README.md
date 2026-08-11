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
│   └── grpc-server/             # Kitex gRPC 服务入口
├── internal/                    # 本项目私有实现，仓库外部不能直接导入
│   ├── app/                     # Composition Root，统一组装全部依赖
│   ├── biz/                     # 业务实体、业务规则、Service 和仓储接口
│   ├── config/                  # 配置结构、文件读取和环境变量覆盖
│   ├── data/                    # 基础设施和数据访问实现
│   │   └── db/                  # PostgreSQL 连接池及数据库相关代码
│   │       ├── model/           # 与数据库记录对应的持久化模型
│   │       └── repo/            # Repository 实现、SQL 和模型转换
│   └── server/                  # HTTP/gRPC 协议适配、路由和 Handler
├── pkg/                         # 确实允许其他项目导入的通用包
│   └── logger/                  # 与业务无关的日志封装
├── configs/                     # 可部署的默认配置文件
├── migrations/                  # 按顺序执行的数据库结构迁移
├── scripts/                     # 代码生成、迁移等开发和运维脚本
├── Dockerfile                   # 构建 HTTP/gRPC 服务镜像
├── docker-compose.yml           # 本地编排 PostgreSQL、迁移和两个服务
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
- 启动 Hertz 或 Kitex；
- HTTP 入口调用 `h.Spin()`，由 Hertz 监听退出信号并执行优雅关闭。

`main.go` 调用 `internal/app.New` 创建完整应用是正常的启动流程，但不应该自己执行 SQL、创建 Repository 或处理业务请求。运行期间的请求仍然由路由进入 Handler。

`cmd/api-server` 的启动形态借鉴 CloudWeGo Hertz 官方
`hertz-examples/bizdemo/hertz_gorm`：构造 Hertz 实例、注册路由，再调用
`h.Spin()`。Spin 默认处理 `SIGINT`、`SIGHUP` 和 `SIGTERM`，并在退出时等待
进行中的请求完成；本项目仍保留 `internal/app` composition root 和
Handler -> Service -> Repository 的依赖注入分层。具体来说，`NewHTTPServer`
只创建 Hertz 与全局 404/405 fallback，业务 service 通过显式的
`RegisterHTTPRoutes` 注入，不把官方示例中的全局 DB 或 Handler 直连方式带入本项目。

### `internal/app`：依赖组装层

这是应用的 Composition Root。它按照 PostgreSQL Pool -> Repository -> Service 的顺序创建对象，并通过 `Services` 集合交给 HTTP、gRPC 服务使用，同时负责关闭数据库连接池。

依赖组装只在程序启动时执行一次，不属于某个 HTTP/gRPC 请求的调用链。以后增加 `OrderService` 时，应在这里增加对应 Repository 和 Service 的组装，而不是继续向 `main.go` 填充初始化细节。

### `internal/biz`：业务层

业务层保存业务实体、业务错误、Repository 接口和 Service 实现。例如 `UserService` 负责用户名、邮箱校验以及创建、查询用户的用例编排。

Repository 接口定义在业务层，是因为业务层决定自己需要哪些持久化能力；数据层只负责实现它。业务层不应依赖 Hertz、Kitex、PostgreSQL 驱动或 HTTP 状态码，因此相同 Service 可以同时被 HTTP 和 gRPC Handler 使用。

### `internal/data`：数据与基础设施层

`data/db` 创建和配置 PostgreSQL 连接池；`data/db/model` 定义与数据库记录对应的持久化模型；`data/db/repo` 实现业务层声明的 Repository 接口，负责 SQL、唯一键冲突以及 DB Model 与业务实体之间的转换。DB Model 不应直接传给 Handler，数据库字段变化也不应直接改变 HTTP DTO。

这一层只处理数据存取，不负责邮箱是否合法、用户是否允许创建等业务规则。未来加入 Redis、Kafka 时，可以分别放在 `internal/data/redis`、`internal/data/kafka`，并继续通过业务层接口隔离具体实现。

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
```

默认 HTTP 监听 `:8080`，gRPC 监听 `:9090`。配置文件为 `configs/app.yaml`，也可以使用 `NINO_HTTP_ADDR`、`NINO_GRPC_ADDR`、`NINO_DATABASE_URL`、`NINO_DB_MAX_CONNS`、`NINO_DB_MIN_CONNS` 和 `NINO_LOG_LEVEL` 覆盖配置。配置文件路径可由 `NINO_CONFIG_PATH` 指定。

## Docker Compose

Compose 会启动 PostgreSQL，运行一次迁移，再启动 HTTP 和 gRPC 服务：

```sh
docker compose up --build
```

HTTP 健康检查地址为 `http://localhost:8080/healthz`。

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
