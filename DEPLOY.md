# Docker 部署说明

## 架构概览

`docker-compose.deploy.yml` 一键拉起以下常驻服务（初始化容器执行完成后会正常退出）：

| 服务 | 镜像 | 对外端口 | 说明 |
|---|---|---|---|
| frontend | nginx:1.27-alpine | **18080** | 前端静态页面，挂载 `./frontend-public`（只读） |
| backend | Dockerfile 多阶段构建 | **18081** | Docker 内部使用 Maven 编译，再通过 JRE 镜像运行 |
| mysql | mysql:8.0 | - | 数据卷 `hmdp_mysql_data`，首次启动自动执行 `docker/mysql/hmdp.sql` 建表 |
| redis | redis:7-alpine | **6381** | 数据卷 `hmdp_redis_data`，密码 `123456`，启动时自动创建 `stream.orders` 消费组 |
| rocketmq-namesrv | apache/rocketmq:5.3.2 | **9876** | RocketMQ 路由服务 |
| rocketmq-broker | apache/rocketmq:5.3.2 | **10909/10911/10912** | 消息持久化、事务回查、重试及延迟消息 |

## 前置条件

1. 启动 **Docker Desktop**
2. 确保 Docker 可以拉取项目所需镜像及 Maven 依赖

不需要在宿主机安装 JDK 或 Maven，后端由 Docker 多阶段构建自动编译。

## 确认前端文件

前端静态资源位于项目根目录 **`./frontend-public`**（nginx 与后端图片上传共享此目录，请勿删除）。

## 一键启动后端及全部依赖

以下命令会自动构建并启动 Spring Boot、MySQL、Redis、RocketMQ NameServer/Broker，同时执行数据库迁移、Redis 初始化和 RocketMQ Topic 初始化；不启动前端 Nginx：

```powershell
docker compose -f docker-compose.deploy.yml up -d --build backend
```

## 一键启动完整项目

需要同时启动前端 Nginx 时执行：

```powershell
docker compose -f docker-compose.deploy.yml up -d --build
```

- `--build`：在 Docker 内编译后端并构建运行镜像，代码变更后必须保留
- 首次启动：MySQL 自动建表，Redis 自动创建秒杀消息队列消费组，RocketMQ 自动创建业务 Topic
- 已有 MySQL 数据卷：`mysql-migrate` 会幂等补建 RocketMQ 本地事务日志表
- 后端启动后自动预热秒杀库存与店铺 GEO 数据

## 访问地址

- 前端页面：**http://localhost:18080**
- 后端 API：http://localhost:18081 （前端经 nginx `/api` 代理访问）

宿主机端口可通过 `FRONTEND_PORT`、`BACKEND_PORT` 覆盖。例如：

```powershell
$env:FRONTEND_PORT = "28080"
$env:BACKEND_PORT = "28081"
docker compose -f docker-compose.deploy.yml up -d --build
```

## 消息链路切换

默认使用 RocketMQ，不会同时向两套队列投递：

- 秒杀下单：`rocketmq`（默认）或 `redis-stream`（回退）
- 店铺缓存失效：`rocketmq`（默认）或 `redis-list`（回退）

默认模式无需设置环境变量，直接执行启动命令即可。需要临时切回 Redis 实现时：

```powershell
$env:SECKILL_QUEUE_MODE = "redis-stream"
$env:CACHE_INVALIDATION_MODE = "redis-list"
docker compose -f docker-compose.deploy.yml up -d --force-recreate backend frontend
```

在同一个 PowerShell 会话中恢复 RocketMQ：

```powershell
$env:SECKILL_QUEUE_MODE = "rocketmq"
$env:CACHE_INVALIDATION_MODE = "rocketmq"
docker compose -f docker-compose.deploy.yml up -d --force-recreate backend frontend
```

环境变量只对当前 PowerShell 会话生效；新终端未设置时自动使用 RocketMQ 默认值。

## 常用命令

| 命令 | 作用 |
|---|---|
| `docker compose -f docker-compose.deploy.yml up -d --build` | 启动 / 重建全部服务 |
| `docker compose -f docker-compose.deploy.yml down` | 停止并移除容器（**保留** MySQL/Redis 数据卷） |
| `docker compose -f docker-compose.deploy.yml down -v` | 停止并**删除数据卷**（数据清空，慎用） |
| `docker compose -f docker-compose.deploy.yml logs -f backend` | 查看后端日志 |
| `docker compose -f docker-compose.deploy.yml restart frontend` | 重启前端（nginx 配置变更后生效） |
| `docker compose -f docker-compose.deploy.yml up -d --build backend` | 启动 / 重建后端及 MySQL、Redis、RocketMQ 等全部依赖 |
| `docker compose -f docker-compose.deploy.yml ps` | 查看服务状态 |

## 数据说明

- MySQL / Redis / RocketMQ Broker 数据分别保存在命名卷 `hmdp_mysql_data`、`hmdp_redis_data`、`hmdp_rocketmq_broker_store`，执行 `down` 后不丢失
- ⚠️ `docker/mysql/hmdp.sql` 只包含**表结构**，不包含业务数据。全新环境（删除数据卷后）启动得到的是空库，店铺 / 优惠券 / 博客等演示数据需手动导入

## 本地开发模式（可选）

仅需 MySQL + Redis（无前后端容器）时使用：

```powershell
docker compose -f docker-compose.local.yml up -d
$env:SECKILL_QUEUE_MODE = "redis-stream"
$env:CACHE_INVALIDATION_MODE = "redis-list"
```

MySQL 映射 `3308`、Redis 映射 `6380`（与 `application.yaml` 默认值一致，密码 `123456`）。设置上述回退变量后，可在同一 PowerShell 会话中通过 IDE 启动后端；若需要调试默认 RocketMQ 链路，请使用完整部署配置。

## 图片上传

- 后端上传目录：`./frontend-public/imgs`（backend 容器可写，nginx 直接提供 `/imgs/...` 访问）
- 上传目录路径可通过环境变量 `UPLOAD_DIR` 覆盖（见 `docker-compose.deploy.yml`）
