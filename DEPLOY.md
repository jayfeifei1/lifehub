# Docker 部署说明

## 架构概览

`docker-compose.deploy.yml` 一键拉起 4 个服务：

| 服务 | 镜像 | 对外端口 | 说明 |
|---|---|---|---|
| frontend | nginx:1.27-alpine | **8080** | 前端静态页面，挂载 `./frontend-public`（只读） |
| backend | Dockerfile 本地构建 | **8081** | Spring Boot 后端，jar 来自 `target/` |
| mysql | mysql:8.0 | - | 数据卷 `hmdp_mysql_data`，首次启动自动执行 `docker/mysql/hmdp.sql` 建表 |
| redis | redis:7-alpine | **6381** | 数据卷 `hmdp_redis_data`，密码 `123456`，启动时自动创建 `stream.orders` 消费组 |

## 前置条件

1. 启动 **Docker Desktop**
2. 安装 **JDK 17 + Maven**（仅代码变更后打包后端需要）

## 第一步：打包后端（仅代码变更后需要）

```powershell
mvn package -DskipTests
```

生成 `target/hm-dianping-0.0.1-SNAPSHOT.jar`，Dockerfile 构建镜像时复制进容器。

## 第二步：确认前端文件

前端静态资源位于项目根目录 **`./frontend-public`**（nginx 与后端图片上传共享此目录，请勿删除）。

## 一键启动

```powershell
docker compose -f docker-compose.deploy.yml up -d --build
```

- `--build`：后端代码变更后必须保留，用于重新构建 backend 镜像
- 首次启动：MySQL 自动建表，Redis 自动创建秒杀消息队列消费组，后端自动预热秒杀库存与店铺 GEO 数据

## 访问地址

- 前端页面：**http://localhost:8080**
- 后端 API：http://localhost:8081 （前端经 nginx `/api` 代理访问）

## 常用命令

| 命令 | 作用 |
|---|---|
| `docker compose -f docker-compose.deploy.yml up -d --build` | 启动 / 重建全部服务 |
| `docker compose -f docker-compose.deploy.yml down` | 停止并移除容器（**保留** MySQL/Redis 数据卷） |
| `docker compose -f docker-compose.deploy.yml down -v` | 停止并**删除数据卷**（数据清空，慎用） |
| `docker compose -f docker-compose.deploy.yml logs -f backend` | 查看后端日志 |
| `docker compose -f docker-compose.deploy.yml restart frontend` | 重启前端（nginx 配置变更后生效） |
| `docker compose -f docker-compose.deploy.yml up -d --build backend` | 仅重建后端 |
| `docker compose -f docker-compose.deploy.yml ps` | 查看服务状态 |

## 数据说明

- MySQL / Redis 数据保存在命名数据卷 `hmdp_mysql_data` / `hmdp_redis_data`，执行 `down` 后不丢失，下次启动继续使用
- ⚠️ `docker/mysql/hmdp.sql` 只包含**表结构**，不包含业务数据。全新环境（删除数据卷后）启动得到的是空库，店铺 / 优惠券 / 博客等演示数据需手动导入

## 本地开发模式（可选）

仅需 MySQL + Redis（无前后端容器）时使用：

```powershell
docker compose -f docker-compose.local.yml up -d
```

MySQL 映射 `3308`、Redis 映射 `6380`（与 `application.yaml` 默认值一致，密码 `123456`），可在 IDE 中直接运行后端调试。

## 图片上传

- 后端上传目录：`./frontend-public/imgs`（backend 容器可写，nginx 直接提供 `/imgs/...` 访问）
- 上传目录路径可通过环境变量 `UPLOAD_DIR` 覆盖（见 `docker-compose.deploy.yml`）
