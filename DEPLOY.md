# Docker 部署说明

## 前置条件

执行以下命令前，请先启动 Docker Desktop。

## 一键启动

在项目根目录执行以下命令，构建并启动前端、后端、MySQL 和 Redis 服务：

```powershell
docker compose -f docker-compose.deploy.yml up -d --build
```

## 前端地址

浏览器访问：[http://localhost:8080](http://localhost:8080)。

## 一键停止

在项目根目录执行以下命令，停止并移除本项目的容器和网络：

```powershell
docker compose -f docker-compose.deploy.yml down
```

停止服务后会保留 MySQL 和 Redis 数据卷，下一次启动仍会使用已有数据。
