# 使用 SeaTunnel 实现 MySQL CDC 同步数据到 ClickHouse

## 概述

本文介绍如何使用 [Apache SeaTunnel](https://seatunnel.apache.org/) 2.3.13 通过 Docker 部署集群模式，实现 MySQL 数据基于 CDC（Change Data Capture，变更数据捕获）实时同步到 ClickHouse。

整体架构为 **MySQL → SeaTunnel (CDC + 字段转换) → ClickHouse**，SeaTunnel 通过读取 MySQL Binlog 捕获数据变更，经过字段类型转换后写入 ClickHouse。

## 前置条件

| 组件 | 要求 |
|------|------|
| MySQL | 5.7+ 或 8.0+，需开启 Binlog（ROW 格式） |
| ClickHouse | 已部署并可访问 |
| Docker & Docker Compose | 已安装 |
| Claude Code | 已安装（用于 AI 生成同步配置） |

---

## 步骤 1：使用 Docker 部署 SeaTunnel 集群

### 1.1 集群角色说明

| 角色 | 职责 |
|------|------|
| **Master** | 集群协调者，负责任务调度、REST API 接口（提交/停止任务） |
| **Worker** | 工作节点，执行实际的数据同步任务 |
| **Client** | 命令行客户端，用于向集群提交同步任务 |

### 1.2 提取容器内默认配置

SeaTunnel 官方镜像内包含默认的 `config/` 和 `lib/` 目录。首次部署时，建议先将这些文件提取到宿主机，方便后续修改。

**方法一：使用临时容器提取（推荐）**

```bash
# 创建临时容器并复制文件
docker create --name seatunnel-temp apache/seatunnel:2.3.13
docker cp seatunnel-temp:/opt/seatunnel/config ./seatunnel/config
docker cp seatunnel-temp:/opt/seatunnel/lib ./seatunnel/lib
docker rm seatunnel-temp
```

**方法二：使用具名卷提取**

```bash
# 1. 创建具名卷并启动临时容器
docker run -d --name seatunnel-temp \
  -v seatunnel_config:/opt/seatunnel/config \
  -v seatunnel_lib:/opt/seatunnel/lib \
  apache/seatunnel:2.3.13

# 2. 从 Docker 卷目录拷贝数据到当前项目
cp -r /var/lib/docker/volumes/seatunnel_config/_data/* ./seatunnel/config/
cp -r /var/lib/docker/volumes/seatunnel_lib/_data/* ./seatunnel/lib/

# 3. 清理
docker rm -f seatunnel-temp
docker volume rm seatunnel_config seatunnel_lib
```

### 1.3 Docker Compose 配置

创建 `docker-compose.yml`，部署 1 个 Master + 2 个 Worker + 1 个 Client：

```yaml
services:
  seatunnel_master:
    image: apache/seatunnel:2.3.13
    container_name: seatunnel_master
    environment:
      # 集群成员列表（所有 master 和 worker 节点）
      - ST_DOCKER_MEMBER_LIST=seatunnel_master:5801,seatunnel_worker_1:5801,seatunnel_worker_2:5801
      # 启用 Hazelcast REST API，允许通过 HTTP 提交和管理任务
      - HZ_NETWORK_RESTAPI_ENABLED=true
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r master
      "
    ports:
      - "5801:5801"   # Hazelcast 集群通信 & REST API 端口
    networks:
      - backend

  seatunnel_worker_1:
    image: apache/seatunnel:2.3.13
    container_name: seatunnel_worker_1
    environment:
      - ST_DOCKER_MEMBER_LIST=seatunnel_master:5801,seatunnel_worker_1:5801,seatunnel_worker_2:5801
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      "
    depends_on:
      - seatunnel_master
    networks:
      - backend

  seatunnel_worker_2:
    image: apache/seatunnel:2.3.13
    container_name: seatunnel_worker_2
    environment:
      - ST_DOCKER_MEMBER_LIST=seatunnel_master:5801,seatunnel_worker_1:5801,seatunnel_worker_2:5801
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      "
    depends_on:
      - seatunnel_master
    networks:
      - backend

  seatunnel-client:
    image: apache/seatunnel:2.3.13
    container_name: seatunnel-client
    environment:
      # Client 只需连接 Master 即可提交任务
      - ST_DOCKER_MEMBER_LIST=seatunnel_master:5801
    tty: true
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
    depends_on:
      - seatunnel_master
    networks:
      - backend

networks:
  backend:
    driver: bridge
```

> **注意：** 如果不需要多 Worker 高可用，可以只部署 1 个 Worker 节点，同步从 `ST_DOCKER_MEMBER_LIST` 中移除多余的节点即可。

启动集群：

```bash
docker compose up -d
```

---

## 步骤 2：安装字段转换插件

SeaTunnel 默认不支持 MySQL 数字时间戳到 ClickHouse 日期类型的自动转换。需要安装第三方 Transform 插件 [seatunnel-transform-fieldconvert](https://github.com/quansitech/seatunnel-transform-fieldconvert)。

**为什么需要此插件？**

MySQL 中常用 `int` / `bigint` 类型存储 Unix 时间戳（如 `1700000000`），但 ClickHouse 中对应的字段通常为 `DateTime` 或 `DateTime64` 类型。SeaTunnel 原生不支持这种类型转换，该插件提供了以下能力：

- `unix_timestamp_to_datetime`：将数字时间戳转换为日期时间类型
- `cast`：类型转换（如 MySQL 的 `tinyint(1)` 转 ClickHouse 的 `Int8`）

```bash
# 下载与 SeaTunnel 版本匹配的 jar 包
wget https://github.com/quansitech/seatunnel-transform-fieldconvert/releases/download/v2.3.13/seatunnel-transform-fieldconvert-2.3.13.jar \
  -O ./seatunnel/lib/seatunnel-transform-fieldconvert-2.3.13.jar
```

> **重要：** 插件版本必须与 SeaTunnel 版本一致，否则可能出现兼容性问题。

---

## 步骤 3：配置 MySQL 开启 Binlog 并创建 CDC 账号

SeaTunnel 的 MySQL-CDC 连接器依赖 Binlog 进行变更数据捕获。必须确保 MySQL 已正确配置。

### 3.1 开启 Binlog

编辑 MySQL 配置文件（通常为 `/etc/my.cnf` 或 `/etc/mysql/mysql.conf.d/mysqld.cnf`），在 `[mysqld]` 段添加：

```ini
[mysqld]
# 1. 必须配置唯一的 Server ID（1 到 2^32-1 之间的整数）
#    注意：如果有多个 MySQL 实例被同一 SeaTunnel 集群同步，每个实例的 server-id 必须不同
server-id = 1

# 2. 开启 Binlog，并指定日志文件名前缀
log-bin = mysql-bin

# 3. 必须设置为 ROW 格式（CDC 无法解析 STATEMENT 或 MIXED 格式的完整数据）
#    注意：MySQL 8.0.17+ 默认已是 ROW 格式，此参数已被标记为弃用，
#    但为了兼容 MySQL 5.7，建议显式设置
binlog_format = ROW

# 4. 必须设置为 FULL（记录行变更前后的完整数据，便于 CDC 识别旧值与新值）
binlog_row_image = FULL

# 5. 推荐：设置 Binlog 过期时间（单位：秒），避免磁盘被占满
binlog_expire_logs_seconds = 604800
```

**修改后需要重启 MySQL 服务：**

```bash
systemctl restart mysql
# 或
systemctl restart mysqld
```

**验证 Binlog 是否生效：**

```sql
-- 检查 binlog 是否开启及格式
SHOW VARIABLES LIKE 'binlog_format';
-- 预期结果: ROW

SHOW VARIABLES LIKE 'log_bin';
-- 预期结果: ON

SHOW VARIABLES LIKE 'binlog_row_image';
-- 预期结果: FULL
```

### 3.2 创建 CDC 同步专用账号

为 SeaTunnel 创建一个独立的 MySQL 账号，仅授予 CDC 同步所需的最小权限：

```sql
-- 1. 创建专门用于 CDC 同步的账号（推荐独立账号，便于权限管理和审计）
CREATE USER 'seatunnel_cdc'@'%' IDENTIFIED BY 'your_password';

-- 2. 授予必要权限
-- REPLICATION SLAVE  : 读取 Binlog 流（CDC 核心）
-- REPLICATION CLIENT : 查询主从状态信息
-- SELECT             : 读取表结构 + 全量快照阶段拉取数据
-- RELOAD             : 锁表或获取一致性全局位置（FLUSH TABLES）
-- SHOW DATABASES     : 遍历数据库名
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'seatunnel_cdc'@'%';

-- 3. 刷新权限使之生效
FLUSH PRIVILEGES;
```

---

## 步骤 4：生成同步配置文件

### 4.1 使用 AI 快速生成配置

打开 [Claude Code](https://code.claude.com/)，在项目目录下执行以下命令，根据数据库结构自动生成同步配置：

> ⚠️ **安全提示：** 请使用开发环境的数据库账号来生成配置，**不要**直接使用生产数据库账号密码。

```bash
/generating-seatunnel-mysql-to-clickhouse-config 帮我生成 donation 数据库的 ClickHouse 同步配置，MySQL 数据库地址 localhost 用户名 root 密码 root
```

该命令会：
1. 连接 MySQL 读取表结构（自动排除视图和框架系统表）
2. 识别需要类型转换的字段（时间戳字段、tinyint 字段）
3. 检测无主键的表并给出警告
4. 生成完整的 HOCON 配置文件

### 4.2 更新配置中的数据库连接信息

生成配置后，需要将 MySQL 连接信息更新为步骤 3 创建的 CDC 专用账号，并补充 ClickHouse 连接信息：

```hocon
source {
  MySQL-CDC {
    username = "seatunnel_cdc"           # 改为 CDC 专用账号
    password = "your_password"            # 改为 CDC 账号密码
    base-url = "jdbc:mysql://<mysql_host>:<mysql_port>"
    server-id = 5656                      # 必须唯一，不能与 MySQL 的 server-id 冲突
    # ...其余配置由 AI 自动生成
  }
}

sink {
  Clickhouse {
    host = "<clickhouse_host>:8123"       # ClickHouse HTTP 接口地址
    database = "<database>"
    username = "<ck_user>"
    password = "<ck_password>"
    # ...其余配置由 AI 自动生成
  }
}
```

> **关于 `server-id`：** 此参数是 CDC 连接器在 MySQL 中注册的身份标识，必须保证全局唯一。如果有多个同步任务或多个 SeaTunnel 实例同步同一 MySQL，每个任务的 `server-id` 必须不同，否则会互相干扰。

### 4.3 放置配置文件

将生成的配置文件放入 `seatunnel/config/` 目录：

```bash
cp mysql_to_clickhouse_donation.conf ./seatunnel/config/
```

---

## 步骤 5：在 ClickHouse 中预先创建数据库

在启动同步任务之前，需要先在 ClickHouse 中创建与 MySQL 同名的数据库。SeaTunnel 的 `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST` 会自动创建表结构，但**不会自动创建数据库**。

```sql
-- 连接到 ClickHouse，创建与 MySQL 同名的数据库
-- 例如 MySQL 中数据库名为 donation，则 ClickHouse 中也创建 donation
CREATE DATABASE IF NOT EXISTS donation;
```

如果同步多个数据库，每个都需要手动创建：

```sql
CREATE DATABASE IF NOT EXISTS db_name_1;
CREATE DATABASE IF NOT EXISTS db_name_2;
```

> **说明：** 如果 ClickHouse 中未提前创建数据库，同步任务启动时会报类似 `Database xxx doesn't exist` 的错误。

---

## 步骤 6：启动和管理同步任务

### 6.1 启动同步任务

```bash
# 在 client 容器中提交同步任务到集群
docker exec -d seatunnel-client \
  ./bin/seatunnel.sh \
    -c ./config/mysql_to_clickhouse_donation.conf \
    -m cluster
```

参数说明：
- `-c`：指定配置文件路径（相对于容器内工作目录）
- `-m cluster`：以集群模式运行（任务由 Master 分发到 Worker 执行）
- `-d`：Docker 后台运行标志

### 6.2 查看运行中的任务

```bash
# 通过 Hazelcast REST API 查看（端口 5801）
curl -s http://localhost:5801/hazelcast/rest/maps/running-jobs | python3 -m json.tool
```

### 6.3 停止任务

```bash
# 先通过 running-jobs 接口获取 JOB_ID，然后停止
curl -s -X POST http://localhost:5801/hazelcast/rest/maps/stop-job \
  -H "Content-Type: application/json" \
  -d '{"jobId": "<JOB_ID>"}'
```

### 6.4 启用 Web 监控界面（可选）

编辑 `seatunnel/config/seatunnel.yaml`，启用 HTTP 监控服务：

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
    history-job-expire-minutes: 1440
    backup-count: 1
    queue-type: blockingqueue
    print-execution-info-interval: 60
    print-job-metrics-info-interval: 60
    slot-service:
      dynamic-slot: true
    checkpoint:
      interval: 10000
      timeout: 60000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          namespace: /tmp/seatunnel/checkpoint_snapshot
          storage.type: hdfs
          fs.defaultFS: file:///tmp/   # 确保该目录有写入权限
    telemetry:
      metric:
        enabled: false
      logs:
        scheduled-deletion-enable: true
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: false
      enable-basic-auth: true
      basic-auth-username: user
      basic-auth-password: password
```

> ⚠️ **注意：**
> - 启用 HTTP 端口 8080 后，需要在 `docker-compose.yml` 的 `seatunnel_master` 服务中添加端口映射 `"8080:8080"`
> - 启用了 `basic-auth` 后，访问 Web 界面或 API 需要携带认证信息：
>   ```bash
>   curl -u user:password http://localhost:8080/hazelcast/rest/maps/running-jobs
>   ```

启用后，在 `docker-compose.yml` 的 `seatunnel_master` 服务中添加端口映射：

```yaml
ports:
  - "5801:5801"
  - "8080:8080"   # HTTP 监控界面端口
```

然后重启服务：

```bash
docker compose up -d
```

---

## 常见问题排查

### 同步任务启动失败

| 现象 | 可能原因 | 解决方法 |
|------|----------|----------|
| `Access denied for user` | CDC 账号权限不足 | 检查步骤 3 的权限授予是否完整 |
| `binlog_format is not ROW` | MySQL Binlog 格式不正确 | 检查步骤 3 的 Binlog 配置 |
| `table doesn't exist` | ClickHouse 中表不存在 | 确认 `schema_save_mode` 设置为 `CREATE_SCHEMA_WHEN_NOT_EXIST` |
| `server-id conflicts` | 多个 CDC 任务使用了相同的 server-id | 修改配置中的 `server-id`，确保全局唯一 |

### 全量同步后无增量数据

1. 检查 Binlog 是否正常写入：`SHOW BINARY LOGS;`
2. 检查 SeaTunnel Worker 日志：`docker logs seatunnel_worker_1`
3. 确认 MySQL 没有清理 Binlog（检查 `binlog_expire_logs_seconds` 配置）

### ClickHouse 中数据重复

ClickHouse 的 `ReplacingMergeTree` 引擎会在后台合并时去重，但查询时可能看到重复数据。可以通过以下方式处理：

```sql
-- 查询时去重（推荐）
SELECT * FROM table_name FINAL WHERE ...;

-- 或手动触发合并
OPTIMIZE TABLE table_name FINAL;
```

---

## 参考资料

- [Apache SeaTunnel 官方文档](https://seatunnel.apache.org/docs/2.3.13/about)
- [SeaTunnel MySQL-CDC 连接器文档](https://seatunnel.apache.org/docs/2.3.13/connector-v2/source/MySQL-CDC)
- [SeaTunnel ClickHouse Sink 文档](https://seatunnel.apache.org/docs/2.3.13/connector-v2/sink/Clickhouse)
- [seatunnel-transform-fieldconvert 插件](https://github.com/quansitech/seatunnel-transform-fieldconvert)
