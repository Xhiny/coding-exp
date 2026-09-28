# SeaTunnel Checkpoint 持久化与恢复实践

> 适用版本：SeaTunnel 2.3.13（Zeta Engine，Hazelcast 5.1）
> 部署形态：Docker Compose（1 master + 2 worker + client）
> 整理时间：2026-09-28，基于生产环境实际案例

---

## 1. 先理清两个不同的"持久化"

很多人把这两件事混在一起，实际上它们解决的是**不同的问题**：

| | checkpoint 存储 | IMap（map-store）持久化 |
|---|---|---|
| 存的是什么 | 任务的**状态快照文件**（binlog 位点、split 状态等），`-r` 恢复用的数据 | 集群的**任务台账**：有哪些任务在跑、任务定义、状态指针、历史任务等（Hazelcast IMap） |
| 配置位置 | `seatunnel.yaml` → `engine.checkpoint.storage` | `hazelcast-master.yaml` / `hazelcast-worker.yaml` → `map.engine*.map-store` |
| 默认行为 | 写到你配置的路径（如容器内 /tmp） | 纯内存，集群全停就丢 |
| 丢了会怎样 | 没法续跑，只能全量重跑 | master 不知道有任务存在，**不会自动恢复任务** |
| 谁需要访问 | 只有 master（任务状态通过网络上报给 master 后落盘）；HA 场景下所有可能成为 master 的节点都要能访问 | **所有节点**（Hazelcast 分区分布在 master 和 worker 上），配置要一致、路径要都能访问 |

**一句话**：

- 只保住 checkpoint 存储 → 重启后能**人工** `seatunnel.sh -r <jobId>` 续跑
- 再加上 map-store → 重启后任务**自动**恢复并续跑

---

## 2. checkpoint 基础概念

### 2.1 配置与优先级

`seatunnel.yaml`（引擎级）：

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 60000        # 毫秒；两次 checkpoint 的间隔
      timeout: 600000        # 毫秒；单次 checkpoint 的超时（超过即失败）
      storage:
        type: hdfs
        max-retained: 10     # 保留最近几个 checkpoint
        plugin-config:
          namespace: /seatunnel/checkpoint
          storage.type: local
          fs.defaultFS: file:///
```

任务级（写在同步任务的 `env` 里，**优先级高于引擎**）：

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 60000
  checkpoint.timeout  = 600000
}
```

取值逻辑（源码 `JobMaster`）：先加载 `seatunnel.yaml` 的 `engine.checkpoint` 作为默认值，再用任务 `env` 里的 `checkpoint.*` 覆盖。

**经验值**：

| 阶段 | interval | timeout |
|------|----------|---------|
| 全量快照（数据量大、Sink 可能追不上） | 60000 | 600000 |
| binlog 增量（稳定后） | 15000~30000 | 120000~300000 |

> 超时太小是 `CheckpointException: Checkpoint expired before completing` 的直接原因；
> interval 太小（如 10s）会增加 barrier 对齐和状态写入开销。

### 2.2 存储路径规则

```
<namespace>/<jobId>/<timestamp>-<random>-<pipelineId>-<checkpointId>.ser
```

- `namespace` 若不以 `/` 结尾，代码会自动补
- `fs.defaultFS` 只是 Hadoop 文件系统的默认 URI；`namespace` 用绝对路径时以 namespace 为准
- `file:///` + `namespace: /xxx` = 本地目录存储（不是真正的 HDFS）

### 2.3 常用接口与脚本

| 用途 | 方式 |
|------|------|
| 查看 checkpoint 概览/历史 | `GET /jobs/checkpoints/<jobId>`、`GET /jobs/checkpoints/history/<jobId>?limit=&status=` |
| 格式化查看 | `show_checkpoints.sh <jobId>`（配套运维脚本，基于 REST API） |
| checkpoint 计数含义 | `triggered/completed/failed/inProgress/restored`；`restored` = 从 checkpoint 恢复启动过几次 |

关于 `state` 字段：2.3.13 的统计口径实际是**各 subtask 上报的非空状态条目个数**（源码 `PendingCheckpoint` 里 `.map(s -> s.length).count()` 的写法所致），不是字节数；数值小且稳定即可。

### 2.4 checkpoint 健康 != 数据在流动

SeaTunnel 的中间队列在收发 barrier 时就会 ack（`IntermediateBlockingQueue.handleRecord` 对 Barrier 立即 ack），所以 **checkpoint 全绿不能证明 Sink 还在消费数据**。判断同步是否卡住，要同时看：

- `SourceReceivedCount` / `SinkWriteCount` 是否在涨
- `IntermediateQueueSize` 是否持续满且不下降（低峰期源端静默后仍不排空 = 卡住）
- Sink 线程栈 / ClickHouse 侧状态

---

## 3. checkpoint 存储落地（生产 Docker 配置）

### 3.1 要求

1. 目录必须**挂载宿主机卷**，不能用容器内 `/tmp` 或容器可写层：
   - 容器重建（`docker compose down/up`）会丢
   - `/tmp` 可能被系统清理
2. HA 场景（多 master 候选）下，所有候选节点都要挂载/共享同一份存储（HDFS 或共享盘）

### 3.2 docker-compose 挂载示例

```yaml
services:
  seatunnel_master:
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
      - ./data/seatunnel-checkpoint:/seatunnel/checkpoint   # ← 新增
```

### 3.3 seatunnel.yaml 配置示例

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 60000
      timeout: 600000
      storage:
        type: hdfs
        max-retained: 10
        plugin-config:
          namespace: /seatunnel/checkpoint
          storage.type: local
          fs.defaultFS: file:///
```

---

## 4. IMap（map-store）持久化

### 4.1 哪个配置文件生效

`bin/seatunnel-cluster.sh` 按启动角色选文件：

| 启动角色 | 使用的配置 |
|----------|-----------|
| `-r master` | `config/hazelcast-master.yaml` |
| `-r worker` | `config/hazelcast-worker.yaml` |
| 不带角色 / `master_and_worker`（hybrid） | `config/hazelcast.yaml` |

> Docker Compose 里 master 一般执行 `seatunnel-cluster.sh -r master`，worker 执行 `-r worker`，
> 所以 **改 `hazelcast-master.yaml` + `hazelcast-worker.yaml`，不用动 `hazelcast.yaml`**。
> 可用以下命令在生产上确认实际加载的文件：
>
> ```bash
> docker exec seatunnel_master ps -ef | grep -o -- '-Dhazelcast.config=[^ ]*'
> docker exec seatunnel_worker_1 ps -ef | grep -o -- '-Dhazelcast.config=[^ ]*'
> ```

### 4.2 配置示例（两个文件都要加，内容一致）

```yaml
hazelcast:
  # ...原有配置...
  map:
    engine*:
      map-store:
        enabled: true
        initial-mode: LAZY          # ⚠️ 不要用 EAGER，见 4.3
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          namespace: /seatunnel/imap
          clusterName: seatunnel-cluster
          storage.type: hdfs
          fs.defaultFS: file:///
```

配套 compose 挂载（master 和所有 worker 都要挂同一个目录）：

```yaml
    volumes:
      - ./data/seatunnel-imap:/seatunnel/imap
```

涉及的 IMap（`engine*` 通配匹配）：

```
engine_runningJobInfo      engine_runningJobState
engine_finishedJobState    engine_finishedJobMetrics
engine_finishedJobVertexInfo  engine_stateTimestamps
engine_ownedSlotProfilesIMap  engine_checkpoint-id-map
engine_runningJobMetrics   engine_checkpoint_monitor
engine_connectorJarRefCounters
```

### 4.3 ⚠️ 生产事故案例：`initial-mode: EAGER` 导致 master 启动失败

**现象**：配置 map-store 后重启，容器是 Up，但 REST `http://localhost:8080` 连不上（HTTP 000）。

**日志**：

```
ERROR [ServiceManagerImpl] Error while initializing service: null
java.lang.NullPointerException: null
    at com.hazelcast.spi.impl.operationservice.impl.Invocation.<init>(Invocation.java:209)
    ...
    at com.hazelcast.map.impl.proxy.MapProxySupport.waitUntilLoaded(MapProxySupport.java:760)
    ...
    at org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService.<init>(CheckpointMonitorService.java:55)
    at org.apache.seatunnel.engine.server.SeaTunnelServer.startMaster(SeaTunnelServer.java:191)
    at org.apache.seatunnel.engine.server.SeaTunnelServer.init(SeaTunnelServer.java:159)
```

**根因链**：

1. `SeaTunnelServer.init()` 里 `startMaster()` 会 new `CheckpointMonitorService`
2. 该服务构造时 `getMap("engine_checkpoint_monitor")`，map 名匹配 `engine*` 通配
3. `initial-mode: EAGER` 让 Hazelcast 在**节点尚未启动完成**时就尝试加载 map（`waitUntilLoaded`）
4. 此时分区/操作服务还没就绪 → NPE
5. `init()` 中断 → 其后的 **Jetty/REST 服务没有启动** → 8080 无监听（但容器进程还活着，所以 `docker ps` 显示 Up）

**修复**：

```bash
cd /mnt/docker-script/seatunnel/config
sed -i 's/initial-mode: EAGER/initial-mode: LAZY/' hazelcast-master.yaml hazelcast-worker.yaml
grep -n "initial-mode" hazelcast-master.yaml hazelcast-worker.yaml   # 确认已改为 LAZY
cd /mnt/docker-script
docker compose restart seatunnel_master
sleep 30
docker compose restart seatunnel_worker_1 seatunnel_worker_2
```

`LAZY` 只是把"启动时全量加载"改成"首次访问时按需加载"，持久化写入不受影响，对 2.3.13 + Hazelcast 5.1 是更稳妥的选择（官方文档示例写的是 EAGER，但该版本组合下会在启动阶段触发上述 NPE）。

---

## 5. 恢复方式与行为

### 5.1 提交方式决定是否全量重跑

| 提交方式 | 行为 |
|----------|------|
| 普通提交（新任务） | 新 jobId，**全量快照重跑** |
| `bin/seatunnel.sh --async -r <jobId> -c <config>` | 用原 jobId，从**最近一次成功的 checkpoint** 续跑 |
| REST `POST /submit-job/upload?...&jobId=<jobId>&isStartWithSavePoint=true` | 同上（`BaseService.submitJobInternal` 读取这两个参数） |

恢复原理（源码 `CheckpointManager`）：`isStartWithSavePoint=true` 时调用
`checkpointStorage.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId)` 找到最近 checkpoint，
设置 checkpoint 编号继续递增，source 从其中的 binlog 位点继续读取。

**前提**：

- checkpoint 存储里还有该 jobId 的数据（文件没丢、新 master 能访问）
- 必须使用**同一份任务配置**（任务定义）
- 找不到 checkpoint 时**不会报错阻止**，而是静默降级为从头全量开始

### 5.2 CLI 注意事项

```bash
# --async 不能省：默认 -cj true 会在客户端退出时取消任务
bin/seatunnel.sh --async -r <jobId> -c config/xxx.conf
```

### 5.3 服务器/集群重启后的行为

| 场景 | 结果 |
|------|------|
| 无 map-store 持久化 | 运行中任务**直接丢失**；需人工恢复：`-r` 续跑（checkpoint 还在）或普通提交全量重跑 |
| 有 map-store 持久化 + checkpoint 存储持久 | master 启动时从 IMap 恢复任务（源码 `CoordinatorService.restoreJobFromMasterActiveSwitch`，以 `restart=true` 初始化），自动从最近 checkpoint 续跑 |
| 有 map-store 持久化，但 checkpoint 文件丢了 | 任务会被拉起来，但找不到 checkpoint → 静默按全新任务从全量开始 |
| 只重启 worker（master 存活） | 该节点上任务失败，job FAILED，需要人工恢复 |

> SeaTunnel 2.3.13 **没有内置的"任务失败自动重试"**：checkpoint 出错会把 pipeline 置为 CANCELING，任务最终 FAILED
> （源码 `SubPlan.handleCheckpointError` 只做 cancel）。

---

## 6. 需要什么效果，就配什么

| 需求 | 需要做的配置 |
|------|--------------|
| 重启后 checkpoint 文件还在，人工 `-r` 能续跑 | 只改 `seatunnel.yaml`：storage 指向宿主机挂载目录 |
| 重启后任务自动恢复、续跑，无需人工 | `seatunnel.yaml` + `hazelcast-master.yaml` + `hazelcast-worker.yaml`（map-store，`initial-mode: LAZY`），所有节点挂载存储目录 |
| 可接受全量重跑 | 什么都不配（现状），注意容器重建会同时丢 checkpoint |

一句话判断：**map-store 是"任务自动恢复"的开关，checkpoint 存储是"能续跑"的前提，两者独立但必须配套。**

---

## 7. 配置后的验证清单

```bash
# 1) 确认节点实际加载的 hazelcast 配置
docker exec seatunnel_master ps -ef | grep -o -- '-Dhazelcast.config=[^ ]*'

# 2) master 启动无异常，REST 正常
docker logs --tail 50 seatunnel_master            # 不应再出现 ServiceManagerImpl NPE
curl -s -u 'admin:Qs123!@#' http://localhost:8080/overview | head -c 200

# 3) 集群 3 个成员都在
docker compose ps
#    master 日志里应能看到 Members {size:3 ...}

# 4) 任务是否自动恢复（有 map-store 时）
./show_jobs.sh running
./show_checkpoints.sh <jobId>    # restored 计数 +1、checkpoint 编号延续

# 5) 验证 checkpoint 文件落盘位置
docker exec seatunnel_master ls -l /seatunnel/checkpoint/<jobId>/ | tail

# 6) 验证 IMap 持久化文件
docker exec seatunnel_master ls -l /seatunnel/imap/ | head
```

**人工恢复操作**：

```bash
# 停止任务
curl -s -u 'admin:Qs123!@#' -X POST http://localhost:8080/stop-job \
  -H 'Content-Type: application/json' \
  -d '{"jobId":"<jobId>","isStopWithSavePoint":false}'

# 或 CLI
bin/seatunnel.sh -can <jobId>

# 从 checkpoint 续跑（注意 --async）
bin/seatunnel.sh --async -r <jobId> -c config/xxx.conf
```

---

## 8. 常见问题速查

| 现象 | 原因 / 处理 |
|------|-------------|
| 容器 Up 但 REST 8080 连不上，日志有 `CheckpointMonitorService ... NullPointerException` | map-store `initial-mode: EAGER` 的启动 NPE → 改 `LAZY`，重启 |
| 重启后任务消失 | 没配 map-store，任务丢失；用 `-r` 续跑或重新提交 |
| `-r` 后从全量开始 | checkpoint 文件丢失/不可访问（容器重建、路径未挂载、换了存储） |
| `Checkpoint expired before completing` | `checkpoint.timeout` 太小 / Sink 写入慢；调大 timeout（任务 env 生效），开 ClickHouse 压缩，检查 CH 负载 |
| 队列差值恒定不降 | 低峰期源端静默后仍不排空 = 卡住；checkpoint 全绿不代表 Sink 在工作，查 Sink 线程栈/CH 状态 |
| `state: 3` 是否正常 | 2.3.13 中它是各 subtask 上报的状态条目数，稳定即可 |
| 任务停止时出现一条 `latestFailed: Pipeline turn to end state.` | 正常伴生现象；`restored +1` 是对应的一次恢复 |

---

## 9. 推荐生产配置汇总

### 9.1 `seatunnel.yaml`

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
      interval: 60000            # 原来是 10000，偏小
      timeout: 600000            # 原来是 60000，全量阶段容易超时
      storage:
        type: hdfs
        max-retained: 10
        plugin-config:
          namespace: /seatunnel/checkpoint   # 宿主机挂载目录
          storage.type: local
          fs.defaultFS: file:///
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
      basic-auth-username: admin
      basic-auth-password: <按实际>
```

### 9.2 `hazelcast-master.yaml` / `hazelcast-worker.yaml` 追加

> 注意：下面这段要放在各自的 `hazelcast:` 根节点下面（`map` 与 `cluster-name`、`network` 同级），
> 不要再出现一个顶层 `map:`。两个文件内容保持一致。

```yaml
  map:
    engine*:
      map-store:
        enabled: true
        initial-mode: LAZY
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          namespace: /seatunnel/imap
          clusterName: seatunnel-cluster
          storage.type: hdfs
          fs.defaultFS: file:///
```

### 9.3 docker-compose 挂载

```yaml
  seatunnel_master:
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
      - ./data/seatunnel-checkpoint:/seatunnel/checkpoint
      - ./data/seatunnel-imap:/seatunnel/imap

  seatunnel_worker_1:
    volumes:
      - ./seatunnel/config:/opt/seatunnel/config
      - ./seatunnel/lib:/opt/seatunnel/lib
      - ./data/seatunnel-imap:/seatunnel/imap     # map-store 需要
  # worker_2 同 worker_1
```

---

## 10. 关键结论

1. **checkpoint 存储和 IMap 持久化是两件事**：前者决定"能不能续跑"，后者决定"要不要人工恢复"。
2. **任何 checkpoint 相关目录都不要放容器内 `/tmp`**，否则容器重建 = 数据全丢 = 只能全量重跑。
3. **map-store 的 `initial-mode` 用 `LAZY`**（2.3.13 + Hazelcast 5.1 下 EAGER 会导致 master 启动 NPE、REST 起不来）。
4. **map-store 配置要覆盖所有节点**（master 用 `hazelcast-master.yaml`，worker 用 `hazelcast-worker.yaml`），且路径都能访问。
5. 恢复提交要带 `--async`，否则客户端退出会把任务取消；恢复找不到 checkpoint 会**静默**降级为全量。
6. 引擎级 `interval/timeout` 要按全量阶段的压力设置（建议 60s / 10min），任务级 env 可覆盖。
