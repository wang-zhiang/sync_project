# SQL Server 到 ClickHouse 数据同步工作脚本

## 📋 项目概述

这是一个企业级的**SQL Server到ClickHouse数据同步工作脚本**，支持完整的数据同步工作流程，包括CDC表清理、数据同步、多节点数据分发和数据核验。

### 核心特性
- ✅ **4步骤工作流程**：CDC清空 → 数据同步 → 节点填充 → 数据核验
- ✅ **多表类型支持**：分布式表、多节点相同表、普通表
- ✅ **高性能同步**：6线程并发、20000条/页、批量提交
- ✅ **多节点分发**：自动将数据分发到7个ClickHouse节点
- ✅ **数据核验**：自动对比源表和各节点数据量
- ✅ **详细日志**：每步操作都有清晰的日志输出

---

## 🏗️ 架构说明

### 数据流向

```
SQL Server (源表)
    ↓
[步骤1] 清空CDC表 (cdc.dbo_xxx_CT)
    ↓
[步骤2] 同步到ClickHouse中间表 (targetTable)
    ↓ [如果是分布式表，实际存储在 targetTable_local]
    ↓
[步骤3] 分发到各节点最终表 (targetTableNode)
    ↓ [并发操作7个节点：192.168.5.104-110]
    ↓
[步骤4] 数据核验（对比源表和各节点数据量）
```

### 表类型说明

| tableType | 类型名称 | 说明 | 清空策略 | 同步目标 |
|-----------|---------|------|---------|---------|
| 0 | 分布式表 | 有分布式表和local表 | 清空 targetTable_local + ON CLUSTER | 写入 targetTable |
| 1 | 多节点相同表 | 7个节点存相同数据 | 清空 targetTable + ON CLUSTER | 写入 targetTable |
| 其他 | 普通表 | 单节点普通表 | 清空 targetTable | 写入 targetTable |

---

## 📁 项目文件

```
dim_skuall/
├── DataSyncService.java      # 核心同步服务（主程序）
├── DatabaseManager.java       # 数据库连接管理器
├── TaskConfig.java            # 配置文件映射类
├── task.yaml                  # 任务配置文件
└── README.md                  # 本文档
```

---

## ⚙️ 配置说明

### task.yaml 配置文件

```yaml
clickhouse:
  host: "192.168.5.110"         # ClickHouse主节点IP
  port: 8123                     # ClickHouse端口
  database: "tmp"                # 数据库名
  username: "default"            # 用户名
  password: "smartpath"          # 密码

tasks:
  - taskId: "skuall"                          # 任务ID（唯一标识）
    source:                                   # SQL Server源配置
      host: "192.168.4.218"
      port: 2599
      database: "datasystem"
      table: "dbo.skuall"                     # 源表名
      user: "CHH"
      password: "Y1v606"
    
    targetTable: "skuall_ready"               # ClickHouse中间表
    targetTableLocal: "skuall_local_ready"    # 中间表的local表（仅tableType=0时需要，否则留空）
    targetTableNode: "skuall"                 # 各节点的最终业务表
    
    tableType: 1                              # 0=分布式表, 1=多节点相同表
    clusterName: "test_ck_cluster"            # ClickHouse集群名称
    
    primaryKeys:                              # 主键（用于排序分页）
      - "Id"
    
    fieldMappings:                            # 字段映射
      - sourceField: "Id"
        targetField: "Id"
      - sourceField: "SKU"
        targetField: "SKU"
      # ... 更多字段映射
```

### 配置参数说明

#### 源表配置 (source)
- `host`: SQL Server服务器IP地址
- `port`: SQL Server端口号
- `database`: 数据库名
- `table`: 源表名（格式：dbo.表名）
- `user`: SQL Server用户名
- `password`: SQL Server密码

#### 目标表配置
- `targetTable`: ClickHouse中间表名（数据同步的目标表）
- `targetTableLocal`: 中间表的local表名
  - **仅当 tableType=0 时需要配置**
  - tableType=1 或其他值时，此项留空或不配置
- `targetTableNode`: 各节点的最终业务表名

#### 其他配置
- `taskId`: 任务唯一标识符
- `tableType`: 表类型（0=分布式表, 1=多节点相同表）
- `clusterName`: ClickHouse集群名称
- `primaryKeys`: 主键列表（用于分页排序）
- `fieldMappings`: 字段映射列表

---

## 🚀 使用方法

### 前置条件

1. **依赖库**
   - HikariCP（数据库连接池）
   - SnakeYAML（YAML配置解析）
   - SQL Server JDBC驱动
   - ClickHouse JDBC驱动

2. **网络要求**
   - 能访问SQL Server (192.168.4.218:2599)
   - 能访问ClickHouse集群 (192.168.5.104-110:8123)

3. **权限要求**
   - SQL Server：SELECT权限、CDC表TRUNCATE权限
   - ClickHouse：SELECT、INSERT、TRUNCATE权限

### 启动步骤

1. **修改配置文件**
   ```bash
   编辑 task.yaml 文件，配置源表、目标表、字段映射等
   ```

2. **修改main方法中的路径**
   ```java
   // 在 DataSyncService.java 的 main 方法中修改配置文件路径
   syncService.loadConfig("你的配置文件路径/task.yaml");
   
   // 设置要同步的taskId
   String taskId = "skuall";  // 修改为你配置的taskId
   ```

3. **运行程序**
   ```bash
   # 直接运行main方法
   java dim.dim_skuall.DataSyncService
   ```

---

## 📊 详细工作流程

### 步骤1：清空SQL Server CDC表

**操作内容**：
- 清空SQL Server的CDC变更捕获表
- CDC表命名规则：`cdc.dbo_[源表名]_CT`
- 例如：源表 `dbo.skuall`，CDC表为 `cdc.dbo_skuall_CT`

**SQL执行**：
```sql
TRUNCATE TABLE cdc.dbo_skuall_CT
```

**日志输出**：
```
============================================================
步骤1：清空SQL Server CDC表
============================================================
源表: dbo.skuall
✓ 已清空CDC表：cdc.dbo_skuall_CT
✓ 步骤1完成，耗时: 125 ms
```

---

### 步骤2：数据同步（SQL Server → ClickHouse中间表）

**操作内容**：
1. **清空目标表**
   - tableType=0：清空 `targetTableLocal` + ON CLUSTER
   - tableType=1：清空 `targetTable` + ON CLUSTER
   
2. **获取源表记录数**
   ```sql
   SELECT COUNT(*) FROM dbo.skuall
   ```

3. **分页并发同步**
   - 每页：20,000条记录
   - 线程数：6个线程并发
   - 批量提交：每1000条提交一次
   - 使用ROW_NUMBER()分页（兼容老版本SQL Server）

4. **数据类型自动转换**
   - 根据ClickHouse目标字段类型自动转换
   - NULL值转换为默认值

**日志输出**：
```
============================================================
步骤2：数据同步（SQL Server -> ClickHouse中间表）
============================================================
源表: dbo.skuall
目标表: skuall_ready
表类型: 多节点相同表
清空多节点相同表: skuall_ready
✓ 目标表清空完成
源表总记录数: 125000
✓ 字段类型映射获取完成
分页数: 7，每页: 20000 条
  页面 [0] 同步完成，记录数: 20000，耗时: 3245 ms
  页面 [1] 同步完成，记录数: 20000，耗时: 3198 ms
  页面 [2] 同步完成，记录数: 20000，耗时: 3267 ms
  页面 [3] 同步完成，记录数: 20000，耗时: 3289 ms
  页面 [4] 同步完成，记录数: 20000，耗时: 3312 ms
  页面 [5] 同步完成，记录数: 20000，耗时: 3276 ms
  页面 [6] 同步完成，记录数: 5000，耗时: 856 ms
✓ 所有分页同步完成
✓ 步骤2完成，总耗时: 23567 ms
```

---

### 步骤3：填充各节点最终表

**操作内容**：
- 并发操作7个ClickHouse节点（192.168.5.104-110）
- 每个节点执行：
  1. 清空最终表：`TRUNCATE TABLE tmp.skuall`
  2. 填充数据：`INSERT INTO tmp.skuall SELECT * FROM tmp.skuall_ready`

**节点列表**：
- 192.168.5.104
- 192.168.5.105
- 192.168.5.106
- 192.168.5.107
- 192.168.5.108
- 192.168.5.109
- 192.168.5.110

**日志输出**：
```
============================================================
步骤3：填充各节点最终表
============================================================
中间表: skuall_ready
最终表: skuall
  处理节点: 192.168.5.104
  ✓ 节点 192.168.5.104 - 清空完成
  ✓ 节点 192.168.5.104 - 数据填充完成
  处理节点: 192.168.5.105
  ✓ 节点 192.168.5.105 - 清空完成
  ✓ 节点 192.168.5.105 - 数据填充完成
  ... [其他5个节点]
✓ 步骤3完成，所有节点处理完毕，耗时: 8745 ms
```

---

### 步骤4：数据核验

**操作内容**：
- 获取SQL Server源表数据量
- 获取7个ClickHouse节点最终表的数据量
- 对比数据量是否一致

**日志输出**：
```
============================================================
步骤4：数据核验
============================================================
SQL Server 源表 [dbo.skuall] 数据量: 125000

ClickHouse 各节点最终表 [skuall] 数据量:
  节点 192.168.5.104: 125000 条
    ✓ 数据量一致
  节点 192.168.5.105: 125000 条
    ✓ 数据量一致
  节点 192.168.5.106: 125000 条
    ✓ 数据量一致
  节点 192.168.5.107: 125000 条
    ✓ 数据量一致
  节点 192.168.5.108: 125000 条
    ✓ 数据量一致
  节点 192.168.5.109: 125000 条
    ✓ 数据量一致
  节点 192.168.5.110: 125000 条
    ✓ 数据量一致

✓ 数据核验完成
```

---

## ⚡ 性能参数

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| BATCH_SIZE | 20000 | 每页记录数 |
| THREAD_POOL_SIZE | 6 | 同步线程数 |
| 批量提交大小 | 1000 | 每1000条提交一次 |
| SQL Server连接池 | 最大10，最小2 | HikariCP配置 |
| ClickHouse连接池 | 最大10，最小2 | 主节点连接池 |
| 节点连接池 | 最大5，最小1 | 7个节点各自的连接池 |

---

## 📝 注意事项

### 1. 配置文件路径
- 确保 `task.yaml` 的路径正确
- 建议使用绝对路径避免路径问题

### 2. targetTableLocal 配置规则
- **tableType=0（分布式表）**：必须配置 `targetTableLocal`
- **tableType=1（多节点相同表）**：`targetTableLocal` 留空或不配置
- 如果配置错误，程序会跳过清空操作并给出警告

### 3. 字段映射
- `sourceField` 必须与SQL Server源表字段名完全一致
- `targetField` 必须与ClickHouse目标表字段名完全一致
- 字段顺序可以不一致，程序会自动匹配

### 4. 主键配置
- `primaryKeys` 用于分页排序，必须配置
- 建议使用有索引的字段作为主键
- 支持多字段联合主键

### 5. 集群名称
- `clusterName` 必须与ClickHouse集群配置中的名称一致
- 用于 ON CLUSTER 子句

### 6. 节点IP
- 当前硬编码7个节点：192.168.5.104-110
- 如需修改节点列表，需要修改 `DatabaseManager.java` 中的 `initClickHouseNodeConnections` 方法

---

## 🔧 常见问题

### Q1: 如何修改节点列表？
**A**: 编辑 `DatabaseManager.java` 的第50行和第77行：
```java
String[] nodeIps = {"192.168.5.104", "192.168.5.105", ...};
```

### Q2: 如何调整同步性能？
**A**: 修改 `DataSyncService.java` 的性能参数：
```java
private static final int BATCH_SIZE = 20000;        // 增大可提升吞吐量
private static final int THREAD_POOL_SIZE = 6;      // 增加线程数
```

### Q3: CDC表清空失败怎么办？
**A**: 
1. 检查SQL Server用户是否有TRUNCATE权限
2. 确认CDC表是否存在
3. 查看CDC表名格式是否为 `cdc.dbo_[表名]_CT`

### Q4: 数据核验显示数量不一致怎么办？
**A**: 
1. 检查步骤2和步骤3的日志，确认是否有报错
2. 手动在ClickHouse上查询各节点数据量
3. 检查网络是否稳定
4. 重新执行同步

### Q5: 如何添加新的表同步？
**A**: 在 `task.yaml` 的 `tasks` 列表中添加新配置：
```yaml
tasks:
  - taskId: "skuall"
    # ... 现有配置
    
  - taskId: "product"        # 新任务
    source:
      host: "192.168.4.218"
      # ... 新表的配置
```

### Q6: 支持哪些数据类型？
**A**: 支持以下ClickHouse类型的自动转换：
- String, FixedString
- Int8/16/32/64, UInt8/16/32/64
- Float32/64, Double, Decimal
- Date, DateTime
- Bool

---

## 📞 技术支持

如有问题，请联系开发团队。

---

## 📜 更新日志

### v2.0 (2025-11-07)
- ✨ 新增4步骤工作流程
- ✨ 新增CDC表自动清空
- ✨ 新增多节点数据分发
- ✨ 新增数据核验功能
- 🔧 重构配置文件结构
- 🔧 优化日志输出
- 📝 完善文档

### v1.0
- 🎉 初始版本
- ✅ 基础数据同步功能


