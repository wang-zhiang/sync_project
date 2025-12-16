# MySQL到SQLServer数据同步工具

## 功能特点

- **多线程同步**：使用3个线程并行处理数据，提高同步效率
- **批量处理**：每批次处理3000条记录，优化性能
- **动态分页**：不依赖特定字段进行分页处理
- **自动建表**：如果目标表不存在，会自动创建
- **增强的错误处理**：连接不稳定时自动重试
- **事务支持**：确保数据一致性，批次失败时回滚
- **进度监控**：实时显示同步进度和预计完成时间

## 使用方法

### 方法1：命令行参数

```
java MySQLToSQLServerSyncRobust <MySQL主机> <MySQL数据库> <MySQL表名> <MySQL用户名> <MySQL密码> <SQLServer主机> <SQLServer数据库> [SQLServer表名] [SQLServer用户名] [SQLServer密码]
```

### 方法2：交互式输入

直接运行程序，按照提示输入相关参数：

```
java MySQLToSQLServerSyncRobust
```

## 配置说明

- **线程数**：固定为3个线程
- **批处理大小**：每批3000条记录
- **最大重试次数**：5次
- **连接超时**：60秒
- **查询超时**：600秒

## 注意事项

1. 确保已添加MySQL和SQL Server的JDBC驱动到类路径
2. 数据库用户需要有足够的权限（读取源表、创建和写入目标表）
3. 对于大表同步，建议增加JVM内存参数，如：`-Xmx4g`

## 示例

```
java -Xmx2g -cp .;mysql-connector-java-8.0.28.jar;mssql-jdbc-9.4.0.jre8.jar MySQLToSQLServerSyncRobust 192.168.1.100 mydb users root password123 192.168.1.200 targetdb
```