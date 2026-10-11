# mysql-proxy-server

MySQL 协议代理：**MySQL 客户端/应用不做任何修改，直接访问金仓 Kingbase（MySQL 兼容模式）数据库**。

代理对外扮演一个 MySQL 8.0 服务端（实现 MySQL wire 协议），对内通过 kingbase-go-driver（PostgreSQL wire 协议）访问金仓，中间完成协议转换、SQL 文本适配和会话状态模拟。

```
MySQL客户端 ──MySQL协议──> mysql-proxy-server ──PG/KES协议──> Kingbase(MySQL兼容模式)
(PHP PDO / JDBC / PyMySQL / Go / Node ...)      协议转换+会话模拟      kingbase-go-driver
```

## 前置条件

- 金仓 Kingbase V8R6 及以上，目标库需开启 **MySQL 兼容模式**（表结构、SQL 语法按 MySQL 方言书写由金仓解析）
- 代理与金仓之间网络可达；代理本身为单库代理，一个监听端口对应一个金仓库

## 快速开始

### 编译

```bash
# 目标平台为 linux（kingbase 驱动仅支持 linux）
GOOS=linux GOARCH=amd64 go build -o mysql-proxy-server .
```

### 配置 data.json

```json
{
  "pair": [
    {
      "host": {
        "port": 24000,
        "username": "root",
        "password": "123"
      },
      "target": {
        "driver": "kingbase",
        "db": "test",
        "schema": "public",
        "timezone": "Asia/Shanghai",
        "ip": "192.168.1.100",
        "port": 54321,
        "username": "system",
        "password": "your_password"
      }
    }
  ]
}
```

`pair` 是数组，可同时配置多组「监听端口 → 数据库」映射。

**host（客户端看到的 MySQL 服务端）**

| 字段 | 说明 |
|---|---|
| port | 代理监听的 TCP 端口，客户端连接此端口 |
| username / password | 客户端登录代理用的账号密码（mysql_native_password 认证） |

**target（后端金仓连接）**

| 字段 | 说明 |
|---|---|
| driver | 固定 `kingbase`（也支持 `postgres`，走同样的 PG 协议逻辑） |
| db | 金仓数据库名 |
| schema | 默认 schema，等价于后端 `search_path`；客户端执行 `use <schema>` 也可动态切换 |
| timezone | 可选，如 `Asia/Shanghai`，影响 timestamptz 类型读取的时区，不填使用金仓默认 |
| ip / port / username / password | 金仓连接信息；password 含空格时需用单引号包裹写法 |

### 启动与验证

```bash
./mysql-proxy-server --logger.level=debug --data_path=./data.json
```

用任意 MySQL 客户端验证：

```bash
mysql -h127.0.0.1 -P24000 -uroot -p123
mysql> select 1;
mysql> show tables;
mysql> insert into t(name) values('abc');
mysql> select last_insert_id();
```

## 客户端接入示例

**PHP PDO**

```php
$pdo = new PDO('mysql:host=127.0.0.1;port=24000;dbname=test;charset=utf8mb4', 'root', '123');
$stmt = $pdo->prepare('insert into sys_user(role, username) values(?, ?)');
$stmt->execute([1, 'test']);
echo $pdo->lastInsertId();
```

**Java JDBC（Connector/J 8.x）**

```
jdbc:mysql://127.0.0.1:24000/test?useSSL=false&allowPublicKeyRetrieval=true
```

连接建立时的 bootstrap 系统变量查询、`SET NAMES` 等会话命令由代理统一应答，MyBatis/JPA 的 `useGeneratedKeys`（insert_id 回填）已支持。

**Python PyMySQL**

```python
import pymysql
conn = pymysql.connect(host='127.0.0.1', port=24000, user='root', password='123', database='test')
```

**Go**

```go
db, _ := sql.Open("mysql", "root:123@tcp(127.0.0.1:24000)/test")
```

**.NET**：MySqlConnector 的连接池复位命令（COM_RESET_CONNECTION）已实现，但该客户端体系整体验证程度低于上述四类。

## 协议与行为说明

### 已实现的协议命令

COM_QUERY、COM_STMT_PREPARE / EXECUTE / CLOSE / SEND_LONG_DATA / RESET、COM_PING、COM_QUIT、COM_INIT_DB、COM_FIELD_LIST、COM_SET_OPTION、COM_CHANGE_USER、COM_RESET_CONNECTION。

### 代理模拟的会话行为

金仓没有 MySQL 的系统变量与部分会话语义，以下由代理直接应答或映射：

| 客户端行为 | 代理的处理 |
|---|---|
| `select @@xxx`（连接 bootstrap 查询） | 内置变量表应答（character_set_*、sql_mode、transaction_isolation、wait_timeout 等） |
| `select version()` | 应答 `8.0.11`（与握手版本号一致） |
| `set names / sql_mode / charset ...` | 吞掉并应答 OK（金仓可能无法解析，不阻断客户端连接流程） |
| `set autocommit=1/0` | 真实映射：0 → 后端 `begin`；1 → 若有未提交事务则 `commit` |
| `begin / start transaction / commit / rollback / savepoint` | 原样透传金仓执行 |
| `select last_insert_id()` | 应答会话内最近一次 insert 的自增 id |
| `use <schema>` | 映射为后端 `set search_path to <schema>` |

### 事务模型

每个客户端连接与一条后端物理连接**一对一绑定**（连接池上限 1），因此文本协议的 `begin/commit/rollback` 与后续语句始终落在同一条金仓连接上，事务语义正确。代价是单个客户端连接内部没有并发（MySQL 协议本身也是一连接一会话，行为一致）。

### 自增主键（LAST_INSERT_ID / getGeneratedKeys）

insert 执行时代理会为有自增主键的表自动追加 `RETURNING <主键列>`，从金仓取回自增 id，同时写入 OK 包的 insert_id 字段（供驱动回填）与会话缓存（供 `last_insert_id()` 查询）。多行 insert 取第一个 id，与 MySQL 语义一致。主键列通过金仓 `information_schema` 自动探测（识别 serial/identity/auto_increment），每个表探测一次后缓存。

### 服务端预编译（COM_STMT_PREPARE）

`select/with/update/delete` 会在金仓侧真实预编译，参数个数与结果列数来自金仓的 describe（对依赖 prepare 元数据的客户端友好）；金仓不支持预编译的语句自动回退为文本协议。占位符统一由 `?` 改写为 `$N`，改写时跳过字符串字面量、标识符引号和注释，`'a?b'` 中的问号不会被误改。

### SQL 透传范围

`select / show / desc / describe / explain / with / table / values / call` 走查询通道，`insert / update / delete / replace / DDL（create/drop/alter/truncate...）/ analyze / flush / grant 等` 与**其余未知语句**一律透传金仓执行（错误原样返回给客户端）。SQL 兼容性的边界取决于金仓 MySQL 兼容模式的覆盖度。

## 已知限制

- **无 TLS**：代理与客户端之间为明文，客户端需允许降级（JDBC 用 `useSSL=false`，MySQL CLI 默认可连）。JDBC Connector/J 9.x 默认强制 SSL，暂不支持
- **不支持多语句**：一次请求多条以 `;` 分隔的 SQL（`allowMultiQueries=true`）不支持
- **不支持 LOAD DATA LOCAL INFILE、binlog 复制（CDC）、游标取数（COM_STMT_FETCH / useCursorFetch）**
- **无压缩协议**（CLIENT_COMPRESS / ZSTD）
- **认证仅 mysql_native_password**，单账号（配置文件里的 host.username/password）
- bigint 自增主键同样支持（代理自行解析 RETURNING 结果），但 `REPLACE`、`INSERT ... SELECT` 等不带 RETURNING 的路径拿不到 insert_id
- 极少数依赖 prepare 元数据强校验的客户端在金仓拒绝预编译的语句上会退化为文本协议行为

## 开发

```bash
go test ./logic/          # 单元测试（SQL 改写/路由逻辑，任意平台可跑）
GOOS=linux go build ./... # 完整构建（kingbase 驱动仅支持 linux）
GOOS=linux go vet ./...
```

`./mysql-proxy-server test` 运行内置的连通性示例（`logic.ClientExample`）。

依赖说明：协议服务端基于 [go-mysql-org/go-mysql](https://github.com/go-mysql-org/go-mysql)；go-frame 为本地 workspace 依赖（见 go.work），`go mod tidy` 在此 workspace 结构下不可直接使用。
