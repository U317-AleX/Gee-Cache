# Gee-KVS

Gee-KVS 是一个基于 Go 语言实现的**分布式缓存系统**（Key-Value Store），参考了 [groupcache](https://github.com/golang/groupcache) 的设计思想。它支持 LRU 缓存淘汰、并发安全访问、防止缓存击穿、基于一致性哈希的节点路由，以及基于 HTTP + Protobuf 的节点间通信。

> 本项目通过由浅入深的方式逐步搭建：LRU 淘汰策略 → 并发安全缓存 → 不可变缓存值 → 防击穿 → 一致性哈希 → HTTP 节点通信 → Protobuf 序列化。

## 核心特性与技术实现

本项目最大的特点是：**每一个功能特性都由一项明确的底层技术支撑**。下表是"功能 → 技术"的对照总览，各模块源码位于 [`src/`](src/) 目录下。

| 功能 | 实现技术 | 源码位置 |
| --- | --- | --- |
| LRU 缓存淘汰策略 | `container/list` 双向链表 + `map` 哈希表 | [`src/lru/lru.go`](src/lru/lru.go) |
| 并发安全的缓存访问 | `sync.Mutex` 互斥锁 + 惰性初始化 | [`src/geecache/cache.go`](src/geecache/cache.go) |
| 缓存值零拷贝/只读保护 | `ByteView` 不可变值对象（值语义实现接口） | [`src/geecache/byteview.go`](src/geecache/byteview.go) |
| 防止缓存击穿（同一 key 只加载一次） | `singleflight` 模式：`sync.Mutex` + `sync.WaitGroup` | [`src/singleflight/singleflight.go`](src/singleflight/singleflight.go) |
| 缓存节点路由（key → 节点映射） | 一致性哈希：虚拟节点 + `hash/crc32` + `sort.Search` 二分查找 | [`src/consistenthash/consistenthash.go`](src/consistenthash/consistenthash.go) |
| 缓存节点间的网络通信 | `net/http` 标准库，实现 `http.Handler`（`ServeHTTP`）接口 | [`src/geecache/http.go`](src/geecache/http.go) |
| 节点间报文的高性能序列化 | Protobuf（proto3）+ `github.com/segmentio/encoding/proto` | [`src/geecachepb/`](src/geecachepb/) |
| 缓存与数据源的解耦（可接入任意数据源） | `Getter` 接口 + `GetterFunc` 函数适配器（接口型函数模式） | [`src/geecache/geecache.go`](src/geecache/geecache.go) |
| 节点发现与节点访问的抽象 | `PeerPicker` / `PeerGetter` 接口抽象 | [`src/geecache/peers.go`](src/geecache/peers.go) |

### 1. LRU 淘汰策略 —— 采用 `container/list` 双向链表 + `map` 实现

`src/lru` 是最底层的核心数据结构，采用 **双向链表 + 哈希表** 的经典组合，使 `Get`/`Add`/`RemoveOldest` 均为 O(1) 复杂度：

- `map[string]*list.Element`：通过 key 在 O(1) 时间内定位链表节点；
- `container/list`（标准库双向链表）：维护访问顺序，队首是最近访问的条目，队尾是最久未使用的条目；
- 命中即 `MoveToFront`，容量超限时从队尾 `RemoveOldest`，直到占用内存不超过 `maxBytes`；
- 支持 `OnEvicted` 回调，在记录被淘汰时通知上层（便于做二次处理或统计）。

```go
type Cache struct {
    maxBytes  int64
    nbytes    int64
    ll        *list.List                // container/list 双向链表，维护 LRU 顺序
    cache     map[string]*list.Element  // key -> 链表节点，O(1) 定位
    OnEvicted func(key string, value Value)
}
```

### 2. 并发安全 —— 采用 `sync.Mutex` 对 LRU 封装实现

`src/geecache/cache.go` 在裸的 `lru.Cache` 外层包了一层 `sync.Mutex`：

- 所有 `add`/`get` 操作先加锁再访问底层 LRU，保证多协程并发读写安全；
- 底层 `lru.Cache` 采用**惰性初始化**（首次使用时才创建），避免在构造函数中传入复杂参数；
- 上层 `Group` 中的分组注册表则使用 `sync.RWMutex` 保护全局 `groups` map，读多写少场景下允许并发读。

### 3. 缓存值的只读保护 —— 采用不可变值对象 `ByteView` 实现

`src/geecache/byteview.go` 实现了 `ByteView`，对应 groupcache 中的核心抽象：

- `ByteView` 只持有 `[]byte`，对外**只读**：实现 `Len()` 接口参与内存统计；
- `ByteSlice()` / `Strings()` 在取值时**拷贝**数据（`cloneBytes`），外部修改不会影响缓存内的真实数据，防止缓存被污染；
- `ByteView` 以**值类型**实现 `lru.Value` 接口，缓存中存取无需额外的指针解引用。

### 4. 防缓存击穿 —— 采用 `singleflight` 模式（`sync.Mutex` + `sync.WaitGroup`）实现

高并发下同一个 key 可能同时有大量请求去回源加载数据（缓存击穿）。`src/singleflight` 采用 Go 官方 `x/sync/singleflight` 相同的思路，用 `sync.Mutex` + `sync.WaitGroup` + `map[string]*call` 实现**调用去重**：

- 第一个到达的协程成为 leader，登记 `call` 后真正执行加载函数 `fn`；
- 后续相同 key 的协程查到在途的 `call`，解锁后阻塞在 `c.wg.Wait()` 上等待；
- leader 执行完毕写入 `val/err` 并 `wg.Done()` 唤醒所有等待者，**所有调用者共享同一次执行结果**；
- 最后从 map 中删除该 `call`，允许后续请求开启新一轮加载。

在 `Group.load()` 中，`singleflight` 进一步保证：**每个 key 无论在本地还是远程节点，同一时刻只会被真实拉取一次**。

### 5. 节点路由 —— 采用一致性哈希（虚拟节点 + `crc32` + 二分查找）实现

分布式场景下，增删节点应尽量少地导致缓存数据重新分布。`src/consistenthash` 采用**带虚拟节点的一致性哈希环**：

- 每个真实节点映射为 `replicas` 个虚拟节点（本项目中 HTTP 层默认 `defaultReplicas = 50`），虚拟节点散列到哈希环上，解决节点少时**数据倾斜**问题；
- 哈希函数可注入（`Hash func(data []byte) uint32`），默认使用标准库 `hash/crc32.ChecksumIEEE`；
- 环上的虚拟节点哈希值保存在有序切片 `keys []int` 中，查找时用 `sort.Search`（二分查找）找到顺时针方向第一个 ≥ key 哈希值的虚拟节点，再通过 `hashMap` 映射回真实节点，复杂度 O(log n)；
- 节点宕机时仅影响环上相邻区间的 key，其余缓存命中不受影响。

### 6. 节点间通信 —— 采用 `net/http` 标准库实现

`src/geecache/http.go` 中的 `HTTPPool` 同时扮演两个角色：

- **服务端**：实现 `http.Handler`（`ServeHTTP`），请求路径约定为 `/_geecache/<groupname>/<key>`，可直接通过 `http.Handle`/`http.ListenAndServe` 挂载；
- **客户端**：`httpGetter` 实现了 `PeerGetter` 接口，用 `http.Get` 请求远端节点的同一约定路径获取缓存值。

通过 HTTP 标准库实现的好处是：零第三方依赖、易于调试（curl 即可访问）、与 Go 的 Web 生态无缝集成。

### 7. 节点间报文序列化 —— 采用 Protobuf（proto3）实现

节点间传输的请求/响应报文在 [`src/geecachepb/geecachepb.proto`](src/geecachepb/geecachepb.proto) 中以 proto3 定义：

```protobuf
message Request {
  string group = 1;
  string key = 2;
}
message Response {
  bytes value = 1;
}
```

- 通过 `protoc --go_out` 生成强类型的 Go 代码，保证跨节点通信的数据契约一致；
- 编解码使用 **`github.com/segmentio/encoding/proto`**（标准库 proto 接口的高性能实现），序列化为二进制后以 `application/octet-stream` 传输；
- 二进制编码体积小、编解码快，比 JSON 更适合高频的节点间缓存交互。

### 8. 数据源解耦 —— 采用 `Getter` 接口 + `GetterFunc` 函数适配器实现

缓存未命中时如何回源（数据库、文件、远程服务……）由使用者决定。`Group` 通过两个技术点实现解耦：

- **`Getter` 接口**：`Group` 只依赖抽象，任何实现了 `Get(key string) ([]byte, error)` 的数据源都能接入；
- **`GetterFunc` 接口型函数**：让"函数"也能实现接口（`type GetterFunc func(key string) ([]byte, error)`），调用方既可传函数字面量（见 `main.go` 中的 `geecache.GetterFunc(func(key string) ...)`），也可传完整实现类型，是 Go 中经典的适配器模式。

### 9. 节点抽象 —— 采用 `PeerPicker` / `PeerGetter` 接口实现

`src/geecache/peers.go` 将"如何挑选节点"与"如何访问节点"抽象为两个接口：

- `PeerPicker`：根据 key 挑选负责该 key 的节点（`HTTPPool` 实现了它，内部使用一致性哈希）；
- `PeerGetter`：从指定节点读取缓存值（`httpGetter` 实现了它）。

核心结构 `Group` 只依赖接口而非具体 HTTP 实现，因此未来可以无痛替换为 TCP/gRPC 等其他通信方式。

## 一次请求的完整流程

```
用户请求 ──► API Server (http://localhost:9999/api?key=Tom)
                │
                ▼
     Group.Get("scores", "Tom")
                │
     ┌──────────▼──────────┐
     │  本地 LRU 缓存命中？ │──命中──► 返回 ByteView
     └──────────┬──────────┘
              未命中
                ▼
     singleflight.Do(key, load)      ← 同 key 并发只执行一次
                │
     ┌──────────▼──────────┐
     │ 一致性哈希挑选节点    │──► 远端节点 ──► HTTP GET
     │ (虚拟节点 + crc32)   │    /_geecache/<group>/<key>
     └──────────┬──────────┘    (Protobuf 编解码)
          本节点 / 失败
                ▼
     回源 Getter.Get(key)（用户自定义数据源）
                ▼
     写入本地 LRU 缓存并返回
```

## 项目结构

```
Gee-KVS
├── go.mod
└── src
    ├── consistenthash          # 一致性哈希（虚拟节点 + crc32 + 二分查找）
    │   ├── consistenthash.go
    │   └── consistenthash_test.go
    ├── geecache                # 缓存主体：Group、并发缓存、HTTP 节点池
    │   ├── byteview.go         # 不可变缓存值
    │   ├── cache.go            # sync.Mutex 封装的并发安全 LRU
    │   ├── geecache.go         # Group：缓存命名空间 + 回源加载
    │   ├── http.go             # HTTPPool：节点服务端/客户端
    │   └── peers.go            # PeerPicker / PeerGetter 接口
    ├── geecachepb              # Protobuf 报文定义与生成代码
    │   ├── geecachepb.proto
    │   └── geecachepb.pb.go
    ├── lru                     # LRU 核心数据结构（双向链表 + map）
    │   ├── lru.go
    │   └── lru_test.go
    ├── singleflight            # 防缓存击穿的调用去重
    │   └── singleflight.go
    └── main                    # 演示程序（多节点 + API 服务）
        ├── main.go
        └── run.sh
```

## 快速开始

### 环境要求

- Go 1.23+

### 编译与测试

```bash
# 编译所有包
go build ./...

# 运行单元测试（lru、consistenthash）
go test ./...
```

### 运行分布式演示

`src/main/run.sh` 会启动 3 个缓存节点（8001/8002/8003）和 1 个 API 服务（9999），并发发起 3 个相同 key 的请求演示防击穿效果：

```bash
cd src/main
./run.sh
```

也可以手动运行单个节点：

```bash
# 启动 API 服务 + 缓存节点
go run src/main/main.go -port=8003 -api=1

# 请求缓存
curl "http://localhost:9999/api?key=Tom"
```

## 致谢

本项目的设计与实现参考了 [geektutu/7days-golang](https://github.com/geektutu/7days-golang) 系列教程（gee-cache）以及 [groupcache](https://github.com/golang/groupcache) 的源码设计。
