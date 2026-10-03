# go-local-pubsub-broker

一个纯 Go 实现的轻量级**进程内**发布/订阅消息代理，零第三方依赖，不依赖数据库、
Docker、云服务或任何网络组件。仅提供后端库、命令行演示和自动化测试。

## 功能特性

- 层级化 topic，使用 `/` 分隔（如 `sensors/livingroom/temperature`）
- 订阅过滤器支持三种匹配方式：
  - 精确匹配：`sensors/livingroom/temperature`
  - `*` 匹配恰好一个层级：`sensors/*/temperature`
  - 末尾 `#` 匹配剩余零个或多个层级：`sensors/#`（可匹配 `sensors` 本身）
- 每个订阅拥有**固定容量缓冲区**；缓冲区满时新消息被丢弃并计入 dropped，
  慢消费者**不会阻塞**其他订阅或发布者
- 同一订阅成功接收的消息严格保持发布顺序
- 多 goroutine 可安全并发发布、订阅、取消订阅
- Retained message：每个 topic 仅保留最新一条，新订阅创建后立即收到所有匹配的
  retained message（先于之后发布的任何消息）
- 取消订阅后不再收到新消息；重复取消安全；Broker 关闭后拒绝新的发布和订阅
- 统计信息：活跃订阅数、retained 数量、成功投递总数、dropped 总数

## 项目结构

| 文件 | 说明 |
| --- | --- |
| `topic.go` | topic 校验与过滤器匹配（`*` / `#`） |
| `broker.go` | Broker、Subscription、统计信息 |
| `topic_test.go` / `broker_test.go` | 自动化测试 |
| `cmd/demo/main.go` | 命令行演示程序 |

## 使用示例

```go
package main

import (
	"fmt"

	"pubsubbroker"
)

func main() {
	broker := pubsubbroker.New()
	defer broker.Close()

	// 创建订阅：过滤器 + 固定容量缓冲区
	sub, err := broker.Subscribe("sensors/#", 16)
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	// 发布消息；retained=true 时该 topic 保留最新一条
	_ = broker.Publish("sensors/livingroom/temperature", []byte("21.5"), true)

	// 新订阅会立即收到匹配的 retained message
	late, _ := broker.Subscribe("sensors/*/temperature", 16)
	fmt.Println(string((<-late.Messages()).Payload)) // 21.5

	msg := <-sub.Messages()
	fmt.Printf("%s: %s\n", msg.Topic, msg.Payload)

	stats := broker.Stats()
	fmt.Printf("subs=%d retained=%d delivered=%d dropped=%d\n",
		stats.Subscriptions, stats.Retained, stats.Delivered, stats.Dropped)
}
```

### API 一览

- `pubsubbroker.New() *Broker`
- `(*Broker).Subscribe(filter string, capacity int) (*Subscription, error)`
- `(*Broker).Publish(topic string, payload []byte, retained bool) error`
- `(*Broker).Stats() Stats`
- `(*Broker).Close()`
- `(*Subscription).Messages() <-chan Message`
- `(*Subscription).Unsubscribe()`（幂等，并发安全）
- `(*Subscription).Dropped() uint64`

## 设计说明

- **顺序保证**：所有发布在同一把写锁下串行投递，因此同一订阅收到的消息顺序
  即发布顺序；订阅创建时 retained message 在持锁状态下先行入队，保证其先于
  之后发布的消息。
- **慢消费者隔离**：投递是对订阅 channel 的非阻塞发送（`select` + `default`），
  缓冲区满即为该订阅丢弃一条并累加 dropped，发布者与其他订阅不受影响。
- **取消订阅**：`Unsubscribe` 通过 `sync.Once` 保证幂等；从订阅表移除并关闭
  channel，已缓冲的消息仍可被消费者排干（drain），之后不会再有新消息。
- **关闭**：`Close` 关闭所有订阅 channel，后续 `Publish`/`Subscribe` 返回
  `pubsubbroker.ErrClosed`。
- **retained**：map 按 topic 保存最新一条消息；新订阅回放多条 retained 时，
  不同 topic 间的顺序不做保证。

## 运行演示

```powershell
go run ./cmd/demo
```

演示依次展示：通配符订阅、retained message、慢消费者丢弃、取消订阅与统计信息。

## 运行测试

```powershell
# 可重复运行的完整测试
go test ./...

# 详细输出
go test ./... -v

# 并发压力（重复 20 轮）
go test ./... -count=20

# 如本机装有 gcc（CGO 可用），可启用竞态检测
go test -race ./...
```

测试覆盖：通配符匹配（精确 / `*` / `#`）、多订阅扇出、慢消费者丢弃且不阻塞他人、
并发发布下的单订阅顺序、retained message（含覆盖与即时回放）、取消订阅（含重复
取消）、Broker 关闭行为以及统计信息。

## 明确不包含

持久化、网络服务、GUI、数据库、Docker 及任何第三方服务——按设计目标均不实现。

