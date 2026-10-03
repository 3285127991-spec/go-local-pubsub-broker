# pubsubbroker

A lightweight, in-process publish/subscribe message broker written in pure
Go. It is a backend library plus a CLI demo: no network service, no
database, no Docker, no cloud or third-party dependencies, no GUI, and no
persistence.

## Features

- Hierarchical topics separated by `/`, with wildcards:
  - `*` matches exactly one topic level.
  - A trailing `#` matches zero or more remaining levels
    (`sensors/#` also matches `sensors` itself).
  - Anything else is an exact match.
- Fixed-capacity buffer per subscription. Slow consumers never block
  publishers or other subscribers: when a buffer is full, new messages for
  that subscription are dropped and counted.
- Per-subscription ordering: messages successfully received by one
  subscription preserve publish order.
- Safe for concurrent use: any number of goroutines may publish, subscribe
  and unsubscribe simultaneously.
- Retained messages: the latest message per topic is kept and delivered to
  new matching subscriptions immediately at subscribe time.
- Unsubscribe stops delivery of new messages; repeated unsubscribe is safe.
- Closing the broker rejects new publishes and subscriptions with
  `ErrClosed` and cancels all active subscriptions.
- Statistics: active subscriptions, retained messages, total delivered and
  total dropped messages.

## Layout

- `broker/` - the library (topic matching + broker implementation).
- `cmd/demo/` - a scripted CLI demo.
- `broker/*_test.go` - automated tests.

## Usage

```go
package main

import (
	"fmt"

	"pubsubbroker/broker"
)

func main() {
	b := broker.New()
	defer b.Close()

	sub, _ := b.Subscribe("sensors/*", 16) // pattern, buffer capacity

	b.Publish("sensors/temp", []byte("21.5"))
	b.PublishRetained("sensors/humidity", []byte("40"))

	msg := <-sub.C()
	fmt.Println(msg.Topic, string(msg.Payload), msg.Retained)

	sub.Unsubscribe()
	fmt.Println(b.Stats())
}
```

API summary:

- `broker.New() *Broker`
- `(*Broker).Subscribe(pattern string, capacity int) (*Subscription, error)`
- `(*Broker).Publish(topic string, payload []byte) error`
- `(*Broker).PublishRetained(topic string, payload []byte) error`
- `(*Broker).Stats() Stats` and `(*Broker).Close() error`
- `(*Subscription).C() <-chan Message`,
  `(*Subscription).Unsubscribe()`,
  `(*Subscription).Dropped() uint64`

Notes:

- Publish topics must be concrete (no `*` or `#`); invalid
  topics and patterns are rejected with `ErrInvalidTopic`.
- Message payloads are shared between subscribers; treat them as read-only.
- A subscription channel is closed on unsubscribe or broker close; buffered
  messages can still be drained before the close is observed.

## Run the demo

```sh
go run ./cmd/demo
```

The demo walks through exact and wildcard subscriptions, retained messages,
slow-consumer drops, unsubscribe, broker close and statistics.

## Run the tests

```sh
go test ./... -count=1
```

For a race-detector run (requires a C compiler such as MinGW gcc on
Windows, because `-race` needs cgo):

```sh
go test ./... -race -count=1
```

Test coverage includes: wildcard matching, multiple subscribers,
slow-consumer drops, per-publisher ordering under concurrent publishes,
retained messages, unsubscribe semantics, broker close, statistics, topic
validation, and a concurrent publish/subscribe/unsubscribe stress test.

## Design notes

- A single broker mutex serializes fan-out. Sends into each subscription
  buffer are non-blocking (`select`/`default`), so a full
  buffer drops the message and increments the dropped counters instead of
  blocking anyone. Because all sends to a given subscription happen under
  the same lock, per-subscription order is preserved.
- Retained messages are stored in a map keyed by topic (latest wins) and
  pushed into a new subscription's buffer before `Subscribe`
  returns, so they are observed before any later live publish.

## Non-goals

Persistence, networking, authentication, GUIs, databases and any external
services are intentionally out of scope.
