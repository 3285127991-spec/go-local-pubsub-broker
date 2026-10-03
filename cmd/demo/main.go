// Command demo is a small scripted walkthrough of the pubsub broker:
// exact and wildcard subscriptions, retained messages, slow-consumer
// drops, and statistics. Run it with:
//
//	go run ./cmd/demo
package main

import (
	"fmt"
	"time"

	"pubsubbroker/broker"
)

func drain(label string, sub *broker.Subscription) {
	for {
		select {
		case msg, ok := <-sub.C():
			if !ok {
				return
			}
			retained := ""
			if msg.Retained {
				retained = " (retained)"
			}
			fmt.Printf("  [%s] received topic=%q payload=%q%s\n", label, msg.Topic, msg.Payload, retained)
		default:
			return
		}
	}
}

func main() {
	b := broker.New()
	defer b.Close()

	fmt.Println("== 1. Exact and wildcard subscriptions ==")
	exact, _ := b.Subscribe("sensors/temp", 8)
	star, _ := b.Subscribe("sensors/*", 8)
	hash, _ := b.Subscribe("sensors/#", 8)

	must(b.Publish("sensors/temp", []byte("21.5")))
	must(b.Publish("sensors/humidity", []byte("40")))
	must(b.Publish("sensors/floor1/temp", []byte("19.0"))) // only matches sensors/#
	drain("exact sensors/temp", exact)
	drain("sensors/*", star)
	drain("sensors/#", hash)

	fmt.Println("== 2. Retained messages ==")
	must(b.PublishRetained("sensors/temp", []byte("22.0")))
	late, _ := b.Subscribe("sensors/*", 8)
	fmt.Println("  new subscriber on sensors/* gets the retained value immediately:")
	drain("late sensors/*", late)

	fmt.Println("== 3. Slow consumer drops (buffer capacity 2, 5 messages) ==")
	slow, _ := b.Subscribe("alerts/#", 2)
	for i := 1; i <= 5; i++ {
		must(b.Publish("alerts/fire", []byte(fmt.Sprintf("alert-%d", i))))
	}
	drain("slow alerts/#", slow)
	fmt.Printf("  dropped for this subscription: %d\n", slow.Dropped())

	fmt.Println("== 4. Unsubscribe and close ==")
	exact.Unsubscribe()
	exact.Unsubscribe() // repeated unsubscribe is safe
	must(b.Publish("sensors/temp", []byte("23.1")))
	fmt.Println("  exact subscriber cancelled; only wildcard subscribers receive 23.1:")
	drain("sensors/*", star)
	drain("sensors/#", hash)

	stats := b.Stats()
	fmt.Println("== 5. Stats ==")
	fmt.Printf("  subscriptions=%d retained=%d delivered=%d dropped=%d\n",
		stats.Subscriptions, stats.Retained, stats.Delivered, stats.Dropped)

	if err := b.Close(); err != nil {
		fmt.Println("close:", err)
	}
	if err := b.Publish("sensors/temp", []byte("x")); err != nil {
		fmt.Println("  publish after close rejected:", err)
	}
	if _, err := b.Subscribe("sensors/#", 1); err != nil {
		fmt.Println("  subscribe after close rejected:", err)
	}

	// Give drained output a moment in case of slow terminals.
	time.Sleep(10 * time.Millisecond)
	fmt.Println("demo done")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
