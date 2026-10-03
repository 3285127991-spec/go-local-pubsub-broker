// Command demo is a small scripted demonstration of the pubsubbroker
// library: wildcard subscriptions, retained messages, slow-consumer drops
// and broker statistics. Run it with:
//
//	go run ./cmd/demo
package main

import (
	"fmt"
	"log"

	"pubsubbroker"
)

// drain prints every message currently buffered in sub without blocking.
func drain(label string, sub *pubsubbroker.Subscription) {
	for {
		select {
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			fmt.Printf("  [%s] topic=%q payload=%s\n", label, msg.Topic, msg.Payload)
		default:
			return
		}
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func main() {
	broker := pubsubbroker.New()
	defer broker.Close()

	fmt.Println("== 1. Wildcard subscriptions ==")
	temp := must(broker.Subscribe("home/*/temperature", 16))
	all := must(broker.Subscribe("home/#", 16))
	doorbell := must(broker.Subscribe("home/doorbell", 16))

	publish := func(topic, payload string, retained bool) {
		if err := broker.Publish(topic, []byte(payload), retained); err != nil {
			log.Fatal(err)
		}
	}
	publish("home/livingroom/temperature", "21.5", false)
	publish("home/bedroom/temperature", "19.0", false)
	publish("home/livingroom/humidity", "45", false)
	publish("home/doorbell", "ring", false)

	fmt.Println("home/*/temperature received:")
	drain("temp", temp)
	fmt.Println("home/# received:")
	drain("all", all)
	fmt.Println("home/doorbell received:")
	drain("doorbell", doorbell)

	fmt.Println("== 2. Retained messages ==")
	publish("home/livingroom/temperature", "22.0", true)
	publish("home/doorbell", "idle", true)
	late := must(broker.Subscribe("home/#", 16))
	fmt.Println("new subscription home/# immediately received retained:")
	drain("late", late)

	fmt.Println("== 3. Slow consumer drops ==")
	slow := must(broker.Subscribe("home/motion", 2))
	for i := 1; i <= 5; i++ {
		publish("home/motion", fmt.Sprintf("event-%d", i), false)
	}
	fmt.Printf("  slow subscription buffered 2, dropped %d\n", slow.Dropped())
	drain("slow", slow)

	fmt.Println("== 4. Unsubscribe and close ==")
	doorbell.Unsubscribe()
	doorbell.Unsubscribe() // repeated cancel is safe
	publish("home/doorbell", "ring-again", false)
	fmt.Printf("  doorbell subscription cancelled; active subscriptions: %d\n", broker.Stats().Subscriptions)

	fmt.Println("== 5. Stats ==")
	stats := broker.Stats()
	fmt.Printf("  subscriptions=%d retained=%d delivered=%d dropped=%d\n",
		stats.Subscriptions, stats.Retained, stats.Delivered, stats.Dropped)

	broker.Close()
	if err := broker.Publish("home/x", []byte("nope"), false); err != nil {
		fmt.Printf("  publish after close rejected: %v\n", err)
	}
}
