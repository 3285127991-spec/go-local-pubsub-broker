package broker

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustSubscribe(t *testing.T, b *Broker, pattern string, capacity int) *Subscription {
	t.Helper()
	sub, err := b.Subscribe(pattern, capacity)
	if err != nil {
		t.Fatalf("Subscribe(%q, %d): %v", pattern, capacity, err)
	}
	return sub
}

func mustPublish(t *testing.T, b *Broker, topic, payload string) {
	t.Helper()
	if err := b.Publish(topic, []byte(payload)); err != nil {
		t.Fatalf("Publish(%q): %v", topic, err)
	}
}

func recvTimeout(t *testing.T, sub *Subscription) Message {
	t.Helper()
	select {
	case msg, ok := <-sub.C():
		if !ok {
			t.Fatalf("subscription %q channel closed unexpectedly", sub.Pattern())
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for message on %q", sub.Pattern())
		return Message{}
	}
}

func expectNoMessage(t *testing.T, sub *Subscription) {
	t.Helper()
	select {
	case msg, ok := <-sub.C():
		if ok {
			t.Fatalf("unexpected message on %q: %+v", sub.Pattern(), msg)
			return
		}
		t.Fatalf("subscription %q channel closed unexpectedly", sub.Pattern())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMultipleSubscribers(t *testing.T) {
	b := New()
	defer b.Close()

	exact := mustSubscribe(t, b, "sensors/temp", 4)
	star := mustSubscribe(t, b, "sensors/*", 4)
	hash := mustSubscribe(t, b, "sensors/#", 4)
	other := mustSubscribe(t, b, "other/#", 4)

	mustPublish(t, b, "sensors/temp", "21.5")
	mustPublish(t, b, "sensors/humidity", "40")
	mustPublish(t, b, "sensors/floor1/temp", "19")

	if got := recvTimeout(t, exact); string(got.Payload) != "21.5" {
		t.Fatalf("exact got %q", got.Payload)
	}
	expectNoMessage(t, exact)

	for _, want := range []string{"21.5", "40"} {
		if got := recvTimeout(t, star); string(got.Payload) != want {
			t.Fatalf("star got %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, star)

	for _, want := range []string{"21.5", "40", "19"} {
		if got := recvTimeout(t, hash); string(got.Payload) != want {
			t.Fatalf("hash got %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, hash)
	expectNoMessage(t, other)
}

func TestSlowConsumerDropsAndDoesNotBlockOthers(t *testing.T) {
	b := New()
	defer b.Close()

	fast := mustSubscribe(t, b, "alerts/#", 16)
	slow := mustSubscribe(t, b, "alerts/#", 1)

	const total = 5
	for i := 0; i < total; i++ {
		mustPublish(t, b, "alerts/fire", fmt.Sprintf("alert-%d", i))
	}

	// The fast subscriber receives everything, in order.
	for i := 0; i < total; i++ {
		msg := recvTimeout(t, fast)
		if want := fmt.Sprintf("alert-%d", i); string(msg.Payload) != want {
			t.Fatalf("fast got %q, want %q", msg.Payload, want)
		}
	}

	// The slow subscriber keeps only the first message; the rest drop.
	if msg := recvTimeout(t, slow); string(msg.Payload) != "alert-0" {
		t.Fatalf("slow got %q, want alert-0", msg.Payload)
	}
	expectNoMessage(t, slow)
	if got := slow.Dropped(); got != total-1 {
		t.Fatalf("slow.Dropped() = %d, want %d", got, total-1)
	}
	if got := b.Stats().Dropped; got != total-1 {
		t.Fatalf("Stats().Dropped = %d, want %d", got, total-1)
	}
}

func TestConcurrentPublishOrder(t *testing.T) {
	b := New()
	defer b.Close()

	const publishers = 8
	const perPublisher = 100
	sub := mustSubscribe(t, b, "orders", publishers*perPublisher)

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for seq := 0; seq < perPublisher; seq++ {
				if err := b.Publish("orders", []byte(fmt.Sprintf("%d:%d", id, seq))); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Messages from different publishers may interleave, but each
	// publisher's sequence must arrive strictly in order.
	last := make(map[int]int)
	for i := 0; i < publishers*perPublisher; i++ {
		msg := recvTimeout(t, sub)
		var id, seq int
		if _, err := fmt.Sscanf(string(msg.Payload), "%d:%d", &id, &seq); err != nil {
			t.Fatalf("bad payload %q: %v", msg.Payload, err)
		}
		prev, seen := last[id]
		if seen && seq != prev+1 {
			t.Fatalf("publisher %d: received seq %d after %d", id, seq, prev)
		}
		if !seen && seq != 0 {
			t.Fatalf("publisher %d: first received seq is %d, want 0", id, seq)
		}
		last[id] = seq
	}
	for id, seq := range last {
		if seq != perPublisher-1 {
			t.Fatalf("publisher %d: last seq %d, want %d", id, seq, perPublisher-1)
		}
	}
	if got := b.Stats().Dropped; got != 0 {
		t.Fatalf("unexpected drops: %d", got)
	}
}

func TestRetainedMessages(t *testing.T) {
	b := New()
	defer b.Close()

	// An existing subscriber receives retained publishes as live messages.
	live := mustSubscribe(t, b, "sensors/#", 8)
	if err := b.PublishRetained("sensors/temp", []byte("v1")); err != nil {
		t.Fatalf("PublishRetained: %v", err)
	}
	if err := b.PublishRetained("sensors/temp", []byte("v2")); err != nil {
		t.Fatalf("PublishRetained: %v", err)
	}
	if err := b.PublishRetained("sensors/humidity", []byte("h1")); err != nil {
		t.Fatalf("PublishRetained: %v", err)
	}
	for _, want := range []string{"v1", "v2", "h1"} {
		if msg := recvTimeout(t, live); string(msg.Payload) != want || !msg.Retained {
			t.Fatalf("live got %+v, want payload %q retained", msg, want)
		}
	}

	// Only the latest message per topic is kept.
	if got := b.Stats().Retained; got != 2 {
		t.Fatalf("Stats().Retained = %d, want 2", got)
	}

	// A new wildcard subscriber immediately receives all matching
	// retained messages before anything published later.
	late := mustSubscribe(t, b, "sensors/*", 8)
	got := map[string]string{}
	for i := 0; i < 2; i++ {
		msg := recvTimeout(t, late)
		if !msg.Retained {
			t.Fatalf("message %+v not marked retained", msg)
		}
		got[msg.Topic] = string(msg.Payload)
	}
	if got["sensors/temp"] != "v2" || got["sensors/humidity"] != "h1" {
		t.Fatalf("late subscriber got %v", got)
	}
	mustPublish(t, b, "sensors/temp", "live-after-subscribe")
	if msg := recvTimeout(t, late); string(msg.Payload) != "live-after-subscribe" || msg.Retained {
		t.Fatalf("expected live message after retained ones, got %+v", msg)
	}

	// A non-matching subscriber receives no retained messages.
	none := mustSubscribe(t, b, "other/#", 4)
	expectNoMessage(t, none)
}

func TestUnsubscribe(t *testing.T) {
	b := New()
	defer b.Close()

	sub := mustSubscribe(t, b, "a/#", 4)
	mustPublish(t, b, "a/b", "first")

	sub.Unsubscribe()
	sub.Unsubscribe() // repeated unsubscribe must be safe

	mustPublish(t, b, "a/b", "second")

	// The buffered message is still readable, then the channel is closed.
	if msg := recvTimeout(t, sub); string(msg.Payload) != "first" {
		t.Fatalf("got %q, want first", msg.Payload)
	}
	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("received message published after unsubscribe")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after unsubscribe")
	}

	if got := b.Stats().Subscriptions; got != 0 {
		t.Fatalf("Stats().Subscriptions = %d, want 0", got)
	}
}

func TestClose(t *testing.T) {
	b := New()

	sub1 := mustSubscribe(t, b, "a/#", 4)
	sub2 := mustSubscribe(t, b, "b/#", 4)
	mustPublish(t, b, "a/x", "buffered")

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if err := b.Publish("a/x", []byte("y")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	if err := b.PublishRetained("a/x", []byte("y")); !errors.Is(err, ErrClosed) {
		t.Fatalf("PublishRetained after Close = %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe("a/#", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close = %v, want ErrClosed", err)
	}

	// Buffered messages remain drainable, then channels report closed.
	if msg := recvTimeout(t, sub1); string(msg.Payload) != "buffered" {
		t.Fatalf("got %q, want buffered", msg.Payload)
	}
	for i, sub := range []*Subscription{sub1, sub2} {
		select {
		case _, ok := <-sub.C():
			if ok {
				t.Fatalf("sub %d received message after close", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sub %d channel not closed", i)
		}
	}
	if got := b.Stats().Subscriptions; got != 0 {
		t.Fatalf("Stats().Subscriptions = %d, want 0", got)
	}
}

func TestStats(t *testing.T) {
	b := New()
	defer b.Close()

	subA := mustSubscribe(t, b, "a/#", 4)
	subB := mustSubscribe(t, b, "a/b", 1)

	for i := 0; i < 3; i++ {
		mustPublish(t, b, "a/b", "x")
	}
	// subA received 3, subB received 1 and dropped 2.
	stats := b.Stats()
	if stats.Subscriptions != 2 || stats.Delivered != 4 || stats.Dropped != 2 || stats.Retained != 0 {
		t.Fatalf("Stats() = %+v, want {Subscriptions:2 Retained:0 Delivered:4 Dropped:2}", stats)
	}

	if err := b.PublishRetained("a/c", []byte("r")); err != nil {
		t.Fatalf("PublishRetained: %v", err)
	}
	stats = b.Stats()
	if stats.Retained != 1 || stats.Delivered != 5 {
		t.Fatalf("Stats() = %+v, want Retained:1 Delivered:5", stats)
	}

	subB.Unsubscribe()
	if got := b.Stats().Subscriptions; got != 1 {
		t.Fatalf("Stats().Subscriptions = %d, want 1", got)
	}
	subA.Unsubscribe()
}

func TestInvalidTopicsAndPatterns(t *testing.T) {
	b := New()
	defer b.Close()

	for _, topic := range []string{"", "a/#", "a/*", "a//b", "/a", "a/"} {
		if err := b.Publish(topic, []byte("x")); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Publish(%q) = %v, want ErrInvalidTopic", topic, err)
		}
		if err := b.PublishRetained(topic, []byte("x")); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("PublishRetained(%q) = %v, want ErrInvalidTopic", topic, err)
		}
	}
	for _, pattern := range []string{"", "a/#/b", "a/b#", "a/b*", "a//b"} {
		if _, err := b.Subscribe(pattern, 1); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Subscribe(%q) = %v, want ErrInvalidTopic", pattern, err)
		}
	}
	if _, err := b.Subscribe("a/b", 0); err == nil {
		t.Error("Subscribe with capacity 0 should fail")
	}
}

// TestConcurrentUse exercises publish, subscribe and unsubscribe from many
// goroutines at once. Run with -race to catch data races.
func TestConcurrentUse(t *testing.T) {
	b := New()
	defer b.Close()

	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = b.Publish("stress/topic", []byte("x"))
				_ = b.PublishRetained("stress/retained", []byte("y"))
			}
		}()
	}
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				sub, err := b.Subscribe("stress/#", 4)
				if err != nil {
					return
				}
				sub.Unsubscribe()
			}
		}()
	}
	wg.Wait()
}
