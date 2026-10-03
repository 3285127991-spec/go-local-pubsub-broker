package pubsubbroker

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// recv receives one message, failing the test if none arrives in time.
func recv(t *testing.T, s *Subscription) Message {
	t.Helper()
	select {
	case msg, ok := <-s.Messages():
		if !ok {
			t.Fatalf("subscription %q channel closed unexpectedly", s.Filter())
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for message on subscription %q", s.Filter())
		return Message{}
	}
}

// expectNoMessage fails the test if a message arrives within a short window.
func expectNoMessage(t *testing.T, s *Subscription) {
	t.Helper()
	select {
	case msg, ok := <-s.Messages():
		if ok {
			t.Fatalf("subscription %q received unexpected message on topic %q", s.Filter(), msg.Topic)
		}
		// Channel closed is an acceptable way to observe "no more messages".
	case <-time.After(50 * time.Millisecond):
	}
}

func mustSubscribe(t *testing.T, b *Broker, filter string, capacity int) *Subscription {
	t.Helper()
	s, err := b.Subscribe(filter, capacity)
	if err != nil {
		t.Fatalf("Subscribe(%q, %d) failed: %v", filter, capacity, err)
	}
	return s
}

func mustPublish(t *testing.T, b *Broker, topic, payload string, retained bool) {
	t.Helper()
	if err := b.Publish(topic, []byte(payload), retained); err != nil {
		t.Fatalf("Publish(%q) failed: %v", topic, err)
	}
}

func TestMultipleSubscriptions(t *testing.T) {
	b := New()
	defer b.Close()

	exact := mustSubscribe(t, b, "sensors/livingroom/temp", 10)
	star := mustSubscribe(t, b, "sensors/*/temp", 10)
	hash := mustSubscribe(t, b, "sensors/#", 10)
	other := mustSubscribe(t, b, "alerts/#", 10)

	mustPublish(t, b, "sensors/livingroom/temp", "21", false)
	mustPublish(t, b, "sensors/kitchen/temp", "19", false)
	mustPublish(t, b, "sensors/kitchen/humidity", "40", false)

	// Exact filter receives only its own topic.
	if msg := recv(t, exact); msg.Topic != "sensors/livingroom/temp" || string(msg.Payload) != "21" {
		t.Fatalf("exact subscription got %+v", msg)
	}
	expectNoMessage(t, exact)

	// "*" receives both temp readings but not humidity.
	for _, want := range []string{"sensors/livingroom/temp", "sensors/kitchen/temp"} {
		if msg := recv(t, star); msg.Topic != want {
			t.Fatalf("star subscription got topic %q, want %q", msg.Topic, want)
		}
	}
	expectNoMessage(t, star)

	// "#" receives everything under sensors/.
	for _, want := range []string{"sensors/livingroom/temp", "sensors/kitchen/temp", "sensors/kitchen/humidity"} {
		if msg := recv(t, hash); msg.Topic != want {
			t.Fatalf("hash subscription got topic %q, want %q", msg.Topic, want)
		}
	}
	expectNoMessage(t, hash)

	// Unrelated filter receives nothing.
	expectNoMessage(t, other)
}

func TestSlowConsumerDropsWithoutBlockingOthers(t *testing.T) {
	b := New()
	defer b.Close()

	slow := mustSubscribe(t, b, "jobs/#", 1)
	fast := mustSubscribe(t, b, "jobs/#", 100)

	const total = 10
	for i := 0; i < total; i++ {
		mustPublish(t, b, "jobs/queue", fmt.Sprintf("job-%d", i), false)
	}

	// The slow consumer never reads: exactly one message fits its buffer.
	if got := slow.Dropped(); got != total-1 {
		t.Fatalf("slow subscription dropped %d messages, want %d", got, total-1)
	}
	// The fast consumer is unaffected and receives every message in order.
	for i := 0; i < total; i++ {
		msg := recv(t, fast)
		if want := fmt.Sprintf("job-%d", i); string(msg.Payload) != want {
			t.Fatalf("fast subscription got payload %q, want %q", msg.Payload, want)
		}
	}
	if got := fast.Dropped(); got != 0 {
		t.Fatalf("fast subscription dropped %d messages, want 0", got)
	}

	stats := b.Stats()
	if stats.Delivered != total+1 {
		t.Fatalf("Stats.Delivered = %d, want %d", stats.Delivered, total+1)
	}
	if stats.Dropped != total-1 {
		t.Fatalf("Stats.Dropped = %d, want %d", stats.Dropped, total-1)
	}
}

func TestConcurrentPublishPreservesOrderPerSubscription(t *testing.T) {
	b := New()
	defer b.Close()

	const (
		publishers      = 8
		messagesEach    = 200
		total           = publishers * messagesEach
		subscriptionBuf = total
	)
	sub := mustSubscribe(t, b, "events/#", subscriptionBuf)

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < messagesEach; i++ {
				payload := fmt.Sprintf("%d-%d", p, i)
				if err := b.Publish("events/stream", []byte(payload), false); err != nil {
					t.Errorf("Publish failed: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Every message arrives exactly once, and the sequence numbers of each
	// individual publisher are strictly increasing.
	last := make([]int, publishers)
	for i := range last {
		last[i] = -1
	}
	for n := 0; n < total; n++ {
		msg := recv(t, sub)
		var p, seq int
		if _, err := fmt.Sscanf(string(msg.Payload), "%d-%d", &p, &seq); err != nil {
			t.Fatalf("malformed payload %q: %v", msg.Payload, err)
		}
		if seq <= last[p] {
			t.Fatalf("publisher %d sequence regressed: %d after %d", p, seq, last[p])
		}
		last[p] = seq
	}
	for p, seq := range last {
		if seq != messagesEach-1 {
			t.Fatalf("publisher %d delivered up to sequence %d, want %d", p, seq, messagesEach-1)
		}
	}
	if got := b.Stats().Delivered; got != total {
		t.Fatalf("Stats.Delivered = %d, want %d", got, total)
	}
}

func TestConcurrentSubscribePublishUnsubscribe(t *testing.T) {
	b := New()
	defer b.Close()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := b.Publish("load/test", []byte("x"), false); err != nil {
					t.Errorf("Publish failed: %v", err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s, err := b.Subscribe("load/#", 4)
				if err != nil {
					t.Errorf("Subscribe failed: %v", err)
					return
				}
				s.Unsubscribe()
				s.Unsubscribe() // repeated cancel must be safe
			}
		}()
	}
	wg.Wait()
	if got := b.Stats().Subscriptions; got != 0 {
		t.Fatalf("Stats.Subscriptions = %d, want 0 after all unsubscribes", got)
	}
}

func TestRetainedMessages(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, "config/db/host", "localhost", true)
	mustPublish(t, b, "config/db/port", "5432", true)
	mustPublish(t, b, "config/cache/size", "100", true)
	mustPublish(t, b, "ephemeral/event", "ignored", false)

	// A new wildcard subscription immediately receives all matching retained
	// messages (order across topics is unspecified).
	sub := mustSubscribe(t, b, "config/#", 10)
	got := make(map[string]string)
	for i := 0; i < 3; i++ {
		msg := recv(t, sub)
		got[msg.Topic] = string(msg.Payload)
	}
	want := map[string]string{
		"config/db/host":    "localhost",
		"config/db/port":    "5432",
		"config/cache/size": "100",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("retained messages = %v, want %v", got, want)
	}
	expectNoMessage(t, sub)

	// Only the latest retained message per topic is kept.
	mustPublish(t, b, "config/db/host", "db.internal", true)
	sub2 := mustSubscribe(t, b, "config/db/host", 10)
	if msg := recv(t, sub2); string(msg.Payload) != "db.internal" {
		t.Fatalf("retained payload = %q, want %q", msg.Payload, "db.internal")
	}
	expectNoMessage(t, sub2)

	// A non-matching subscription receives no retained messages.
	sub3 := mustSubscribe(t, b, "other/#", 10)
	expectNoMessage(t, sub3)

	if got := b.Stats().Retained; got != 3 {
		t.Fatalf("Stats.Retained = %d, want 3", got)
	}
}

func TestRetainedDeliveredBeforeNewPublications(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, "a/b", "retained", true)
	sub := mustSubscribe(t, b, "a/#", 10)
	mustPublish(t, b, "a/b", "live", false)

	if msg := recv(t, sub); string(msg.Payload) != "retained" {
		t.Fatalf("first message = %q, want retained", msg.Payload)
	}
	if msg := recv(t, sub); string(msg.Payload) != "live" {
		t.Fatalf("second message = %q, want live", msg.Payload)
	}
}

func TestUnsubscribe(t *testing.T) {
	b := New()
	defer b.Close()

	sub := mustSubscribe(t, b, "news/#", 10)
	mustPublish(t, b, "news/tech", "one", false)
	sub.Unsubscribe()

	// Buffered messages remain available after unsubscribe.
	if msg := recv(t, sub); string(msg.Payload) != "one" {
		t.Fatalf("buffered message = %q, want %q", msg.Payload, "one")
	}
	// The channel is closed, so no new messages can ever arrive.
	if _, ok := <-sub.Messages(); ok {
		t.Fatal("expected closed channel after Unsubscribe")
	}

	mustPublish(t, b, "news/tech", "two", false)
	if got := b.Stats().Delivered; got != 1 {
		t.Fatalf("Stats.Delivered = %d, want 1 (no delivery after unsubscribe)", got)
	}
	if got := b.Stats().Subscriptions; got != 0 {
		t.Fatalf("Stats.Subscriptions = %d, want 0", got)
	}

	// Repeated and concurrent cancels are safe.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.Unsubscribe()
		}()
	}
	wg.Wait()
}

func TestClose(t *testing.T) {
	b := New()
	sub := mustSubscribe(t, b, "x/#", 10)
	mustPublish(t, b, "x/y", "buffered", false)

	b.Close()
	b.Close() // double close must be safe

	if err := b.Publish("x/y", []byte("nope"), false); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe("x/#", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close = %v, want ErrClosed", err)
	}

	// Buffered messages can still be drained, then the channel is closed.
	if msg := recv(t, sub); string(msg.Payload) != "buffered" {
		t.Fatalf("buffered message = %q, want %q", msg.Payload, "buffered")
	}
	if _, ok := <-sub.Messages(); ok {
		t.Fatal("expected subscription channel to be closed after Close")
	}
	if got := b.Stats().Subscriptions; got != 0 {
		t.Fatalf("Stats.Subscriptions = %d, want 0 after Close", got)
	}
}

func TestStats(t *testing.T) {
	b := New()
	defer b.Close()

	s1 := mustSubscribe(t, b, "m/#", 2)
	mustSubscribe(t, b, "m/a", 10)
	mustPublish(t, b, "m/a", "1", true) // retained + delivered to both subs
	mustPublish(t, b, "m/a", "2", false)
	mustPublish(t, b, "m/a", "3", false) // s1 buffer (cap 2) is now full

	if got := s1.Dropped(); got != 1 {
		t.Fatalf("s1.Dropped() = %d, want 1", got)
	}

	stats := b.Stats()
	if stats.Subscriptions != 2 {
		t.Fatalf("Stats.Subscriptions = %d, want 2", stats.Subscriptions)
	}
	if stats.Retained != 1 {
		t.Fatalf("Stats.Retained = %d, want 1", stats.Retained)
	}
	// s1 received 2 (cap), s2 received 3.
	if stats.Delivered != 5 {
		t.Fatalf("Stats.Delivered = %d, want 5", stats.Delivered)
	}
	if stats.Dropped != 1 {
		t.Fatalf("Stats.Dropped = %d, want 1", stats.Dropped)
	}

	// A new subscription receives the retained message, bumping Delivered.
	mustSubscribe(t, b, "m/#", 10)
	if got := b.Stats().Delivered; got != 6 {
		t.Fatalf("Stats.Delivered after retained replay = %d, want 6", got)
	}
}

func TestInvalidInputs(t *testing.T) {
	b := New()
	defer b.Close()

	if _, err := b.Subscribe("", 1); err == nil {
		t.Fatal("Subscribe with empty filter: want error")
	}
	if _, err := b.Subscribe("a/#/b", 1); err == nil {
		t.Fatal("Subscribe with misplaced #: want error")
	}
	if _, err := b.Subscribe("a", 0); err == nil {
		t.Fatal("Subscribe with zero capacity: want error")
	}
	if err := b.Publish("", []byte("x"), false); err == nil {
		t.Fatal("Publish with empty topic: want error")
	}
	if err := b.Publish("a/#", []byte("x"), false); err == nil {
		t.Fatal("Publish with wildcard topic: want error")
	}
	// Failed calls must not change stats.
	if got := b.Stats(); got.Subscriptions != 0 || got.Delivered != 0 || got.Dropped != 0 {
		t.Fatalf("stats changed after invalid calls: %+v", got)
	}
}

func TestPayloadIsCopied(t *testing.T) {
	b := New()
	defer b.Close()

	sub := mustSubscribe(t, b, "p", 1)
	payload := []byte("abc")
	mustPublish(t, b, "p", string(payload), false)
	for i := range payload {
		payload[i] = 'x'
	}
	if msg := recv(t, sub); string(msg.Payload) != "abc" {
		t.Fatalf("payload mutated after publish: got %q", msg.Payload)
	}
}

// TestTopicsSortedDeterminism guards the retained-replay documentation claim
// that order across topics is unspecified by simply ensuring all arrive.
func TestRetainedReplayCoversAllTopics(t *testing.T) {
	b := New()
	defer b.Close()

	topics := []string{"r/1", "r/2", "r/3", "r/4", "r/5"}
	for _, topic := range topics {
		mustPublish(t, b, topic, "v", true)
	}
	sub := mustSubscribe(t, b, "r/#", len(topics))
	seen := make([]string, 0, len(topics))
	for range topics {
		seen = append(seen, recv(t, sub).Topic)
	}
	sort.Strings(seen)
	if fmt.Sprint(seen) != fmt.Sprint(topics) {
		t.Fatalf("retained replay topics = %v, want %v", seen, topics)
	}
}
