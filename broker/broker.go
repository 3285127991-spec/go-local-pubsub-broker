package broker

import (
	"errors"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned by Publish and Subscribe after the broker has been
// closed.
var ErrClosed = errors.New("broker: closed")

// Message is a single published message delivered to subscribers.
//
// Payload must be treated as read-only by consumers: the same slice may be
// shared between multiple subscriptions.
type Message struct {
	Topic    string
	Payload  []byte
	Retained bool
}

// Stats is a point-in-time snapshot of broker counters.
type Stats struct {
	Subscriptions int    // currently active subscriptions
	Retained      int    // retained messages currently stored
	Delivered     uint64 // total messages successfully delivered to subscription buffers
	Dropped       uint64 // total messages dropped because a subscription buffer was full
}

// Subscription receives messages matching one topic pattern on its channel.
// The channel has a fixed capacity; when it is full, new messages for this
// subscription are dropped (and counted) instead of blocking publishers.
type Subscription struct {
	broker  *Broker
	id      uint64
	pattern string
	ch      chan Message
	once    sync.Once
	dropped atomic.Uint64
}

// C returns the receive-only channel of messages for this subscription.
// The channel is closed when the subscription is cancelled or the broker
// is closed; buffered messages can still be drained before the close is
// observed.
func (s *Subscription) C() <-chan Message {
	return s.ch
}

// Pattern returns the topic pattern this subscription was created with.
func (s *Subscription) Pattern() string {
	return s.pattern
}

// Dropped returns how many messages were dropped for this subscription
// because its buffer was full.
func (s *Subscription) Dropped() uint64 {
	return s.dropped.Load()
}

// Unsubscribe cancels the subscription: it is removed from the broker and
// its channel is closed after any already-buffered messages. No new
// messages are delivered afterwards. Calling Unsubscribe more than once is
// safe and has no additional effect.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		s.broker.removeSub(s.id)
	})
}

// Broker is a concurrency-safe in-process pub/sub broker. The zero value
// is not usable; create one with New.
type Broker struct {
	mu       sync.Mutex
	closed   bool
	nextID   uint64
	subs     map[uint64]*Subscription
	retained map[string]Message

	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// New returns a ready-to-use Broker.
func New() *Broker {
	return &Broker{
		subs:     make(map[uint64]*Subscription),
		retained: make(map[string]Message),
	}
}

// Subscribe creates a subscription for pattern with a receive buffer of
// capacity messages. capacity must be at least 1.
//
// Any retained messages matching pattern are placed into the buffer before
// Subscribe returns, so they are received before any message published
// afterwards. Retained messages that do not fit in the buffer are dropped
// and counted.
func (b *Broker) Subscribe(pattern string, capacity int) (*Subscription, error) {
	if !isValidPattern(pattern) {
		return nil, ErrInvalidTopic
	}
	if capacity < 1 {
		return nil, errors.New("broker: capacity must be >= 1")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}

	b.nextID++
	sub := &Subscription{
		broker:  b,
		id:      b.nextID,
		pattern: pattern,
		ch:      make(chan Message, capacity),
	}
	b.subs[sub.id] = sub

	// Deliver retained messages immediately, in map iteration order. This
	// happens under the broker lock, so no live publish can interleave.
	for _, msg := range b.retained {
		if matchTopic(pattern, msg.Topic) {
			b.deliver(sub, msg)
		}
	}
	return sub, nil
}

// Publish sends a non-retained message to all matching subscribers.
func (b *Broker) Publish(topic string, payload []byte) error {
	return b.publish(topic, payload, false)
}

// PublishRetained stores payload as the retained message for topic
// (replacing any previous one) and also delivers it to all current
// matching subscribers.
func (b *Broker) PublishRetained(topic string, payload []byte) error {
	return b.publish(topic, payload, true)
}

func (b *Broker) publish(topic string, payload []byte, retain bool) error {
	if !isValidPublishTopic(topic) {
		return ErrInvalidTopic
	}
	// Copy once so later caller mutation cannot affect subscribers; all
	// subscribers share this single copy and must not modify it.
	cp := make([]byte, len(payload))
	copy(cp, payload)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	msg := Message{Topic: topic, Payload: cp}
	if retain {
		msg.Retained = true
		b.retained[topic] = msg
	}
	for _, sub := range b.subs {
		if matchTopic(sub.pattern, topic) {
			b.deliver(sub, msg)
		}
	}
	return nil
}

// deliver performs a non-blocking send into the subscription buffer.
// It must be called with b.mu held, which serializes all sends to a given
// subscription and therefore preserves publish order per subscription.
func (b *Broker) deliver(sub *Subscription, msg Message) {
	select {
	case sub.ch <- msg:
		b.delivered.Add(1)
	default:
		sub.dropped.Add(1)
		b.dropped.Add(1)
	}
}

// removeSub deletes the subscription and closes its channel. It runs under
// the broker lock, so it cannot race with an in-flight deliver.
func (b *Broker) removeSub(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, ok := b.subs[id]
	if !ok {
		return
	}
	delete(b.subs, id)
	close(sub.ch)
}

// Close shuts the broker down. All active subscriptions are cancelled and
// their channels closed; subsequent Publish and Subscribe calls return
// ErrClosed. Close is idempotent.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for id, sub := range b.subs {
		delete(b.subs, id)
		close(sub.ch)
	}
	return nil
}

// Stats returns a snapshot of the broker's counters.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	subs := len(b.subs)
	retained := len(b.retained)
	b.mu.Unlock()
	return Stats{
		Subscriptions: subs,
		Retained:      retained,
		Delivered:     b.delivered.Load(),
		Dropped:       b.dropped.Load(),
	}
}
