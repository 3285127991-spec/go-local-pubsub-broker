package pubsubbroker

import (
	"errors"
	"sync"
)

// ErrClosed is returned by Publish and Subscribe after the broker is closed.
var ErrClosed = errors.New("pubsubbroker: broker is closed")

// Message is a single published message delivered to subscribers.
type Message struct {
	Topic   string
	Payload []byte
}

// Stats is a point-in-time snapshot of broker counters.
type Stats struct {
	// Subscriptions is the number of active subscriptions.
	Subscriptions int
	// Retained is the number of topics holding a retained message.
	Retained int
	// Delivered is the total number of messages successfully placed into
	// subscription buffers (including retained messages delivered on subscribe).
	Delivered uint64
	// Dropped is the total number of messages discarded because a
	// subscription buffer was full.
	Dropped uint64
}

// Subscription receives published messages matching its filter. Messages are
// delivered on the channel returned by Messages in publish order. The channel
// is closed when the subscription is cancelled or the broker is closed.
type Subscription struct {
	broker *Broker
	id     uint64
	filter string
	ch     chan Message
	once   sync.Once

	// dropped is guarded by broker.mu.
	dropped uint64
}

// Messages returns the channel on which matching messages are delivered.
func (s *Subscription) Messages() <-chan Message {
	return s.ch
}

// Filter returns the topic filter this subscription was created with.
func (s *Subscription) Filter() string {
	return s.filter
}

// Dropped returns the number of messages discarded for this subscription
// because its buffer was full.
func (s *Subscription) Dropped() uint64 {
	s.broker.mu.RLock()
	defer s.broker.mu.RUnlock()
	return s.dropped
}

// Unsubscribe cancels the subscription: no new messages are delivered and the
// message channel is closed so consumers can drain buffered messages. It is
// safe to call Unsubscribe multiple times and from multiple goroutines.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		s.broker.unsubscribe(s.id)
	})
}

// Broker is a concurrency-safe in-process publish/subscribe broker. The zero
// value is not usable; create one with New.
type Broker struct {
	mu        sync.RWMutex
	closed    bool
	nextID    uint64
	subs      map[uint64]*Subscription
	retained  map[string]Message
	delivered uint64
	dropped   uint64
}

// New returns a new, ready-to-use Broker.
func New() *Broker {
	return &Broker{
		subs:     make(map[uint64]*Subscription),
		retained: make(map[string]Message),
	}
}

// Subscribe creates a subscription for the given filter with a fixed-capacity
// buffer of capacity messages. All currently retained messages matching the
// filter are immediately placed into the buffer (in unspecified order), ahead
// of any message published after Subscribe returns. It returns ErrClosed if
// the broker is closed and an error if the filter is invalid or capacity is
// not positive.
func (b *Broker) Subscribe(filter string, capacity int) (*Subscription, error) {
	if capacity <= 0 {
		return nil, errors.New("pubsubbroker: subscription capacity must be positive")
	}
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	b.nextID++
	s := &Subscription{
		broker: b,
		id:     b.nextID,
		filter: filter,
		ch:     make(chan Message, capacity),
	}
	b.subs[s.id] = s
	for topic, msg := range b.retained {
		if matchFilter(filter, topic) {
			b.deliver(s, msg)
		}
	}
	return s, nil
}

// Publish delivers a message to every subscription whose filter matches
// topic. Delivery to a subscription is non-blocking: if its buffer is full
// the message is discarded for that subscription and counted as dropped,
// without affecting other subscriptions. If retained is true, the message
// replaces any previously retained message on the same topic and is delivered
// to future matching subscriptions. It returns ErrClosed if the broker is
// closed and an error if the topic is invalid.
func (b *Broker) Publish(topic string, payload []byte, retained bool) error {
	if err := validateTopic(topic); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	msg := Message{Topic: topic, Payload: append([]byte(nil), payload...)}
	if retained {
		b.retained[topic] = msg
	}
	for _, s := range b.subs {
		if matchFilter(s.filter, topic) {
			b.deliver(s, msg)
		}
	}
	return nil
}

// deliver places msg into the subscription buffer, dropping and counting it
// when the buffer is full. Callers must hold b.mu for writing, which
// serializes all publishes and therefore preserves per-subscription order.
func (b *Broker) deliver(s *Subscription, msg Message) {
	select {
	case s.ch <- msg:
		b.delivered++
	default:
		s.dropped++
		b.dropped++
	}
}

// unsubscribe removes the subscription and closes its channel.
func (b *Broker) unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(s.ch)
	}
}

// Stats returns a snapshot of the broker counters.
func (b *Broker) Stats() Stats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return Stats{
		Subscriptions: len(b.subs),
		Retained:      len(b.retained),
		Delivered:     b.delivered,
		Dropped:       b.dropped,
	}
}

// Close shuts the broker down: all subscription channels are closed and
// subsequent Publish and Subscribe calls fail with ErrClosed. Close is safe
// to call multiple times.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id, s := range b.subs {
		close(s.ch)
		delete(b.subs, id)
	}
}
