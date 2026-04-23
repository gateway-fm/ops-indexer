package events

import (
	"encoding/json"
	"sync"
)

type EventType string

const (
	EventBlockNew        EventType = "block:new"
	EventTxNew           EventType = "tx:new"
	EventAddressActivity EventType = "address:activity"
	EventPriceUpdate     EventType = "price:update"
	EventSyncStatus      EventType = "sync:status"
)

type Event struct {
	Type EventType       `json:"type"`
	Data json.RawMessage `json:"data"`
}

func NewEvent(eventType EventType, data interface{}) (*Event, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Event{
		Type: eventType,
		Data: jsonData,
	}, nil
}

type Subscriber func(event *Event)

type Bus struct {
	mu          sync.RWMutex
	subscribers map[EventType][]chan *Event
	closed      bool
}

func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[EventType][]chan *Event),
	}
}

func (b *Bus) Subscribe(eventType EventType) <-chan *Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *Event, 100)
	b.subscribers[eventType] = append(b.subscribers[eventType], ch)
	return ch
}

func (b *Bus) SubscribeAll() <-chan *Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *Event, 100)

	eventTypes := []EventType{
		EventBlockNew,
		EventTxNew,
		EventAddressActivity,
		EventPriceUpdate,
		EventSyncStatus,
	}

	for _, et := range eventTypes {
		b.subscribers[et] = append(b.subscribers[et], ch)
	}

	return ch
}

func (b *Bus) Unsubscribe(ch <-chan *Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for eventType, subs := range b.subscribers {
		for i, sub := range subs {
			if sub == ch {
				b.subscribers[eventType] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
}

func (b *Bus) Publish(event *Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return
	}

	subs, ok := b.subscribers[event.Type]
	if !ok {
		return
	}

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
		}
	}
}

func (b *Bus) PublishNewBlock(block interface{}) error {
	event, err := NewEvent(EventBlockNew, block)
	if err != nil {
		return err
	}
	b.Publish(event)
	return nil
}

func (b *Bus) PublishNewTransaction(tx interface{}) error {
	event, err := NewEvent(EventTxNew, tx)
	if err != nil {
		return err
	}
	b.Publish(event)
	return nil
}

func (b *Bus) PublishAddressActivity(address string, activity interface{}) error {
	data := map[string]interface{}{
		"address":  address,
		"activity": activity,
	}
	event, err := NewEvent(EventAddressActivity, data)
	if err != nil {
		return err
	}
	b.Publish(event)
	return nil
}

func (b *Bus) PublishPriceUpdate(price interface{}) error {
	event, err := NewEvent(EventPriceUpdate, price)
	if err != nil {
		return err
	}
	b.Publish(event)
	return nil
}

func (b *Bus) PublishSyncStatus(status interface{}) error {
	event, err := NewEvent(EventSyncStatus, status)
	if err != nil {
		return err
	}
	b.Publish(event)
	return nil
}

func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	closedChans := make(map[chan *Event]bool)
	for _, subs := range b.subscribers {
		for _, ch := range subs {
			if !closedChans[ch] {
				close(ch)
				closedChans[ch] = true
			}
		}
	}

	b.subscribers = make(map[EventType][]chan *Event)
}
