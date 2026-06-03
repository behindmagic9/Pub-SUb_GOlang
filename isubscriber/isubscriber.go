package isubscriber

import(
	"observer/event"
)

type Isubscriber interface {
	Update(topic string, data event.Event);
}
