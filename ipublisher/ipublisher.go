package ipublisher

import(
	"observer/event"
)

type ipublisher interface{
	Publish(evnt *event.Event)
}
