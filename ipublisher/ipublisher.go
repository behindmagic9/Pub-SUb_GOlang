package ipublisher

import(
	"observer/event"
)

type ipublisher interface{
	Publish(event.Event)
}
