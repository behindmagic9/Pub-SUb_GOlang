package isubscriber

type Isubscriber interface {
	Update(topic string, data any);
}
