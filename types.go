package kafkac

import "errors"

var ErrorPaused = errors.New("paused")

type Message struct {
	Key   []byte
	Value []byte
}

type RestoreFunc func(message *Message, err error, canceled *bool)

type Alarm interface {
	Alarm(message string)
}

type ProducerRecover interface {
	Save(message Message) error
	Restore(fn RestoreFunc)
	CloseNotify() chan struct{}
}

type PartitionRule interface {
	Partition(topic string, key, value []byte) int
}
