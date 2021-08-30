// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

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
