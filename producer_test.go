package kafkac

import (
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	reco, err := NewFileRecover("quickstart-events", "/tmp", 0, 1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	p := NewProducer("quickstart-events", &RoundRobinPartitionRule{PartitionNum: 1}, nil, reco, []string{"localhost:9092"})
	if p == nil {
		t.Fatalf("new producer fail")
	}

	reco.Restore(func(message *Message, err error, canceled *bool) {
		if err := p.PublishMessage(*message); err != nil {
			t.Fatalf("restore fail, %s", err.Error())
		}
	})

	if err := p.PublishMessage(Message{Key: []byte("hello"), Value: []byte("kafka1")}); err != nil {
		t.Fatalf(err.Error())
	}
	close(p.recover.CloseNotify())
	time.Sleep(time.Second)
}
