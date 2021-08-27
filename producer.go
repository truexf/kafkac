package kafkac

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	brokerAddressList []string
	topic             string
	partitionRule     PartitionRule
	alarm             Alarm
	recover           ProducerRecover
	partitionConn     map[int]*kafka.Conn
	connLock          sync.RWMutex
	pauseUnixTime     int64 //消息发布发生错误的是，将暂停发送10秒，消息暂存于recover，pauseTime记录暂停开始的时间戳
}

func NewProducer(topic string, partitionRule PartitionRule, alarm Alarm, recover ProducerRecover, brokerAddrList []string) *Producer {
	if recover == nil || len(brokerAddrList) == 0 || partitionRule == nil || topic == "" {
		return nil
	}

	return &Producer{
		brokerAddressList: brokerAddrList,
		topic:             topic,
		partitionRule:     partitionRule,
		alarm:             alarm,
		recover:           recover,
		partitionConn:     make(map[int]*kafka.Conn),
	}
}

func (m *Producer) removePartitionConn(conn *kafka.Conn) {
	m.connLock.Lock()
	defer m.connLock.Unlock()

	for k, v := range m.partitionConn {
		if v == conn {
			conn.Close()
			delete(m.partitionConn, k)
		}
	}
}

func (m *Producer) dial(partition int) (*kafka.Conn, error) {
	if time.Now().Unix()-atomic.LoadInt64(&m.pauseUnixTime) <= 10 {
		return nil, ErrorPaused
	}
	m.connLock.RLock()
	conn, ok := m.partitionConn[partition]
	m.connLock.RUnlock()
	if ok {
		return conn, nil
	}
	for _, addr := range m.brokerAddressList {
		conn, err := kafka.DialLeader(context.Background(), "tcp", addr, m.topic, partition)
		if err != nil {
			continue
		}
		m.connLock.Lock()
		m.partitionConn[partition] = conn
		m.connLock.Unlock()
		return conn, nil
	}
	return nil, fmt.Errorf("dial to [%s] fail", strings.Join(m.brokerAddressList, ","))
}

func (m *Producer) PublishMessage(msg Message) error {
	partition := m.partitionRule.Partition(m.topic, msg.Key, msg.Value)
	conn, err := m.dial(partition)
	if err != nil {
		if err != ErrorPaused {
			if m.alarm != nil {
				m.alarm.Alarm(fmt.Sprintf("dial broker fail, %s", err.Error()))
			}
		}
		return m.recover.Save(msg)
	}
	if _, err := conn.WriteMessages(kafka.Message{Key: msg.Key, Value: msg.Value}); err != nil {
		ret := m.recover.Save(msg)
		m.removePartitionConn(conn)
		atomic.StoreInt64(&m.pauseUnixTime, time.Now().Unix())
		if m.alarm != nil {
			m.alarm.Alarm(fmt.Sprintf("publish to broker fail, %s", err.Error()))
		}
		return ret
	}
	return nil
}

func (m *Producer) PublishMessages(msgs []Message, partition int) (int, error) {
	conn, err := m.dial(partition)
	if err != nil {
		if err != ErrorPaused {
			if m.alarm != nil {
				m.alarm.Alarm(fmt.Sprintf("dial broker fail, %s", err.Error()))
			}
		}
		for i, v := range msgs {
			if err := m.recover.Save(v); err != nil {
				return i, err
			}
		}
		return len(msgs), nil
	}

	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, v := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message{Key: v.Key, Value: v.Value})
	}
	n, err := conn.WriteMessages(kafkaMsgs...)
	if err != nil {
		m.removePartitionConn(conn)
		atomic.StoreInt64(&m.pauseUnixTime, time.Now().Unix())
	}
	remain := len(kafkaMsgs) - n
	if remain > 0 {
		for i := n; i < len(msgs); i++ {
			if err := m.recover.Save(msgs[i]); err == nil {
				remain--
			} else {
				return len(msgs) - remain, err
			}
		}
	}
	return len(msgs), nil
}
