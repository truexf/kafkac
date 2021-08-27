package kafkac

import (
	"net"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

// brokerAddr host:port
func ListTopics(brokerAddr string) ([]string, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	ret := make([]string, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	return ret, nil
}

// use when auto.create.topics.enable='false'
func CreateTopic(brokerAddr string, topic string, NumPartitions, ReplicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     NumPartitions,
			ReplicationFactor: ReplicationFactor,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	return nil
}
