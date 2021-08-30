// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package kafkac

import (
	"hash/fnv"
	"sync"
)

type RoundRobinPartitionRule struct {
	PartitionNum   int
	partitionIndex int
	sync.Mutex
}

func (m *RoundRobinPartitionRule) Partition(topic string, key, value []byte) int {
	m.Lock()
	defer m.Unlock()
	ret := m.partitionIndex
	m.partitionIndex++
	if m.partitionIndex >= m.PartitionNum {
		m.partitionIndex = 0
	}
	return ret
}

type KeyHashPartitionRule struct {
	PartitionNum int
}

func (m *KeyHashPartitionRule) Partition(topic string, key, value []byte) int {
	h := fnv.New32()
	h.Write(key)
	return int(h.Sum32()) % m.PartitionNum
}
