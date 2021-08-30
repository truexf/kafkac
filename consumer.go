// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package kafkac

// 由于kafka是持久化和基于消费position的可回溯消费，因此consumer端无需再过度封装。
// 无论是低层api还是高层api，基于pattition或是组消费，kafka-go已经做得足够好。
// 特此说明。
type Consumer struct {
}
