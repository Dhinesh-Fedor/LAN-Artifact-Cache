// Atomic counters for node-level statistics.
package stats

import "sync/atomic"

type Stats struct {
	Hits, Misses, Bytes atomic.Uint64
}
