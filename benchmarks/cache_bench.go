// Measures sustained write throughput of the local cache.
package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/Fedoraaa/lan-artifact-cache/internal/cache"
)

func main() {
	d, _ := os.MkdirTemp("", "p2cache-bench")
	defer os.RemoveAll(d)

	s, _ := cache.NewStorage(d)
	c := cache.New(s)

	data := make([]byte, 1<<20)
	start := time.Now()

	for i := 0; i < 100; i++ {
		_ = c.Put(fmt.Sprintf("%032d", i), bytes.NewReader(data))
	}

	elapsed := time.Since(start)
	fmt.Printf("puts=100 bytes=%d elapsed=%s throughput=%.2f MB/s\n",
		len(data)*100, elapsed,
		float64(len(data)*100)/(1024*1024)/elapsed.Seconds())
}
