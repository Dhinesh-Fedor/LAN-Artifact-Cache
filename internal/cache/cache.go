// Cache wraps Storage with hit/miss counters.
package cache

import (
	"io"
	"os"
	"sync"
)

type Cache struct {
	Storage      *Storage
	mu           sync.RWMutex
	hits, misses uint64
}

func New(s *Storage) *Cache {
	return &Cache{Storage: s}
}

func (c *Cache) Has(hash string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ok := c.Storage.Has(hash)
	if (ok) {
		c.hits++
	} else {
		c.misses++
	}

	return ok
}

func (c *Cache) Put(hash string, r io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Storage.Put(hash, r)
}

func (c *Cache) Open(hash string) (*os.File, error) {
	return c.Storage.Open(hash)
}

func (c *Cache) Stats() (uint64, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.hits, c.misses
}
