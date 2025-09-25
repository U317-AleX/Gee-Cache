package geecache

import (
	"sync"

	"github.com/U317-AleX/Gee-KVS/lru"
)

// cache is a encapsulation of lru.Cache for implementing concurrency feature
type cache struct {
	mu sync.Mutex // concurrent lock
	lru *lru.Cache // underlying object
	cacheBytes int64 // the scale of cache
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// lazy initialization of c.lru
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), ok
	}
	return
}
