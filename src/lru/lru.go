package lru

import "container/list"

type Cache struct {
	maxBytes int64
	nbytes int64
	ll *list.List // doubly linked list to store entry
	cache map[string]*list.Element // map key to doubly linked list entry
	OnEvicted func(key string, value Value) // optional and executed when an entry is purged
}

type entry struct {
	key string // remain key here for delete key-value in map Cache.cache
	value Value
}

type Value interface {
	Len() int
}

func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes: maxBytes,
		ll: list.New(),
		cache: make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get finds a value using key and move it to the front according to LRU
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest deletes the element in the back according to LRU
// if a callback function onEvicted was set, it will call this function 
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add a key-value pair to the Cache
// after Add, if the used memory is bigger than the maxBytes
// Call RemoveOldest() till nbytes less than maxBytes 
func (c *Cache) Add (key string, value Value) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Len return number of cache entries
func (c *Cache) Len() int {
	return c.ll.Len()
}