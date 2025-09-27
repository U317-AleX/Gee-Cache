package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps bytes to unit32
type Hash func(data []byte) uint32

// Map constains all hashed keys
type Map struct {
	hash Hash // Hash function which maps keys to unit32
	replicas int // number of virtual nodes
	keys []int // the hash values of virtual node names
	hashMap map[int]string // map virtual nodes to real nodes
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash: fn,
		replicas: replicas,
		hashMap: make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some keys to the hash
// Attention: key here is the name of a real node
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			// hash is the hash value of virtual node names
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			// map hash to real node name "key"
			m.hashMap[hash] = key
		}
	}
	// sort keys to ensure it's order
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key
// Attention: key here is the key of stored k-v pair
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	// get the hash value of the key
	hash := int(m.hash([]byte(key)))
	// search from keys, find the first one whose value larger than the hash value of key
	// return it's index to "var idx"
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// return the real node name
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
