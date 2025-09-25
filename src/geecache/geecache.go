package geecache

import (
	"fmt"
	"log"
	"sync"
)

// this file contains the main structure of GeeCache,
// including interacting with external users,
// controlling the storage of cache,
// and getting the values.

// A Getter loads data for a key.
type Getter interface {
	Get(key string) ([]byte, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(key string) ([]byte, error)

// Get implements Getter interface funciton.
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// You could see function GetterFunc as a Object has method Get, 
// so it could be viewed as Getter Object.
// The benefit here is GetterFunc could be transed as a Object while also as a Func.

// A Group is a cache namespace and associated data loaded spread over
type Group struct {
	name string
	getter Getter // method defining how to get data from date source, implemented by user.
	mainCache cache // underlying object of cache.
}

var (
	mu sync.RWMutex
	groups = make(map[string]*Group) // map names to groups.
)

// NewGroup create a new instance of Group.
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name: name,
		getter: getter,
		mainCache: cache{cacheBytes: cacheBytes},
	}
	groups[name] = g
	return g
}

// Get a group by its name.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	// if get from cache, return
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}
	// else fetch data using method provided by user
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	return g.getLocally(key)
}

func (g *Group) getLocally(key string) (ByteView, error) {
	// use getter to get data source
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	// push new value into cache
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}