package geecache

import (
	"fmt"
	"log"
	"sync"

	"github.com/U317-AleX/Gee-KVS/src/singleflight"
	pb "github.com/U317-AleX/Gee-KVS/src/geecachepb"
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
	name      string
	getter    Getter // method defining how to get data from date source, implemented by user.
	mainCache cache  // underlying object of cache.
	peers     PeerPicker
	loader    *singleflight.Group // use singleflight.Group to make sure that each key is only fetched once
}

var (
	mu     sync.RWMutex
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
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
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

// RegisterPeers register a PeerPicker for choosed remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
}

// load picks a peer and try to get value from it
// if anything went wrong, load locally
func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}

		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
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

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

