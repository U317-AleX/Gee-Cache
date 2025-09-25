package geecache

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("test")
	if v, _ := f.Get("test"); !reflect.DeepEqual(v, expect) {
		t.Error("callback failed")
	}
}

var db = map[string]string {
	"Tom": "630",
	"Jack": "589",
	"Sam": "567",
}

func TestGet(t *testing.T) {
	// counts for the number a key be loaded
	loadCounts := make(map[string]int, len(db))

	// define a group
	gee := NewGroup("scores", 2<<10, 
	// define a Getter which fetches data from var db
	GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil			
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		// test gee.Get()
		if view, err := gee.Get(k); err != nil || view.Strings() != v {
			t.Fatal("failed to get value of Tom")
		}
		// if loadCounts > 1, means data was lost from cache
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	// cache hit
	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknown should be empty, but %s got", view)
	}
}