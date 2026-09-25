package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDo verifies that when multiple goroutines call Do with the same key
// concurrently, the user function is executed only once, and all callers
// receive the same result.
// TestDo 验证：多个协程并发调用同一 key 的 Do 时，用户函数只执行一次，
// 且所有调用者获得相同的结果。
func TestDo(t *testing.T) {
	var g Group

	// callCount counts how many times the user function is actually executed.
	// callCount 记录用户函数实际执行的次数。
	var callCount int32

	// numCallers simulates concurrent callers.
	// numCallers 模拟并发调用者数量。
	const numCallers = 100

	var wg sync.WaitGroup
	wg.Add(numCallers)

	// results stores the result returned to each caller.
	// results 存储每个调用者收到的结果。
	results := make([]string, numCallers)
	errs := make([]error, numCallers)

	for i := 0; i < numCallers; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := g.Do("key1", func() (interface{}, error) {
				// Simulate slow data loading.
				// 模拟慢速数据加载。
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt32(&callCount, 1)
				return "value1", nil
			})
			if val != nil {
				results[idx] = val.(string)
			}
			errs[idx] = err
		}(i)
	}

	// Wait for all goroutines to finish.
	// 等待所有协程完成。
	wg.Wait()

	// The user function should have been called exactly once.
	// 用户函数应只被调用一次。
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("expected fn to be called 1 time, got %d", got)
	}

	// All callers should receive the same result and no error.
	// 所有调用者应收到相同的结果且无错误。
	for i := 0; i < numCallers; i++ {
		if results[i] != "value1" {
			t.Fatalf("caller %d got %q, want %q", i, results[i], "value1")
		}
		if errs[i] != nil {
			t.Fatalf("caller %d got unexpected error: %v", i, errs[i])
		}
	}
}

// TestDoErr verifies that when the user function returns an error,
// all concurrent callers receive the same error.
// TestDoErr 验证：用户函数返回错误时，所有并发调用者收到相同的错误。
func TestDoErr(t *testing.T) {
	var g Group

	var callCount int32

	errSentinel := errors.New("fake error")

	const numCallers = 50
	var wg sync.WaitGroup
	wg.Add(numCallers)

	errs := make([]error, numCallers)

	for i := 0; i < numCallers; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := g.Do("key_err", func() (interface{}, error) {
				atomic.AddInt32(&callCount, 1)
				return nil, errSentinel
			})
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("expected fn to be called 1 time, got %d", got)
	}

	for i := 0; i < numCallers; i++ {
		if !errors.Is(errs[i], errSentinel) {
			t.Fatalf("caller %d got %v, want %v", i, errs[i], errSentinel)
		}
	}
}

// TestDoDifferentKeys verifies that calls with different keys
// are executed independently and not merged.
// TestDoDifferentKeys 验证：不同 key 的调用独立执行，不会被合并。
func TestDoDifferentKeys(t *testing.T) {
	var g Group

	var callCount int32

	const numKeys = 5
	const callersPerKey = 20

	var wg sync.WaitGroup
	wg.Add(numKeys * callersPerKey)

	for k := 0; k < numKeys; k++ {
		key := "key" + string(rune('A'+k))
		for c := 0; c < callersPerKey; c++ {
			go func(k string) {
				defer wg.Done()
				g.Do(k, func() (interface{}, error) {
					atomic.AddInt32(&callCount, 1)
					return k, nil
				})
			}(key)
		}
	}

	wg.Wait()

	// Each key should have its function called exactly once.
	// 每个 key 的函数应只执行一次，所以总共执行 numKeys 次。
	if got := atomic.LoadInt32(&callCount); got != numKeys {
		t.Fatalf("expected fn to be called %d times, got %d", numKeys, got)
	}
}

// TestDoSequential verifies that sequential calls to the same key
// after the first one completes are NOT merged.
// TestDoSequential 验证：同一 key 的串行调用（第一次完成后再调用）不会被合并。
func TestDoSequential(t *testing.T) {
	var g Group

	var callCount int32

	// First call.
	// 第一次调用。
	g.Do("key", func() (interface{}, error) {
		atomic.AddInt32(&callCount, 1)
		return "first", nil
	})

	// Second call (after first completes, should NOT be merged).
	// 第二次调用（第一次完成后调用，不应被合并）。
	val, _ := g.Do("key", func() (interface{}, error) {
		atomic.AddInt32(&callCount, 1)
		return "second", nil
	})

	if got := atomic.LoadInt32(&callCount); got != 2 {
		t.Fatalf("expected fn to be called 2 times, got %d", got)
	}

	if val.(string) != "second" {
		t.Fatalf("expected 'second', got %v", val)
	}
}