package singleflight

import "sync"

// call represents an in-flight or completed Do call.
// call 在多协程调用同一个 key 的场景下作为协调对象使用。
type call struct {
	wg  sync.WaitGroup // wg is used to block concurrent callers until the first one finishes.
	val interface{}    // val holds the result value returned by the user function.
	err error          // err holds the error returned by the user function.
}

// Group is the core structure of singleflight.
// It deduplicates concurrent calls to the same key so that
// only the first caller actually executes the function,
// and the rest simply wait and reuse the result.
// Group 是 singleflight 的核心结构体。
// 它对同一 key 的并发调用进行去重：
// 只有第一个调用者真正执行函数，其余调用者等待并复用结果。
type Group struct {
	mu    sync.Mutex       // mu protects the calls map below.
	calls map[string]*call // calls maps a key to its corresponding in-flight call.
}

// Do executes the given function once for a given key.
// If multiple goroutines call Do with the same key concurrently,
// only the first one will invoke fn; the others will block
// until the first one finishes and then share the same result.
// Do 针对同一个 key 只执行一次用户函数。
// 如果多个协程同时对同一 key 调用 Do，
// 只有第一个协程会执行 fn，其他协程会阻塞等待，
// 最终共享同一个结果（值和错误），从而避免缓存击穿。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Lock the Group to safely access the shared calls map.
	// 加锁保护共享的 calls map。
	g.mu.Lock()
	// Lazy initialization: create the map on first use.
	// 延迟初始化：首次调用时才创建 map，节省内存。
	if g.calls == nil {
		g.calls = make(map[string]*call)
	}

	// If there is already an in-flight call for this key,
	// the current goroutine should NOT execute fn again.
	// Instead, it unlocks the Group and waits for the result.
	// 如果该 key 已有正在进行的调用，当前协程不重复执行 fn，
	// 而是解锁后等待已有调用完成，复用其结果。
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		// Block until the first caller finishes and calls wg.Done().
		// 阻塞等待第一个调用者完成（wg.Done）。
		c.wg.Wait()
		return c.val, c.err
	}

	// No in-flight call exists for this key, so this goroutine
	// becomes the "leader" and is responsible for executing fn.
	// 该 key 没有正在进行的调用，当前协程成为"leader"，
	// 负责实际执行用户函数。
	c := &call{}
	// Add 1 to the WaitGroup before registering it in the map
	// so that any subsequent caller that finds this call will
	// block on c.wg.Wait() until we call c.wg.Done().
	// 在注册到 map 之前给 WaitGroup 加 1，
	// 这样后续到达的调用者找到该 call 后会阻塞在 c.wg.Wait()，
	// 直到我们调用 c.wg.Done() 才被唤醒。
	c.wg.Add(1)
	// Register this call in the map so later callers can find it.
	// 将该 call 注册到 map 中，后续的调用者即可找到并复用。
	g.calls[key] = c
	// Unlock the Group now that the call is registered.
	// Other goroutines can proceed to check the map.
	// 注册完成后解锁，其他协程可以继续访问 map。
	g.mu.Unlock()

	// Execute the user-provided function.
	// This is the ONLY goroutine that actually runs fn for this key.
	// 执行用户函数，这是该 key 唯一一次真正执行。
	c.val, c.err = fn()
	// Signal all waiting goroutines that the result is ready.
	// 唤醒所有等待该 key 结果的协程。
	c.wg.Done()

	// Lock again to remove the completed call from the map.
	// This allows future calls for the same key to start fresh.
	// 再次加锁，从 map 中移除已完成的 call，
	// 以便后续对该 key 的调用可以重新开始。
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	// Return the result to the leader caller.
	// 向 leader 调用者返回结果。
	return c.val, c.err
}
