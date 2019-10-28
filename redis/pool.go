// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ ConnWithTimeout = (*activeConn)(nil)
	_ ConnWithTimeout = (*errorConn)(nil)
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection method (Do, Send,
// Receive, Flush, Err) when the maximum number of database connections in the
// pool has been reached.
var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")

var (
	errPoolClosed = errors.New("redigo: connection pool closed")
	errConnClosed = errors.New("redigo: connection closed")
)

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a package level variable. The pool configuration used
// here is an example, not a recommendation.
//
//  func newPool(addr string) *redis.Pool {
//    return &redis.Pool{
//      MaxIdle: 3,
//      IdleTimeout: 240 * time.Second,
//      // Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
//      Dial: func () (redis.Conn, error) { return redis.Dial("tcp", addr) },
//    }
//  }
//
//  var (
//    pool *redis.Pool
//    redisServer = flag.String("redisServer", ":6379", "")
//  )
//
//  func main() {
//    flag.Parse()
//    pool = newPool(*redisServer)
//    ...
//  }
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn := pool.Get()
//      defer conn.Close()
//      ...
//  }
//
// Use the Dial function to authenticate connections with the AUTH command or
// select a database with the SELECT command:
//
//  pool := &redis.Pool{
//    // Other pool configuration not shown in this example.
//    Dial: func () (redis.Conn, error) {
//      c, err := redis.Dial("tcp", server)
//      if err != nil {
//        return nil, err
//      }
//      if _, err := c.Do("AUTH", password); err != nil {
//        c.Close()
//        return nil, err
//      }
//      if _, err := c.Do("SELECT", db); err != nil {
//        c.Close()
//        return nil, err
//      }
//      return c, nil
//    },
//  }
//
// Use the TestOnBorrow function to check the health of an idle connection
// before the connection is returned to the application. This example PINGs
// connections that have been idle more than a minute:
//
//  pool := &redis.Pool{
//    // Other pool configuration not shown in this example.
//    TestOnBorrow: func(c redis.Conn, t time.Time) error {
//      if time.Since(t) < time.Minute {
//        return nil
//      }
//      _, err := c.Do("PING")
//      return err
//    },
//  }
// 连接池对象管理
type Pool struct {
	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	// 建立连接回调操作， 让用户主动去建立连接
	Dial func() (Conn, error)

	// DialContext is an application supplied function for creating and configuring a
	// connection with the given context.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	// 带着context上下文的 超时时间的建立连接操作
	DialContext func(ctx context.Context) (Conn, error)
	
	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error

	// Maximum number of idle connections in the pool.
	// 最大闲置连接数
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	// 最多连接数， 
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	// 闲置连接的closed时间， 即闲置连接，多长时间不用后， 自动清除， 是从当前连接【放回put】开始计时
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	// 当连接全部被使用完， 是否继续等待连接空出， 还是直接返回，告知客户端，无可用的连接
	Wait bool

	// Close connections older than this duration. If the value is zero, then
	// the pool does not close connections based on age.
	// 最大连接生命周期, 是指从【创建开始】计时
	MaxConnLifetime time.Duration
	
	// 是否已经初始化，最大连接总数
	chInitialized uint32 // set to 1 when field ch is initialized

	// 锁管理， 管理以下字段的修改
	mu           sync.Mutex    // mu protects the following fields
	closed       bool          // set to true when the pool is closed.
	active       int           // the number of open connections in the pool
	// 活跃连接池对象， 从此队列获取名额
	ch           chan struct{} // limits open connections when p.Wait is true
	// 闲置连接列表
	idle         idleList      // idle connections
	// 等待连接总数
	waitCount    int64         // total number of connections waited for.
	// 新连接等待时长
	waitDuration time.Duration // total time waited for new connections.
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
// 初始化 连接池
func NewPool(newFn func() (Conn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
// 从连接池，获取一次连接实例
func (p *Pool) Get() Conn {
	pc, err := p.get(nil)
	if err != nil {
		return errorConn{err}
	}
	return &activeConn{p: p, pc: pc}
}

// GetContext gets a connection using the provided context.
//
// The provided Context must be non-nil. If the context expires before the
// connection is complete, an error is returned. Any expiration on the context
// will not affect the returned connection.
//
// If the function completes without error, then the application must close the
// returned connection.
// 携带context， 获取连接， 如果处于wait等待阶段，并且context已经过期， 则提前返回
func (p *Pool) GetContext(ctx context.Context) (Conn, error) {
	pc, err := p.get(ctx)
	if err != nil {
		return errorConn{err}, err
	}
	return &activeConn{p: p, pc: pc}, nil
}

// PoolStats contains pool statistics.
// 连接池状态管理结构体， 这里统计连接池信息
type PoolStats struct {
	// ActiveCount is the number of connections in the pool. The count includes
	// idle connections and connections in use.
	ActiveCount int
	// IdleCount is the number of idle connections in the pool.
	IdleCount int

	// WaitCount is the total number of connections waited for.
	// This value is currently not guaranteed to be 100% accurate.
	WaitCount int64

	// WaitDuration is the total time blocked waiting for a new connection.
	// This value is currently not guaranteed to be 100% accurate.
	WaitDuration time.Duration
}

// Stats returns pool's statistics.
// 获取统计信息
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	stats := PoolStats{
		ActiveCount:  p.active,
		IdleCount:    p.idle.count,
		WaitCount:    p.waitCount,
		WaitDuration: p.waitDuration,
	}
	p.mu.Unlock()

	return stats
}

// ActiveCount returns the number of connections in the pool. The count
// includes idle connections and connections in use.
// 在活连接总数
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// IdleCount returns the number of idle connections in the pool.
// 闲置连接总数
func (p *Pool) IdleCount() int {
	p.mu.Lock()
	idle := p.idle.count
	p.mu.Unlock()
	return idle
}

// Close releases the resources used by the pool.
// 连接池关闭， 释放所有资源， todo
func (p *Pool) Close() error {
	p.mu.Lock()
	// 避免重复关闭
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.idle.count
	pc := p.idle.front
	p.idle.count = 0
	p.idle.front, p.idle.back = nil, nil
	if p.ch != nil {
		close(p.ch)
	}
	p.mu.Unlock()
	for ; pc != nil; pc = pc.next {
		pc.c.Close()
	}
	return nil
}

// 懒初始化 连接
func (p *Pool) lazyInit() {
	// Fast path.
	//判断是否已经初始化过， 如果已经操作过， 则提前 返回
	if atomic.LoadUint32(&p.chInitialized) == 1 {
		return
	}
	// Slow path.
	p.mu.Lock()
	if p.chInitialized == 0 {
		// 初始化channel, 长度为 最大连接数
		p.ch = make(chan struct{}, p.MaxActive)
		if p.closed {
			close(p.ch)
		} else {
			// 打入channel 为最大连接数
			for i := 0; i < p.MaxActive; i++ {
				p.ch <- struct{}{}
			}
		}
		atomic.StoreUint32(&p.chInitialized, 1)
	}
	p.mu.Unlock()
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
// 获取连接实例
func (p *Pool) get(ctx context.Context) (*poolConn, error) {
	// 1. 检查是否需要等待空闲连接， 如果需要等待， 则需要等待
	// 2. 从后往前，检查闲置连接链表是否超时，IdleTimeout， 超时则释放连接
	// 3. 从前往后， 检查闲置连接表是否有没有超时的连接， 如果有，则返回当前连接， 否则的话，释放连接
	// 4. 创建连接， 如果失败， 将本次申请的名额，重新放回channel

	// Handle limit for p.Wait == true.
	var waited time.Duration
	// 如果已经处于等待wait状态， 并且 最大连接数大于0
	if p.Wait && p.MaxActive > 0 {
		// 进行懒加载操作， 初始化channel
		p.lazyInit()

		// wait indicates if we believe it will block so its not 100% accurate
		// however for stats it should be good enough.
		// 判断实际上是否需要等待， 如果未消费总数==0, 则确定一定需要等待
		wait := len(p.ch) == 0
		var start time.Time
		if wait {
			start = time.Now()
		}
		// 如果没有ctx， 则直接从p.ch中，获取连接实例
		if ctx == nil {
			<-p.ch
		} else {
			// 阻塞， p.ch, ctx.Done 看谁先完成
			select {
			case <-p.ch:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if wait {
			// 计算等待时长
			waited = time.Since(start)
		}
	}

	p.mu.Lock()

	// 等待统计
	if waited > 0 {
		// 等待次数+1
		p.waitCount++
		// 等待时长累加
		p.waitDuration += waited
	}

	// Prune stale connections at the back of the idle list.
	if p.IdleTimeout > 0 {
		n := p.idle.count
		// 从后往前检查， 释放已经到达过期时间的闲置连接 实例
		for i := 0; i < n && p.idle.back != nil && p.idle.back.t.Add(p.IdleTimeout).Before(nowFunc()); i++ {
			pc := p.idle.back
			p.idle.popBack()
			p.mu.Unlock()
			pc.c.Close()
			p.mu.Lock()
			p.active--
		}
	}

	// Get idle connection from the front of idle list.
	// 从idlelist 的前面，获取闲置连接总数
	for p.idle.front != nil {
		pc := p.idle.front
		p.idle.popFront()
		p.mu.Unlock()
		if (p.TestOnBorrow == nil || p.TestOnBorrow(pc.c, pc.t) == nil) &&
			(p.MaxConnLifetime == 0 || nowFunc().Sub(pc.created) < p.MaxConnLifetime) {
			// 成功获取连接
			return pc, nil
		}
		// 连接已经超时， 需要重新创建连接
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	// Check for pool closed before dialing a new connection.
	// 检查连接池是否关闭
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("redigo: get on closed pool")
	}

	// Handle limit for p.Wait == false.
	// 不需要等待，并且连接已经用完， 则直接报错
	if !p.Wait && p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}
	// 再活连接+1
	p.active++
	p.mu.Unlock()
	c, err := p.dial(ctx)
	// 创建连接失败
	if err != nil {
		c = nil
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			// 本次创建连接失败， 将本次创建连接名额，重新送回
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
	}
	return &poolConn{c: c, created: nowFunc()}, err
}
// 建立连接, 如果有超时时间，则调用dialContext, 否则的话， 调用dial
func (p *Pool) dial(ctx context.Context) (Conn, error) {
	if p.DialContext != nil {
		return p.DialContext(ctx)
	}
	if p.Dial != nil {
		return p.Dial()
	}
	return nil, errors.New("redigo: must pass Dial or DialContext to pool")
}

// 将连接放回的 连接池
func (p *Pool) put(pc *poolConn, forceClose bool) error {
	p.mu.Lock()
	// 先检查连接是否关闭
	if !p.closed && !forceClose {
		pc.t = nowFunc()
		p.idle.pushFront(pc)
		// 如果闲置连接已经达到最大总数， 则从后往前弹出，一个
		if p.idle.count > p.MaxIdle {
			pc = p.idle.back
			p.idle.popBack()
		} else {
			pc = nil
		}
	}
	// 释放从后往前弹出的连接
	if pc != nil {
		p.mu.Unlock()
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	//
	if p.ch != nil && !p.closed {
		// 交还连接实例到 channel
		p.ch <- struct{}{}
	}
	p.mu.Unlock()
	return nil
}

// 在活跃连接对象
type activeConn struct {
	p     *Pool // 所属连接池
	pc    *poolConn // 所属的从连接池获取的对象
	state int
}

var (
	sentinel     []byte // 哨兵 todo
	sentinelOnce sync.Once // 哨兵次数 todo
)

// 初始化哨兵参数
func initSentinel() {
	p := make([]byte, 64)
	if _, err := rand.Read(p); err == nil {
		sentinel = p
	} else {
		h := sha1.New()
		io.WriteString(h, "Oops, rand failed. Use time instead.")
		io.WriteString(h, strconv.FormatInt(time.Now().UnixNano(), 10))
		sentinel = h.Sum(nil)
	}
}

// 关闭当前连接
func (ac *activeConn) Close() error {
	pc := ac.pc
	if pc == nil {
		return nil
	}
	ac.pc = nil

	if ac.state&connectionMultiState != 0 {
		pc.c.Send("DISCARD")
		ac.state &^= (connectionMultiState | connectionWatchState)
	} else if ac.state&connectionWatchState != 0 {
		pc.c.Send("UNWATCH")
		ac.state &^= connectionWatchState
	}
	if ac.state&connectionSubscribeState != 0 {
		pc.c.Send("UNSUBSCRIBE")
		pc.c.Send("PUNSUBSCRIBE")
		// To detect the end of the message stream, ask the server to echo
		// a sentinel value and read until we see that value.
		sentinelOnce.Do(initSentinel)
		pc.c.Send("ECHO", sentinel)
		pc.c.Flush()
		for {
			p, err := pc.c.Receive()
			if err != nil {
				break
			}
			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				ac.state &^= connectionSubscribeState
				break
			}
		}
	}
	pc.c.Do("")
	ac.p.put(pc, ac.state != 0 || pc.c.Err() != nil)
	return nil
}

func (ac *activeConn) Err() error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	return pc.c.Err()
}

// 发送命令
func (ac *activeConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return pc.c.Do(commandName, args...)
}

// 带着超时时间
func (ac *activeConn) DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return cwt.DoWithTimeout(timeout, commandName, args...)
}

// 发送命令 到client buf, 和flush配套使用
func (ac *activeConn) Send(commandName string, args ...interface{}) error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return pc.c.Send(commandName, args...)
}

// 将所有命令刷出--->实际发出请求, 和send配套使用
func (ac *activeConn) Flush() error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	return pc.c.Flush()
}

// 接收请求
func (ac *activeConn) Receive() (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	return pc.c.Receive()
}

// 带过期时间的接收
func (ac *activeConn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	return cwt.ReceiveWithTimeout(timeout)
}

type errorConn struct{ err error }

func (ec errorConn) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConn) DoWithTimeout(time.Duration, string, ...interface{}) (interface{}, error) {
	return nil, ec.err
}
func (ec errorConn) Send(string, ...interface{}) error                     { return ec.err }
func (ec errorConn) Err() error                                            { return ec.err }
func (ec errorConn) Close() error                                          { return nil }
func (ec errorConn) Flush() error                                          { return ec.err }
func (ec errorConn) Receive() (interface{}, error)                         { return nil, ec.err }
func (ec errorConn) ReceiveWithTimeout(time.Duration) (interface{}, error) { return nil, ec.err }

// 闲置连接总数
type idleList struct {
	count       int
	front, back *poolConn
}

// 实际连接实例， 管理conn
type poolConn struct {
	c          Conn
	t          time.Time //放回连接池的时间
	created    time.Time // 创建连接池时间  created >= t
	next, prev *poolConn
}

// 将pc插到 双联表首位
func (l *idleList) pushFront(pc *poolConn) {
	pc.next = l.front
	pc.prev = nil
	if l.count == 0 {
		l.back = pc
	} else {
		l.front.prev = pc
	}
	l.front = pc
	l.count++
	return
}

// 从双联表头部弹出取数据
func (l *idleList) popFront() {
	pc := l.front
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.next.prev = nil
		l.front = pc.next
	}
	pc.next, pc.prev = nil, nil
}

// 从双联表尾部弹出取数据
func (l *idleList) popBack() {
	pc := l.back
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.prev.next = nil
		l.back = pc.prev
	}
	pc.next, pc.prev = nil, nil
}
