// Copyright (c) 2023 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && !gc_opt

package gnet

import (
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2/internal/gfd"
)

type connMatrix struct {
	connCount int32
	connMap   map[int]*conn
	lock      sync.RWMutex
}

func (cm *connMatrix) init() {
	cm.connMap = make(map[int]*conn)
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	cnt := cm.loadCount()
	list := make([]*conn, 0, cnt)
	cm.lock.RLock()
	for _, v := range cm.connMap {
		v := v
		list = append(list, v)
	}
	cm.lock.RUnlock()
	for _, c := range list {
		if c != nil {
			if !f(c) {
				return
			}
		}
	}
}

func (cm *connMatrix) incCount(_ int, delta int32) {
	atomic.AddInt32(&cm.connCount, delta)
}

func (cm *connMatrix) loadCount() (n int32) {
	return atomic.LoadInt32(&cm.connCount)
}

func (cm *connMatrix) addConn(c *conn, index int) {
	c.gfd = gfd.NewGFD(c.fd, index, 0, 0)
	cm.lock.Lock()
	cm.connMap[c.fd] = c
	cm.lock.Unlock()
	cm.incCount(0, 1)
}

func (cm *connMatrix) delConn(c *conn) {
	cm.lock.Lock()
	delete(cm.connMap, c.fd)
	cm.lock.Unlock()
	cm.incCount(0, -1)
}

func (cm *connMatrix) getConn(fd int) *conn {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.connMap[fd]
}
