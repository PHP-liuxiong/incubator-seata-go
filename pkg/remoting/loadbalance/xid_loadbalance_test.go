/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package loadbalance

import (
	"sync"
	"testing"

	getty "github.com/apache/dubbo-getty"
	"github.com/stretchr/testify/assert"
)

type xidLoadBalanceTestSession struct {
	getty.Session
	remoteAddr string
	closed     bool
}

func (s *xidLoadBalanceTestSession) RemoteAddr() string {
	return s.remoteAddr
}

func (s *xidLoadBalanceTestSession) IsClosed() bool {
	return s.closed
}

func TestXidLoadBalance(t *testing.T) {
	sessions := &sync.Map{}
	sessions.Store(&xidLoadBalanceTestSession{remoteAddr: "127.0.0.1:8000"}, 8000)
	sessions.Store(&xidLoadBalanceTestSession{remoteAddr: "127.0.0.1:8001", closed: true}, 8001)
	sessions.Store(&xidLoadBalanceTestSession{remoteAddr: "127.0.0.1:8002"}, 8002)

	testCases := []struct {
		name        string
		sessions    *sync.Map
		xid         string
		returnAddrs []string
	}{
		{
			name:        "normal",
			sessions:    sessions,
			xid:         "127.0.0.1:8000:111",
			returnAddrs: []string{"127.0.0.1:8000"},
		},
		{
			name:        "session is closed",
			sessions:    sessions,
			xid:         "127.0.0.1:8001:111",
			returnAddrs: []string{"127.0.0.1:8000", "127.0.0.1:8002"},
		},
		{
			name:        "xid is not exist",
			sessions:    sessions,
			xid:         "127.0.0.1:9000:111",
			returnAddrs: []string{"127.0.0.1:8000", "127.0.0.1:8002"},
		},
	}
	for _, test := range testCases {
		session := XidLoadBalance(test.sessions, test.xid)
		assert.Contains(t, test.returnAddrs, session.RemoteAddr())
	}
}
