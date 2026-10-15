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

package getty

import (
	"sync"
	"testing"
	"time"

	getty "github.com/apache/dubbo-getty"
	"github.com/stretchr/testify/assert"

	"github.com/seata/seata-go/pkg/protocol/message"
	"github.com/seata/seata-go/pkg/remoting/config"
)

type xidRoutingTestSession struct {
	getty.Session
	remoteAddr string
	writes     int
}

func (s *xidRoutingTestSession) RemoteAddr() string {
	return s.remoteAddr
}

func (s *xidRoutingTestSession) IsClosed() bool {
	return false
}

func (s *xidRoutingTestSession) Stat() string {
	return s.remoteAddr
}

func (s *xidRoutingTestSession) WritePkg(interface{}, time.Duration) (int, int, error) {
	s.writes++
	return 0, 0, nil
}

func TestGettyRemotingRoutesByXIDFromRpcMessageBody(t *testing.T) {
	const xid = "192.168.183.246:8091:1441983395560036523"

	originalSessionManager := sessionManager
	originalSeataConfig := config.GetSeataConfig()
	defer func() {
		sessionManager = originalSessionManager
		config.IniConfig(originalSeataConfig)
	}()
	config.IniConfig(&config.SeataConfig{LoadBalanceType: "XID"})

	tests := []struct {
		name string
		body interface{}
		send func(*GettyRemoting, message.RpcMessage) error
	}{
		{
			name: "sync request",
			body: message.GlobalCommitRequest{
				AbstractGlobalEndRequest: message.AbstractGlobalEndRequest{Xid: xid},
			},
			send: func(remoting *GettyRemoting, rpcMessage message.RpcMessage) error {
				_, err := remoting.SendSync(rpcMessage, nil, nil)
				return err
			},
		},
		{
			name: "async response",
			body: message.BranchCommitResponse{
				AbstractBranchEndResponse: message.AbstractBranchEndResponse{Xid: xid},
			},
			send: func(remoting *GettyRemoting, rpcMessage message.RpcMessage) error {
				return remoting.SendASync(rpcMessage, nil, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := &xidRoutingTestSession{remoteAddr: "192.168.183.246:8091"}
			other := &xidRoutingTestSession{remoteAddr: "192.168.183.242:8091"}
			manager := &SessionManager{}
			manager.allSessions.Store(target, true)
			manager.allSessions.Store(other, true)
			sessionManager = manager

			remoting := &GettyRemoting{
				futures:     &sync.Map{},
				mergeMsgMap: &sync.Map{},
			}
			rpcMessage := message.RpcMessage{ID: 1, Body: test.body}

			assert.NoError(t, test.send(remoting, rpcMessage))
			assert.Equal(t, 1, target.writes)
			assert.Equal(t, 0, other.writes)
		})
	}
}

func TestSessionManagerGetXidUnwrapsRpcMessage(t *testing.T) {
	const xid = "192.168.183.246:8091:1441983395560036523"
	manager := &SessionManager{}

	assert.Equal(t, xid, manager.getXid(message.RpcMessage{
		Body: message.GlobalCommitRequest{
			AbstractGlobalEndRequest: message.AbstractGlobalEndRequest{Xid: xid},
		},
	}))
	assert.Equal(t, xid, manager.getXid(&message.RpcMessage{
		Body: message.BranchCommitResponse{
			AbstractBranchEndResponse: message.AbstractBranchEndResponse{Xid: xid},
		},
	}))
	assert.Empty(t, manager.getXid(message.RpcMessage{Body: message.HeartBeatMessagePing}))
	assert.Empty(t, manager.getXid(nil))
}
