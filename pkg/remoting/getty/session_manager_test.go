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
	"sync/atomic"
	"testing"

	"github.com/seata/seata-go/pkg/discovery"
)

type testCloseableClient struct {
	closed int32
}

func (c *testCloseableClient) Close() {
	atomic.StoreInt32(&c.closed, 1)
}

func TestSessionManagerRefreshServerListRemovesStaleAddress(t *testing.T) {
	manager := &SessionManager{}
	clients := make(map[string]*testCloseableClient)
	manager.startClient = func(instance *discovery.ServiceInstance) closeableClient {
		client := &testCloseableClient{}
		clients[serverAddress(instance)] = client
		return client
	}

	manager.refreshServerList([]*discovery.ServiceInstance{
		{Addr: "127.0.0.1", Port: 8091},
		{Addr: "127.0.0.2", Port: 8091},
	})
	if len(clients) != 2 {
		t.Fatalf("expected two getty clients, got %d", len(clients))
	}

	manager.refreshServerList([]*discovery.ServiceInstance{
		{Addr: "127.0.0.2", Port: 8091},
	})
	if atomic.LoadInt32(&clients["127.0.0.1:8091"].closed) != 1 {
		t.Fatal("stale getty client was not closed")
	}
	if manager.isServerAddressAvailable("127.0.0.1:8091") {
		t.Fatal("stale server address is still selectable")
	}
	if !manager.isServerAddressAvailable("127.0.0.2:8091") {
		t.Fatal("active server address is not selectable")
	}
}

func TestSessionManagerReconnectServerAddressReplacesClient(t *testing.T) {
	manager := &SessionManager{}
	clients := make([]*testCloseableClient, 0, 2)
	manager.startClient = func(*discovery.ServiceInstance) closeableClient {
		client := &testCloseableClient{}
		clients = append(clients, client)
		return client
	}

	instance := &discovery.ServiceInstance{Addr: "127.0.0.1", Port: 8091}
	manager.refreshServerList([]*discovery.ServiceInstance{instance})
	manager.reconnectServerAddress(serverAddress(instance))

	if len(clients) != 2 {
		t.Fatalf("expected old and replacement clients, got %d", len(clients))
	}
	if atomic.LoadInt32(&clients[0].closed) != 1 {
		t.Fatal("old Getty client was not closed")
	}
	if atomic.LoadInt32(&clients[1].closed) != 0 {
		t.Fatal("replacement Getty client was closed unexpectedly")
	}
}
