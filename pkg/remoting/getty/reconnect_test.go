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
	"net"
	"sync/atomic"
	"testing"
	"time"

	gettylib "github.com/apache/dubbo-getty"
)

type reconnectTestListener struct{}

func (reconnectTestListener) OnOpen(gettylib.Session) error           { return nil }
func (reconnectTestListener) OnClose(gettylib.Session)                {}
func (reconnectTestListener) OnError(gettylib.Session, error)         {}
func (reconnectTestListener) OnCron(gettylib.Session)                 {}
func (reconnectTestListener) OnMessage(gettylib.Session, interface{}) {}

func TestGettyClientReconnectsAfterPeerDisconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	client := gettylib.NewTCPClient(
		gettylib.WithServerAddress(listener.Addr().String()),
		gettylib.WithConnectionNumber(1),
		gettylib.WithReconnectInterval(10_000_000),
	)
	defer client.Close()

	var opened int32
	go client.RunEventLoop(func(session gettylib.Session) error {
		session.SetPkgHandler(rpcPkgHandler)
		session.SetEventListener(reconnectTestListener{})
		atomic.AddInt32(&opened, 1)
		return nil
	})

	first, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	_ = first.Close()

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&opened) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&opened); got < 2 {
		t.Fatalf("Getty did not reconnect after peer disconnect, opened sessions: %d", got)
	}

	second, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
}
