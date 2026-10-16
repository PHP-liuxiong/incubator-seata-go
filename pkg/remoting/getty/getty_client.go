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
	"fmt"
	"sync"

	dubboGetty "github.com/apache/dubbo-getty"
	gxtime "github.com/dubbogo/gost/time"
	"go.uber.org/atomic"

	"github.com/seata/seata-go/pkg/protocol/codec"
	"github.com/seata/seata-go/pkg/protocol/message"
	"github.com/seata/seata-go/pkg/util/log"
)

var (
	gettyRemotingClient     *GettyRemotingClient
	onceGettyRemotingClient = &sync.Once{}
)

type GettyRemotingClient struct {
	idGenerator *atomic.Uint32
}

func GetGettyRemotingClient() *GettyRemotingClient {
	if gettyRemotingClient == nil {
		onceGettyRemotingClient.Do(func() {
			gettyRemotingClient = &GettyRemotingClient{
				idGenerator: &atomic.Uint32{},
			}
		})
	}
	return gettyRemotingClient
}

func (client *GettyRemotingClient) SendAsyncRequest(msg interface{}) error {
	return client.sendAsyncRequest(msg, nil)
}

// SendAsyncRequestToSession sends a request through the specified Seata Server
// session. It is used for registration messages that must not be load balanced.
func (client *GettyRemotingClient) SendAsyncRequestToSession(msg interface{}, session dubboGetty.Session) error {
	return client.sendAsyncRequest(msg, session)
}

func (client *GettyRemotingClient) sendAsyncRequest(msg interface{}, session dubboGetty.Session) error {
	var msgType message.GettyRequestType
	if _, ok := msg.(message.HeartBeatMessage); ok {
		msgType = message.GettyRequestTypeHeartbeatRequest
	} else {
		msgType = message.GettyRequestTypeRequestOneway
	}
	rpcMessage := message.RpcMessage{
		ID:         int32(client.idGenerator.Inc()),
		Type:       msgType,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return GetGettyRemotingInstance().SendASync(rpcMessage, session, client.asyncCallback)
}

func (client *GettyRemotingClient) SendAsyncResponse(msgID int32, msg interface{}) error {
	rpcMessage := message.RpcMessage{
		ID:         msgID,
		Type:       message.GettyRequestTypeResponse,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return GetGettyRemotingInstance().SendASync(rpcMessage, nil, nil)
}

func (client *GettyRemotingClient) SendSyncRequest(msg interface{}) (interface{}, error) {
	return client.sendSyncRequest(msg, nil)
}

// SendSyncRequestToSession sends a request through the specified Seata Server
// session instead of selecting one through the load balancer.
func (client *GettyRemotingClient) SendSyncRequestToSession(msg interface{}, session dubboGetty.Session) (interface{}, error) {
	return client.sendSyncRequest(msg, session)
}

func (client *GettyRemotingClient) sendSyncRequest(msg interface{}, session dubboGetty.Session) (interface{}, error) {
	rpcMessage := message.RpcMessage{
		ID:         int32(client.idGenerator.Inc()),
		Type:       message.GettyRequestTypeRequestSync,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return GetGettyRemotingInstance().SendSync(rpcMessage, session, client.syncCallback)
}

func (g *GettyRemotingClient) asyncCallback(reqMsg message.RpcMessage, respMsg *message.MessageFuture) (interface{}, error) {
	go g.syncCallback(reqMsg, respMsg)
	return nil, nil
}

func (g *GettyRemotingClient) syncCallback(reqMsg message.RpcMessage, respMsg *message.MessageFuture) (interface{}, error) {
	select {
	case <-gxtime.GetDefaultTimerWheel().After(RpcRequestTimeout):
		GetGettyRemotingInstance().RemoveMergedMessageFuture(reqMsg.ID)
		log.Errorf("wait resp timeout: %#v", reqMsg)
		return nil, fmt.Errorf("wait response timeout, request: %#v", reqMsg)
	case <-respMsg.Done:
		return respMsg.Response, respMsg.Err
	}
}
