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

package discovery

import (
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type fakeNacosNamingClient struct {
	subscribeParam *vo.SubscribeParam
	unsubscribed   bool
}

func (f *fakeNacosNamingClient) SelectInstances(vo.SelectInstancesParam) ([]model.Instance, error) {
	return []model.Instance{{
		Ip:      "127.0.0.1",
		Port:    8091,
		Weight:  1,
		Healthy: true,
		Enable:  true,
	}}, nil
}

func (f *fakeNacosNamingClient) Subscribe(param *vo.SubscribeParam) error {
	f.subscribeParam = param
	return nil
}

func (f *fakeNacosNamingClient) Unsubscribe(*vo.SubscribeParam) error {
	f.unsubscribed = true
	return nil
}

func (f *fakeNacosNamingClient) CloseClient() {}

func TestNacosRegistryServiceSubscribePublishesAddressChanges(t *testing.T) {
	client := &fakeNacosNamingClient{}
	service := &NacosRegistryService{
		client: client,
		config: NacosConfig{
			Application: "seata-server",
			Group:       "SEATA_GROUP",
			Cluster:     "default",
		},
	}
	events := make(chan RegistryChangeEvent, 2)
	subscription, err := service.Subscribe("default_tx_group", func(event RegistryChangeEvent) {
		events <- event
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-events:
		if event.Key != "default_tx_group" || event.Instances[0].Addr != "127.0.0.1" {
			t.Fatalf("unexpected initial event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial Nacos snapshot")
	}

	client.subscribeParam.SubscribeCallback([]model.Instance{{
		Ip:      "127.0.0.2",
		Port:    8091,
		Weight:  1,
		Healthy: true,
		Enable:  true,
	}}, nil)
	select {
	case event := <-events:
		if event.Instances[0].Addr != "127.0.0.2" {
			t.Fatalf("unexpected changed event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for changed Nacos snapshot")
	}

	client.subscribeParam.SubscribeCallback(nil, nil)
	select {
	case event := <-events:
		if len(event.Instances) != 0 {
			t.Fatalf("expected empty snapshot during server restart, got: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for empty Nacos snapshot")
	}

	subscription.Unsubscribe()
	if !client.unsubscribed {
		t.Fatal("Nacos subscription was not unsubscribed")
	}
}
