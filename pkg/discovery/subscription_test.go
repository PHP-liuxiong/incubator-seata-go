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
)

func TestRegistryChangeSubscriptionPublishesInitialAndLatestSnapshot(t *testing.T) {
	events := make(chan RegistryChangeEvent, 2)
	subscription := newRegistryChangeSubscription(func(event RegistryChangeEvent) {
		events <- event
	})
	subscription.initialize(RegistryChangeEvent{
		Key:       "group-a",
		Instances: []*ServiceInstance{{Addr: "127.0.0.1", Port: 8091}},
	}, nil)
	subscription.start()

	select {
	case event := <-events:
		if event.Key != "group-a" || len(event.Instances) != 1 {
			t.Fatalf("unexpected initial event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial event")
	}

	subscription.publish(RegistryChangeEvent{Key: "group-a", Instances: []*ServiceInstance{{Addr: "127.0.0.2", Port: 8091}}})
	select {
	case event := <-events:
		if event.Instances[0].Addr != "127.0.0.2" {
			t.Fatalf("unexpected latest event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for latest event")
	}

	subscription.Unsubscribe()
	subscription.Unsubscribe()
	subscription.publish(RegistryChangeEvent{Key: "group-a", Instances: []*ServiceInstance{{Addr: "127.0.0.3", Port: 8091}}})
	select {
	case event := <-events:
		t.Fatalf("received event after unsubscribe: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}
}
