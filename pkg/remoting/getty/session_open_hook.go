/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

	dubboGetty "github.com/apache/dubbo-getty"
)

var (
	sessionOpenHookMu sync.RWMutex
	sessionOpenHook   func(dubboGetty.Session) error
)

// SetSessionOpenHook registers work that must run after a new Getty session
// is opened. The RM package uses this to re-register cached resources without
// making the remoting package depend on the RM package.
func SetSessionOpenHook(hook func(dubboGetty.Session) error) {
	sessionOpenHookMu.Lock()
	sessionOpenHook = hook
	sessionOpenHookMu.Unlock()
}

func runSessionOpenHook(session dubboGetty.Session) error {
	sessionOpenHookMu.RLock()
	hook := sessionOpenHook
	sessionOpenHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(session)
}
