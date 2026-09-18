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
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type NacosRegistryService struct {
	client        naming_client.INamingClient
	config        NacosConfig
	serviceConfig *ServiceConfig
}

func newNacosRegistryService(serviceConfig *ServiceConfig, config *NacosConfig) (RegistryService, error) {
	if config == nil {
		return nil, fmt.Errorf("nacos config is nil")
	}

	serverAddr, port, err := parseNacosServerAddr(config.ServerAddr)
	if err != nil {
		return nil, err
	}

	clientOptions := []constant.ClientOption{
		constant.WithNamespaceId(config.Namespace),
		constant.WithNotLoadCacheAtStart(true),
	}
	if config.Username != "" {
		clientOptions = append(clientOptions, constant.WithUsername(config.Username))
	}
	if config.Password != "" {
		clientOptions = append(clientOptions, constant.WithPassword(config.Password))
	}
	if config.AccessKey != "" {
		clientOptions = append(clientOptions, constant.WithAccessKey(config.AccessKey))
	}
	if config.SecretKey != "" {
		clientOptions = append(clientOptions, constant.WithSecretKey(config.SecretKey))
	}

	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: constant.NewClientConfig(clientOptions...),
		ServerConfigs: []constant.ServerConfig{
			*constant.NewServerConfig(serverAddr, port, constant.WithContextPath("/nacos")),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos naming client: %w", err)
	}

	return &NacosRegistryService{
		client:        client,
		config:        *config,
		serviceConfig: serviceConfig,
	}, nil
}

func (s *NacosRegistryService) Lookup(key string) ([]*ServiceInstance, error) {
	serviceName := s.config.Application
	if serviceName == "" {
		serviceName = "seata-server"
	}
	groupName := s.config.Group
	if groupName == "" {
		groupName = "SEATA_GROUP"
	}

	cluster := s.config.Cluster
	if s.serviceConfig != nil {
		if mappedCluster, ok := s.serviceConfig.VgroupMapping[key]; ok && mappedCluster != "" {
			cluster = mappedCluster
		}
	}
	if cluster == "" {
		cluster = "default"
	}
	clusters := []string(nil)
	if cluster != "" {
		clusters = []string{cluster}
	}

	instances, err := s.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
		HealthyOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup nacos service %q: %w", serviceName, err)
	}

	result := make([]*ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		if instance.Ip == "" || instance.Port == 0 {
			continue
		}
		if instance.Port > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("nacos service %q returned invalid port %d", serviceName, instance.Port)
		}
		result = append(result, &ServiceInstance{
			Addr: instance.Ip,
			Port: int(instance.Port),
		})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("nacos service %q has no healthy instances", serviceName)
	}
	return result, nil
}

func (s *NacosRegistryService) Close() {
	if s.client != nil {
		s.client.CloseClient()
	}
}

func parseNacosServerAddr(serverAddr string) (string, uint64, error) {
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return "", 0, fmt.Errorf("nacos server address is empty")
	}
	if !strings.Contains(serverAddr, "://") {
		serverAddr = "nacos://" + serverAddr
	}

	parsed, err := url.Parse(serverAddr)
	if err != nil {
		return "", 0, fmt.Errorf("parse nacos server address %q: %w", serverAddr, err)
	}
	host := parsed.Hostname()
	if host == "" {
		return "", 0, fmt.Errorf("nacos server address %q has no host", serverAddr)
	}

	port := uint64(8848)
	if portText := parsed.Port(); portText != "" {
		port, err = strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return "", 0, fmt.Errorf("invalid nacos server port %q", portText)
		}
	}
	return host, port, nil
}
