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
package org.apache.dubbo.rpc.cluster.support.registry;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.PREFERRED_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_ZONE;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_ZONE_FORCE;
import static org.apache.dubbo.common.constants.RegistryConstants.ZONE_KEY;

/**
 * When there're more than one registry for subscription.
 * 当有多个注册表可供订阅时。
 * <p>
 * This extension provides a strategy to decide how to distribute traffics among them: 这个扩展提供了一个策略来决定如何在它们之间分配流量：
 * 1. registry marked as 'preferred=true' has the highest priority. 标记为“preferred=true”的注册表具有最高优先级
 * 2. check the zone the current request belongs, pick the registry that has the same zone first. 检查当前请求所属的区域，首先选择具有相同区域的注册表
 * 3. Evenly balance traffic between all registries based on each registry's weight.根据每个注册表的权重均衡所有注册表之间的流量
 * 4. Pick anyone that's available.挑选任何可用的
 */
public class ZoneAwareClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(ZoneAwareClusterInvoker.class);

    public ZoneAwareClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        // First, pick the invoker (XXXClusterInvoker) that comes from the local registry, distinguish by a 'preferred' key.首先，选择Invoker使用本地注册表，通过'preferred'键来区分
        for (Invoker<T> invoker : invokers) {
            ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>) invoker;
            if (clusterInvoker.isAvailable() && clusterInvoker.getRegistryUrl()
                    .getParameter(PREFERRED_KEY, false)) {
                return clusterInvoker.invoke(invocation);
            }
        }

        // providers in the registry with the same zone 注册表中同一个区域的提供者
        String zone = invocation.getAttachment(REGISTRY_ZONE);
        if (StringUtils.isNotEmpty(zone)) {
            for (Invoker<T> invoker : invokers) {
                ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>) invoker;
                //同一个区域
                if (clusterInvoker.isAvailable() && zone.equals(clusterInvoker.getRegistryUrl().getParameter(ZONE_KEY))) {
                    return clusterInvoker.invoke(invocation);
                }
            }
            String force = invocation.getAttachment(REGISTRY_ZONE_FORCE);
            if (StringUtils.isNotEmpty(force) && "true".equalsIgnoreCase(force)) {
                throw new IllegalStateException("No registry instance in zone or no available providers in the registry, zone: "
                        + zone
                        + ", registries: " + invokers.stream().map(invoker -> ((ClusterInvoker<T>) invoker).getRegistryUrl().toString()).collect(Collectors.joining(",")));
            }
        }


        // load balance among all registries, with registry weight count in. 所有注册表之间的负载平衡，使用注册表权重计数
        Invoker<T> balancedInvoker = select(loadbalance, invocation, invokers, null);
        if (balancedInvoker.isAvailable()) {
            return balancedInvoker.invoke(invocation);
        }

        // If none of the invokers has a preferred signal or is picked by the loadbalancer, pick the first one available. 如果调用程序中没有一个具有首选信号或由负载均衡选择，选择第一个可用的调用
        for (Invoker<T> invoker : invokers) {
            ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>) invoker;
            if (clusterInvoker.isAvailable()) {
                return clusterInvoker.invoke(invocation);
            }
        }

        //if none available,just pick one 如果没有可用的，选择第一个
        return invokers.get(0).invoke(invocation);
    }

}
