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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * This class select one provider from multiple providers randomly.类从多个Invoker中随机选择一个Invoker。
 * You can define weights for each provider:您可以为每个提供者定义权重：
 * If the weights are all the same then it will use random.nextInt(number of invokers).如果权重都相同，则它将使用“随机”.nextInt（Invoker的数量）。
 * If the weights are different then it will use random.nextInt(w1 + w2 + ... + wn) 如果权重不同，则将使用“随机”.weight.nextInt（w1+w2+…+wn）
 * Note that if the performance of the machine is better than others, you can set a larger 注意，如果机器的性能优于其他机器，则可以设置更大的权重。
 * If the performance is not so good, you can set a smaller weight.如果性能不太好，可以设置较小的权重。
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * Select one invoker between a list using a random criteria
     * @param invokers List of possible invokers
     * @param url URL
     * @param invocation Invocation
     * @param <T>
     * @return The selected invoker
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers Invoker数量
        int length = invokers.size();
        //不需要权重负载均衡，轮询返还
        if (!needWeightLoadBalance(invokers,invocation)){
            return invokers.get(ThreadLocalRandom.current().nextInt(length));
        }

        // Every invoker has the same weight? 每个Invoker拥有相同权重
        boolean sameWeight = true;
        // the maxWeight of every invokers, the minWeight = 0 or the maxWeight of the last invoker 每个Invoker的最高权重，最小权重等于0或最大权重
        int[] weights = new int[length];
        // The sum of weights 权重总值
        int totalWeight = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            // Sum
            totalWeight += weight;
            // save for later use 保存一会使用
            weights[i] = totalWeight;
            if (sameWeight && totalWeight != weight * (i + 1)) {
                sameWeight = false;
            }
        }
        //如果有三个Invoker，则weights数组是这样的{100,200,300}
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.如果（不是每个调用程序都有相同的权重&至少有一个调用程序的权重>0），则根据totalWeight随机选择。
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.基于随机数返回一个Invoker
            for (int i = 0; i < length; i++) {
                //根据totalWeight产生随机数，随机数小于哪个Invoker的权重则返回
                if (offset < weights[i]) {
                    return invokers.get(i);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.如果所有Invoker都是相同的权重值或totalWeight=0，轮询返还
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

    /**
     * 需要权重负载均衡
     * @param invokers
     * @param invocation
     * @param <T>
     * @return
     */
    private <T> boolean needWeightLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {

        Invoker invoker = invokers.get(0);
        URL invokerUrl = invoker.getUrl();
        // Multiple registry scenario, load balance among multiple registries.多注册表场景，多个注册表之间的负载均衡
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(invokerUrl.getServiceInterface())) {
            String weight = invokerUrl.getParameter(REGISTRY_KEY + "." + WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                return true;
            }
        } else {
            String weight = invokerUrl.getMethodParameter(invocation.getMethodName(), WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                return true;
            }else {
                String timeStamp = invoker.getUrl().getParameter(TIMESTAMP_KEY);
                if (StringUtils.isNotEmpty(timeStamp)) {
                    return true;
                }
            }
        }
        return false;
    }


}
