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
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * 最少活跃负载均衡
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.筛选活动调用数最少的调用程序数，并计算这些调用程序的权重和数量。
 * If there is only one invoker, use the invoker directly;如果只有一个调用程序，则直接使用调用程序；
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;如果有多个调用程序，且权重不相同，则根据总权重随机；
 * if there are multiple invokers and the same weight, then randomly called.如果有多个调用程序且权重相同，则随机调用。
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers Invoker数量
        int length = invokers.size();
        // The least active value of all invokers Invoker中最小的活动值
        int leastActive = -1;
        // The number of invokers having the same least active value (leastActive) 具有相同最小活动值（leastractive）的Invoker数量
        int leastCount = 0;
        // The index of invokers having the same least active value (leastActive) 具有相同最小活动值（leastractive）的Invoker索引
        int[] leastIndexes = new int[length];
        // the weight of every invokers 每个Invoker的权重
        int[] weights = new int[length];
        // The sum of the warmup weights of all the least active invokers 所有活动最少的Invoker的预热权重之和
        int totalWeight = 0;
        // The weight of the first least active invoker 第一个活动最少的Invoker的权重
        int firstWeight = 0;
        // Every least active invoker has the same weight value? 每个最不活跃的Invoker都有相同的权重值
        boolean sameWeight = true;


        // Filter out all the least active invokers 过滤掉所有活动最少的Invoker
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // Get the active number of the invoker 获取Invoker的活跃数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // Get the weight of the invoker's configuration. The default value is 100.获取Invoker配置的权重，默认值为100
            int afterWarmup = getWeight(invoker, invocation);
            // save for later use 保存一会使用
            weights[i] = afterWarmup;
            // If it is the first invoker or the active number of the invoker is less than the current least active number 如果是第一个调用程序，或者调用程序的活动编号小于当前最小活动编号
            if (leastActive == -1 || active < leastActive) {
                // Reset the active number of the current invoker to the least active number 将当前调用程序的活动编号重置为最小活动编号
                leastActive = active;
                // Reset the number of least active invokers 重置活动最少的调用程序数
                leastCount = 1;
                // Put the first least active invoker first in leastIndexes 将活动最少的调用程序放在leastIndexes的第一位
                leastIndexes[0] = i;
                // Reset totalWeight 重置总权重
                totalWeight = afterWarmup;
                // Record the weight the first least active invoker 记录最不活跃调用程序的权重
                firstWeight = afterWarmup;
                // Each invoke has the same weight (only one invoker here) 每个调用具有相同的权重（此处只有一个调用程序）
                sameWeight = true;
                // If current invoker's active value equals with leaseActive, then accumulating.如果当前调用程序的活动值等于leaseActive，则累加。
            } else if (active == leastActive) {
                // Record the index of the least active invoker in leastIndexes order 按最小索引顺序记录活动最少的调用程序的索引
                leastIndexes[leastCount++] = i;
                // Accumulate the total weight of the least active invoker 累积活动最少的调用程序的总权重
                totalWeight += afterWarmup;
                // If every invoker has the same weight? 如果每个调用程序都有相同的权重？
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // Choose an invoker from all the least active invokers 从所有最不活跃的调用程序中选择一个调用程序
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly. 如果只有一个调用程序具有最少的活动值，则直接返回该调用程序。
            return invokers.get(leastIndexes[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight. 如果（不是每个调用程序都有相同的权重&至少有一个调用程序的权重>0），则根据totalWeight随机选择。
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.基于随机值返回调用程序。
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.如果所有调用程序都具有相同的权重值或totalWeight=0，则均匀返回。
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
