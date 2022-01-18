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
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ShortestResponseLoadBalance
 * 短响应负载均衡
 * </p>
 * Filter the number of invokers with the shortest response time of 筛选成功调用响应时间最短的调用程序的数量，
 * success calls and count the weights and quantities of these invokers in last slide window.并在最后一个滑动窗口中计算这些调用程序的权重和数量。
 * If there is only one invoker, use the invoker directly; 如果只有一个调用程序，则直接使用调用程序；
 * if there are multiple invokers and the weights are not the same, then random according to the total weight; 如果有多个调用程序，且权重不相同，则根据总权重随机；
 * if there are multiple invokers and the same weight, then randomly called. 如果有多个调用程序且权重相同，则随机调用。
 */
public class ShortestResponseLoadBalance extends AbstractLoadBalance implements ScopeModelAware {

    public static final String NAME = "shortestresponse";

    private int slidePeriod = 30_000;
    //每个接口的方法都有一个滑动窗口
    private ConcurrentMap<RpcStatus, SlideWindowData> methodMap = new ConcurrentHashMap<>();

    private AtomicBoolean onResetSlideWindow = new AtomicBoolean(false);

    private volatile long lastUpdateTime = System.currentTimeMillis();

    private ExecutorService executorService;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        slidePeriod = applicationModel.getModelEnvironment().getConfiguration().getInt(Constants.SHORTEST_RESPONSE_SLIDE_PERIOD, 30_000);
        executorService = applicationModel.getApplicationExecutorRepository().getSharedExecutor();
    }

    /**
     * 滑动窗口数据
     */
    protected static class SlideWindowData {

        private long succeededOffset;
        private long succeededElapsedOffset;
        private RpcStatus rpcStatus;

        public SlideWindowData(RpcStatus rpcStatus) {
            this.rpcStatus = rpcStatus;
            this.succeededOffset = 0;
            this.succeededElapsedOffset = 0;
        }

        /**
         * 重置
         */
        public void reset() {
            this.succeededOffset = rpcStatus.getSucceeded();
            this.succeededElapsedOffset = rpcStatus.getSucceededElapsed();
        }

        /**
         * 获取成功的平均运行时间
         * @return
         */
        private long getSucceededAverageElapsed() {
            //成功数量
            long succeed = this.rpcStatus.getSucceeded() - this.succeededOffset;
            if (succeed == 0) {
                return 0;
            }
            //一个窗口期间内成功运行时间/成功数量
            return (this.rpcStatus.getSucceededElapsed() - this.succeededElapsedOffset) / succeed;
        }

        /**
         * 获取运行时间响应
         * @return
         */
        public long getEstimateResponse() {
            int active = this.rpcStatus.getActive() + 1;
            //平均成功时间*数量
            return getSucceededAverageElapsed() * active;
        }
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers Invoker数量
        int length = invokers.size();
        // Estimated shortest response time of all invokers 所有Invoker的估计最短响应时间
        long shortestResponse = Long.MAX_VALUE;
        // The number of invokers having the same estimated shortest response time 具有相同估计最短响应时间的Invoker数量
        int shortestCount = 0;
        // The index of invokers having the same estimated shortest response time 具有相同估计最短响应时间的Invoker的索引
        int[] shortestIndexes = new int[length];
        // the weight of every invokers 每个Invoker的权重
        int[] weights = new int[length];
        // The sum of the warmup weights of all the shortest response  invokers 所有最短响应调用程序的预热权重之和
        int totalWeight = 0;
        // The weight of the first shortest response invokers 第一个最短响应调用程序的权重
        int firstWeight = 0;
        // Every shortest response invoker has the same weight value? 每个最短响应调用程序都有相同的权重值
        boolean sameWeight = true;

        // Filter out all the shortest response invokers 筛选出所有最短响应Invoker
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
            SlideWindowData slideWindowData = methodMap.computeIfAbsent(rpcStatus, SlideWindowData::new);

            // Calculate the estimated response time from the product of active connections and succeeded average elapsed time. 根据活动连接和成功平均运行时间的乘积计算估计响应时间。
            long estimateResponse = slideWindowData.getEstimateResponse();
            //获取预热后的权重
            int afterWarmup = getWeight(invoker, invocation);
            weights[i] = afterWarmup;
            // Same as LeastActiveLoadBalance 跟最少调用负载均衡策略一样
            if (estimateResponse < shortestResponse) {//当前Invoker平均时间小于最小时间
                shortestResponse = estimateResponse;//当前Invoker平均时间赋值给最小时间
                shortestCount = 1;//最小时间数为1
                shortestIndexes[0] = i;//此索引放入最小响应索引数组
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            } else if (estimateResponse == shortestResponse) {
                shortestIndexes[shortestCount++] = i;
                totalWeight += afterWarmup;
                if (sameWeight && i > 0
                    && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        //每30秒重置下滑动窗口
        if (System.currentTimeMillis() - lastUpdateTime > slidePeriod
            && onResetSlideWindow.compareAndSet(false, true)) {
            //reset slideWindowData in async way 使用异步方式重置滑动窗口
            executorService.execute(() -> {
                methodMap.values().forEach(SlideWindowData::reset);
                lastUpdateTime = System.currentTimeMillis();
                onResetSlideWindow.set(false);
            });
        }

        if (shortestCount == 1) {
            return invokers.get(shortestIndexes[0]);
        }
        //没有同样权重且总权重大于0
        if (!sameWeight && totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < shortestCount; i++) {
                int shortestIndex = shortestIndexes[i];
                //总权重-某个Invoker权重后小于0返回
                offsetWeight -= weights[shortestIndex];
                if (offsetWeight < 0) {
                    return invokers.get(shortestIndex);
                }
            }
        }
        //随机获取
        return invokers.get(shortestIndexes[ThreadLocalRandom.current().nextInt(shortestCount)]);
    }
}
