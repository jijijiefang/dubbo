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
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;

/**
 * ConsistentHashLoadBalance
 * 一致性Hash负载均衡
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "consistenthash";
    /**
     * Hash nodes name
     */
    public static final String HASH_NODES = "hash.nodes";
    /**
     * Hash arguments name
     */
    public static final String HASH_ARGUMENTS = "hash.arguments";
    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();
    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String methodName = RpcUtils.getMethodName(invocation);
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        // using the hashcode of list to compute the hash only pay attention to the elements in the list 使用list的hashcode来计算hash，只需注意列表中的元素
        int invokersHashCode = getCorrespondingHashCode(invokers);
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != invokersHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, invokersHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        //使用一致性Hash选择器选择
        return selector.select(invocation);
    }

    /**
     * get hash code of invokers
     * Make this method to public in order to use this method in test case
     * @param invokers
     * @return
     */
    public <T> int getCorrespondingHashCode(List<Invoker<T>> invokers){
        return invokers.hashCode();
    }

    /**
     * 一致性Hash选择器
     * @param <T>
     */
    private static final class ConsistentHashSelector<T> {
        //虚拟Invoker
        private final TreeMap<Long, Invoker<T>> virtualInvokers;
        private final int replicaNumber;
        private final int identityHashCode;

        private final int[] argumentIndex;

        /**
         * key: server(invoker) address 服务Invoker地址
         * value: count of requests accept by certain server 特定服务器接受的请求数
         */
        private Map<String, AtomicLong> serverRequestCountMap = new ConcurrentHashMap<>();

        /**
         * count of total requests accept by all servers
         * 所有服务器接受的请求总数
         */
        private AtomicLong totalRequestCount;

        /**
         * count of current servers(invokers)
         * 当前服务器（Invoker）计数
         */
        private int serverCount;

        /**
         * the ratio which allow count of requests accept by each server
         * overrate average (totalRequestCount/serverCount).
         * 1.5 is recommended, in the future we can make this param configurable
         * 允许每个服务器接受的请求数超过平均值的比率（totalRequestCount/serverCount）。建议使用1.5，将来我们可以配置此参数
         */
        private static final double OVERLOAD_RATIO_THREAD = 1.5F;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            //获取Hash环的节点数量，默认为160个
            this.replicaNumber = url.getMethodParameter(methodName, HASH_NODES, 160);
            String[] index = COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, HASH_ARGUMENTS, "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    byte[] digest = Bytes.getMD5(address + i);
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        //虚拟节点放入TreeMap
                        virtualInvokers.put(m, invoker);
                    }
                }
            }

            totalRequestCount = new AtomicLong(0);
            serverCount = invokers.size();
            serverRequestCountMap.clear();
        }

        /**
         * 选择
         * @param invocation
         * @return
         */
        public Invoker<T> select(Invocation invocation) {
            String key = toKey(invocation.getArguments());
            byte[] digest = Bytes.getMD5(key);
            return selectForKey(hash(digest, 0));
        }
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        /**
         * 根据
         * @param hash
         * @return
         */
        private Invoker<T> selectForKey(long hash) {
            //返回与大于或等于给定键的最小键关联的键值映射，如果没有这样的键，则返回null
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);
            if (entry == null) {
                //获取第一个节点
                entry = virtualInvokers.firstEntry();
            }

            String serverAddress = entry.getValue().getUrl().getAddress();

            /**
             * The following part of codes aims to select suitable invoker.以下代码的目的是选择合适的调用者。
             * This part is not complete thread safety.这部分是不完全线程安全。
             * However, in the scene of consumer-side load balance,然而，在消费者端负载平衡的场景中，
             * thread race for this part of codes 这部分代码的线程竞争
             * (execution time cost for this part of codes without any IO or (这部分代码的执行时间成本没有任何IO或网络操作非常低)很少会发生。
             * network operation is very low) will rarely occur. And even in 甚至在极端情况下，一些请求被分配给调用者高于OVERLOAD_RATIO_THREAD将不会产生显著影响这个新算法的效果。
             * extreme case, a few requests are assigned to an invoker which
             * is above OVERLOAD_RATIO_THREAD will not make a significant impact
             * on the effect of this new algorithm.
             * And make this part of codes synchronized will reduce efficiency of 将这部分代码同步会降低效率每一个要求。在我看来，这不值得。所以它不是这个部分的问题是不完全线程安全。
             * every request. In my opinion, this is not worth. So it is not a
             * problem for this part is not complete thread safety.
             */
            //(总请求数/当前服务请求数)*1.5
            double overloadThread = ((double) totalRequestCount.get() / (double) serverCount) * OVERLOAD_RATIO_THREAD;
            /**
             * Find a valid server node: 查找有效的服务器节点：
             * 1. Not have accept request yet 还没有接受请求
             * or
             * 2. Not have overloaded (request count already accept < thread (average request count * overloadRatioAllowed ))
             * 没有超过负载（请求计数已接受<线程（平均请求计数重载允许））
             */
            while (serverRequestCountMap.containsKey(serverAddress)
                && serverRequestCountMap.get(serverAddress).get() >= overloadThread) {
                /**
                 * If server node is not valid, get next node
                 * 如果节点的服务不可用，获取下一个节点
                 */
                entry = getNextInvokerNode(virtualInvokers, entry);
                serverAddress = entry.getValue().getUrl().getAddress();
            }
            if (!serverRequestCountMap.containsKey(serverAddress)) {
                serverRequestCountMap.put(serverAddress, new AtomicLong(1));
            } else {
                serverRequestCountMap.get(serverAddress).incrementAndGet();
            }
            totalRequestCount.incrementAndGet();

            return entry.getValue();
        }

        /**
         * 获取下一个Invoker节点
         * @param virtualInvokers
         * @param entry
         * @return
         */
        private Map.Entry<Long, Invoker<T>> getNextInvokerNode(TreeMap<Long, Invoker<T>> virtualInvokers, Map.Entry<Long, Invoker<T>> entry){
            //返回与严格大于给定键的最小键关联的键值映射，如果没有这样的键，则返回null
            Map.Entry<Long, Invoker<T>> nextEntry = virtualInvokers.higherEntry(entry.getKey());
            if(nextEntry == null){
                return virtualInvokers.firstEntry();
            }
            return nextEntry;
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }
    }

}
