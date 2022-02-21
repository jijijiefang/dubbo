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
package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.api.Http2WireProtocol;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.ssl.SslContext;

import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_ENABLE_PUSH_KEY;
import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_HEADER_TABLE_SIZE_KEY;
import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_INITIAL_WINDOW_SIZE_KEY;
import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_MAX_CONCURRENT_STREAMS_KEY;
import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_MAX_FRAME_SIZE_KEY;
import static org.apache.dubbo.rpc.Constants.H2_SETTINGS_MAX_HEADER_LIST_SIZE_KEY;

/**
 * Triple Http2协议
 */
@Activate
public class TripleHttp2Protocol extends Http2WireProtocol implements ScopeModelAware {
    private FrameworkModel frameworkModel;
    private ApplicationModel applicationModel;

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
    }

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    @Override
    public void close() {
        super.close();
    }

    /**
     * 配置服务端pipeline
     * @param url
     * @param pipeline
     * @param sslContext
     */
    @Override
    public void configServerPipeline(URL url, ChannelPipeline pipeline, SslContext sslContext) {
        final Configuration config = ConfigurationUtils.getGlobalConfiguration(applicationModel);
        //http2帧编解码器
        final Http2FrameCodec codec = Http2FrameCodecBuilder.forServer()
            .gracefulShutdownTimeoutMillis(10000)//正常关机超时毫秒数
            .initialSettings(new Http2Settings()//初始化设置
                .headerTableSize(config.getInt(H2_SETTINGS_HEADER_TABLE_SIZE_KEY, 4096))//"dubbo.rpc.tri.header-table-size"
                .maxConcurrentStreams(config.getInt(H2_SETTINGS_MAX_CONCURRENT_STREAMS_KEY, Integer.MAX_VALUE))//"dubbo.rpc.tri.max-concurrent-streams"
                .initialWindowSize(config.getInt(H2_SETTINGS_INITIAL_WINDOW_SIZE_KEY, 1 << 20))//"dubbo.rpc.tri.initial-window-size"
                .maxFrameSize(config.getInt(H2_SETTINGS_MAX_FRAME_SIZE_KEY, 2 << 14))//"dubbo.rpc.tri.max-frame-size"
                .maxHeaderListSize(config.getInt(H2_SETTINGS_MAX_HEADER_LIST_SIZE_KEY, 8192)))//"dubbo.rpc.tri.max-header-list-size"
            .frameLogger(SERVER_LOGGER)
            .build();
        final Http2MultiplexHandler handler = new Http2MultiplexHandler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) {
                final ChannelPipeline p = ch.pipeline();
                p.addLast(new TripleCommandOutBoundHandler());//Triple命令输出处理类
                p.addLast(new TripleHttp2FrameServerHandler(frameworkModel));//TripleHttp2帧服务端处理类
                // TODO constraint MAX DATA_SIZE
                p.addLast(new GrpcDataDecoder(Integer.MAX_VALUE, false));//gRPC数据解码器
                p.addLast(new TripleServerInboundHandler());
            }
        });
        pipeline.addLast(codec, new TripleServerConnectionHandler(), handler);
    }

    /**
     * 配置客户端pipeline
     * @param url
     * @param pipeline
     * @param sslContext
     */
    @Override
    public void configClientPipeline(URL url, ChannelPipeline pipeline, SslContext sslContext) {
        final Configuration config = ConfigurationUtils.getGlobalConfiguration(applicationModel);
        final Http2FrameCodec codec = Http2FrameCodecBuilder.forClient()
            .gracefulShutdownTimeoutMillis(10000)//正常关机超时毫秒数
            .initialSettings(new Http2Settings()//初始化设置
                .headerTableSize(config.getInt(H2_SETTINGS_HEADER_TABLE_SIZE_KEY, 4096))//"dubbo.rpc.tri.header-table-size"
                .pushEnabled(config.getBoolean(H2_SETTINGS_ENABLE_PUSH_KEY, false))//"dubbo.rpc.tri.enable-push"
                .maxConcurrentStreams(config.getInt(H2_SETTINGS_MAX_CONCURRENT_STREAMS_KEY, Integer.MAX_VALUE))//"dubbo.rpc.tri.max-concurrent-streams"
                .initialWindowSize(config.getInt(H2_SETTINGS_INITIAL_WINDOW_SIZE_KEY, 1 << 20))//"dubbo.rpc.tri.initial-window-size"
                .maxFrameSize(config.getInt(H2_SETTINGS_MAX_FRAME_SIZE_KEY, 2 << 14))//"dubbo.rpc.tri.max-frame-size"
                .maxHeaderListSize(config.getInt(H2_SETTINGS_MAX_HEADER_LIST_SIZE_KEY, 8192)))//"dubbo.rpc.tri.max-header-list-size"
            .frameLogger(CLIENT_LOGGER)
            .build();
        final Http2MultiplexHandler handler = new Http2MultiplexHandler(new TripleClientHandler(frameworkModel));
        pipeline.addLast(codec, handler, new TripleClientRequestHandler(frameworkModel));
    }
}
