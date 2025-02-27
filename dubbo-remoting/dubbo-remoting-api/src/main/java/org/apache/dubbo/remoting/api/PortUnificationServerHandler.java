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
package org.apache.dubbo.remoting.api;

import org.apache.dubbo.common.URL;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.List;

/**
 * 端口统一服务器handler
 */
public class PortUnificationServerHandler extends ByteToMessageDecoder {

    private final SslContext sslCtx;
    private final URL url;
    private final List<WireProtocol> protocols;
    private final DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public PortUnificationServerHandler(URL url, List<WireProtocol> protocols) {
        this(url, null,protocols);
    }

    public PortUnificationServerHandler(URL url, SslContext sslCtx, List<WireProtocol> protocols) {
        this.url = url;
        this.sslCtx = sslCtx;
        this.protocols = protocols;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    public DefaultChannelGroup getChannels() {
        return channels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        channels.add(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        channels.remove(ctx.channel());
    }

    /**
     * 服务端解码
     * @param ctx
     * @param in
     * @param out
     * @throws Exception
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first five bytes to detect a protocol. 使用前五个字节检测协议
        if (in.readableBytes() < 5) {
            return;
        }

        for (final WireProtocol protocol : protocols) {
            in.markReaderIndex();
            final ProtocolDetector.Result result = protocol.detector().detect(ctx, in);
            in.resetReaderIndex();
            switch (result) {
                case UNRECOGNIZED:
                    continue;
                case RECOGNIZED:
                    //配置服务端pipeline
                    protocol.configServerPipeline(url, ctx.pipeline(), sslCtx);
                    //配置完成之后pipeline移除当前处理类
                    ctx.pipeline().remove(this);
                case NEED_MORE_DATA:
                    return;
                default:
                    return;
            }
        }
        // Unknown protocol; discard everything and close the connection.
        in.clear();
        ctx.close();
    }

}
