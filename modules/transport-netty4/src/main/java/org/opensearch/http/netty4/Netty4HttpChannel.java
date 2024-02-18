/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpResponse;
import org.opensearch.transport.netty4.Netty4TcpChannel;

import java.net.InetSocketAddress;
import java.util.Optional;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

public class Netty4HttpChannel implements HttpChannel {

    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final ChannelPipeline inboundPipeline;

    Netty4HttpChannel(Channel channel) {
        this(channel, null);
    }

    Netty4HttpChannel(Channel channel, ChannelPipeline inboundPipeline) {
        this.channel = channel;
        this.inboundPipeline = inboundPipeline;
        Netty4TcpChannel.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        channel.writeAndFlush(response, Netty4TcpChannel.addPromise(listener, channel));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() {
        channel.close();
    }

    public @Nullable ChannelPipeline inboundPipeline() {
        return inboundPipeline;
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        Object handler = getNettyChannel().pipeline().get(name);

        if (handler == null && inboundPipeline() != null) {
            handler = inboundPipeline().get(name);
        }

        if (handler != null && clazz.isInstance(handler) == true) {
            return Optional.of((T) handler);
        }

        return Optional.empty();
    }

    @Override
    public String toString() {
        return "Netty4HttpChannel{" + "localAddress=" + getLocalAddress() + ", remoteAddress=" + getRemoteAddress() + '}';
    }
}
