/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.async;

import static org.neo4j.driver.internal.async.connection.PandaChannelAttributes.setTerminationReason;
import static org.neo4j.driver.internal.async.connection.PandaChannelAttributes.poolId;

import io.grpc.ManagedChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.stub.StreamObserver;
import org.grapheco.pandadb.network.PandaQueryServiceGrpc;
import org.neo4j.driver.Query;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.PandaConverter;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.panda.PandaProtocol;
import org.neo4j.driver.internal.async.connection.PandaChannelAttributes;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.ServerVersion;

/**
 * This connection represents a simple network connection to a remote server. It wraps a channel obtained from a connection pool. The life cycle of this
 * connection start from the moment the channel is borrowed out of the pool and end at the time the connection is released back to the pool.
 */
public class PandaNetworkConnection implements Connection {
//    private final Logger log;
    private final ManagedChannel channel;
    private final PandaQueryServiceGrpc.PandaQueryServiceStub stub;
    private final String serverAgent;
    private final BoltServerAddress serverAddress;
    private final ServerVersion serverVersion;
    private final BoltProtocol protocol;
    private final ExecutorService pool;
    private final CompletableFuture<Void> releaseFuture;

    private final Clock clock;

    private final AtomicReference<Status> status = new AtomicReference<>(Status.OPEN);
    private final MetricsListener metricsListener;
    private final ListenerEvent inUseEvent;

    private final Long connectionReadTimeout;
//    private ChannelHandler connectionReadTimeoutHandler;

    public PandaNetworkConnection(
            ManagedChannel channel,
            ExecutorService pool,
            Clock clock,
            MetricsListener metricsListener,
            Logging logging
    ) {
        this.pool = pool;
//        this.log = logging.getLog(getClass());
        this.channel = channel;
        this.stub =  PandaQueryServiceGrpc.newStub(channel);
//        this.messageDispatcher = PandaChannelAttributes.messageDispatcher(channel);
        this.serverAgent = PandaChannelAttributes.serverAgent(channel);
        this.serverAddress = PandaChannelAttributes.serverAddress(channel);
        this.serverVersion = PandaChannelAttributes.serverVersion(channel);
        this.protocol = PandaProtocol.INSTANCE;
        this.releaseFuture = CompletableFuture.runAsync(() -> {}, pool);
        this.clock = clock;
        this.metricsListener = metricsListener;
        this.inUseEvent = metricsListener.createListenerEvent();
        this.connectionReadTimeout =
                PandaChannelAttributes.connectionReadTimeout(channel).orElse(null);
        metricsListener.afterConnectionCreated(poolId(this.channel), this.inUseEvent);
    }

    public ExecutorService getPool() {return this.pool;}

    @Override
    public boolean isOpen() {
        return status.get() == Status.OPEN;
    }

    @Override
    public void enableAutoRead() {
//        if (isOpen()) {
//            setAutoRead(true);
//        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableAutoRead() {
//        if (isOpen()) {
//            setAutoRead(false);
//        }

        throw new UnsupportedOperationException();
    }

//    public PandaResult query(Query query) {
//        var pq = PandaConverter.convertQuery(query);
//        return new PandaResult(stub.query(pq), pool);
//    }

    public void queryAsync(Query query, StreamObserver<org.grapheco.pandadb.network.Query.QueryResponse> so) {
        stub.query(PandaConverter.convertQuery(query), so);
    }

    @Override
    public void flush() {
//        if (verifyOpen(null, null)) {
//            flushInEventLoop();
//        }
    }

    @Override
    public void write(Message message, ResponseHandler handler) {
//        if (verifyOpen(handler, null)) {
//            writeMessageInEventLoop(message, handler, false);
//        }
    }

    @Override
    public void write(Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2) {
//        if (verifyOpen(handler1, handler2)) {
//            writeMessagesInEventLoop(message1, handler1, message2, handler2, false);
//        }
    }

    @Override
    public void writeAndFlush(Message message, ResponseHandler handler) {
//        if (verifyOpen(handler, null)) {
//            writeMessageInEventLoop(message, handler, true);
//        }
        // TODO this.stub.writeMessage();
    }

    @Override
    public void writeAndFlush(Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2) {
//        if (verifyOpen(handler1, handler2)) {
//            // TODO this.stub.writeMessagesInEventLoop(message1, handler1, message2, handler2, true);
//        }
    }

    @Override
    public CompletionStage<Void> reset() {
//        return release();
//        CompletableFuture<Void> result = new CompletableFuture<>();
//        ResetResponseHandler handler = new ResetResponseHandler(messageDispatcher, result);
//        writeResetMessageIfNeeded(handler, true);
//        return result;
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Void> release() {
        System.out.println("release pandanetworkconnection");
        if (status.compareAndSet(Status.OPEN, Status.RELEASED)) {
            channel.shutdown(); //TODO channel.writeAndFlush(ResetMessage.RESET)
            PandaChannelAttributes.purgeChannel(channel);
            metricsListener.afterConnectionReleased(poolId(this.channel), this.inUseEvent);
        }
//        pool.shutdownNow();
        return releaseFuture;
    }

    @Override
    public void terminateAndRelease(String reason) {
        if (status.compareAndSet(Status.OPEN, Status.TERMINATED)) {
            setTerminationReason(channel, reason);
            CompletableFuture.runAsync(() -> channel.shutdownNow(), pool)
                .exceptionally(throwable -> null)
                .whenComplete((ignored, throwable) -> {
                    PandaChannelAttributes.purgeChannel(channel);
                    releaseFuture.complete(null);
                    metricsListener.afterConnectionReleased(poolId(this.channel), this.inUseEvent);
                });
        }
    }

    @Override
    public String serverAgent() {
        return serverAgent;
    }

    @Override
    public BoltServerAddress serverAddress() {
        return serverAddress;
    }

    @Override
    public ServerVersion serverVersion() {
        return serverVersion;
    }

    @Override
    public BoltProtocol protocol() {
        return protocol;
    }

//    private void writeResetMessageIfNeeded(ResponseHandler resetHandler, boolean isSessionReset) {
//        channel.eventLoop().execute(() -> {
//            if (isSessionReset && !isOpen()) {
//                resetHandler.onSuccess(emptyMap());
//            } else {
//                // auto-read could've been disabled, re-enable it to automatically receive response for RESET
//                setAutoRead(true);
//
//                messageDispatcher.enqueue(resetHandler);
//                channel.writeAndFlush(ResetMessage.RESET).addListener(future -> registerConnectionReadTimeout(channel));
//            }
//        });
//    }

//    private void flushInEventLoop() {
////        channel.eventLoop().execute(() -> {
////            channel.flush();
////            registerConnectionReadTimeout(channel);
////        });
//    }

//    private void writeMessageInEventLoop(Message message, ResponseHandler handler, boolean flush) {
//        channel.eventLoop().execute(() -> {
//            messageDispatcher.enqueue(handler);
//
//            if (flush) {
//                channel.writeAndFlush(message).addListener(future -> registerConnectionReadTimeout(channel));
//            } else {
//                channel.write(message, channel.voidPromise());
//            }
//        });

//    }

//    private void writeMessagesInEventLoop(
//            Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2, boolean flush) {
//        channel.eventLoop().execute(() -> {
//            messageDispatcher.enqueue(handler1);
//            messageDispatcher.enqueue(handler2);
//
//            channel.write(message1, channel.voidPromise());
//
//            if (flush) {
//                channel.writeAndFlush(message2).addListener(future -> registerConnectionReadTimeout(channel));
//            } else {
//                channel.write(message2, channel.voidPromise());
//            }
//        });
//    }

//    private void setAutoRead(boolean value) {
//        channel.config().setAutoRead(value);
//    }

//    private boolean verifyOpen(ResponseHandler handler1, ResponseHandler handler2) {
//        Status connectionStatus = this.status.get();
//        switch (connectionStatus) {
//            case OPEN:
//                return true;
//            case RELEASED:
//                Exception error =
//                        new IllegalStateException("Connection has been released to the pool and can't be used");
//                if (handler1 != null) {
//                    handler1.onFailure(error);
//                }
//                if (handler2 != null) {
//                    handler2.onFailure(error);
//                }
//                return false;
//            case TERMINATED:
//                Exception terminatedError =
//                        new IllegalStateException("Connection has been terminated and can't be used");
//                if (handler1 != null) {
//                    handler1.onFailure(terminatedError);
//                }
//                if (handler2 != null) {
//                    handler2.onFailure(terminatedError);
//                }
//                return false;
//            default:
//                throw new IllegalStateException("Unknown status: " + connectionStatus);
//        }
//    }

//    private void registerConnectionReadTimeout(Channel channel) {
//        if (!channel.eventLoop().inEventLoop()) {
//            throw new IllegalStateException("This method may only be called in the EventLoop");
//        }
//
//        if (connectionReadTimeout != null && connectionReadTimeoutHandler == null) {
//            connectionReadTimeoutHandler = new ConnectionReadTimeoutHandler(connectionReadTimeout, TimeUnit.SECONDS);
//            channel.pipeline().addFirst(connectionReadTimeoutHandler);
//            log.debug("Added ConnectionReadTimeoutHandler");
//            messageDispatcher.setBeforeLastHandlerHook((messageType) -> {
//                channel.pipeline().remove(connectionReadTimeoutHandler);
//                connectionReadTimeoutHandler = null;
//                messageDispatcher.setBeforeLastHandlerHook(null);
//                log.debug("Removed ConnectionReadTimeoutHandler");
//            });
//        }
//    }

    private enum Status {
        OPEN,
        RELEASED,
        TERMINATED
    }
}
