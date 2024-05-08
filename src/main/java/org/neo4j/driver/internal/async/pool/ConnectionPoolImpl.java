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
package org.neo4j.driver.internal.async.pool;

import static java.lang.String.format;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setAuthorizationStateListener;
import static org.neo4j.driver.internal.async.connection.PandaChannelAttributes.setPoolId;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completeWithNullIfNoError;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLockAsync;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.neo4j.driver.internal.shaded.io.netty.bootstrap.Bootstrap;
import org.neo4j.driver.internal.shaded.io.netty.channel.Channel;
import org.neo4j.driver.internal.shaded.io.netty.channel.EventLoopGroup;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.PandaNetworkConnection;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddress;

public class ConnectionPoolImpl implements ConnectionPool {
    private final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private final int connectionAcquisitionTimeout = 10; // s
    private final int awaitTerminationTimeout = 10; // s
    private final Logger log;
    private final MetricsListener metricsListener;
    private final ReadWriteLock addressToPoolLock = new ReentrantReadWriteLock();
    private final Map<BoltServerAddress, ExecutorService> addressToPool = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    public ConnectionPoolImpl(
            ChannelConnector connector,
            Bootstrap bootstrap,
            PoolSettings settings,
            MetricsListener metricsListener,
            Logging logging,
            Clock clock,
            boolean ownsEventLoopGroup) {
        this.metricsListener = metricsListener;
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<Connection> acquire(BoltServerAddress address) {
        log.trace("Acquiring a connection from pool towards %s", address);

        assertNotClosed();
        ExecutorService pool = getOrCreatePool(address);

        ListenerEvent acquireEvent = metricsListener.createListenerEvent();
        metricsListener.beforeAcquiringOrCreating(address.toString(), acquireEvent);

        return CompletableFuture.supplyAsync(() -> createPandaNetworkConnection(address, pool), pool).
                orTimeout(connectionAcquisitionTimeout, TimeUnit.SECONDS).
                handle((connection, error) -> {
            try {
                processAcquisitionError(pool, address, error);
                assertNotClosed(address, pool);
//                setAuthorizationStateListener(channel, channelHealthChecker);
                metricsListener.afterAcquiredOrCreated(address.toString(), acquireEvent);
                return connection;
            } finally {
                metricsListener.afterAcquiringOrCreating(pool.toString());
            }
        });
    }

    @Override
    public void retainAll(Set<BoltServerAddress> addressesToRetain) {
        executeWithLock(addressToPoolLock.writeLock(), () -> {
            Iterator<Map.Entry<BoltServerAddress, ExecutorService>> entryIterator =
                    addressToPool.entrySet().iterator();
            while (entryIterator.hasNext()) {
                var entry = entryIterator.next();
                BoltServerAddress address = entry.getKey();
                if (!addressesToRetain.contains(address)) {
                    int activeChannels = inUseConnections(address);
                    if (activeChannels == 0) {
                        // address is not present in updated routing table and has no active connections
                        // it's now safe to terminate corresponding connection pool and forget about it
                        ExecutorService pool = entry.getValue();
                        entryIterator.remove();
                        if (pool != null) {
                            log.info(
                                    "Closing connection pool towards %s, it has no active connections "
                                            + "and is not in the routing table registry.",
                                    address);
                            closePoolInBackground(address, pool);
                        }
                    }
                }
            }
        });
    }

    @Override
    public int inUseConnections(ServerAddress address) {
        return ((ThreadPoolExecutor)getPool(address)).getActiveCount();
    }

    @Override
    public int idleConnections(ServerAddress address) {
        return POOL_SIZE - inUseConnections(address);
    }

    @Override
    public CompletionStage<Void> close() {
        if (closed.compareAndSet(false, true)) {
            executeWithLockAsync(addressToPoolLock.writeLock(), () -> {
                // We can only shutdown event loop group when all netty pools are fully closed,
                // otherwise the netty pools might missing threads (from event loop group) to execute clean ups.
                return closeAllPools().whenComplete((ignored, pollCloseError) -> {
                    addressToPool.clear();
                    completeWithNullIfNoError(closeFuture, pollCloseError);
                });
            });
        }
        return closeFuture;
    }

    @Override
    public boolean isOpen(BoltServerAddress address) {
        return executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.containsKey(address));
    }

    @Override
    public String toString() {
        return executeWithLock(
                addressToPoolLock.readLock(), () -> "ConnectionPoolImpl{" + "pools=" + addressToPool + '}');
    }

    private void processAcquisitionError(ExecutorService pool, BoltServerAddress serverAddress, Throwable error) {
        Throwable cause = Futures.completionExceptionCause(error);
        if (cause != null) {
            if (cause instanceof TimeoutException) {
                // NettyChannelPool returns future failed with TimeoutException if acquire operation takes more than
                // configured time, translate this exception to a prettier one and re-throw
                metricsListener.afterTimedOutToAcquireOrCreate(serverAddress.toString());
                throw new ClientException(
                        "Unable to acquire connection from the pool within configured maximum time of "
                                + connectionAcquisitionTimeout + "s");
            } else if (pool.isShutdown()) {
                // There is a race condition where a thread tries to acquire a connection while the pool is closed by
                // another concurrent thread.
                // Treat as failed to obtain connection for a direct driver. For a routing driver, this error should be
                // retried.
                throw new ServiceUnavailableException(
                        format("Connection pool for server %s is closed while acquiring a connection.", serverAddress),
                        cause);
            } else {
                // some unknown error happened during connection acquisition, propagate it
                throw new CompletionException(cause);
            }
        }
    }

    private void assertNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException(CONNECTION_POOL_CLOSED_ERROR_MESSAGE);
        }
    }

    private void assertNotClosed(BoltServerAddress address, ExecutorService pool) {
        if (closed.get()) {
            closePoolInBackground(address, pool);
            executeWithLock(addressToPoolLock.writeLock(), () -> addressToPool.remove(address));
            assertNotClosed();
        }
    }

    private PandaNetworkConnection createPandaNetworkConnection(BoltServerAddress address, ExecutorService pool) {
        var managedChannel = NettyChannelBuilder.forAddress(address.host(), address.port()).usePlaintext().build();
        setPoolId(managedChannel, address.toString());
        return new PandaNetworkConnection(managedChannel, pool, null, metricsListener, null);
    }

    ExecutorService getPool(ServerAddress address) {
        return executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.get((BoltServerAddress) address));
    }

    ExecutorService newPool() {
        return Executors.newFixedThreadPool(POOL_SIZE);
    }

    private ExecutorService getOrCreatePool(BoltServerAddress address) {
        ExecutorService existingPool =
                executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.get(address));
        return existingPool != null
                ? existingPool
                : executeWithLock(addressToPoolLock.writeLock(), () -> {
                    ExecutorService pool = addressToPool.get(address);
                    if (pool == null) {
                        pool = newPool();
                        // before the connection pool is added I can register the metrics for the pool.
                        metricsListener.registerPoolMetrics(
                                address.toString(),
                                address,
                                () -> this.inUseConnections(address),
                                () -> this.idleConnections(address));
                        addressToPool.put(address, pool);
                    }
                    return pool;
                });
    }

    private CompletionStage<Void> closePool(ExecutorService pool) {
        log.info("closePool");
        return CompletableFuture.runAsync(() -> {pool.shutdown();
            try {
                pool.awaitTermination(awaitTerminationTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).whenComplete((ignored, error) ->
        // after the connection pool is removed/close, I can remove its metrics.
        metricsListener.removePoolMetrics(pool.toString()));
    }

    private void closePoolInBackground(BoltServerAddress address, ExecutorService pool) {
        // Close in the background
        closePool(pool).whenComplete((ignored, error) -> {
            if (error != null) {
                log.warn(format("An error occurred while closing connection pool towards %s.", address), error);
            }
        });
    }

    private CompletableFuture<Void> closeAllPools() {
        return CompletableFuture.allOf(addressToPool.entrySet().stream()
                .map(entry -> {
                    BoltServerAddress address = entry.getKey();
                    ExecutorService pool = entry.getValue();
                    log.info("Closing connection pool towards %s", address);
                    // Wait for all pools to be closed.
                    return closePool(pool).toCompletableFuture();
                })
                .toArray(CompletableFuture[]::new));
    }
}
