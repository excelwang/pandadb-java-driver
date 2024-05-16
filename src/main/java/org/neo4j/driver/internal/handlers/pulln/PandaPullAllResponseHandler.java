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
package org.neo4j.driver.internal.handlers.pulln;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.internal.messaging.request.DiscardMessage.newDiscardAllMessage;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.grapheco.pandadb.network.Query.QueryResponse;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.summary.ResultSummary;

public class PandaPullAllResponseHandler implements PullAllResponseHandler, StreamObserver<QueryResponse> {

    private final Query query;
    protected final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;
    private final PullResponseCompletionListener completionListener;
    private final boolean syncSignals;

    private State state;
    private long toRequest;
    private BiConsumer<Record, Throwable> recordConsumer = null;
    private BiConsumer<ResultSummary, Throwable> summaryConsumer = null;
    private static final Queue<Record> UNINITIALIZED_RECORDS = Iterables.emptyQueue();
    private final long fetchSize;
    private final long lowRecordWatermark;
    private final long highRecordWatermark;

    // initialized lazily when first record arrives
    private Queue<Record> records = UNINITIALIZED_RECORDS;

    private ResultSummary summary;
    private Throwable failure;
    private boolean isAutoPullEnabled = true;

    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<ResultSummary> summaryFuture;

    private boolean isDone = false;

    public PandaPullAllResponseHandler(
            Query query,
            RunResponseHandler runResponseHandler,
            Connection connection,
            MetadataExtractor metadataExtractor,
            PullResponseCompletionListener completionListener,
            long fetchSize) {

        this.query = requireNonNull(query);
        this.runResponseHandler = requireNonNull(runResponseHandler);
        this.metadataExtractor = requireNonNull(metadataExtractor);
        this.connection = requireNonNull(connection);
        this.completionListener = requireNonNull(completionListener);
        this.syncSignals = true;

        this.state = State.READY_STATE;
        this.fetchSize = fetchSize;

        // For pull everything ensure conditions for disabling auto pull are never met
        if (fetchSize == UNLIMITED_FETCH_SIZE) {
            this.highRecordWatermark = Long.MAX_VALUE;
            this.lowRecordWatermark = Long.MAX_VALUE;
        } else {
            this.highRecordWatermark = (long) (fetchSize * 0.7);
            this.lowRecordWatermark = (long) (fetchSize * 0.3);
        }

        installRecordAndSummaryConsumers();
    }

    private void installRecordAndSummaryConsumers() {
        installRecordConsumer((record, error) -> {
            if (record != null) {
                enqueueRecord(record);
                completeRecordFuture(record);
            }
            //  if ( error != null ) Handled by summary.error already
            if (record == null && error == null) {
                // complete
                completeRecordFuture(null);
            }
        });

        installSummaryConsumer((summary, error) -> {
            if (error != null) {
                handleFailure(error);
            }
            if (summary != null) {
                this.summary = summary;
                completeSummaryFuture(summary);
            }

            if (error == null && summary == null) // has_more
            {
                if (isAutoPullEnabled) {
                    request(fetchSize);
                }
            }
        });
    }

    private void handleFailure(Throwable error) {
        // error has not been propagated to the user, remember it
        if (!failRecordFuture(error) && !failSummaryFuture(error)) {
            failure = error;
        }
    }

    @Override
    public synchronized CompletionStage<Record> peekAsync() {

//        return resultFuture.thenApply(result -> result.peek());

        Record record = records.peek();
        if (record == null) {
            if (isDone) {
                return completedWithValueIfNoFailure(null);
            }

            if (recordFuture == null) {
                recordFuture = new CompletableFuture<>();
            }
            return recordFuture;
        } else {
            return completedFuture(record);
        }
    }

    @Override
    public synchronized CompletionStage<Record> nextAsync() {
        return peekAsync().thenApply(ignore -> dequeueRecord());
    }

    @Override
    public synchronized CompletionStage<ResultSummary> consumeAsync() {
        records.clear();
        if (isDone) {
            return completedWithValueIfNoFailure(summary);
        } else {
            cancel();
            if (summaryFuture == null) {
                summaryFuture = new CompletableFuture<>();
            }

            return summaryFuture;
        }
    }

    @Override
    public synchronized <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction) {
        return pullAllAsync().thenApply(summary -> recordsAsList(mapFunction));
    }

    @Override
    public synchronized CompletionStage<Throwable> pullAllFailureAsync() {
        return pullAllAsync().handle((ignore, error) -> error);
    }

    @Override
    public void prePopulateRecords() {
        request(fetchSize);
    }

    private synchronized CompletionStage<ResultSummary> pullAllAsync() {
        if (isDone) {
            return completedWithValueIfNoFailure(summary);
        } else {
            request(UNLIMITED_FETCH_SIZE);
            if (summaryFuture == null) {
                summaryFuture = new CompletableFuture<>();
            }

            return summaryFuture;
        }
    }

    private void enqueueRecord(Record record) {
        if (records == UNINITIALIZED_RECORDS) {
            records = new ArrayDeque<>();
        }

        records.add(record);

        // too many records in the queue, pause auto request gathering
        if (records.size() > highRecordWatermark) {
            isAutoPullEnabled = false;
        }
    }

    private Record dequeueRecord() {
        Record record = records.poll();

        if (records.size() <= lowRecordWatermark) {
            // if not in streaming state we need to restart streaming
            if (state() != State.STREAMING_STATE) {
                request(fetchSize);
            }
            isAutoPullEnabled = true;
        }

        return record;
    }

    private <T> List<T> recordsAsList(Function<Record, T> mapFunction) {
        System.out.println("recordsAsList");
        if (!isDone) {
            throw new IllegalStateException("Can't get records as list because SUCCESS or FAILURE did not arrive");
        }

        List<T> result = new ArrayList<>(records.size());
        while (!records.isEmpty()) {
            Record record = records.poll();
            result.add(mapFunction.apply(record));
        }
        return result;
    }

    private Throwable extractFailure() {
        if (failure == null) {
            throw new IllegalStateException("Can't extract failure because it does not exist");
        }

        Throwable error = failure;
        failure = null; // propagate failure only once
        return error;
    }

    private void completeRecordFuture(Record record) {
        if (recordFuture != null) {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete(record);
        }
    }

    private void completeSummaryFuture(ResultSummary summary) {
        if (summaryFuture != null) {
            CompletableFuture<ResultSummary> future = summaryFuture;
            summaryFuture = null;
            future.complete(summary);
        }
    }

    private boolean failRecordFuture(Throwable error) {
        if (recordFuture != null) {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.completeExceptionally(error);
            return true;
        }
        return false;
    }

    private boolean failSummaryFuture(Throwable error) {
        if (summaryFuture != null) {
            CompletableFuture<ResultSummary> future = summaryFuture;
            summaryFuture = null;
            future.completeExceptionally(error);
            return true;
        }
        return false;
    }

    private <T> CompletionStage<T> completedWithValueIfNoFailure(T value) {
        if (failure != null) {
            return failedFuture(extractFailure());
        } else if (value == null) {
            return completedWithNull();
        } else {
            return completedFuture(value);
        }
    }
    @Override
    public void onSuccess(Map<String, Value> metadata) {
        System.out.println("Call PandaPullAllResponseHandler.onSuccess!!");
        State newState;
        BiConsumer<Record, Throwable> recordConsumer = null;
        BiConsumer<ResultSummary, Throwable> summaryConsumer = null;
        ResultSummary summary = null;
        Neo4jException exception = null;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onSuccess(this, metadata);
            newState = state;
            if (newState == State.SUCCEEDED_STATE) {
                completionListener.afterSuccess(metadata);
                try {
                    summary = extractResultSummary(metadata);
                } catch (Neo4jException e) {
                    summary = extractResultSummary(emptyMap());
                    exception = e;
                }
                recordConsumer = this.recordConsumer;
                summaryConsumer = this.summaryConsumer;
                if (syncSignals) {
                    complete(summaryConsumer, recordConsumer, summary, exception);
                }
                dispose();
            } else if (newState == State.READY_STATE) {
                if (toRequest > 0 || toRequest == UNLIMITED_FETCH_SIZE) {
                    request(toRequest);
                    toRequest = 0;
                }
                // summary consumer use (null, null) to identify done handling of success with has_more
                this.summaryConsumer.accept(null, null);
            }
        }
        if (!syncSignals && newState == State.SUCCEEDED_STATE) {
            complete(summaryConsumer, recordConsumer, summary, exception);
        }
    }

    @Override
    public void onFailure(Throwable error) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

//    @Override
    public synchronized void request(long size) {
        assertRecordAndSummaryConsumerInstalled();
        state.request(this, size);
    }

//    @Override
    public synchronized void cancel() {
        assertRecordAndSummaryConsumerInstalled();
        state.cancel(this);
    }

    protected void writePull(long n) {
        connection.writeAndFlush(new PullMessage(n, runResponseHandler.queryId()), this);
    }

    protected void discardAll() {
        connection.writeAndFlush(newDiscardAllMessage(runResponseHandler.queryId()), this);
    }

    public synchronized void installSummaryConsumer(BiConsumer<ResultSummary, Throwable> summaryConsumer) {
        if (this.summaryConsumer != null) {
            throw new IllegalStateException("Summary consumer already installed.");
        }
        this.summaryConsumer = summaryConsumer;
    }

    public synchronized void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        if (this.recordConsumer != null) {
            throw new IllegalStateException("Record consumer already installed.");
        }
        this.recordConsumer = recordConsumer;
    }

    private ResultSummary extractResultSummary(Map<String, Value> metadata) {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        return metadataExtractor.extractSummary(query, connection, resultAvailableAfter, metadata);
    }

    private void addToRequest(long toAdd) {
        if (toRequest == UNLIMITED_FETCH_SIZE) {
            return;
        }
        if (toAdd == UNLIMITED_FETCH_SIZE) {
            // pull all
            toRequest = UNLIMITED_FETCH_SIZE;
            return;
        }

        if (toAdd <= 0) {
            throw new IllegalArgumentException(
                    "Cannot request record amount that is less than or equal to 0. Request amount: " + toAdd);
        }
        toRequest += toAdd;
        if (toRequest <= 0) // toAdd is already at least 1, we hit buffer overflow
        {
            toRequest = Long.MAX_VALUE;
        }
    }

    private void assertRecordAndSummaryConsumerInstalled() {
        if (isDone) {
            // no need to check if we've finished.
            return;
        }
        if (recordConsumer == null || summaryConsumer == null) {
            throw new IllegalStateException(format(
                    "Access record stream without record consumer and/or summary consumer. "
                            + "Record consumer=%s, Summary consumer=%s",
                    recordConsumer, summaryConsumer));
        }
    }

    private void complete(
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            BiConsumer<Record, Throwable> recordConsumer,
            ResultSummary summary,
            Throwable error) {
        // we first inform the summary consumer to ensure when streaming finished, summary is definitely available.
        summaryConsumer.accept(summary, error);
        // record consumer use (null, null) to identify the end of record stream
        recordConsumer.accept(null, error);
    }

    private void dispose() {
        // release the reference to the consumers who hold the reference to subscribers which shall be released when
        // subscription is completed.
        this.recordConsumer = null;
        this.summaryConsumer = null;
    }

    protected State state() {
        return state;
    }

    protected void state(State state) {
        this.state = state;
    }

    /**
     * Receives a value from the stream.
     *
     * <p>Can be called many times but is never called after {@link #onError(Throwable)} or {@link
     * #onCompleted()} are called.
     *
     * <p>Unary calls must invoke onNext at most once.  Clients may invoke onNext at most once for
     * server streaming calls, but may receive many onNext callbacks.  Servers may invoke onNext at
     * most once for client streaming calls, but may receive many onNext callbacks.
     *
     * <p>If an exception is thrown by an implementation the caller is expected to terminate the
     * stream by calling {@link #onError(Throwable)} with the caught exception prior to
     * propagating it.
     *
     * @param value the value passed to the stream
     */
    @Override
    public void onNext(QueryResponse value) {
        var map = PandaConverter.convertResponse2NeoValues(value);
        var keys = map._1;
        Value[] fields = map._2;
        State newState;
        Record record = null;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onRecord(this, fields);
            newState = state;
            if (newState == State.STREAMING_STATE) {
                record = new InternalRecord(keys, fields);
                if (syncSignals) {
                    recordConsumer.accept(record, null);
                }
            }
        }
        if (!syncSignals && newState == State.STREAMING_STATE) {
            recordConsumer.accept(record, null);
        }
    }

    /**
     * Receives a terminating error from the stream.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onError} no further calls to any method are
     * allowed.
     *
     * <p>{@code t} should be a {@link StatusException} or {@link
     * StatusRuntimeException}, but other {@code Throwable} types are possible. Callers should
     * generally convert from a {@link Status} via {@link Status#asException()} or
     * {@link Status#asRuntimeException()}. Implementations should generally convert to a
     * {@code Status} via {@link Status#fromThrowable(Throwable)}.
     *
     * @param error the error occurred on the stream
     */
    @Override
    public void onError(Throwable error) {
        BiConsumer<Record, Throwable> recordConsumer;
        BiConsumer<ResultSummary, Throwable> summaryConsumer;
        ResultSummary summary;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onFailure(this, error);
            completionListener.afterFailure(error);
            summary = extractResultSummary(emptyMap());
            recordConsumer = this.recordConsumer;
            summaryConsumer = this.summaryConsumer;
            if (syncSignals) {
                complete(summaryConsumer, recordConsumer, summary, error);
            }
            dispose();
        }
        if (!syncSignals) {
            complete(summaryConsumer, recordConsumer, summary, error);
        }
    }

    /**
     * Receives a notification of successful stream completion.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onCompleted} no further calls to any method
     * are allowed.
     */
    @Override
    public void onCompleted() {
        this.isDone = true;
        Map<String, Value> metadata = Map.of();//TODO fulfill metadata
        State newState;
        BiConsumer<Record, Throwable> recordConsumer = null;
        BiConsumer<ResultSummary, Throwable> summaryConsumer = null;
        ResultSummary summary = null;
        Neo4jException exception = null;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onSuccess(this, metadata);
            newState = state;
            if (newState == State.SUCCEEDED_STATE) {
                completionListener.afterSuccess(metadata);
                try {
                    summary = extractResultSummary(metadata);
                } catch (Neo4jException e) {
                    summary = extractResultSummary(emptyMap());
                    exception = e;
                }
                recordConsumer = this.recordConsumer;
                summaryConsumer = this.summaryConsumer;
                if (syncSignals) {
                    complete(summaryConsumer, recordConsumer, summary, exception);
                }
                dispose();
            } else if (newState == State.READY_STATE) {
                if (toRequest > 0 || toRequest == UNLIMITED_FETCH_SIZE) {
                    request(toRequest);
                    toRequest = 0;
                }
                // summary consumer use (null, null) to identify done handling of success with has_more
                this.summaryConsumer.accept(null, null);
            }
        }
        if (!syncSignals && newState == State.SUCCEEDED_STATE) {
            complete(summaryConsumer, recordConsumer, summary, exception);
        }
    }

    enum State {
        READY_STATE {
            @Override
            void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(PandaPullAllResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(PandaPullAllResponseHandler context, Value[] fields) {
                context.state(READY_STATE);
            }

            @Override
            void request(PandaPullAllResponseHandler context, long n) {
                context.state(STREAMING_STATE);
                context.writePull(n);
            }

            @Override
            void cancel(PandaPullAllResponseHandler context) {
                context.state(CANCELLED_STATE);
                context.discardAll();
            }
        },
        STREAMING_STATE {
            @Override
            void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata) {
                if (metadata.getOrDefault("has_more", BooleanValue.FALSE).asBoolean()) {
                    context.state(READY_STATE);
                } else {
                    context.state(SUCCEEDED_STATE);
                }
            }

            @Override
            void onFailure(PandaPullAllResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(PandaPullAllResponseHandler context, Value[] fields) {
                context.state(STREAMING_STATE);
            }

            @Override
            void request(PandaPullAllResponseHandler context, long n) {
                context.state(STREAMING_STATE);
                context.addToRequest(n);
            }

            @Override
            void cancel(PandaPullAllResponseHandler context) {
                context.state(CANCELLED_STATE);
            }
        },
        CANCELLED_STATE {
            @Override
            void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata) {
                if (metadata.getOrDefault("has_more", BooleanValue.FALSE).asBoolean()) {
                    context.state(CANCELLED_STATE);
                    context.discardAll();
                } else {
                    context.state(SUCCEEDED_STATE);
                }
            }

            @Override
            void onFailure(PandaPullAllResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(PandaPullAllResponseHandler context, Value[] fields) {
                context.state(CANCELLED_STATE);
            }

            @Override
            void request(PandaPullAllResponseHandler context, long n) {
                context.state(CANCELLED_STATE);
            }

            @Override
            void cancel(PandaPullAllResponseHandler context) {
                context.state(CANCELLED_STATE);
            }
        },
        SUCCEEDED_STATE {
            @Override
            void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(PandaPullAllResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(PandaPullAllResponseHandler context, Value[] fields) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void request(PandaPullAllResponseHandler context, long n) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void cancel(PandaPullAllResponseHandler context) {
                context.state(SUCCEEDED_STATE);
            }
        },
        FAILURE_STATE {
            @Override
            void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(PandaPullAllResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(PandaPullAllResponseHandler context, Value[] fields) {
                context.state(FAILURE_STATE);
            }

            @Override
            void request(PandaPullAllResponseHandler context, long n) {
                context.state(FAILURE_STATE);
            }

            @Override
            void cancel(PandaPullAllResponseHandler context) {
                context.state(FAILURE_STATE);
            }
        };

        abstract void onSuccess(PandaPullAllResponseHandler context, Map<String, Value> metadata);

        abstract void onFailure(PandaPullAllResponseHandler context, Throwable error);

        abstract void onRecord(PandaPullAllResponseHandler context, Value[] fields);

        abstract void request(PandaPullAllResponseHandler context, long n);

        abstract void cancel(PandaPullAllResponseHandler context);
    }

}
