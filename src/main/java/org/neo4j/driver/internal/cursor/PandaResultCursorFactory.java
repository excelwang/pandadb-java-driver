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
package org.neo4j.driver.internal.cursor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import org.neo4j.driver.internal.async.PandaNetworkConnection;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Objects.requireNonNull;

/**
 * Bolt V4
 */
public class PandaResultCursorFactory implements ResultCursorFactory {//TODO support valillan driver

//    private final PandaResult result;

    private final RunResponseHandler runHandler;
    private final PandaNetworkConnection connection;

    private final PullResponseHandler pullHandler;
    private final PullAllResponseHandler pullAllHandler;
    private final CompletableFuture<Void> runFuture;

//    private final CompletableFuture<Void> runFuture;

//    public PandaResultCursorFactory(
//            PandaNetworkConnection pc,
//            PandaResult result) {
////            CompletableFuture<Void> runFuture) {
//        requireNonNull(result);
////        requireNonNull(runFuture);
//        this.pool = pc.getPool();
//        this.result = result;
////        this.runFuture = runFuture;
//    }

    public PandaResultCursorFactory(
            PandaNetworkConnection connection,
            RunResponseHandler runHandler,
            CompletableFuture<Void> runFuture,
            PullResponseHandler pullHandler,
            PullAllResponseHandler pullAllHandler) {
        requireNonNull(connection);
        requireNonNull(runHandler);
        requireNonNull(pullHandler);
        requireNonNull(pullAllHandler);
        this.connection = connection;
        this.runHandler = runHandler;
        this.runFuture = runFuture;
        this.pullHandler = pullHandler;
        this.pullAllHandler = pullAllHandler;
    }

//    @Override
//    public CompletionStage<AsyncResultCursor> asyncResult() {//todo add batch pull
//        return CompletableFuture.supplyAsync(() -> new DisposableAsyncResultCursor(new PandaAsyncResultCursor(null, result)), pool);
////        return runFuture.handle((ignored, error) ->
////                );
//    }
//
//    @Override
//    public CompletionStage<RxResultCursor> rxResult() {
////        connection.writeAndFlush(runMessage, runHandler);
////        return runFuture.handle((ignored, error) -> new RxResultCursorImpl(error, runHandler, pullHandler));
//        throw new UnsupportedOperationException();
//    }
    @Override
    public CompletionStage<AsyncResultCursor> asyncResult() {
        // only write and flush messages when async result is wanted.
//        connection.write(runMessage, runHandler); // queues the run message, will be flushed with pull message together
        pullAllHandler.prePopulateRecords();
        return runFuture.handle((ignored, error) ->
                new DisposableAsyncResultCursor(new AsyncResultCursorImpl(error, runHandler, pullAllHandler)));
    }

    @Override
    public CompletionStage<RxResultCursor> rxResult() {
//        connection.writeAndFlush(runMessage, runHandler);
        return runFuture.handle((ignored, error) -> new RxResultCursorImpl(error, runHandler, pullHandler));
    }
}
