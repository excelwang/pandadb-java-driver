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

import org.neo4j.driver.PandaResult;

import static java.util.Objects.requireNonNull;

/**
 * Bolt V4
 */
public class PandaResultCursorFactory implements ResultCursorFactory {//TODO support valillan driver

    private final PandaResult result;

//    private final CompletableFuture<Void> runFuture;

    public PandaResultCursorFactory(
            PandaResult result) {
//            CompletableFuture<Void> runFuture) {
        requireNonNull(result);
//        requireNonNull(runFuture);
        this.result = result;
//        this.runFuture = runFuture;
    }

    @Override
    public CompletionStage<AsyncResultCursor> asyncResult() {//todo add batch pull
        return CompletableFuture.completedStage(new DisposableAsyncResultCursor(new PandaAsyncResultCursor(null, result)));
//        return runFuture.handle((ignored, error) ->
//                );
    }

    @Override
    public CompletionStage<RxResultCursor> rxResult() {
//        connection.writeAndFlush(runMessage, runHandler);
//        return runFuture.handle((ignored, error) -> new RxResultCursorImpl(error, runHandler, pullHandler));
        throw new UnsupportedOperationException();
    }
}
