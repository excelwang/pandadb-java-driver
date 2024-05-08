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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.PandaResult;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class PandaAsyncResultCursor implements AsyncResultCursor {
    private final Throwable runError;

    private final PandaResult result;

    public PandaAsyncResultCursor(
            Throwable runError, PandaResult result) {
        this.runError = runError;
        this.result = result;
    }

    @Override
    public List<String> keys() {
        return result.keys();
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync() {
        return completedFuture(null);
    }

    @Override
    public CompletionStage<Record> nextAsync() {
        try {
            return completedFuture(result.next());
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Record> peekAsync() {
        try {
            return completedFuture(result.peek());
        } catch (Exception e) {
           return failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Record> singleAsync() {
        try {
            return completedFuture(result.single());
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        internalForEachAsync(action, resultFuture);
        return resultFuture.thenCompose(ignore -> consumeAsync());
    }

    @Override
    public CompletionStage<List<Record>> listAsync() {
        return listAsync(Function.identity());
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction) {
        return completedFuture(result.list(mapFunction));
    }

    @Override
    public CompletionStage<Throwable> discardAllFailureAsync() {
        // runError has priority over other errors and is expected to have been reported to user by now
//        return consumeAsync().handle((summary, error) -> runError != null ? null : error);
        return completedFuture(null);
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        // runError has priority over other errors and is expected to have been reported to user by now
        return consumeAsync().handle((summary, error) -> runError != null ? null : error);
    }

    private void internalForEachAsync(Consumer<Record> action, CompletableFuture<Void> resultFuture) {
        CompletionStage<Record> recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync((record, completionError) -> {
            Throwable error = Futures.completionExceptionCause(completionError);
            if (error != null) {
                resultFuture.completeExceptionally(error);
            } else if (record != null) {
                try {
                    action.accept(record);
                } catch (Throwable actionError) {
                    resultFuture.completeExceptionally(actionError);
                    return;
                }
                internalForEachAsync(action, resultFuture);
            } else {
                resultFuture.complete(null);
            }
        });
    }

    @Override
    public CompletableFuture<AsyncResultCursor> mapSuccessfulRunCompletionAsync() {
        return runError != null ? Futures.failedFuture(runError) : CompletableFuture.completedFuture(this);
    }
}
