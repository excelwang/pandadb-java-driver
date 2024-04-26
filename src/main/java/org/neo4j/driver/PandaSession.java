package org.neo4j.driver;

import org.neo4j.driver.internal.InternalTransaction;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.AbstractQueryRunner;
import org.grapheco.pandadb.network.PandaQueryServiceGrpc;
import org.grapheco.lynx.lynxrpc.LynxValueSerializer;
import org.grapheco.lynx.lynxrpc.LynxValueDeserializer;
import org.grapheco.lynx.lynxrpc.LynxByteBufFactory;
import com.google.protobuf.ByteString;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;

public class PandaSession extends AbstractQueryRunner implements Session {

    private AtomicBoolean closed = new AtomicBoolean(false);

    private final LynxValueSerializer lynxSerializer = new LynxValueSerializer();

    private final PandaQueryServiceGrpc.PandaQueryServiceBlockingStub stub;

    private final Logging logging;

    public PandaSession(PandaQueryServiceGrpc.PandaQueryServiceBlockingStub stub, Logging logging) {
        this.stub = stub;
        this.logging = logging;
    }

    @Override
    public Result run(Query query) {
        return run(query, TransactionConfig.empty());
    }

    @Override
    public Result run(String query, TransactionConfig config) {
        return run(query, emptyMap(), config);
    }

    @Override
    public Result run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return run(new Query(query, parameters), config);
    }

    @Override
    public Result run(Query query, TransactionConfig config) {
        var requestBuilder = org.grapheco.pandadb.network.Query.QueryRequest.newBuilder();
        requestBuilder.setStatement(query.text());
        var params = query.parameters().asMap();
        params.forEach((k, v) -> {
            requestBuilder.putParameters(k, encodeParamValue(v));
        });
        var request = requestBuilder.build();
        var response = this.stub.query(request);
        var lynxValueDeserializer = new LynxValueDeserializer();
        var byteBuf = LynxByteBufFactory.getByteBuf();
        return new PandaResult(lynxValueDeserializer, byteBuf, response, this.logging);
    }

    private ByteString encodeParamValue(Object paramValue) {
        if (paramValue instanceof List) {
            paramValue = ((List<?>) paramValue).toArray();//TODO change lynx source code.
        }
        return ByteString.copyFrom(lynxSerializer.encodeAny(paramValue));
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public void close() {
        closed.compareAndSet(false, true); //todo check closed already
    }

    @Override
    public Transaction beginTransaction() {
        return beginTransaction(TransactionConfig.empty());
    }

    @Override
    public Transaction beginTransaction(TransactionConfig config) {
//        UnmanagedTransaction tx = Futures.blockingGet(
//                session.beginTransactionAsync(config),
//                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while starting a transaction"));
//        return new InternalTransaction(tx);
        return null;
    }

    @Override
    public <T> T readTransaction(TransactionWork<T> work) {
        return readTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> T readTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.READ, work, config);
    }

    @Override
    public <T> T writeTransaction(TransactionWork<T> work) {
        return writeTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> T writeTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.WRITE, work, config);
    }

    @Override
    public Bookmark lastBookmark() {
        //return session.lastBookmark();
        return null;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void reset() {
//        Futures.blockingGet(
//                session.resetAsync(),
//                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while resetting the session"));
    }

    private <T> T transaction(AccessMode mode, TransactionWork<T> work, TransactionConfig config) {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
//        return session.retryLogic().retry(() -> {
//            try (Transaction tx = beginTransaction(mode, config)) {
//
//                T result = work.execute(tx);
//                if (tx.isOpen()) {
//                    // commit tx if a user has not explicitly committed or rolled back the transaction
//                    tx.commit();
//                }
//                return result;
//            }
//        });
        return null;
    }

    private Transaction beginTransaction(AccessMode mode, TransactionConfig config) {
        UnmanagedTransaction tx = new UnmanagedTransaction(null, null, 0, null);
        return new InternalTransaction(tx);
    }
}