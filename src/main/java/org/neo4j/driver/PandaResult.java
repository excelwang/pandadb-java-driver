package org.neo4j.driver;

import org.grapheco.lynx.lynxrpc.LynxValueDeserializer;
import org.grapheco.lynx.types.composite.LynxMap;
import org.grapheco.pandadb.network.Query;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PandaResult implements Result{

    private final Iterator<Query.QueryResponse> responseIter;
    private final LynxValueDeserializer lynxDeserializer;

    private List<String> keys = null;

    private final Logging logging;
    private final ByteBuf byteBuf;//TODO move
    private Record prefetchedRecord = null;

    public PandaResult(LynxValueDeserializer lynxDeserializer, ByteBuf byteBuf, Iterator<Query.QueryResponse> responseIter, Logging logging) {
        this.responseIter = responseIter;
        this.lynxDeserializer = lynxDeserializer;
        this.byteBuf = byteBuf;
        this.logging = logging;
        this.keys = hasNext()? prefetchedRecord.keys(): new LinkedList<String>();
    }

    /**
     * Retrieve the keys of the records this result contains.
     *
     * @return all keys
     */
    @Override
    public List<String> keys() {
        return this.keys;
    }

    /**
     * Test if there is another record we can navigate to in this result.
     *
     * @return true if {@link #next()} will return another record
     */
    @Override
    public boolean hasNext() {
        if (prefetchedRecord != null) return true;
        try {
            if (! this.responseIter.hasNext()) return false;
        } catch (io.grpc.StatusRuntimeException e) {
            throw new org.neo4j.driver.exceptions.ClientException(e.getMessage());
        }
        var re = convertResponse(this.responseIter.next());
        if (re==null) return false;
        this.prefetchedRecord = re;
        return true;
    }

    /**
     * Investigate the next upcoming record without moving forward in the result.
     *
     * @return the next record
     * @throws NoSuchRecordException if there is no record left in the stream
     */
    @Override
    public Record peek() {
        if (hasNext()) return prefetchedRecord;
        throw new NoSuchRecordException("");
    }

    /**
     * Navigate to and retrieve the next {@link Record} in this result.
     *
     * @return the next record
     * @throws NoSuchRecordException if there is no record left in the stream
     */
    @Override
    public Record next() {
        if (prefetchedRecord != null) {
            var ret = prefetchedRecord;
            prefetchedRecord = null;
            return ret;
        }
        throw new NoSuchRecordException("");//TODO really need?
    }

    /**
     * Return the first record in the result, failing if there is not exactly
     * one record left in the stream
     * <p>
     * Calling this method always exhausts the result, even when {@link NoSuchRecordException} is thrown.
     *
     * @return the first and only record in the stream
     * @throws NoSuchRecordException if there is not exactly one record left in the stream
     */
    @Override
    public Record single() throws NoSuchRecordException {
        if(!hasNext()) throw new NoSuchRecordException("");
        var ret = next();
        if(hasNext()) throw new NoSuchRecordException("");
        return ret;
    }

    /**
     * Convert this result to a sequential {@link Stream} of records.
     * <p>
     * Result is exhausted when a terminal operation on the returned stream is executed.
     *
     * @return sequential {@link Stream} of records. Empty stream if this result has already been consumed or is empty.
     */
    @Override
    public Stream<Record> stream() {
        Spliterator<Record> spliterator =
                Spliterators.spliteratorUnknownSize(this, Spliterator.IMMUTABLE | Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Retrieve and store the entire result stream.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the query that
     * yielded this result returns a finite stream. Some queries can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @return list of all remaining immutable records
     */
    @Override
    public List<Record> list() {
        List<Record> l = new LinkedList<>();
        while (hasNext()) {
            l.add(next());
        }
        return l;
    }

    /**
     * Retrieve and store a projection of the entire result.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the query that
     * yielded this result returns a finite stream. Some queries can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @param mapFunction a function to map from Record to T. See {@link Records} for some predefined functions.
     * @return list of all mapped remaining immutable records
     */
    @Override
    public <T> List<T> list(Function<Record, T> mapFunction) {
        List<T> l = new LinkedList<>();
        while (hasNext()) {
            l.add(mapFunction.apply(next()));
        }
        return l;
    }

    /**
     * Return the result summary.
     * <p>
     * If the records in the result is not fully consumed, then calling this method will exhausts the result.
     * <p>
     * If you want to access unconsumed records after summary, you shall use {@link Result#list()} to buffer all records into memory before summary.
     *
     * @return a summary for the whole query result.
     */
    @Override
    public ResultSummary consume() {
        return null;
    }

    private Record convertResponse(Query.QueryResponse r) {
        var lv = lynxDeserializer.decodeLynxValue(byteBuf.writeBytes(r.getResultInBytes().toByteArray()));
        if (lv instanceof LynxMap) {
            var map = (LynxMap) lv;
            if (map.value().isEmpty()) return null;
            return new PandaRecord(map.value());
        }
        else {
            logging.getLog(getClass()).warn("QueryResponse is not a Map");//TODO no output in junit tests
            return  null;
        }
    }
}
