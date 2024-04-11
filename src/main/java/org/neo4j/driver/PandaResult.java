package org.neo4j.driver;

import org.grapheco.lynx.lynxrpc.LynxValueDeserializer;
import org.grapheco.lynx.types.composite.LynxMap;
import org.grapheco.pandadb.network.Query;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class PandaResult implements Result{

    private final Iterator<Query.QueryResponse> response;
    private final LynxValueDeserializer lynxDeserializer;
    private final ByteBuf byteBuf;//TODO move
    private Record next = null;

    public PandaResult(LynxValueDeserializer lynxDeserializer, ByteBuf byteBuf, Iterator<Query.QueryResponse> response) {
        this.response = response;
        this.lynxDeserializer = lynxDeserializer;
        this.byteBuf = byteBuf;
    }

    /**
     * Retrieve the keys of the records this result contains.
     *
     * @return all keys
     */
    @Override
    public List<String> keys() {
        if (hasNext()) {
            peek(); //changed this.next
            return next.keys();
        }
        return new LinkedList<String>();
    }

    /**
     * Test if there is another record we can navigate to in this result.
     *
     * @return true if {@link #next()} will return another record
     */
    @Override
    public boolean hasNext() {
        if (next != null) return true;
        return this.response.hasNext();
    }

    /**
     * Navigate to and retrieve the next {@link Record} in this result.
     *
     * @return the next record
     * @throws NoSuchRecordException if there is no record left in the stream
     */
    @Override
    public Record next() {
        if (next != null) {
            var ret = next;
            next = null;
            return ret;
        }
        if (! this.response.hasNext()) throw new NoSuchRecordException("");//TODO really need?
        return convertResponse(this.response.next());
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
        var ret = peek();
        next = null;
        if(this.response.hasNext()) throw new NoSuchRecordException("");
        return ret;
    }

    /**
     * Investigate the next upcoming record without moving forward in the result.
     *
     * @return the next record
     * @throws NoSuchRecordException if there is no record left in the stream
     */
    @Override
    public Record peek() {
        if (next!=null) return next;
        if (! this.response.hasNext()) throw new NoSuchRecordException("");
        next = convertResponse(this.response.next());
        return next;
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
        return Stream.empty();//TODO ??
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
        if (next!=null) {
            l.add(next);
            next = null;
        }
        while (this.response.hasNext()) {
            var r = this.response.next();
            l.add(convertResponse(r));
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
        if (next!=null) {
            l.add(mapFunction.apply(next));
            next = null;
        }
        while (this.response.hasNext()) {
            var r = this.response.next();
            l.add(mapFunction.apply(convertResponse(r)));
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
        var map =  (LynxMap) lynxDeserializer.decodeLynxValue(byteBuf.writeBytes(r.getResultInBytes().toByteArray()));
        return new PandaRecord(map.value());
    }
}
