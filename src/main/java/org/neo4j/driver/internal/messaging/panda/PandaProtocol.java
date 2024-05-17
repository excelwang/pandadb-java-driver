package org.neo4j.driver.internal.messaging.panda;

import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4BasicPullHandler;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;

import org.neo4j.driver.*;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.async.PandaNetworkConnection;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.PandaResultCursorFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.*;
import org.neo4j.driver.internal.handlers.pulln.PandaPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.shaded.io.netty.channel.Channel;
import org.neo4j.driver.internal.shaded.io.netty.channel.ChannelPromise;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class PandaProtocol implements BoltProtocol {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(4, 100);
    public static final BoltProtocol INSTANCE = new PandaProtocol();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_first", "t_last");

    @Override
    public void initializeChannel(// this method only used by connectionPool
            String userAgent,
            AuthToken authToken,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise) {//TODO enable auth
//        Channel channel = channelInitializedPromise.channel();
//        HelloMessage message;
//
//        if (routingContext.isServerRoutingEnabled()) {
//            message = new HelloMessage(
//                    userAgent,
//                    ((InternalAuthToken) authToken).toMap(),
//                    routingContext.toMap(),
//                    includeDateTimeUtcPatchInHello());
//        } else {
//            message = new HelloMessage(
//                    userAgent, ((InternalAuthToken) authToken).toMap(), null, includeDateTimeUtcPatchInHello());
//        }
//
//        HelloResponseHandler handler = new HelloResponseHandler(channelInitializedPromise, version());
//
//        messageDispatcher(channel).enqueue(handler);
//        channel.writeAndFlush(message, channel.voidPromise());
        throw new UnsupportedOperationException("initializeChannel");
    }

    @Override
    public void prepareToCloseChannel(Channel channel) {// TODO enable deauth?
//        InboundMessageDispatcher messageDispatcher = messageDispatcher(channel);
//
//        GoodbyeMessage message = GoodbyeMessage.GOODBYE;
//        messageDispatcher.enqueue(NoOpResponseHandler.INSTANCE);
//        channel.writeAndFlush(message, channel.voidPromise());
//
//        messageDispatcher.prepareToCloseChannel();
        throw new UnsupportedOperationException("prepareToCloseChannel");
    }

    @Override
    public CompletionStage<Void> beginTransaction(
            Connection connection, Bookmark bookmark, TransactionConfig config, Logging logging) {
//        try {
//            verifyDatabaseNameBeforeTransaction(connection.databaseName());
//        } catch (Exception error) {
//            return Futures.failedFuture(error);
//        }

        CompletableFuture<Void> beginTxFuture = new CompletableFuture<>();
        BeginMessage beginMessage = new BeginMessage(
                bookmark, config, connection.databaseName(), connection.mode(), connection.impersonatedUser(), logging);
        connection.writeAndFlush(beginMessage, new BeginTxResponseHandler(beginTxFuture));//todo send beginMessage to server
//        return beginTxFuture;
        return CompletableFuture.runAsync(()->{});
    }

    @Override
    public CompletionStage<Bookmark> commitTransaction(Connection connection) {
        CompletableFuture<Bookmark> commitFuture = new CompletableFuture<>();
        connection.writeAndFlush(COMMIT, new CommitTxResponseHandler(commitFuture));
//        return commitFuture;
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection) {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        connection.writeAndFlush(ROLLBACK, new RollbackTxResponseHandler(rollbackFuture));
//        return rollbackFuture;

        return CompletableFuture.completedStage(null);
    }

    @Override
    public ResultCursorFactory runInAutoCommitTransaction(
            Connection connection,
            Query query,
            BookmarkHolder bookmarkHolder,
            TransactionConfig config,
            long fetchSize,
            Logging logging) {
//        verifyDatabaseNameBeforeTransaction(connection.databaseName());
//        RunWithMetadataMessage runMessage = autoCommitTxRunMessage(
//                query,
//                config,
//                connection.databaseName(),
//                connection.mode(),
//                bookmarkHolder.getBookmark(),
//                connection.impersonatedUser(),
//                logging);
        System.out.println("runInAutoCommitTransaction");
        return buildResultCursorFactory(connection, query, bookmarkHolder, null, fetchSize);
    }

    @Override
    public ResultCursorFactory runInUnmanagedTransaction(
            Connection connection, Query query, UnmanagedTransaction tx, long fetchSize) {
//        RunWithMetadataMessage runMessage = unmanagedTxRunMessage(query);
        return buildResultCursorFactory(connection, query, BookmarkHolder.NO_OP, tx, fetchSize);
    }

    protected ResultCursorFactory buildResultCursorFactory(
            Connection connection,
            Query query,
            BookmarkHolder bookmarkHolder,//TODO enable bookmark, fetchSize, and UnmanagedTransaction
            UnmanagedTransaction tx,
            long fetchSize) {
//        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        var pc = (PandaNetworkConnection)((DirectConnection) connection).connection();

        var runFuture = CompletableFuture.runAsync(()->{} , pc.getPool());
        RunResponseHandler runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR, connection, tx);
//        var cf = CompletableFuture.supplyAsync(() -> stub.query(PandaConverter.convertQuery(query)));//TODO pass exception
//        return cf.thenApply(response -> new PandaResult(response)).exceptionally(throwable -> {
//            throw new ClientException(throwable.getMessage());
//        });

        PandaPullAllResponseHandler pullAllHandler =
                newPandaPullAllHandler(query, runHandler, pc, bookmarkHolder, tx, fetchSize);
        pc.queryAsync(query, pullAllHandler);//TODO move to proper func
        PullResponseHandler pullHandler = newBoltV4BasicPullHandler(query, runHandler, connection, bookmarkHolder, tx);
//        return new PandaResultCursorFactory(pc, pr, runHandler, runFuture, pullHandler, pullAllHandler);

        System.out.println("buildResultCursorFactory");
        return new PandaResultCursorFactory(pc, runHandler, runFuture, pullHandler, pullAllHandler);
//        return new ResultCursorFactoryImpl(connection, runMessage, runHandler, runFuture, pullHandler, pullAllHandler);
    }


    /**
     * Instantiate {@link MessageFormat} used by this Bolt protocol verison.
     *
     * @return new message format.
     */
    @Override
    public MessageFormat createMessageFormat() {
        System.out.println("createMessageFormat");
        return new PandaMessageFormat();
    }

    /**
     * Returns the protocol version. It can be used for version specific error messages.
     *
     * @return the protocol version.
     */
    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    public static PandaPullAllResponseHandler newPandaPullAllHandler(
            Query query,
            RunResponseHandler runHandler,
            PandaNetworkConnection connection,
            BookmarkHolder bookmarkHolder,
            UnmanagedTransaction tx,
            long fetchSize) {
        PullResponseCompletionListener completionListener =
                createPullResponseCompletionListener(connection, bookmarkHolder, tx);

        return new PandaPullAllResponseHandler(
                query, runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, completionListener, fetchSize);
    }

    private static PullResponseCompletionListener createPullResponseCompletionListener(
            Connection connection, BookmarkHolder bookmarkHolder, UnmanagedTransaction tx) {
        return tx != null
                ? new TransactionPullResponseCompletionListener(tx)
                : new SessionPullResponseCompletionListener(connection, bookmarkHolder);
    }

}
