package org.neo4j.driver.internal.messaging.panda;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4AutoPullHandler;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4BasicPullHandler;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import org.neo4j.driver.*;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.PandaNetworkConnection;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.PandaResultCursorFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactoryImpl;
import org.neo4j.driver.internal.handlers.*;
import org.neo4j.driver.internal.handlers.pulln.AutoPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.shaded.io.netty.channel.Channel;
import org.neo4j.driver.internal.shaded.io.netty.channel.ChannelPromise;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

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
        connection.writeAndFlush(beginMessage, new BeginTxResponseHandler(beginTxFuture));
        return beginTxFuture;
    }

    @Override
    public CompletionStage<Bookmark> commitTransaction(Connection connection) {
        CompletableFuture<Bookmark> commitFuture = new CompletableFuture<>();
        connection.writeAndFlush(COMMIT, new CommitTxResponseHandler(commitFuture));
        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection) {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        connection.writeAndFlush(ROLLBACK, new RollbackTxResponseHandler(rollbackFuture));
        return rollbackFuture;
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

//        var cf = CompletableFuture.supplyAsync(() -> stub.query(PandaConverter.convertQuery(query)));//TODO pass exception
//        return cf.thenApply(response -> new PandaResult(response)).exceptionally(throwable -> {
//            throw new ClientException(throwable.getMessage());
//        });
        var pc = (PandaNetworkConnection)((DirectConnection) connection).connection();
        var pr = pc.query(query);

//        PullAllResponseHandler pullAllHandler =
//                newBoltV4AutoPullHandler(query, runHandler, connection, bookmarkHolder, tx, fetchSize);
//        PullResponseHandler pullHandler = newBoltV4BasicPullHandler(query, runHandler, connection, bookmarkHolder, tx);
        return new PandaResultCursorFactory(pr);//, runFuture);
    }


    /**
     * Instantiate {@link MessageFormat} used by this Bolt protocol verison.
     *
     * @return new message format.
     */
    @Override
    public MessageFormat createMessageFormat() {
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

}
