package org.neo4j.driver;

import org.grapheco.pandadb.network.PandaQueryServiceGrpc;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class PandaDriver implements Driver{

    private AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger log;
    private final ManagedChannel channel;
    private PandaQueryServiceGrpc.PandaQueryServiceStub stub = null;
    private PandaQueryServiceGrpc.PandaQueryServiceBlockingStub blockingStub = null;

    public PandaDriver(String host, int port, Logging logging){
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.log = logging.getLog(getClass());
    }

    private PandaQueryServiceGrpc.PandaQueryServiceStub getStub() {
        if (this.stub == null) {
            this.stub =  PandaQueryServiceGrpc.newStub(channel);
        }
        return this.stub;
    }

    private PandaQueryServiceGrpc.PandaQueryServiceBlockingStub getBlockingStub() {
        if (this.blockingStub == null) {
            this.blockingStub =  PandaQueryServiceGrpc.newBlockingStub(channel);
        }
        return this.blockingStub;
    }

    @Override
    public boolean isEncrypted() {
        return false;
    }

    @Override
    public Session session() {
        return session(null);
    }

    @Override
    public Session session(SessionConfig sessionConfig) {
        return new PandaSession(getBlockingStub());
    }

    @Override
    public RxSession rxSession() {
        return null;
    }

    @Override
    public RxSession rxSession(SessionConfig sessionConfig) {
        return null;
    }

    @Override
    public AsyncSession asyncSession() {
        return asyncSession(null);
    }

    @Override
    public AsyncSession asyncSession(SessionConfig sessionConfig) {
        return null; //todo new PandaAsyncSession(getStub());
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(15, TimeUnit.SECONDS);
            channel.shutdownNow();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing driver instance %s", hashCode());
            return completedFuture(null);//channel.shutdown();
        }
        return completedFuture(null);//todo opt
    }

    @Override
    public Metrics metrics() {
        return null;
    }

    @Override
    public boolean isMetricsEnabled() {
        return false;
    }

    @Override
    public TypeSystem defaultTypeSystem() {
        return null;
    }

    @Override
    public void verifyConnectivity() {

    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync() {
        return null;
    }

    @Override
    public boolean supportsMultiDb() {
        return false;
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDbAsync() {
        return null;
    }
}
