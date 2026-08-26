package fastpay.server;

import fastpay.fraud.FraudGuard;
import fastpay.ledger.InMemoryLedger;
import fastpay.security.AuthInterceptor;
import fastpay.security.RuntimeConfig;
import fastpay.security.Tls;
import fastpay.security.ValidationInterceptor;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FastPayServer {
    public static final int DEFAULT_PORT = 6565;

    private final ScheduledExecutorService workerPool;
    private final Server server;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public FastPayServer(int port) throws IOException {
        this(port, RuntimeConfig.plaintext(), new InMemoryLedger(), new FraudGuard());
    }

    public FastPayServer(int port, RuntimeConfig config) throws IOException {
        this(port, config, new InMemoryLedger(), new FraudGuard());
    }

    public FastPayServer(int port, RuntimeConfig config, InMemoryLedger ledger, FraudGuard fraudGuard)
            throws IOException {
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.workerPool = Executors.newScheduledThreadPool(threads);
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
                .addService(new FastPayServiceImpl(workerPool, ledger, fraudGuard))
                .intercept(new ValidationInterceptor())
                .intercept(new AuthInterceptor(config.authToken()))
                .maxInboundMessageSize(16 * 1024 * 1024)
                .directExecutor();
        if (config.tls()) {
            builder.sslContext(Tls.serverContext(config.cert(), config.key()));
        }
        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("FastPay gRPC server started on port " + getPort());
    }

    public int getPort() {
        return server.getPort();
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        server.shutdown();
        workerPool.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        RuntimeConfig config = RuntimeConfig.fromEnv();
        FastPayServer server = new FastPayServer(DEFAULT_PORT, config);
        server.start();
        System.out.println("tls=" + config.tls() + " auth=" + (config.authToken().isBlank() ? "off" : "Bearer token"));
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.awaitTermination();
    }
}
