package fastpay.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FastPayServer {
    public static final int DEFAULT_PORT = 6565;

    private final ScheduledExecutorService workerPool;
    private final Server server;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public FastPayServer(int port) {
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.workerPool = Executors.newScheduledThreadPool(threads);
        this.server = ServerBuilder.forPort(port)
                .addService(new FastPayServiceImpl(workerPool))
                .maxInboundMessageSize(16 * 1024 * 1024)
                .directExecutor()
                .build();
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
        FastPayServer server = new FastPayServer(DEFAULT_PORT);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.awaitTermination();
    }
}
