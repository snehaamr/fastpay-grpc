package fastpay.server;

import fastpay.fraud.FraudGuard;
import fastpay.ledger.InMemoryLedger;
import fastpay.security.AuthInterceptor;
import fastpay.security.RateLimitInterceptor;
import fastpay.security.RuntimeConfig;
import fastpay.security.Tls;
import fastpay.security.ValidationInterceptor;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FastPayServer {
    public static final int DEFAULT_PORT = 6565;

    private final ScheduledExecutorService workerPool;
    private final Server server;
    private final InMemoryLedger ledger;
    private final HealthStatusManager health;
    private final boolean closeLedger;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public FastPayServer(int port) throws IOException {
        this(port, RuntimeConfig.plaintext(), new InMemoryLedger(), new FraudGuard(), true);
    }

    public FastPayServer(int port, RuntimeConfig config) throws IOException {
        this(port, config, openLedger(config), new FraudGuard(), true);
    }

    public FastPayServer(int port, RuntimeConfig config, InMemoryLedger ledger, FraudGuard fraudGuard)
            throws IOException {
        this(port, config, ledger, fraudGuard, false);
    }

    private FastPayServer(
            int port,
            RuntimeConfig config,
            InMemoryLedger ledger,
            FraudGuard fraudGuard,
            boolean closeLedger
    ) throws IOException {
        this.ledger = ledger;
        this.closeLedger = closeLedger;
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.workerPool = Executors.newScheduledThreadPool(threads);
        this.health = new HealthStatusManager();
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        health.setStatus("fastpay.FastPay", HealthCheckResponse.ServingStatus.SERVING);
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
                .addService(new FastPayServiceImpl(workerPool, ledger, fraudGuard))
                .addService(health.getHealthService())
                .addService(newReflectionService())
                .intercept(new ValidationInterceptor())
                .intercept(new RateLimitInterceptor(config.rateLimitQps(), config.rateLimitBurst()))
                .intercept(new AuthInterceptor(ledger.tokenStore()))
                .maxInboundMessageSize(16 * 1024 * 1024)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .permitKeepAliveTime(5, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true);
        if (config.tls()) {
            if (config.mtls()) {
                Tls.ensureLocalhostMtls(
                        config.trustCert(), config.cert(), config.key(),
                        config.clientCert(), config.clientKey());
                builder.sslContext(Tls.serverContext(
                        config.cert(), config.key(), config.trustCert(), true));
            } else {
                Tls.ensureLocalhostCerts(config.cert(), config.key(), config.trustCert());
                builder.sslContext(Tls.serverContext(config.cert(), config.key()));
            }
        }
        this.server = builder.build();
    }

    private static InMemoryLedger openLedger(RuntimeConfig config) throws IOException {
        Path parent = config.db().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return new InMemoryLedger(config.db(), config.paymentsToken(), config.adminToken());
    }

    @SuppressWarnings("deprecation")
    private static io.grpc.BindableService newReflectionService() {
        return ProtoReflectionService.newInstance();
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
        health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        health.setStatus("fastpay.FastPay", HealthCheckResponse.ServingStatus.NOT_SERVING);
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
        if (closeLedger) {
            ledger.close();
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        RuntimeConfig config = RuntimeConfig.fromEnv();
        FastPayServer server = new FastPayServer(DEFAULT_PORT, config);
        server.start();
        System.out.println("tls=" + formatTls(config) + " db=" + config.db()
                + " rate_limit=" + formatRateLimit(config)
                + " roles=pay-token/admin-token (override FASTPAY_PAY_TOKEN / FASTPAY_ADMIN_TOKEN)");
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.awaitTermination();
    }

    private static String formatRateLimit(RuntimeConfig config) {
        if (config.rateLimitQps() <= 0) {
            return "off";
        }
        return config.rateLimitQps() + "/s burst=" + config.rateLimitBurst();
    }

    private static String formatTls(RuntimeConfig config) {
        if (config.mtls()) {
            return "mtls";
        }
        if (config.tls()) {
            return "server";
        }
        return "off";
    }
}
