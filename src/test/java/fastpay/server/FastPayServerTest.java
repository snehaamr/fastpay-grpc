package fastpay.server;

import fastpay.client.FastPayClient;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.PaymentStatus;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.security.Auth;
import fastpay.security.RuntimeConfig;
import fastpay.security.Tls;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FastPayServerTest {
    @Test
    void clientCanTalkToStartedServer() throws Exception {
        FastPayServer server = new FastPayServer(0);
        server.start();
        FastPayClient client = new FastPayClient("127.0.0.1", server.getPort());
        try {
            client.runUnary();
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    void healthCheckDoesNotRequireAuth() throws Exception {
        FastPayServer server = new FastPayServer(0);
        server.start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
        try {
            HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                    .check(HealthCheckRequest.getDefaultInstance());
            assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.stop();
        }
    }

    @Test
    void healthIsNotRateLimited() throws Exception {
        Path db = Files.createTempFile("fastpay-rl", ".db");
        Files.deleteIfExists(db);
        RuntimeConfig config = RuntimeConfig.plaintext().withDb(db).withRateLimit(1, 1);
        FastPayServer server = new FastPayServer(0, config);
        server.start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
        try {
            Channel authed = ClientInterceptors.intercept(
                    channel,
                    MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(Auth.PAYMENTS_TOKEN))
            );
            FastPayGrpc.FastPayBlockingStub stub = FastPayGrpc.newBlockingStub(authed)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
            stub.processTransaction(TransactionRequest.newBuilder()
                    .setTransactionId("rl-health-1")
                    .setAccountFrom("ACC-111")
                    .setAccountTo("ACC-222")
                    .setAmountCents(1)
                    .setCurrency("USD")
                    .build());
            StatusRuntimeException ex = assertThrows(
                    StatusRuntimeException.class,
                    () -> stub.processTransaction(TransactionRequest.newBuilder()
                            .setTransactionId("rl-health-2")
                            .setAccountFrom("ACC-111")
                            .setAccountTo("ACC-222")
                            .setAmountCents(1)
                            .setCurrency("USD")
                            .build())
            );
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.getStatus().getCode());
            HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                    .check(HealthCheckRequest.getDefaultInstance());
            assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.stop();
        }
    }

    @Test
    void runSampleStartsServerWhenNothingIsListening() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path db = Files.createTempFile("fastpay-sample", ".db");
        Files.deleteIfExists(db);
        FastPayClient.runSample("127.0.0.1", port, true, RuntimeConfig.plaintext().withDb(db));
    }

    @Test
    void metricsHttpIsOffWhenPortIsZero() throws Exception {
        FastPayServer server = new FastPayServer(0);
        server.start();
        try {
            assertEquals(-1, server.metricsPort());
        } finally {
            server.stop();
        }
    }

    @Test
    void metricsScrapeIncludesRpcLedgerFraudAndRateLimit() throws Exception {
        Path db = Files.createTempFile("fastpay-metrics", ".db");
        Files.deleteIfExists(db);
        int metricsPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            metricsPort = socket.getLocalPort();
        }
        RuntimeConfig config = RuntimeConfig.plaintext()
                .withDb(db)
                .withRateLimit(0.001, 2)
                .withMetricsPort(metricsPort);
        FastPayServer server = new FastPayServer(0, config);
        server.start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
        try {
            assertEquals(metricsPort, server.metricsPort());
            Channel authed = ClientInterceptors.intercept(
                    channel,
                    MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(Auth.PAYMENTS_TOKEN))
            );
            FastPayGrpc.FastPayBlockingStub stub = FastPayGrpc.newBlockingStub(authed)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
            stub.processTransaction(tx("metrics-ok", 1));

            FastPayGrpc.FastPayStub async = FastPayGrpc.newStub(authed)
                    .withDeadlineAfter(15, TimeUnit.SECONDS);
            CountDownLatch latch = new CountDownLatch(1);
            List<TransactionResponse> live = new ArrayList<>();
            StreamObserver<TransactionRequest> reqObs = async.liveTransactions(new StreamObserver<>() {
                @Override
                public void onNext(TransactionResponse value) {
                    live.add(value);
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
            reqObs.onNext(tx("metrics-fraud", 100_001));
            reqObs.onCompleted();
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(1, live.size());
            assertFalse(live.get(0).getSuccess());
            assertEquals(PaymentStatus.FLAGGED, live.get(0).getStatus());

            StatusRuntimeException ex = assertThrows(
                    StatusRuntimeException.class,
                    () -> stub.processTransaction(tx("metrics-rl", 1))
            );
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.getStatus().getCode());

            assertEquals(1.0d, server.metrics().rpcCount("ProcessTransaction", Status.Code.OK));
            assertEquals(1.0d, server.metrics().rpcCount("ProcessTransaction", Status.Code.RESOURCE_EXHAUSTED));
            assertEquals(1.0d, server.metrics().rateLimitRejectCount());
            assertEquals(1.0d, server.metrics().fraudCount("amount"));
            assertTrue(server.metrics().ledgerCount() >= 2);

            String body = scrape(server.metricsPort());
            assertTrue(body.contains("fastpay_rpc_requests_total"));
            assertTrue(body.contains("fastpay_ledger_duration_seconds"));
            assertTrue(body.contains("fastpay_fraud_flags_total"));
            assertTrue(body.contains("fastpay_rate_limit_rejects_total"));
            assertTrue(body.contains("reason=\"amount\""));
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.stop();
        }
    }

    @Test
    void clientCanTalkOverTls() throws Exception {
        Path dir = Files.createTempDirectory("fastpay-tls");
        Path cert = dir.resolve("server.crt");
        Path key = dir.resolve("server.key");
        Path ca = dir.resolve("ca.crt");
        Path db = dir.resolve("fastpay.db");
        Tls.ensureLocalhostCerts(cert, key, ca);
        RuntimeConfig tls = new RuntimeConfig(
                true,
                cert,
                key,
                ca,
                db,
                Auth.PAYMENTS_TOKEN,
                Auth.ADMIN_TOKEN,
                RuntimeConfig.DEFAULT_RATE_LIMIT_QPS,
                RuntimeConfig.DEFAULT_RATE_LIMIT_BURST,
                0
        );
        FastPayServer server = new FastPayServer(0, tls);
        server.start();
        FastPayClient client = new FastPayClient("localhost", server.getPort(), tls);
        try {
            client.runUnary();
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    private static TransactionRequest tx(String id, long amountCents) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmountCents(amountCents)
                .setCurrency("USD")
                .build();
    }

    private static String scrape(int port) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals(200, response.statusCode());
        return response.body();
    }
}
