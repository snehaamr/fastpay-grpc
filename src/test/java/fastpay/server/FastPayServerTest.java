package fastpay.server;

import fastpay.client.FastPayClient;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
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
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void clientCanTalkOverTls() throws Exception {
        Path dir = Files.createTempDirectory("fastpay-tls");
        Path cert = dir.resolve("server.crt");
        Path key = dir.resolve("server.key");
        Path ca = dir.resolve("ca.crt");
        Path db = dir.resolve("fastpay.db");
        Tls.ensureLocalhostCerts(cert, key, ca);
        RuntimeConfig tls = RuntimeConfig.plaintext()
                .withDb(db)
                .withTls(true, cert, key, ca);
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
}
