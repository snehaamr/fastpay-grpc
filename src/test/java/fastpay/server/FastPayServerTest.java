package fastpay.server;

import fastpay.client.FastPayClient;
import fastpay.security.Auth;
import fastpay.security.RuntimeConfig;
import fastpay.security.Tls;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        RuntimeConfig tls = new RuntimeConfig(
                true,
                cert,
                key,
                ca,
                db,
                Auth.PAYMENTS_TOKEN,
                Auth.ADMIN_TOKEN
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
}
