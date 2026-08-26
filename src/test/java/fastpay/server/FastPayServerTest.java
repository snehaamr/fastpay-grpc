package fastpay.server;

import fastpay.client.FastPayClient;
import fastpay.security.Auth;
import fastpay.security.RuntimeConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

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
    void runSampleStartsServerWhenNothingIsListening() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        FastPayClient.runSample("127.0.0.1", port, true);
    }

    @Test
    @EnabledIf("certsExist")
    void clientCanTalkOverTls() throws Exception {
        RuntimeConfig tls = new RuntimeConfig(
                true,
                Path.of("certs/server.crt"),
                Path.of("certs/server.key"),
                Path.of("certs/ca.crt"),
                Auth.DEFAULT_TOKEN
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

    static boolean certsExist() {
        return Files.isRegularFile(Path.of("certs/server.crt"))
                && Files.isRegularFile(Path.of("certs/server.key"));
    }
}
