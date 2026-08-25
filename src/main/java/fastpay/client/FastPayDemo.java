package fastpay.client;

import fastpay.server.FastPayServer;

/**
 * Starts the gRPC server, runs the sample client against it, then shuts down.
 * Use {@code ./gradlew runDemo} so you do not need a second terminal.
 */
public final class FastPayDemo {
    public static void main(String[] args) throws Exception {
        FastPayServer server = new FastPayServer(FastPayServer.DEFAULT_PORT);
        server.start();
        try {
            FastPayClient client = new FastPayClient("127.0.0.1", server.getPort());
            try {
                client.runUnary();
                client.runBidi();
            } finally {
                client.shutdown();
            }
        } finally {
            server.stop();
        }
    }
}
