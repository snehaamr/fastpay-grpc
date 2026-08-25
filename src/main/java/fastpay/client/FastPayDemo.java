package fastpay.client;

/**
 * Starts the gRPC server if needed, runs the sample client, then shuts down
 * any server this process started.
 */
public final class FastPayDemo {
    public static void main(String[] args) throws Exception {
        FastPayClient.main(args);
    }
}
