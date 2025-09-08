package fastpay.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FastPayServer {
    public static void main(String[] args) throws Exception {
        int port = 6565;
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        ScheduledExecutorService workerPool = Executors.newScheduledThreadPool(threads);

        Server server = NettyServerBuilder.forAddress(new InetSocketAddress(port))
                .addService(new FastPayServiceImpl(workerPool))
                .maxInboundMessageSize(16 * 1024 * 1024) // tuneable
                .directExecutor() // low latency path
                .build();

        server.start();
        System.out.println("🚀 FastPay gRPC server started on port " + port);
        server.awaitTermination();
    }
}
