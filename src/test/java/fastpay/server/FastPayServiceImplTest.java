package fastpay.server;

import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FastPayServiceImplTest {
    private Server server;
    private ManagedChannel channel;
    private ScheduledExecutorService workerPool;
    private FastPayGrpc.FastPayBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        workerPool = Executors.newScheduledThreadPool(2);
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new FastPayServiceImpl(workerPool))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
        stub = FastPayGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (workerPool != null) {
            workerPool.shutdownNow();
        }
    }

    @Test
    void processTransactionReturnsSuccess() {
        TransactionRequest request = TransactionRequest.newBuilder()
                .setTransactionId("txn-123")
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmount(250.75)
                .setCurrency("USD")
                .build();

        TransactionResponse response = stub.processTransaction(request);

        assertTrue(response.getSuccess());
        assertEquals("txn-123", response.getTransactionId());
        assertTrue(response.getMessage().contains("250.75"));
        assertTrue(response.getMessage().contains("ACC-111"));
        assertTrue(response.getMessage().contains("ACC-222"));
        assertTrue(response.getProcessingNanos() >= 0);
        assertTrue(response.getServerTimestampNanos() > 0);
    }
}
