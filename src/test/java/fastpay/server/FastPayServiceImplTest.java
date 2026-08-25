package fastpay.server;

import fastpay.ledger.InMemoryLedger;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FastPayServiceImplTest {
    private Server server;
    private ManagedChannel channel;
    private ScheduledExecutorService workerPool;
    private InMemoryLedger ledger;
    private FastPayGrpc.FastPayBlockingStub stub;
    private FastPayGrpc.FastPayStub asyncStub;

    @BeforeEach
    void setUp() throws Exception {
        ledger = new InMemoryLedger();
        workerPool = Executors.newScheduledThreadPool(2);
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new FastPayServiceImpl(workerPool, ledger))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
        stub = FastPayGrpc.newBlockingStub(channel);
        asyncStub = FastPayGrpc.newStub(channel);
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
        TransactionRequest request = request("txn-123", "ACC-111", "ACC-222", 250.75);

        TransactionResponse response = stub.processTransaction(request);

        assertTrue(response.getSuccess());
        assertEquals("txn-123", response.getTransactionId());
        assertTrue(response.getMessage().contains("250.75"));
        assertTrue(response.getMessage().contains("ACC-111"));
        assertTrue(response.getMessage().contains("ACC-222"));
        assertTrue(response.getProcessingNanos() >= 0);
        assertTrue(response.getServerTimestampNanos() > 0);
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 25075, ledger.balanceCents("ACC-111"));
    }

    @Test
    void processTransactionReplayDoesNotMoveMoneyTwice() {
        TransactionRequest request = request("txn-idemp", "ACC-111", "ACC-222", 10.00);
        TransactionResponse first = stub.processTransaction(request);
        TransactionResponse second = stub.processTransaction(request);

        assertTrue(first.getSuccess());
        assertTrue(second.getSuccess());
        assertEquals(first.getMessage(), second.getMessage());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 1000, ledger.balanceCents("ACC-111"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS + 1000, ledger.balanceCents("ACC-222"));
    }

    @Test
    void processTransactionRejectsInvalidRequest() {
        TransactionRequest request = request("", "ACC-111", "ACC-222", 1.00);
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processTransaction(request)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    void processTransactionInsufficientFunds() {
        TransactionResponse response = stub.processTransaction(
                request("txn-nsf", "ACC-POOR", "ACC-222", 5.00)
        );
        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("Insufficient funds"));
        assertEquals(InMemoryLedger.POOR_OPENING_CENTS, ledger.balanceCents("ACC-POOR"));
    }

    @Test
    void uploadTransactionsPostsEachItem() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<TransactionResponse> responses = new ArrayList<>();
        StreamObserver<TransactionRequest> reqObs = asyncStub.uploadTransactions(new StreamObserver<>() {
            @Override
            public void onNext(TransactionResponse value) {
                responses.add(value);
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
        reqObs.onNext(request("bulk-1", "ACC-111", "ACC-222", 1.00));
        reqObs.onNext(request("bulk-1", "ACC-111", "ACC-222", 1.00));
        reqObs.onNext(request("bulk-2", "ACC-111", "ACC-222", 2.00));
        reqObs.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).getSuccess());
        assertTrue(responses.get(0).getMessage().contains("Posted 2"));
        assertTrue(responses.get(0).getMessage().contains("replayed=1"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 300, ledger.balanceCents("ACC-111"));
    }

    @Test
    void transactionStatusStreamsPostedPayment() {
        stub.processTransaction(request("txn-status", "ACC-111", "ACC-222", 1.00));
        Iterator<TransactionResponse> statuses = stub.transactionStatus(request("txn-status", "ACC-111", "ACC-222", 1.00));
        List<String> messages = new ArrayList<>();
        statuses.forEachRemaining(resp -> messages.add(resp.getMessage()));
        assertEquals(3, messages.size());
        assertTrue(messages.get(0).contains("initiated"));
        assertTrue(messages.get(1).contains("authorized"));
        assertTrue(messages.get(2).contains("settled"));
    }

    @Test
    void transactionStatusUnknownId() {
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.transactionStatus(request("missing", "ACC-111", "ACC-222", 1.00)).hasNext()
        );
        assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    }

    private static TransactionRequest request(String id, String from, String to, double amount) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom(from)
                .setAccountTo(to)
                .setAmount(amount)
                .setCurrency("USD")
                .build();
    }
}
