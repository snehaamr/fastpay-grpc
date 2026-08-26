package fastpay.server;

import fastpay.fraud.FraudGuard;
import fastpay.ledger.InMemoryLedger;
import fastpay.proto.AccountQuery;
import fastpay.proto.AccountView;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.ListTransactionsQuery;
import fastpay.proto.ListTransactionsView;
import fastpay.proto.PaymentStatus;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.security.Auth;
import fastpay.security.AuthInterceptor;
import fastpay.security.ValidationInterceptor;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
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
    private String serverName;

    @BeforeEach
    void setUp() throws Exception {
        ledger = new InMemoryLedger();
        workerPool = Executors.newScheduledThreadPool(2);
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .intercept(new ValidationInterceptor())
                .intercept(new AuthInterceptor(Auth.DEFAULT_TOKEN))
                .addService(new FastPayServiceImpl(workerPool, ledger))
                .build()
                .start();
        bindAuthedStubs(serverName);
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
        TransactionRequest request = request("txn-123", "ACC-111", "ACC-222", 25075);

        TransactionResponse response = stub.processTransaction(request);

        assertTrue(response.getSuccess());
        assertEquals(PaymentStatus.SETTLED, response.getStatus());
        assertEquals("txn-123", response.getTransactionId());
        assertTrue(response.getMessage().contains("250.75"));
        assertEquals(25075, response.getAmountCents());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 25075, ledger.balanceCents("ACC-111"));
    }

    @Test
    void processTransactionReplayDoesNotMoveMoneyTwice() {
        TransactionRequest request = request("txn-idemp", "ACC-111", "ACC-222", 1000);
        TransactionResponse first = stub.processTransaction(request);
        TransactionResponse second = stub.processTransaction(request);

        assertTrue(first.getSuccess());
        assertFalse(first.getReplayed());
        assertTrue(second.getSuccess());
        assertTrue(second.getReplayed());
        assertEquals(PaymentStatus.SETTLED, second.getStatus());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 1000, ledger.balanceCents("ACC-111"));
    }

    @Test
    void processTransactionRejectsInvalidRequest() {
        TransactionRequest request = request("", "ACC-111", "ACC-222", 100);
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processTransaction(request)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    void missingAuthIsUnauthenticated() {
        ManagedChannel raw = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try {
            FastPayGrpc.FastPayBlockingStub naked = FastPayGrpc.newBlockingStub(raw);
            StatusRuntimeException ex = assertThrows(
                    StatusRuntimeException.class,
                    () -> naked.processTransaction(request("txn-noauth", "ACC-111", "ACC-222", 100))
            );
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        } finally {
            raw.shutdownNow();
        }
    }

    @Test
    void processTransactionInsufficientFunds() {
        TransactionResponse response = stub.processTransaction(
                request("txn-nsf", "ACC-POOR", "ACC-222", 500)
        );
        assertFalse(response.getSuccess());
        assertEquals(PaymentStatus.FAILED, response.getStatus());
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
        reqObs.onNext(request("bulk-1", "ACC-111", "ACC-222", 100));
        reqObs.onNext(request("bulk-1", "ACC-111", "ACC-222", 100));
        reqObs.onNext(request("bulk-2", "ACC-111", "ACC-222", 200));
        reqObs.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).getSuccess());
        assertTrue(responses.get(0).getMessage().contains("Posted 2"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 300, ledger.balanceCents("ACC-111"));
    }

    @Test
    void transactionStatusStreamsPostedPayment() {
        stub.processTransaction(request("txn-status", "ACC-111", "ACC-222", 100));
        Iterator<TransactionResponse> statuses = stub.transactionStatus(
                request("txn-status", "ACC-111", "ACC-222", 100));
        List<PaymentStatus> steps = new ArrayList<>();
        statuses.forEachRemaining(resp -> steps.add(resp.getStatus()));
        assertEquals(List.of(PaymentStatus.PENDING, PaymentStatus.AUTHORIZED, PaymentStatus.SETTLED), steps);
    }

    @Test
    void transactionStatusUnknownId() {
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.transactionStatus(request("missing", "ACC-111", "ACC-222", 100)).hasNext()
        );
        assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    }

    @Test
    void getAccountReturnsPostedBalance() {
        stub.processTransaction(request("txn-bal", "ACC-111", "ACC-222", 1000));
        AccountView view = stub.getAccount(AccountQuery.newBuilder().setAccountId("ACC-111").build());
        assertEquals("ACC-111", view.getAccountId());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 1000, view.getBalanceCents());
    }

    @Test
    void getAccountUnknown() {
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.getAccount(AccountQuery.newBuilder().setAccountId("ACC-MISSING").build())
        );
        assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    }

    @Test
    void listTransactionsFiltersByAccount() {
        stub.processTransaction(request("txn-list-1", "ACC-111", "ACC-222", 100));
        stub.processTransaction(request("txn-list-2", "ACC-AAA", "ACC-BBB", 200));
        ListTransactionsView view = stub.listTransactions(ListTransactionsQuery.newBuilder()
                .setAccountId("ACC-111")
                .setLimit(10)
                .build());
        assertEquals(1, view.getPaymentsCount());
        assertEquals("txn-list-1", view.getPayments(0).getTransactionId());
        assertEquals(100, view.getPayments(0).getAmountCents());
        assertEquals(PaymentStatus.SETTLED, view.getPayments(0).getStatus());
    }

    @Test
    void liveStreamFlagsAmountOverThreshold() throws Exception {
        restartWithFraud(new FraudGuard(500, 50, 10_000));
        CountDownLatch latch = new CountDownLatch(1);
        List<TransactionResponse> responses = new ArrayList<>();
        StreamObserver<TransactionRequest> reqObs = asyncStub.liveTransactions(new StreamObserver<>() {
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
        reqObs.onNext(request("fraud-amt", "ACC-111", "ACC-222", 501));
        reqObs.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, responses.size());
        assertFalse(responses.get(0).getSuccess());
        assertEquals(PaymentStatus.FLAGGED, responses.get(0).getStatus());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS, ledger.balanceCents("ACC-111"));
    }

    @Test
    void liveStreamFlagsVelocity() throws Exception {
        restartWithFraud(new FraudGuard(1_000_000, 2, 60_000));
        CountDownLatch latch = new CountDownLatch(1);
        List<TransactionResponse> responses = new ArrayList<>();
        StreamObserver<TransactionRequest> reqObs = asyncStub.liveTransactions(new StreamObserver<>() {
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
        reqObs.onNext(request("vel-1", "ACC-111", "ACC-222", 100));
        reqObs.onNext(request("vel-2", "ACC-111", "ACC-222", 100));
        reqObs.onNext(request("vel-3", "ACC-111", "ACC-222", 100));
        reqObs.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, responses.size());
        assertTrue(responses.get(0).getSuccess());
        assertTrue(responses.get(1).getSuccess());
        assertFalse(responses.get(2).getSuccess());
        assertEquals(PaymentStatus.FLAGGED, responses.get(2).getStatus());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 200, ledger.balanceCents("ACC-111"));
    }

    private void restartWithFraud(FraudGuard guard) throws Exception {
        tearDown();
        ledger = new InMemoryLedger();
        workerPool = Executors.newScheduledThreadPool(2);
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .intercept(new ValidationInterceptor())
                .intercept(new AuthInterceptor(Auth.DEFAULT_TOKEN))
                .addService(new FastPayServiceImpl(workerPool, ledger, guard))
                .build()
                .start();
        bindAuthedStubs(serverName);
    }

    private void bindAuthedStubs(String name) {
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        Channel authed = ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(Auth.DEFAULT_TOKEN))
        );
        stub = FastPayGrpc.newBlockingStub(authed).withDeadlineAfter(5, TimeUnit.SECONDS);
        asyncStub = FastPayGrpc.newStub(authed).withDeadlineAfter(15, TimeUnit.SECONDS);
    }

    private static TransactionRequest request(String id, String from, String to, long amountCents) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom(from)
                .setAccountTo(to)
                .setAmountCents(amountCents)
                .setCurrency("USD")
                .build();
    }
}
