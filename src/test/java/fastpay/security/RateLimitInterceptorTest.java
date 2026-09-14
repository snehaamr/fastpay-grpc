package fastpay.security;

import fastpay.ledger.Ledger;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.server.FastPayServiceImpl;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimitInterceptorTest {
    private io.grpc.Server server;
    private ManagedChannel channel;
    private ScheduledExecutorService workerPool;
    private Ledger ledger;

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
        if (ledger != null) {
            ledger.close();
        }
    }

    @Test
    void burstThenRejectsSameToken() throws Exception {
        AtomicLong now = new AtomicLong(0);
        FastPayGrpc.FastPayBlockingStub stub = start(1.0, 2, now::get, Auth.PAYMENTS_TOKEN);

        stub.processTransaction(tx("rl-1"));
        stub.processTransaction(tx("rl-2"));
        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processTransaction(tx("rl-3"))
        );
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.getStatus().getCode());
        assertTrue(ex.getStatus().getDescription().contains("rate limit exceeded"));
        assertEquals("1000", ex.getTrailers().get(RateLimitInterceptor.RETRY_PUSHBACK_MS));
    }

    @Test
    void refillsAfterElapsedTime() throws Exception {
        AtomicLong now = new AtomicLong(0);
        FastPayGrpc.FastPayBlockingStub stub = start(1.0, 1, now::get, Auth.PAYMENTS_TOKEN);

        stub.processTransaction(tx("rl-refill-1"));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, assertThrows(
                StatusRuntimeException.class,
                () -> stub.processTransaction(tx("rl-refill-2"))
        ).getStatus().getCode());

        now.addAndGet(1_000_000_000L);
        stub.processTransaction(tx("rl-refill-3"));
    }

    @Test
    void separateTokensHaveSeparateBuckets() throws Exception {
        AtomicLong now = new AtomicLong(0);
        start(1.0, 1, now::get, Auth.PAYMENTS_TOKEN);
        FastPayGrpc.FastPayBlockingStub pay = stubFor(Auth.PAYMENTS_TOKEN);
        FastPayGrpc.FastPayBlockingStub admin = stubFor(Auth.ADMIN_TOKEN);

        pay.processTransaction(tx("rl-pay"));
        admin.processTransaction(tx("rl-admin"));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, assertThrows(
                StatusRuntimeException.class,
                () -> pay.processTransaction(tx("rl-pay-2"))
        ).getStatus().getCode());
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, assertThrows(
                StatusRuntimeException.class,
                () -> admin.processTransaction(tx("rl-admin-2"))
        ).getStatus().getCode());
    }

    @Test
    void disabledLimiterAllowsBurst() throws Exception {
        FastPayGrpc.FastPayBlockingStub stub = start(0, 1, System::nanoTime, Auth.PAYMENTS_TOKEN);
        for (int i = 0; i < 8; i++) {
            stub.processTransaction(tx("rl-off-" + i));
        }
    }

    @Test
    void streamingLimitsEachMessage() throws Exception {
        AtomicLong now = new AtomicLong(0);
        start(1.0, 2, now::get, Auth.PAYMENTS_TOKEN);
        FastPayGrpc.FastPayStub async = asyncStubFor(Auth.PAYMENTS_TOKEN);

        CountDownLatch latch = new CountDownLatch(1);
        List<Status.Code> errors = new ArrayList<>();
        List<TransactionResponse> responses = new ArrayList<>();
        StreamObserver<TransactionRequest> reqObs = async.liveTransactions(new StreamObserver<>() {
            @Override
            public void onNext(TransactionResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException ex) {
                    errors.add(ex.getStatus().getCode());
                }
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        reqObs.onNext(tx("rl-live-1"));
        reqObs.onNext(tx("rl-live-2"));
        reqObs.onNext(tx("rl-live-3"));
        reqObs.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, responses.size());
        assertEquals(List.of(Status.Code.RESOURCE_EXHAUSTED), errors);
    }

    @Test
    void bucketKeyHashesToken() {
        String pay = RateLimitInterceptor.bucketKey(Auth.bearer(Auth.PAYMENTS_TOKEN));
        String admin = RateLimitInterceptor.bucketKey(Auth.bearer(Auth.ADMIN_TOKEN));
        assertFalse(pay.contains(Auth.PAYMENTS_TOKEN));
        assertEquals(TokenStore.sha256(Auth.PAYMENTS_TOKEN), pay);
        assertFalse(pay.equals(admin));
    }

    private FastPayGrpc.FastPayBlockingStub start(
            double qps,
            int burst,
            java.util.function.LongSupplier clock,
            String token
    ) throws Exception {
        ledger = new Ledger();
        workerPool = Executors.newScheduledThreadPool(2);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .intercept(new ValidationInterceptor())
                .intercept(new RateLimitInterceptor(qps, burst, clock))
                .intercept(new AuthInterceptor(ledger.tokenStore()))
                .addService(new FastPayServiceImpl(workerPool, ledger))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return stubFor(token);
    }

    private FastPayGrpc.FastPayBlockingStub stubFor(String token) {
        Channel authed = ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(token))
        );
        return FastPayGrpc.newBlockingStub(authed).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private FastPayGrpc.FastPayStub asyncStubFor(String token) {
        Channel authed = ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(token))
        );
        return FastPayGrpc.newStub(authed).withDeadlineAfter(15, TimeUnit.SECONDS);
    }

    private static TransactionRequest tx(String id) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmountCents(1)
                .setCurrency("USD")
                .build();
    }
}
