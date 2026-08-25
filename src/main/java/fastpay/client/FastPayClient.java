package fastpay.client;

import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FastPayClient {
    private final ManagedChannel channel;
    private final FastPayGrpc.FastPayBlockingStub blockingStub;
    private final FastPayGrpc.FastPayStub asyncStub;

    public FastPayClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = FastPayGrpc.newBlockingStub(channel);
        this.asyncStub = FastPayGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void runUnary() {
        TransactionRequest req = TransactionRequest.newBuilder()
                .setTransactionId("txn-123")
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmount(250.75)
                .setCurrency("USD")
                .setClientTimestampNanos(System.nanoTime())
                .build();

        TransactionResponse resp = blockingStub.processTransaction(req);
        System.out.println("Unary response: " + resp.getMessage());
        TransactionResponse replay = blockingStub.processTransaction(req);
        System.out.println("Idempotent replay: " + replay.getMessage());
    }

    public void runBidi() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<TransactionRequest> reqObs = asyncStub.liveTransactions(new StreamObserver<>() {
            @Override
            public void onNext(TransactionResponse resp) {
                System.out.println("Live response: " + resp.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        for (int i = 0; i < 5; i++) {
            TransactionRequest req = TransactionRequest.newBuilder()
                    .setTransactionId("txn-" + i)
                    .setAccountFrom("ACC-AAA")
                    .setAccountTo("ACC-BBB")
                    .setAmount(100 + i)
                    .setCurrency("USD")
                    .setClientTimestampNanos(System.nanoTime())
                    .build();
            reqObs.onNext(req);
        }
        reqObs.onCompleted();
        latch.await(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        FastPayClient client = new FastPayClient("127.0.0.1", 6565);
        try {
            client.runUnary();
            client.runBidi();
        } finally {
            client.shutdown();
        }
    }
}
