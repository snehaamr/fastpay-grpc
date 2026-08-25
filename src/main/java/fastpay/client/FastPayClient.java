package fastpay.client;

import fastpay.ledger.InMemoryLedger;
import fastpay.proto.AccountQuery;
import fastpay.proto.AccountView;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.server.FastPayServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
        System.out.println("Unary response: " + resp.getMessage() + " replayed=" + resp.getReplayed());
        TransactionResponse replay = blockingStub.processTransaction(req);
        System.out.println("Idempotent replay: " + replay.getMessage() + " replayed=" + replay.getReplayed());
        printAccount("ACC-111");
        printAccount("ACC-222");
    }

    private void printAccount(String accountId) {
        AccountView view = blockingStub.getAccount(AccountQuery.newBuilder().setAccountId(accountId).build());
        System.out.println(view.getAccountId() + " balance="
                + InMemoryLedger.formatAmount(view.getBalanceCents()) + " " + view.getCurrency());
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

    /**
     * Runs the sample unary + live calls. If {@code startLocalServerIfNeeded} is true and
     * nothing accepts connections on {@code port}, starts an in-process server first.
     */
    public static void runSample(String host, int port, boolean startLocalServerIfNeeded) throws Exception {
        FastPayServer localServer = null;
        int targetPort = port;
        if (startLocalServerIfNeeded && !isReachable(host, port)) {
            localServer = new FastPayServer(port);
            localServer.start();
            targetPort = localServer.getPort();
            System.out.println("No server was listening; started a local FastPay server for this client run.");
        }
        FastPayClient client = new FastPayClient(host, targetPort);
        try {
            client.runUnary();
            client.runBidi();
        } finally {
            client.shutdown();
            if (localServer != null) {
                localServer.stop();
            }
        }
    }

    static boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 500);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        runSample("127.0.0.1", FastPayServer.DEFAULT_PORT, true);
    }
}
