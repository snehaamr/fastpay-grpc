package fastpay.client;

import fastpay.ledger.Ledger;
import fastpay.proto.AccountQuery;
import fastpay.proto.AccountView;
import fastpay.proto.ApiKeyRole;
import fastpay.proto.CreateApiKeyRequest;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.ListAccountsQuery;
import fastpay.proto.ListAccountsView;
import fastpay.proto.ListJournalQuery;
import fastpay.proto.ListJournalView;
import fastpay.proto.ListTransactionsQuery;
import fastpay.proto.OpenAccountRequest;
import fastpay.proto.PaymentRecord;
import fastpay.proto.PaymentStatus;
import fastpay.proto.RefundRequest;
import fastpay.proto.RevokeApiKeyRequest;
import fastpay.proto.TransactionQuery;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.security.Auth;
import fastpay.security.RuntimeConfig;
import fastpay.security.Tls;
import fastpay.server.FastPayServer;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FastPayClient {
    private final ManagedChannel channel;
    private final FastPayGrpc.FastPayBlockingStub blockingStub;
    private final FastPayGrpc.FastPayBlockingStub adminStub;
    private final FastPayGrpc.FastPayStub asyncStub;

    public FastPayClient(String host, int port) throws IOException {
        this(host, port, RuntimeConfig.plaintext());
    }

    public FastPayClient(String host, int port, RuntimeConfig config) throws IOException {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        if (config.tls()) {
            builder.sslContext(Tls.clientContext(config.trustCert()));
        } else {
            builder.usePlaintext();
        }
        this.channel = builder.build();
        Channel authed = ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(config.authToken()))
        );
        Channel admin = ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(Auth.metadata(config.adminToken()))
        );
        this.blockingStub = FastPayGrpc.newBlockingStub(authed).withDeadlineAfter(5, TimeUnit.SECONDS);
        this.adminStub = FastPayGrpc.newBlockingStub(admin).withDeadlineAfter(5, TimeUnit.SECONDS);
        this.asyncStub = FastPayGrpc.newStub(authed).withDeadlineAfter(15, TimeUnit.SECONDS);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void runUnary() {
        TransactionRequest req = TransactionRequest.newBuilder()
                .setTransactionId("txn-123")
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmountCents(25075)
                .setCurrency("USD")
                .setMemo("invoice 123")
                .setClientTimestampNanos(System.nanoTime())
                .build();

        TransactionResponse resp = blockingStub.processTransaction(req);
        System.out.println("Unary response: " + resp.getMessage()
                + " status=" + resp.getStatus() + " replayed=" + resp.getReplayed()
                + " memo=" + resp.getMemo());
        TransactionResponse replay = blockingStub.processTransaction(req);
        System.out.println("Idempotent replay: " + replay.getMessage()
                + " status=" + replay.getStatus() + " replayed=" + replay.getReplayed());
        printAccount("ACC-111");
        printAccount("ACC-222");

        Iterator<TransactionResponse> statuses = blockingStub.transactionStatus(
                TransactionQuery.newBuilder().setTransactionId("txn-123").build());
        List<PaymentStatus> steps = new ArrayList<>();
        statuses.forEachRemaining(status -> steps.add(status.getStatus()));
        System.out.println("Status stream: " + steps);

        PaymentRecord payment = blockingStub.getPayment(
                TransactionQuery.newBuilder().setTransactionId("txn-123").build());
        System.out.println("GetPayment: " + payment.getTransactionId()
                + " status=" + payment.getStatus()
                + " memo=" + payment.getMemo()
                + " created_at_millis=" + payment.getCreatedAtMillis());
    }

    public void runOpenRefundAndJournal() {
        String accountId = "ACC-NEW-" + System.currentTimeMillis();
        AccountView opened = blockingStub.openAccount(OpenAccountRequest.newBuilder()
                .setAccountId(accountId)
                .setOpeningCents(50_000)
                .setCurrency("USD")
                .build());
        System.out.println("Opened " + opened.getAccountId() + " balance="
                + Ledger.formatAmount(opened.getBalanceCents()) + " " + opened.getCurrency());

        TransactionResponse refund = blockingStub.refundTransaction(RefundRequest.newBuilder()
                .setTransactionId("txn-123")
                .build());
        System.out.println("Refund: " + refund.getMessage()
                + " status=" + refund.getStatus() + " replayed=" + refund.getReplayed());
        printAccount("ACC-111");
        printAccount("ACC-222");

        ListTransactionsQuery listQuery = ListTransactionsQuery.newBuilder()
                .setAccountId("ACC-111")
                .setLimit(5)
                .build();
        var payments = blockingStub.listTransactions(listQuery);
        payments.getPaymentsList().forEach(payment ->
                System.out.println("Payment " + payment.getTransactionId()
                        + " " + payment.getStatus()
                        + " refund_of=" + payment.getRefundOf()));
        if (!payments.getNextPageToken().isBlank()) {
            System.out.println("More payments (next_page_token set)");
        }

        ListAccountsView accounts = blockingStub.listAccounts(ListAccountsQuery.newBuilder()
                .setLimit(3)
                .build());
        accounts.getAccountsList().forEach(account ->
                System.out.println("Account " + account.getAccountId()
                        + " balance=" + Ledger.formatAmount(account.getBalanceCents())));
        if (!accounts.getNextPageToken().isBlank()) {
            ListAccountsView page2 = blockingStub.listAccounts(ListAccountsQuery.newBuilder()
                    .setLimit(3)
                    .setPageToken(accounts.getNextPageToken())
                    .build());
            System.out.println("Account page 2: " + page2.getAccountsCount() + " more");
        }

        ListJournalView journal = adminStub.listJournal(ListJournalQuery.newBuilder()
                .setAccountId("ACC-111")
                .setLimit(6)
                .build());
        journal.getEntriesList().forEach(entry ->
                System.out.println("Journal " + entry.getTransactionId()
                        + " " + entry.getAccountId()
                        + " delta=" + entry.getDeltaCents()));

        String keyLabel = "demo-" + System.currentTimeMillis();
        var created = adminStub.createApiKey(CreateApiKeyRequest.newBuilder()
                .setLabel(keyLabel)
                .setRole(ApiKeyRole.PAYMENTS)
                .build());
        System.out.println("Created API key label=" + created.getLabel()
                + " role=" + created.getRole()
                + " token=" + created.getToken().substring(0, Math.min(12, created.getToken().length())) + "...");
        var revoked = adminStub.revokeApiKey(RevokeApiKeyRequest.newBuilder()
                .setLabel(keyLabel)
                .build());
        System.out.println("Revoked API key label=" + revoked.getLabel());
    }

    public void runBulk() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<TransactionResponse> responses = new ArrayList<>();
        StreamObserver<TransactionRequest> reqObs = asyncStub.uploadTransactions(new StreamObserver<>() {
            @Override
            public void onNext(TransactionResponse value) {
                responses.add(value);
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
        long suffix = System.currentTimeMillis();
        reqObs.onNext(request("bulk-" + suffix + "-a", "ACC-111", "ACC-222", 150));
        reqObs.onNext(request("bulk-" + suffix + "-b", "ACC-111", "ACC-222", 250));
        reqObs.onCompleted();
        latch.await(5, TimeUnit.SECONDS);
        if (!responses.isEmpty()) {
            System.out.println("Bulk upload: " + responses.get(0).getMessage());
        }
    }

    private void printAccount(String accountId) {
        AccountView view = blockingStub.getAccount(AccountQuery.newBuilder().setAccountId(accountId).build());
        System.out.println(view.getAccountId() + " balance="
                + Ledger.formatAmount(view.getBalanceCents()) + " " + view.getCurrency());
    }

    public void runBidi() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<TransactionRequest> reqObs = asyncStub.liveTransactions(new StreamObserver<>() {
            @Override
            public void onNext(TransactionResponse resp) {
                System.out.println("Live response: " + resp.getMessage() + " status=" + resp.getStatus());
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
            reqObs.onNext(request("txn-" + i, "ACC-AAA", "ACC-BBB", 10_000 + (i * 100L)));
        }
        reqObs.onCompleted();
        latch.await(5, TimeUnit.SECONDS);
    }

    public static void runSample(String host, int port, boolean startLocalServerIfNeeded) throws Exception {
        runSample(host, port, startLocalServerIfNeeded, RuntimeConfig.plaintext());
    }

    public static void runSample(String host, int port, boolean startLocalServerIfNeeded, RuntimeConfig config)
            throws Exception {
        FastPayServer localServer = null;
        int targetPort = port;
        if (startLocalServerIfNeeded && !isReachable(host, port)) {
            localServer = new FastPayServer(port, config);
            localServer.start();
            targetPort = localServer.getPort();
            System.out.println("No server was listening; started a local FastPay server for this client run.");
        }
        FastPayClient client = new FastPayClient(host, targetPort, config);
        try {
            client.runUnary();
            client.runOpenRefundAndJournal();
            client.runBulk();
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

    private static TransactionRequest request(String id, String from, String to, long amountCents) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom(from)
                .setAccountTo(to)
                .setAmountCents(amountCents)
                .setCurrency("USD")
                .setClientTimestampNanos(System.nanoTime())
                .build();
    }

    public static void main(String[] args) throws Exception {
        RuntimeConfig config = RuntimeConfig.fromEnv();
        runSample("127.0.0.1", FastPayServer.DEFAULT_PORT, true, config);
    }
}
