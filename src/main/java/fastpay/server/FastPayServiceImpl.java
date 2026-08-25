package fastpay.server;

import fastpay.ledger.AccountSnapshot;
import fastpay.ledger.InMemoryLedger;
import fastpay.ledger.InvalidTransactionException;
import fastpay.ledger.PostedTransaction;
import fastpay.ledger.SubmitResult;
import fastpay.proto.AccountQuery;
import fastpay.proto.AccountView;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.ListTransactionsQuery;
import fastpay.proto.ListTransactionsView;
import fastpay.proto.PaymentRecord;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FastPayServiceImpl extends FastPayGrpc.FastPayImplBase {
    private static final Logger log = LoggerFactory.getLogger(FastPayServiceImpl.class);

    private final ScheduledExecutorService workerPool;
    private final InMemoryLedger ledger;

    public FastPayServiceImpl(ScheduledExecutorService workerPool) {
        this(workerPool, new InMemoryLedger());
    }

    public FastPayServiceImpl(ScheduledExecutorService workerPool, InMemoryLedger ledger) {
        this.workerPool = workerPool;
        this.ledger = ledger;
    }

    private long nowNanos() {
        return System.nanoTime();
    }

    @Override
    public void processTransaction(TransactionRequest req, StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        workerPool.execute(() -> {
            try {
                SubmitResult result = ledger.submit(req);
                complete(respObs, toResponse(result, start));
            } catch (InvalidTransactionException e) {
                respObs.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("processTransaction failed", e);
                respObs.onError(Status.INTERNAL.withDescription("ledger error").asRuntimeException());
            }
        });
    }

    @Override
    public StreamObserver<TransactionRequest> uploadTransactions(StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        return new StreamObserver<>() {
            final AtomicInteger posted = new AtomicInteger();
            final AtomicInteger failed = new AtomicInteger();
            final AtomicInteger replayed = new AtomicInteger();
            final AtomicLong totalCents = new AtomicLong();

            @Override
            public void onNext(TransactionRequest req) {
                try {
                    SubmitResult result = ledger.submit(req);
                    if (result.replayed()) {
                        replayed.incrementAndGet();
                    } else if (result.transaction().success()) {
                        posted.incrementAndGet();
                        totalCents.addAndGet(result.transaction().amountCents());
                    } else {
                        failed.incrementAndGet();
                    }
                } catch (InvalidTransactionException e) {
                    failed.incrementAndGet();
                }
            }

            @Override
            public void onError(Throwable t) {
                log.warn("uploadTransactions stream failed", t);
            }

            @Override
            public void onCompleted() {
                boolean success = failed.get() == 0;
                String message = "Posted " + posted.get()
                        + ", failed=" + failed.get()
                        + ", replayed=" + replayed.get()
                        + ", total=" + InMemoryLedger.formatAmount(totalCents.get());
                respObs.onNext(TransactionResponse.newBuilder()
                        .setTransactionId("bulk-upload")
                        .setSuccess(success)
                        .setMessage(message)
                        .setServerTimestampNanos(nowNanos())
                        .setProcessingNanos(nowNanos() - start)
                        .build());
                respObs.onCompleted();
            }
        };
    }

    @Override
    public void transactionStatus(TransactionRequest req, StreamObserver<TransactionResponse> respObs) {
        workerPool.execute(() -> {
            try {
                var posted = ledger.find(req.getTransactionId());
                if (posted.isEmpty()) {
                    respObs.onError(Status.NOT_FOUND
                            .withDescription("unknown transaction_id: " + req.getTransactionId())
                            .asRuntimeException());
                    return;
                }
                PostedTransaction tx = posted.get();
                String[] steps = tx.success()
                        ? new String[]{"initiated", "authorized", "settled"}
                        : new String[]{"initiated", "rejected", "not settled"};
                for (int i = 0; i < steps.length; i++) {
                    respObs.onNext(TransactionResponse.newBuilder()
                            .setTransactionId(tx.transactionId())
                            .setSuccess(tx.success())
                            .setMessage("Status " + steps[i] + " for transaction " + tx.transactionId())
                            .setServerTimestampNanos(nowNanos())
                            .setProcessingNanos(1000L * (i + 1))
                            .build());
                }
                respObs.onCompleted();
            } catch (RuntimeException e) {
                log.error("transactionStatus failed", e);
                respObs.onError(Status.INTERNAL.withDescription("status error").asRuntimeException());
            }
        });
    }

    @Override
    public StreamObserver<TransactionRequest> liveTransactions(StreamObserver<TransactionResponse> respObs) {
        return new StreamObserver<>() {
            @Override
            public void onNext(TransactionRequest req) {
                long start = nowNanos();
                try {
                    SubmitResult result = ledger.submit(req);
                    respObs.onNext(toResponse(result, start));
                } catch (InvalidTransactionException e) {
                    respObs.onNext(TransactionResponse.newBuilder()
                            .setTransactionId(req.getTransactionId())
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .setServerTimestampNanos(nowNanos())
                            .setProcessingNanos(nowNanos() - start)
                            .build());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.warn("liveTransactions stream failed", t);
            }

            @Override
            public void onCompleted() {
                respObs.onCompleted();
            }
        };
    }

    @Override
    public void getAccount(AccountQuery req, StreamObserver<AccountView> respObs) {
        try {
            AccountSnapshot snapshot = ledger.getAccount(req.getAccountId());
            respObs.onNext(AccountView.newBuilder()
                    .setAccountId(snapshot.accountId())
                    .setBalanceCents(snapshot.balanceCents())
                    .setCurrency(snapshot.currency())
                    .build());
            respObs.onCompleted();
        } catch (InvalidTransactionException e) {
            Status status = e.getMessage().contains("required")
                    ? Status.INVALID_ARGUMENT
                    : Status.NOT_FOUND;
            respObs.onError(status.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void listTransactions(ListTransactionsQuery req, StreamObserver<ListTransactionsView> respObs) {
        ListTransactionsView.Builder view = ListTransactionsView.newBuilder();
        for (PostedTransaction payment : ledger.listPayments(req.getAccountId(), req.getLimit())) {
            view.addPayments(PaymentRecord.newBuilder()
                    .setTransactionId(payment.transactionId())
                    .setAccountFrom(payment.accountFrom())
                    .setAccountTo(payment.accountTo())
                    .setAmountCents(payment.amountCents())
                    .setCurrency(payment.currency())
                    .setSuccess(payment.success())
                    .setMessage(payment.message())
                    .build());
        }
        respObs.onNext(view.build());
        respObs.onCompleted();
    }

    private TransactionResponse toResponse(SubmitResult result, long startNanos) {
        PostedTransaction posted = result.transaction();
        return TransactionResponse.newBuilder()
                .setTransactionId(posted.transactionId())
                .setSuccess(posted.success())
                .setMessage(posted.message())
                .setReplayed(result.replayed())
                .setServerTimestampNanos(nowNanos())
                .setProcessingNanos(nowNanos() - startNanos)
                .build();
    }

    private static void complete(StreamObserver<TransactionResponse> respObs, TransactionResponse resp) {
        respObs.onNext(resp);
        respObs.onCompleted();
    }
}
