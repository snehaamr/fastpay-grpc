package fastpay.server;

import fastpay.fraud.FraudGuard;
import fastpay.ledger.AccountSnapshot;
import fastpay.ledger.InMemoryLedger;
import fastpay.ledger.InvalidTransactionException;
import fastpay.ledger.JournalEntry;
import fastpay.ledger.Page;
import fastpay.ledger.PostedTransaction;
import fastpay.ledger.SubmitResult;
import fastpay.proto.AccountQuery;
import fastpay.proto.AccountView;
import fastpay.proto.FastPayGrpc;
import fastpay.proto.JournalRecord;
import fastpay.proto.ListAccountsQuery;
import fastpay.proto.ListAccountsView;
import fastpay.proto.ListJournalQuery;
import fastpay.proto.ListJournalView;
import fastpay.proto.ListTransactionsQuery;
import fastpay.proto.ListTransactionsView;
import fastpay.proto.OpenAccountRequest;
import fastpay.proto.PaymentRecord;
import fastpay.proto.PaymentStatus;
import fastpay.proto.RefundRequest;
import fastpay.proto.TransactionQuery;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import fastpay.security.AuthContext;
import fastpay.security.Role;
import io.grpc.Context;
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
    private final FraudGuard fraudGuard;

    public FastPayServiceImpl(ScheduledExecutorService workerPool) {
        this(workerPool, new InMemoryLedger(), new FraudGuard());
    }

    public FastPayServiceImpl(ScheduledExecutorService workerPool, InMemoryLedger ledger) {
        this(workerPool, ledger, new FraudGuard());
    }

    public FastPayServiceImpl(ScheduledExecutorService workerPool, InMemoryLedger ledger, FraudGuard fraudGuard) {
        this.workerPool = workerPool;
        this.ledger = ledger;
        this.fraudGuard = fraudGuard;
    }

    private long nowNanos() {
        return System.nanoTime();
    }

    @Override
    public void processTransaction(TransactionRequest req, StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                if (context.isCancelled()) {
                    respObs.onError(Status.CANCELLED.withDescription("client cancelled").asRuntimeException());
                    return;
                }
                SubmitResult result = ledger.submit(req);
                complete(respObs, toResponse(result, start));
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("processTransaction failed", e);
                respObs.onError(Status.INTERNAL.withDescription("ledger error").asRuntimeException());
            }
        }));
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
                        .setStatus(success ? PaymentStatus.SETTLED : PaymentStatus.FAILED)
                        .setServerTimestampNanos(nowNanos())
                        .setProcessingNanos(nowNanos() - start)
                        .build());
                respObs.onCompleted();
            }
        };
    }

    @Override
    public void transactionStatus(TransactionQuery req, StreamObserver<TransactionResponse> respObs) {
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                if (context.isCancelled()) {
                    respObs.onError(Status.CANCELLED.asRuntimeException());
                    return;
                }
                var posted = ledger.find(req.getTransactionId());
                if (posted.isEmpty()) {
                    respObs.onError(Status.NOT_FOUND
                            .withDescription("unknown transaction_id: " + req.getTransactionId())
                            .asRuntimeException());
                    return;
                }
                PostedTransaction tx = posted.get();
                PaymentStatus[] steps = statusSteps(tx.status());
                for (int i = 0; i < steps.length; i++) {
                    if (context.isCancelled()) {
                        respObs.onError(Status.CANCELLED.asRuntimeException());
                        return;
                    }
                    respObs.onNext(TransactionResponse.newBuilder()
                            .setTransactionId(tx.transactionId())
                            .setSuccess(tx.success())
                            .setStatus(steps[i])
                            .setAmountCents(tx.amountCents())
                            .setMessage("Status " + steps[i].name().toLowerCase()
                                    + " for transaction " + tx.transactionId())
                            .setServerTimestampNanos(nowNanos())
                            .setProcessingNanos(1000L * (i + 1))
                            .build());
                }
                respObs.onCompleted();
            } catch (RuntimeException e) {
                log.error("transactionStatus failed", e);
                respObs.onError(Status.INTERNAL.withDescription("status error").asRuntimeException());
            }
        }));
    }

    @Override
    public StreamObserver<TransactionRequest> liveTransactions(StreamObserver<TransactionResponse> respObs) {
        return new StreamObserver<>() {
            @Override
            public void onNext(TransactionRequest req) {
                long start = nowNanos();
                try {
                    var fraud = fraudGuard.evaluate(req);
                    SubmitResult result;
                    if (fraud.isPresent()) {
                        result = ledger.reject(req, PaymentStatus.FLAGGED, fraud.get());
                    } else {
                        result = ledger.submit(req);
                        if (!result.replayed() && result.transaction().success()) {
                            fraudGuard.recordLivePayment(req.getAccountFrom());
                        }
                    }
                    respObs.onNext(toResponse(result, start));
                } catch (InvalidTransactionException e) {
                    respObs.onNext(TransactionResponse.newBuilder()
                            .setTransactionId(req.getTransactionId())
                            .setSuccess(false)
                            .setStatus(PaymentStatus.FAILED)
                            .setMessage(e.getMessage())
                            .setAmountCents(req.getAmountCents())
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
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                AccountSnapshot snapshot = ledger.getAccount(req.getAccountId());
                respObs.onNext(toAccountView(snapshot));
                respObs.onCompleted();
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("getAccount failed", e);
                respObs.onError(Status.INTERNAL.withDescription("account error").asRuntimeException());
            }
        }));
    }

    @Override
    public void getPayment(TransactionQuery req, StreamObserver<PaymentRecord> respObs) {
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                var posted = ledger.find(req.getTransactionId());
                if (posted.isEmpty()) {
                    respObs.onError(Status.NOT_FOUND
                            .withDescription("unknown transaction_id: " + req.getTransactionId())
                            .asRuntimeException());
                    return;
                }
                respObs.onNext(toPaymentRecord(posted.get()));
                respObs.onCompleted();
            } catch (RuntimeException e) {
                log.error("getPayment failed", e);
                respObs.onError(Status.INTERNAL.withDescription("payment error").asRuntimeException());
            }
        }));
    }

    @Override
    public void listTransactions(ListTransactionsQuery req, StreamObserver<ListTransactionsView> respObs) {
        Role role = AuthContext.currentRole();
        if (req.getAccountId().isBlank() && role != Role.ADMIN) {
            respObs.onError(Status.PERMISSION_DENIED
                    .withDescription("admin token required to list all payments")
                    .asRuntimeException());
            return;
        }
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                Page<PostedTransaction> page = ledger.listPayments(
                        req.getAccountId(), req.getLimit(), req.getPageToken());
                ListTransactionsView.Builder view = ListTransactionsView.newBuilder();
                for (PostedTransaction payment : page.items()) {
                    view.addPayments(toPaymentRecord(payment));
                }
                if (page.hasNextPage()) {
                    view.setNextPageToken(page.nextPageToken());
                }
                respObs.onNext(view.build());
                respObs.onCompleted();
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("listTransactions failed", e);
                respObs.onError(Status.INTERNAL.withDescription("list error").asRuntimeException());
            }
        }));
    }

    @Override
    public void listAccounts(ListAccountsQuery req, StreamObserver<ListAccountsView> respObs) {
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                Page<AccountSnapshot> page = ledger.listAccounts(req.getLimit(), req.getPageToken());
                ListAccountsView.Builder view = ListAccountsView.newBuilder();
                for (AccountSnapshot snapshot : page.items()) {
                    view.addAccounts(toAccountView(snapshot));
                }
                if (page.hasNextPage()) {
                    view.setNextPageToken(page.nextPageToken());
                }
                respObs.onNext(view.build());
                respObs.onCompleted();
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("listAccounts failed", e);
                respObs.onError(Status.INTERNAL.withDescription("list error").asRuntimeException());
            }
        }));
    }

    @Override
    public void openAccount(OpenAccountRequest req, StreamObserver<AccountView> respObs) {
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                long opening = req.getOpeningCents() == 0
                        ? InMemoryLedger.DEFAULT_OPENING_CENTS
                        : req.getOpeningCents();
                ledger.openAccount(req.getAccountId(), opening, req.getCurrency());
                respObs.onNext(toAccountView(ledger.getAccount(req.getAccountId())));
                respObs.onCompleted();
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("openAccount failed", e);
                respObs.onError(Status.INTERNAL.withDescription("account error").asRuntimeException());
            }
        }));
    }

    @Override
    public void refundTransaction(RefundRequest req, StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                SubmitResult result = ledger.refund(req.getTransactionId(), req.getRefundId());
                complete(respObs, toResponse(result, start));
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("refundTransaction failed", e);
                respObs.onError(Status.INTERNAL.withDescription("refund error").asRuntimeException());
            }
        }));
    }

    @Override
    public void listJournal(ListJournalQuery req, StreamObserver<ListJournalView> respObs) {
        if (AuthContext.currentRole() != Role.ADMIN) {
            respObs.onError(Status.PERMISSION_DENIED
                    .withDescription("admin token required to list journal entries")
                    .asRuntimeException());
            return;
        }
        Context context = Context.current();
        workerPool.execute(context.wrap(() -> {
            try {
                Page<JournalEntry> page = ledger.journalEntries(
                        req.getAccountId(), req.getLimit(), req.getPageToken());
                ListJournalView.Builder view = ListJournalView.newBuilder();
                for (JournalEntry entry : page.items()) {
                    view.addEntries(JournalRecord.newBuilder()
                            .setTransactionId(entry.transactionId())
                            .setAccountId(entry.accountId())
                            .setDeltaCents(entry.deltaCents())
                            .setCurrency(entry.currency())
                            .build());
                }
                if (page.hasNextPage()) {
                    view.setNextPageToken(page.nextPageToken());
                }
                respObs.onNext(view.build());
                respObs.onCompleted();
            } catch (InvalidTransactionException e) {
                respObs.onError(invalidStatus(e).asRuntimeException());
            } catch (RuntimeException e) {
                log.error("listJournal failed", e);
                respObs.onError(Status.INTERNAL.withDescription("journal error").asRuntimeException());
            }
        }));
    }

    private TransactionResponse toResponse(SubmitResult result, long startNanos) {
        PostedTransaction posted = result.transaction();
        return TransactionResponse.newBuilder()
                .setTransactionId(posted.transactionId())
                .setSuccess(posted.success())
                .setMessage(posted.message())
                .setReplayed(result.replayed())
                .setStatus(posted.status())
                .setAmountCents(posted.amountCents())
                .setServerTimestampNanos(nowNanos())
                .setProcessingNanos(nowNanos() - startNanos)
                .build();
    }

    private static AccountView toAccountView(AccountSnapshot snapshot) {
        return AccountView.newBuilder()
                .setAccountId(snapshot.accountId())
                .setBalanceCents(snapshot.balanceCents())
                .setCurrency(snapshot.currency())
                .build();
    }

    static PaymentRecord toPaymentRecord(PostedTransaction payment) {
        PaymentRecord.Builder record = PaymentRecord.newBuilder()
                .setTransactionId(payment.transactionId())
                .setAccountFrom(payment.accountFrom())
                .setAccountTo(payment.accountTo())
                .setAmountCents(payment.amountCents())
                .setCurrency(payment.currency())
                .setSuccess(payment.success())
                .setStatus(payment.status())
                .setMessage(payment.message())
                .setCreatedAtMillis(payment.createdAtMillis());
        if (payment.refundOf() != null && !payment.refundOf().isBlank()) {
            record.setRefundOf(payment.refundOf());
        }
        return record.build();
    }

    static Status invalidStatus(InvalidTransactionException e) {
        String message = e.getMessage() == null ? "invalid request" : e.getMessage();
        if (message.startsWith("unknown ")) {
            return Status.NOT_FOUND.withDescription(message);
        }
        if (message.contains("already exists")) {
            return Status.ALREADY_EXISTS.withDescription(message);
        }
        return Status.INVALID_ARGUMENT.withDescription(message);
    }

    static PaymentStatus[] statusSteps(PaymentStatus terminal) {
        return switch (terminal) {
            case SETTLED, AUTHORIZED -> new PaymentStatus[]{
                    PaymentStatus.PENDING, PaymentStatus.AUTHORIZED, PaymentStatus.SETTLED
            };
            case FLAGGED -> new PaymentStatus[]{PaymentStatus.PENDING, PaymentStatus.FLAGGED};
            default -> new PaymentStatus[]{PaymentStatus.PENDING, PaymentStatus.FAILED};
        };
    }

    private static void complete(StreamObserver<TransactionResponse> respObs, TransactionResponse resp) {
        respObs.onNext(resp);
        respObs.onCompleted();
    }
}
