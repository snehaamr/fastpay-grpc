package fastpay.ledger;

import fastpay.proto.TransactionRequest;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory accounts and transaction log.
 * {@code transaction_id} is the idempotency key: a replay returns the original
 * outcome and does not post a second debit/credit.
 */
public final class InMemoryLedger {
    public static final long DEFAULT_OPENING_CENTS = 1_000_000L; // $10,000.00
    public static final long POOR_OPENING_CENTS = 100L; // $1.00

    private final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> accountLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PostedTransaction> byTransactionId = new ConcurrentHashMap<>();

    public InMemoryLedger() {
        openAccount("ACC-111", DEFAULT_OPENING_CENTS);
        openAccount("ACC-222", DEFAULT_OPENING_CENTS);
        openAccount("ACC-AAA", DEFAULT_OPENING_CENTS);
        openAccount("ACC-BBB", DEFAULT_OPENING_CENTS);
        openAccount("ACC-POOR", POOR_OPENING_CENTS);
    }

    public void openAccount(String accountId, long openingCents) {
        if (accountId == null || accountId.isBlank()) {
            throw new InvalidTransactionException("account id is required");
        }
        Account created = new Account(accountId, openingCents);
        Account existing = accounts.putIfAbsent(accountId, created);
        if (existing != null) {
            throw new InvalidTransactionException("account already exists: " + accountId);
        }
    }

    public long balanceCents(String accountId) {
        Account account = requireAccount(accountId);
        synchronized (lockFor(accountId)) {
            return account.balanceCents;
        }
    }

    public Optional<PostedTransaction> find(String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    /**
     * Validates and posts {@code request}. Duplicate {@code transaction_id} values
     * return the stored result without moving money again.
     */
    public PostedTransaction submit(TransactionRequest request) {
        validate(request);
        String transactionId = request.getTransactionId();
        return byTransactionId.compute(transactionId, (id, existing) -> {
            if (existing != null) {
                return existing;
            }
            return postNew(request);
        });
    }

    private PostedTransaction postNew(TransactionRequest request) {
        long cents = toCents(request.getAmount());
        String fromId = request.getAccountFrom();
        String toId = request.getAccountTo();
        Account from = requireAccount(fromId);
        Account to = requireAccount(toId);

        String first = fromId.compareTo(toId) < 0 ? fromId : toId;
        String second = fromId.compareTo(toId) < 0 ? toId : fromId;
        synchronized (lockFor(first)) {
            synchronized (lockFor(second)) {
                if (from.balanceCents < cents) {
                    return new PostedTransaction(
                            request.getTransactionId(),
                            fromId,
                            toId,
                            cents,
                            request.getCurrency(),
                            false,
                            "Insufficient funds: " + fromId + " has " + formatAmount(from.balanceCents)
                                    + " " + request.getCurrency() + ", need " + formatAmount(cents)
                    );
                }
                from.balanceCents -= cents;
                to.balanceCents += cents;
                return new PostedTransaction(
                        request.getTransactionId(),
                        fromId,
                        toId,
                        cents,
                        request.getCurrency(),
                        true,
                        "Processed " + formatAmount(cents) + " " + request.getCurrency()
                                + " from " + fromId + " to " + toId
                );
            }
        }
    }

    static void validate(TransactionRequest request) {
        if (request.getTransactionId().isBlank()) {
            throw new InvalidTransactionException("transaction_id is required");
        }
        if (request.getAccountFrom().isBlank()) {
            throw new InvalidTransactionException("account_from is required");
        }
        if (request.getAccountTo().isBlank()) {
            throw new InvalidTransactionException("account_to is required");
        }
        if (request.getAccountFrom().equals(request.getAccountTo())) {
            throw new InvalidTransactionException("account_from and account_to must differ");
        }
        if (request.getCurrency().isBlank()) {
            throw new InvalidTransactionException("currency is required");
        }
        double amount = request.getAmount();
        if (!Double.isFinite(amount) || amount <= 0) {
            throw new InvalidTransactionException("amount must be a positive finite number");
        }
        if (toCents(amount) <= 0) {
            throw new InvalidTransactionException("amount is too small to represent in cents");
        }
    }

    static long toCents(double amount) {
        return Math.round(amount * 100.0d);
    }

    public static String formatAmount(long cents) {
        return String.format(Locale.US, "%.2f", cents / 100.0d);
    }

    private Account requireAccount(String accountId) {
        Account account = accounts.get(accountId);
        if (account == null) {
            throw new InvalidTransactionException("unknown account: " + accountId);
        }
        return account;
    }

    private Object lockFor(String accountId) {
        return accountLocks.computeIfAbsent(accountId, id -> new Object());
    }

    private static final class Account {
        final String id;
        long balanceCents;

        Account(String id, long balanceCents) {
            this.id = id;
            this.balanceCents = balanceCents;
        }
    }
}
