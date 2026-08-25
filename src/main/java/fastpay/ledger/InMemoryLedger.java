package fastpay.ledger;

import fastpay.proto.TransactionRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory accounts, double-entry journal, and transaction log.
 * {@code transaction_id} is the idempotency key: a replay returns the original
 * outcome and does not post a second debit/credit.
 */
public final class InMemoryLedger {
    public static final long DEFAULT_OPENING_CENTS = 1_000_000L; // $10,000.00
    public static final long POOR_OPENING_CENTS = 100L; // $1.00
    public static final String DEFAULT_CURRENCY = "USD";

    private final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> accountLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PostedTransaction> byTransactionId = new ConcurrentHashMap<>();
    private final List<PostedTransaction> history = new ArrayList<>();
    private final List<JournalEntry> journal = new ArrayList<>();

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
        if (openingCents < 0) {
            throw new InvalidTransactionException("opening balance cannot be negative");
        }
        Account created = new Account(accountId, openingCents, DEFAULT_CURRENCY);
        Account existing = accounts.putIfAbsent(accountId, created);
        if (existing != null) {
            throw new InvalidTransactionException("account already exists: " + accountId);
        }
        synchronized (journal) {
            journal.add(new JournalEntry("opening:" + accountId, accountId, openingCents, DEFAULT_CURRENCY));
        }
    }

    public long balanceCents(String accountId) {
        return getAccount(accountId).balanceCents();
    }

    public AccountSnapshot getAccount(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            throw new InvalidTransactionException("account_id is required");
        }
        Account account = requireAccount(accountId);
        synchronized (lockFor(accountId)) {
            return new AccountSnapshot(account.id, account.balanceCents, account.currency);
        }
    }

    public Optional<PostedTransaction> find(String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    public List<PostedTransaction> listPayments(String accountId, int limit) {
        int cap = limit <= 0 ? 50 : Math.min(limit, 500);
        List<PostedTransaction> snapshot;
        synchronized (history) {
            snapshot = new ArrayList<>(history);
        }
        return snapshot.stream()
                .filter(payment -> accountId == null || accountId.isBlank()
                        || payment.accountFrom().equals(accountId)
                        || payment.accountTo().equals(accountId))
                .limit(cap)
                .toList();
    }

    public List<JournalEntry> journalEntries(String accountId) {
        synchronized (journal) {
            return journal.stream()
                    .filter(entry -> accountId == null || accountId.isBlank()
                            || entry.accountId().equals(accountId))
                    .toList();
        }
    }

    /**
     * Validates and posts {@code request}. Duplicate {@code transaction_id} values
     * return the stored result without moving money again.
     */
    public SubmitResult submit(TransactionRequest request) {
        validate(request);
        String transactionId = request.getTransactionId();
        boolean[] replayed = {false};
        PostedTransaction posted = byTransactionId.compute(transactionId, (id, existing) -> {
            if (existing != null) {
                replayed[0] = true;
                return existing;
            }
            return postNew(request);
        });
        return new SubmitResult(posted, replayed[0]);
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
                    PostedTransaction rejected = new PostedTransaction(
                            request.getTransactionId(),
                            fromId,
                            toId,
                            cents,
                            request.getCurrency(),
                            false,
                            "Insufficient funds: " + fromId + " has " + formatAmount(from.balanceCents)
                                    + " " + request.getCurrency() + ", need " + formatAmount(cents)
                    );
                    recordHistory(rejected);
                    return rejected;
                }
                from.balanceCents -= cents;
                to.balanceCents += cents;
                PostedTransaction posted = new PostedTransaction(
                        request.getTransactionId(),
                        fromId,
                        toId,
                        cents,
                        request.getCurrency(),
                        true,
                        "Processed " + formatAmount(cents) + " " + request.getCurrency()
                                + " from " + fromId + " to " + toId
                );
                synchronized (journal) {
                    journal.add(new JournalEntry(posted.transactionId(), fromId, -cents, posted.currency()));
                    journal.add(new JournalEntry(posted.transactionId(), toId, cents, posted.currency()));
                }
                recordHistory(posted);
                return posted;
            }
        }
    }

    private void recordHistory(PostedTransaction posted) {
        synchronized (history) {
            history.add(posted);
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
        final String currency;
        long balanceCents;

        Account(String id, long balanceCents, String currency) {
            this.id = id;
            this.balanceCents = balanceCents;
            this.currency = currency;
        }
    }
}
