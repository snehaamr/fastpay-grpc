package fastpay.ledger;

import fastpay.proto.PaymentStatus;
import fastpay.proto.TransactionRequest;
import fastpay.security.Auth;
import fastpay.security.Role;
import fastpay.security.TokenStore;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * SQLite-backed ledger (file or in-memory). {@code transaction_id} is the
 * idempotency key.
 */
public final class InMemoryLedger implements AutoCloseable {
    public static final long DEFAULT_OPENING_CENTS = 1_000_000L;
    public static final long POOR_OPENING_CENTS = 100L;
    public static final String DEFAULT_CURRENCY = "USD";

    private final Connection conn;
    private final Object lock = new Object();
    private final TokenStore tokens = new TokenStore();

    public InMemoryLedger() {
        this(memoryUrl(), Auth.PAYMENTS_TOKEN, Auth.ADMIN_TOKEN);
    }

    public InMemoryLedger(Path dbFile) {
        this(dbFile, Auth.PAYMENTS_TOKEN, Auth.ADMIN_TOKEN);
    }

    public InMemoryLedger(Path dbFile, String paymentsToken, String adminToken) {
        this("jdbc:sqlite:" + dbFile.toAbsolutePath(), paymentsToken, adminToken);
    }

    InMemoryLedger(String jdbcUrl, String paymentsToken, String adminToken) {
        try {
            this.conn = DriverManager.getConnection(jdbcUrl);
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA foreign_keys = ON");
                pragma.execute("PRAGMA busy_timeout = 5000");
            }
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA journal_mode = WAL");
            } catch (SQLException ignored) {
                // in-memory databases may not support WAL
            }
            initSchema();
            seedAccountsIfEmpty();
            seedApiKeys(paymentsToken, adminToken);
            loadApiKeys();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to open ledger database", e);
        }
    }

    public TokenStore tokenStore() {
        return tokens;
    }

    public static InMemoryLedger file(Path dbFile) {
        return new InMemoryLedger(dbFile);
    }

    private static String memoryUrl() {
        return "jdbc:sqlite:file:fastpay-" + UUID.randomUUID() + "?mode=memory&cache=shared";
    }

    private void initSchema() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                      id TEXT PRIMARY KEY,
                      currency TEXT NOT NULL,
                      balance_cents INTEGER NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                      transaction_id TEXT PRIMARY KEY,
                      account_from TEXT NOT NULL,
                      account_to TEXT NOT NULL,
                      amount_cents INTEGER NOT NULL,
                      currency TEXT NOT NULL,
                      success INTEGER NOT NULL,
                      message TEXT NOT NULL,
                      status TEXT NOT NULL,
                      created_at INTEGER NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS journal (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      transaction_id TEXT NOT NULL,
                      account_id TEXT NOT NULL,
                      delta_cents INTEGER NOT NULL,
                      currency TEXT NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS api_keys (
                      token_hash TEXT PRIMARY KEY,
                      role TEXT NOT NULL,
                      label TEXT NOT NULL
                    )
                    """);
        }
    }

    private void seedAccountsIfEmpty() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM accounts")) {
            if (rs.next() && rs.getInt(1) > 0) {
                return;
            }
        }
        openAccount("ACC-111", DEFAULT_OPENING_CENTS);
        openAccount("ACC-222", DEFAULT_OPENING_CENTS);
        openAccount("ACC-AAA", DEFAULT_OPENING_CENTS);
        openAccount("ACC-BBB", DEFAULT_OPENING_CENTS);
        openAccount("ACC-POOR", POOR_OPENING_CENTS);
    }

    private void seedApiKeys(String paymentsToken, String adminToken) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM api_keys")) {
            if (rs.next() && rs.getInt(1) > 0) {
                return;
            }
        }
        insertApiKey(paymentsToken, Role.PAYMENTS, "payments");
        insertApiKey(adminToken, Role.ADMIN, "admin");
    }

    private void insertApiKey(String rawToken, Role role, String label) throws SQLException {
        if (rawToken == null || rawToken.isBlank()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO api_keys(token_hash, role, label) VALUES (?, ?, ?)")) {
            ps.setString(1, TokenStore.sha256(rawToken));
            ps.setString(2, role.name());
            ps.setString(3, label);
            ps.executeUpdate();
        }
        tokens.put(rawToken, role);
    }

    private void loadApiKeys() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT token_hash, role FROM api_keys")) {
            while (rs.next()) {
                tokens.putHash(rs.getString(1), Role.valueOf(rs.getString(2)));
            }
        }
    }

    public void openAccount(String accountId, long openingCents) {
        if (accountId == null || accountId.isBlank()) {
            throw new InvalidTransactionException("account id is required");
        }
        if (openingCents < 0) {
            throw new InvalidTransactionException("opening balance cannot be negative");
        }
        synchronized (lock) {
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO accounts(id, currency, balance_cents) VALUES (?, ?, ?)")) {
                    ps.setString(1, accountId);
                    ps.setString(2, DEFAULT_CURRENCY);
                    ps.setLong(3, openingCents);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO journal(transaction_id, account_id, delta_cents, currency) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, "opening:" + accountId);
                    ps.setString(2, accountId);
                    ps.setLong(3, openingCents);
                    ps.setString(4, DEFAULT_CURRENCY);
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                if (String.valueOf(e.getMessage()).contains("UNIQUE")) {
                    throw new InvalidTransactionException("account already exists: " + accountId);
                }
                throw new IllegalStateException(e);
            }
        }
    }

    public long balanceCents(String accountId) {
        return getAccount(accountId).balanceCents();
    }

    public AccountSnapshot getAccount(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            throw new InvalidTransactionException("account_id is required");
        }
        synchronized (lock) {
            return requireAccount(accountId);
        }
    }

    public Optional<PostedTransaction> find(String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            return Optional.empty();
        }
        synchronized (lock) {
            return findUnlocked(transactionId);
        }
    }

    public List<PostedTransaction> listPayments(String accountId, int limit) {
        int cap = limit <= 0 ? 50 : Math.min(limit, 500);
        synchronized (lock) {
            String sql = (accountId == null || accountId.isBlank())
                    ? "SELECT * FROM payments ORDER BY created_at ASC LIMIT ?"
                    : "SELECT * FROM payments WHERE account_from = ? OR account_to = ? ORDER BY created_at ASC LIMIT ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (accountId == null || accountId.isBlank()) {
                    ps.setInt(1, cap);
                } else {
                    ps.setString(1, accountId);
                    ps.setString(2, accountId);
                    ps.setInt(3, cap);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<PostedTransaction> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(mapPayment(rs));
                    }
                    return rows;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public List<JournalEntry> journalEntries(String accountId) {
        synchronized (lock) {
            String sql = (accountId == null || accountId.isBlank())
                    ? "SELECT transaction_id, account_id, delta_cents, currency FROM journal ORDER BY id"
                    : "SELECT transaction_id, account_id, delta_cents, currency FROM journal WHERE account_id = ? ORDER BY id";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (accountId != null && !accountId.isBlank()) {
                    ps.setString(1, accountId);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<JournalEntry> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(new JournalEntry(
                                rs.getString(1), rs.getString(2), rs.getLong(3), rs.getString(4)));
                    }
                    return rows;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public SubmitResult submit(TransactionRequest request) {
        validate(request);
        synchronized (lock) {
            try {
                conn.setAutoCommit(false);
                Optional<PostedTransaction> existing = findUnlocked(request.getTransactionId());
                if (existing.isPresent()) {
                    conn.commit();
                    return new SubmitResult(existing.get(), true);
                }
                PostedTransaction posted = postNew(request);
                conn.commit();
                return new SubmitResult(posted, false);
            } catch (RuntimeException e) {
                rollbackQuietly();
                throw e;
            } catch (SQLException e) {
                rollbackQuietly();
                throw new IllegalStateException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // ignore
                }
            }
        }
    }

    public SubmitResult reject(TransactionRequest request, PaymentStatus status, String message) {
        validate(request);
        synchronized (lock) {
            try {
                conn.setAutoCommit(false);
                Optional<PostedTransaction> existing = findUnlocked(request.getTransactionId());
                if (existing.isPresent()) {
                    conn.commit();
                    return new SubmitResult(existing.get(), true);
                }
                PostedTransaction rejected = new PostedTransaction(
                        request.getTransactionId(),
                        request.getAccountFrom(),
                        request.getAccountTo(),
                        request.getAmountCents(),
                        request.getCurrency(),
                        false,
                        message,
                        status
                );
                insertPayment(rejected);
                conn.commit();
                return new SubmitResult(rejected, false);
            } catch (RuntimeException e) {
                rollbackQuietly();
                throw e;
            } catch (SQLException e) {
                rollbackQuietly();
                throw new IllegalStateException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // ignore
                }
            }
        }
    }

    private PostedTransaction postNew(TransactionRequest request) throws SQLException {
        long cents = request.getAmountCents();
        String fromId = request.getAccountFrom();
        String toId = request.getAccountTo();
        AccountSnapshot from = requireAccount(fromId);
        AccountSnapshot to = requireAccount(toId);
        if (from.balanceCents() < cents) {
            PostedTransaction rejected = new PostedTransaction(
                    request.getTransactionId(),
                    fromId,
                    toId,
                    cents,
                    request.getCurrency(),
                    false,
                    "Insufficient funds: " + fromId + " has " + formatAmount(from.balanceCents())
                            + " " + request.getCurrency() + ", need " + formatAmount(cents),
                    PaymentStatus.FAILED
            );
            insertPayment(rejected);
            return rejected;
        }
        updateBalance(fromId, from.balanceCents() - cents);
        updateBalance(toId, to.balanceCents() + cents);
        PostedTransaction posted = new PostedTransaction(
                request.getTransactionId(),
                fromId,
                toId,
                cents,
                request.getCurrency(),
                true,
                "Processed " + formatAmount(cents) + " " + request.getCurrency()
                        + " from " + fromId + " to " + toId,
                PaymentStatus.SETTLED
        );
        insertPayment(posted);
        insertJournal(posted.transactionId(), fromId, -cents, posted.currency());
        insertJournal(posted.transactionId(), toId, cents, posted.currency());
        return posted;
    }

    private Optional<PostedTransaction> findUnlocked(String transactionId) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM payments WHERE transaction_id = ?")) {
            ps.setString(1, transactionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapPayment(rs));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private AccountSnapshot requireAccount(String accountId) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, balance_cents, currency FROM accounts WHERE id = ?")) {
            ps.setString(1, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new InvalidTransactionException("unknown account: " + accountId);
                }
                return new AccountSnapshot(rs.getString(1), rs.getLong(2), rs.getString(3));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void updateBalance(String accountId, long balanceCents) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE accounts SET balance_cents = ? WHERE id = ?")) {
            ps.setLong(1, balanceCents);
            ps.setString(2, accountId);
            ps.executeUpdate();
        }
    }

    private void insertPayment(PostedTransaction posted) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO payments(transaction_id, account_from, account_to, amount_cents, currency,
                  success, message, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            ps.setString(1, posted.transactionId());
            ps.setString(2, posted.accountFrom());
            ps.setString(3, posted.accountTo());
            ps.setLong(4, posted.amountCents());
            ps.setString(5, posted.currency());
            ps.setInt(6, posted.success() ? 1 : 0);
            ps.setString(7, posted.message());
            ps.setString(8, posted.status().name());
            ps.setLong(9, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private void insertJournal(String txnId, String accountId, long delta, String currency) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO journal(transaction_id, account_id, delta_cents, currency) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, txnId);
            ps.setString(2, accountId);
            ps.setLong(3, delta);
            ps.setString(4, currency);
            ps.executeUpdate();
        }
    }

    private static PostedTransaction mapPayment(ResultSet rs) throws SQLException {
        return new PostedTransaction(
                rs.getString("transaction_id"),
                rs.getString("account_from"),
                rs.getString("account_to"),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                rs.getInt("success") != 0,
                rs.getString("message"),
                PaymentStatus.valueOf(rs.getString("status"))
        );
    }

    private void rollbackQuietly() {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // ignore
        }
    }

    public static void validate(TransactionRequest request) {
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
        if (request.getAmountCents() <= 0) {
            throw new InvalidTransactionException("amount_cents must be a positive integer");
        }
    }

    public static String formatAmount(long cents) {
        return String.format(Locale.US, "%.2f", cents / 100.0d);
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException ignored) {
            // ignore
        }
    }
}
