package fastpay.ledger;

import fastpay.proto.PaymentStatus;
import fastpay.proto.TransactionRequest;
import fastpay.security.Auth;
import fastpay.security.Role;
import fastpay.security.TokenStore;
import fastpay.webhook.PaymentEvents;

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
 * SQLite-backed ledger (file or in-memory). Formerly {@code InMemoryLedger}.
 * {@code transaction_id} is the idempotency key. Settled, failed, and flagged
 * payments also write a transactional webhook outbox row.
 */
public final class Ledger implements AutoCloseable {
    public static final long DEFAULT_OPENING_CENTS = 1_000_000L;
    public static final long POOR_OPENING_CENTS = 100L;
    public static final String DEFAULT_CURRENCY = "USD";
    public static final int MAX_MEMO_LENGTH = 280;

    private final Connection conn;
    private final Object lock = new Object();
    private final TokenStore tokens = new TokenStore();

    public Ledger() {
        this(memoryUrl(), Auth.PAYMENTS_TOKEN, Auth.ADMIN_TOKEN);
    }

    public Ledger(Path dbFile) {
        this(dbFile, Auth.PAYMENTS_TOKEN, Auth.ADMIN_TOKEN);
    }

    public Ledger(Path dbFile, String paymentsToken, String adminToken) {
        this("jdbc:sqlite:" + dbFile.toAbsolutePath(), paymentsToken, adminToken);
    }

    Ledger(String jdbcUrl, String paymentsToken, String adminToken) {
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
            migrateSchema();
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

    public static Ledger file(Path dbFile) {
        return new Ledger(dbFile);
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
                      created_at INTEGER NOT NULL,
                      refund_of TEXT,
                      memo TEXT
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
            statement.execute("CREATE INDEX IF NOT EXISTS payments_created_at ON payments(created_at)");
            statement.execute("CREATE INDEX IF NOT EXISTS payments_created_txn ON payments(created_at, transaction_id)");
            statement.execute("CREATE INDEX IF NOT EXISTS journal_account ON journal(account_id)");
            statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS api_keys_label ON api_keys(label)");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS webhook_outbox (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      transaction_id TEXT NOT NULL,
                      event_type TEXT NOT NULL,
                      payload TEXT NOT NULL,
                      status TEXT NOT NULL,
                      attempts INTEGER NOT NULL,
                      next_attempt_at INTEGER NOT NULL,
                      last_error TEXT,
                      created_at INTEGER NOT NULL,
                      UNIQUE (transaction_id, event_type)
                    )
                    """);
            statement.execute("CREATE INDEX IF NOT EXISTS outbox_pending ON webhook_outbox(status, next_attempt_at)");
        }
    }

    private void migrateSchema() throws SQLException {
        if (!hasColumn("payments", "refund_of")) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("ALTER TABLE payments ADD COLUMN refund_of TEXT");
            }
        }
        if (!hasColumn("payments", "memo")) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("ALTER TABLE payments ADD COLUMN memo TEXT");
            }
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE INDEX IF NOT EXISTS payments_refund_of ON payments(refund_of)");
            statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS api_keys_label ON api_keys(label)");
            statement.execute("CREATE INDEX IF NOT EXISTS outbox_pending ON webhook_outbox(status, next_attempt_at)");
        }
    }

    private boolean hasColumn(String table, String column) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void seedAccountsIfEmpty() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM accounts")) {
            if (rs.next() && rs.getInt(1) > 0) {
                return;
            }
        }
        openAccount("ACC-111", DEFAULT_OPENING_CENTS, DEFAULT_CURRENCY);
        openAccount("ACC-222", DEFAULT_OPENING_CENTS, DEFAULT_CURRENCY);
        openAccount("ACC-AAA", DEFAULT_OPENING_CENTS, DEFAULT_CURRENCY);
        openAccount("ACC-BBB", DEFAULT_OPENING_CENTS, DEFAULT_CURRENCY);
        openAccount("ACC-POOR", POOR_OPENING_CENTS, DEFAULT_CURRENCY);
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

    public CreatedApiKey createApiKey(String label, Role role) {
        String resolved = normalizeLabel(label);
        if (role == null) {
            throw new InvalidTransactionException("role is required");
        }
        String token = newRawToken();
        synchronized (lock) {
            try {
                insertApiKey(token, role, resolved);
            } catch (SQLException e) {
                if (String.valueOf(e.getMessage()).contains("UNIQUE")) {
                    throw new InvalidTransactionException("api key already exists: " + resolved);
                }
                throw new IllegalStateException(e);
            }
        }
        return new CreatedApiKey(resolved, role, token);
    }

    public String revokeApiKey(String rawToken, String label) {
        synchronized (lock) {
            try {
                String hash;
                String resolvedLabel;
                Role role;
                if (!blank(rawToken)) {
                    hash = TokenStore.sha256(rawToken);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT token_hash, role, label FROM api_keys WHERE token_hash = ?")) {
                        ps.setString(1, hash);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) {
                                throw new InvalidTransactionException("unknown api key");
                            }
                            role = Role.valueOf(rs.getString(2));
                            resolvedLabel = rs.getString(3);
                        }
                    }
                } else if (!blank(label)) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT token_hash, role, label FROM api_keys WHERE label = ?")) {
                        ps.setString(1, normalizeLabel(label));
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) {
                                throw new InvalidTransactionException("unknown api key");
                            }
                            hash = rs.getString(1);
                            role = Role.valueOf(rs.getString(2));
                            resolvedLabel = rs.getString(3);
                        }
                    }
                } else {
                    throw new InvalidTransactionException("token or label is required");
                }
                if (role == Role.ADMIN && countAdminKeys() <= 1) {
                    throw new InvalidTransactionException("cannot revoke the last admin key");
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM api_keys WHERE token_hash = ?")) {
                    ps.setString(1, hash);
                    ps.executeUpdate();
                }
                tokens.removeHash(hash);
                return resolvedLabel;
            } catch (InvalidTransactionException e) {
                throw e;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private int countAdminKeys() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT COUNT(*) FROM api_keys WHERE role = 'ADMIN'")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public static String newRawToken() {
        byte[] bytes = new byte[24];
        new java.security.SecureRandom().nextBytes(bytes);
        return "fpk_" + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static String normalizeLabel(String label) {
        if (label == null || label.isBlank()) {
            throw new InvalidTransactionException("label is required");
        }
        String resolved = label.trim();
        if (resolved.length() > 64) {
            throw new InvalidTransactionException("label must be at most 64 characters");
        }
        return resolved;
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
        openAccount(accountId, openingCents, DEFAULT_CURRENCY);
    }

    public void openAccount(String accountId, long openingCents, String currency) {
        if (accountId == null || accountId.isBlank()) {
            throw new InvalidTransactionException("account_id is required");
        }
        if (openingCents < 0) {
            throw new InvalidTransactionException("opening balance cannot be negative");
        }
        String resolvedCurrency = (currency == null || currency.isBlank())
                ? DEFAULT_CURRENCY
                : currency.trim().toUpperCase(Locale.US);
        synchronized (lock) {
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO accounts(id, currency, balance_cents) VALUES (?, ?, ?)")) {
                    ps.setString(1, accountId);
                    ps.setString(2, resolvedCurrency);
                    ps.setLong(3, openingCents);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO journal(transaction_id, account_id, delta_cents, currency) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, "opening:" + accountId);
                    ps.setString(2, accountId);
                    ps.setLong(3, openingCents);
                    ps.setString(4, resolvedCurrency);
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
        return listPayments(accountId, limit, null).items();
    }

    public Page<PostedTransaction> listPayments(String accountId, int limit, String pageToken) {
        int cap = limit <= 0 ? 50 : Math.min(limit, 500);
        PageTokens.PaymentCursor cursor = blank(pageToken) ? null : PageTokens.decodePayment(pageToken);
        boolean filterAccount = !blank(accountId);
        synchronized (lock) {
            StringBuilder sql = new StringBuilder("SELECT * FROM payments WHERE 1=1");
            if (filterAccount) {
                sql.append(" AND (account_from = ? OR account_to = ?)");
            }
            if (cursor != null) {
                sql.append(" AND (created_at < ? OR (created_at = ? AND transaction_id < ?))");
            }
            sql.append(" ORDER BY created_at DESC, transaction_id DESC LIMIT ?");
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                if (filterAccount) {
                    ps.setString(idx++, accountId);
                    ps.setString(idx++, accountId);
                }
                if (cursor != null) {
                    ps.setLong(idx++, cursor.createdAtMillis());
                    ps.setLong(idx++, cursor.createdAtMillis());
                    ps.setString(idx++, cursor.transactionId());
                }
                ps.setInt(idx, cap + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    List<PostedTransaction> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(mapPayment(rs));
                    }
                    return page(rows, cap, last -> PageTokens.encodePayment(
                            last.createdAtMillis(), last.transactionId()));
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public Page<AccountSnapshot> listAccounts(int limit, String pageToken) {
        int cap = limit <= 0 ? 50 : Math.min(limit, 500);
        String afterId = blank(pageToken) ? null : PageTokens.decodeAccount(pageToken);
        synchronized (lock) {
            String sql = afterId == null
                    ? "SELECT id, balance_cents, currency FROM accounts ORDER BY id ASC LIMIT ?"
                    : "SELECT id, balance_cents, currency FROM accounts WHERE id > ? ORDER BY id ASC LIMIT ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (afterId == null) {
                    ps.setInt(1, cap + 1);
                } else {
                    ps.setString(1, afterId);
                    ps.setInt(2, cap + 1);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<AccountSnapshot> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(new AccountSnapshot(rs.getString(1), rs.getLong(2), rs.getString(3)));
                    }
                    return page(rows, cap, last -> PageTokens.encodeAccount(last.accountId()));
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public List<JournalEntry> journalEntries(String accountId) {
        synchronized (lock) {
            boolean filterAccount = !blank(accountId);
            String sql = filterAccount
                    ? "SELECT id, transaction_id, account_id, delta_cents, currency FROM journal WHERE account_id = ? ORDER BY id"
                    : "SELECT id, transaction_id, account_id, delta_cents, currency FROM journal ORDER BY id";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (filterAccount) {
                    ps.setString(1, accountId);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<JournalEntry> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(new JournalEntry(
                                rs.getLong(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getLong(4),
                                rs.getString(5)));
                    }
                    return rows;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public List<JournalEntry> journalEntries(String accountId, int limit) {
        return journalEntries(accountId, limit, null).items();
    }

    public Page<JournalEntry> journalEntries(String accountId, int limit, String pageToken) {
        int cap = limit <= 0 ? 50 : Math.min(limit, 1000);
        Long afterId = blank(pageToken) ? null : PageTokens.decodeJournal(pageToken);
        boolean filterAccount = !blank(accountId);
        synchronized (lock) {
            StringBuilder sql = new StringBuilder(
                    "SELECT id, transaction_id, account_id, delta_cents, currency FROM journal WHERE 1=1");
            if (filterAccount) {
                sql.append(" AND account_id = ?");
            }
            if (afterId != null) {
                sql.append(" AND id < ?");
            }
            sql.append(" ORDER BY id DESC LIMIT ?");
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                if (filterAccount) {
                    ps.setString(idx++, accountId);
                }
                if (afterId != null) {
                    ps.setLong(idx++, afterId);
                }
                ps.setInt(idx, cap + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    List<JournalEntry> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(new JournalEntry(
                                rs.getLong(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getLong(4),
                                rs.getString(5)));
                    }
                    return page(rows, cap, last -> PageTokens.encodeJournal(last.id()));
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static <T> Page<T> page(List<T> rows, int cap, java.util.function.Function<T, String> token) {
        String next = "";
        if (rows.size() > cap) {
            rows = new ArrayList<>(rows.subList(0, cap));
            next = token.apply(rows.get(rows.size() - 1));
        }
        return new Page<>(List.copyOf(rows), next);
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
                        status,
                        request.getMemo()
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

    public SubmitResult refund(String originalTransactionId, String refundId) {
        if (originalTransactionId == null || originalTransactionId.isBlank()) {
            throw new InvalidTransactionException("transaction_id is required");
        }
        String id = (refundId == null || refundId.isBlank())
                ? "refund:" + originalTransactionId
                : refundId;
        synchronized (lock) {
            try {
                conn.setAutoCommit(false);
                Optional<PostedTransaction> existingById = findUnlocked(id);
                if (existingById.isPresent()) {
                    conn.commit();
                    return new SubmitResult(existingById.get(), true);
                }
                Optional<PostedTransaction> existingRefund = findRefundOf(originalTransactionId);
                if (existingRefund.isPresent()) {
                    conn.commit();
                    return new SubmitResult(existingRefund.get(), true);
                }
                PostedTransaction original = findUnlocked(originalTransactionId)
                        .orElseThrow(() -> new InvalidTransactionException(
                                "unknown transaction_id: " + originalTransactionId));
                if (original.isRefund()) {
                    throw new InvalidTransactionException("cannot refund a refund");
                }
                if (!original.success() || original.status() != PaymentStatus.SETTLED) {
                    throw new InvalidTransactionException("only settled payments can be refunded");
                }
                String payer = original.accountTo();
                String payee = original.accountFrom();
                long cents = original.amountCents();
                AccountSnapshot source = requireAccount(payer);
                AccountSnapshot dest = requireAccount(payee);
                if (source.balanceCents() < cents) {
                    throw new InvalidTransactionException(
                            "Insufficient funds to refund: " + payer + " has "
                                    + formatAmount(source.balanceCents()) + " " + original.currency()
                                    + ", need " + formatAmount(cents));
                }
                updateBalance(payer, source.balanceCents() - cents);
                updateBalance(payee, dest.balanceCents() + cents);
                PostedTransaction posted = new PostedTransaction(
                        id,
                        payer,
                        payee,
                        cents,
                        original.currency(),
                        true,
                        "Refunded " + formatAmount(cents) + " " + original.currency()
                                + " of " + original.transactionId()
                                + " from " + payer + " to " + payee,
                        PaymentStatus.SETTLED,
                        System.currentTimeMillis(),
                        original.transactionId(),
                        original.memo() == null ? "" : original.memo()
                );
                insertPayment(posted);
                insertJournal(posted.transactionId(), payer, -cents, posted.currency());
                insertJournal(posted.transactionId(), payee, cents, posted.currency());
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

    private PostedTransaction postNew(TransactionRequest request) throws SQLException {
        long cents = request.getAmountCents();
        String fromId = request.getAccountFrom();
        String toId = request.getAccountTo();
        AccountSnapshot from = requireAccount(fromId);
        AccountSnapshot to = requireAccount(toId);
        String currency = request.getCurrency().trim().toUpperCase(Locale.US);
        if (!from.currency().equalsIgnoreCase(currency) || !to.currency().equalsIgnoreCase(currency)) {
            throw new InvalidTransactionException("currency mismatch: request " + currency
                    + " vs accounts " + from.currency() + "/" + to.currency());
        }
        if (from.balanceCents() < cents) {
            PostedTransaction rejected = new PostedTransaction(
                    request.getTransactionId(),
                    fromId,
                    toId,
                    cents,
                    request.getCurrency(),
                    false,
                    "Insufficient funds: " + fromId + " has " + formatAmount(from.balanceCents())
                            + " " + currency + ", need " + formatAmount(cents),
                    PaymentStatus.FAILED,
                    request.getMemo()
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
                currency,
                true,
                "Processed " + formatAmount(cents) + " " + currency
                        + " from " + fromId + " to " + toId,
                PaymentStatus.SETTLED,
                request.getMemo()
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

    private Optional<PostedTransaction> findRefundOf(String originalTransactionId) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM payments WHERE refund_of = ? LIMIT 1")) {
            ps.setString(1, originalTransactionId);
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
                  success, message, status, created_at, refund_of, memo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            ps.setString(1, posted.transactionId());
            ps.setString(2, posted.accountFrom());
            ps.setString(3, posted.accountTo());
            ps.setLong(4, posted.amountCents());
            ps.setString(5, posted.currency());
            ps.setInt(6, posted.success() ? 1 : 0);
            ps.setString(7, posted.message());
            ps.setString(8, posted.status().name());
            ps.setLong(9, posted.createdAtMillis());
            ps.setString(10, posted.refundOf());
            ps.setString(11, posted.memo() == null ? "" : posted.memo());
            ps.executeUpdate();
        }
        insertOutbox(posted);
    }

    private void insertOutbox(PostedTransaction posted) throws SQLException {
        var event = PaymentEvents.eventType(posted.status());
        if (event.isEmpty()) {
            return;
        }
        String eventType = event.get();
        String payload = PaymentEvents.payload(posted, eventType);
        long now = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO webhook_outbox(transaction_id, event_type, payload, status, attempts,
                  next_attempt_at, last_error, created_at)
                VALUES (?, ?, ?, ?, 0, ?, NULL, ?)
                """)) {
            ps.setString(1, posted.transactionId());
            ps.setString(2, eventType);
            ps.setString(3, payload);
            ps.setString(4, OutboxRecord.PENDING);
            ps.setLong(5, now);
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            if (String.valueOf(e.getMessage()).toUpperCase(Locale.US).contains("UNIQUE")) {
                return;
            }
            throw e;
        }
    }

    public List<OutboxRecord> pendingOutbox(int limit) {
        int cap = limit <= 0 ? 20 : Math.min(limit, 100);
        long now = System.currentTimeMillis();
        synchronized (lock) {
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT id, transaction_id, event_type, payload, status, attempts, next_attempt_at, last_error
                    FROM webhook_outbox
                    WHERE status = ? AND next_attempt_at <= ?
                    ORDER BY id ASC
                    LIMIT ?
                    """)) {
                ps.setString(1, OutboxRecord.PENDING);
                ps.setLong(2, now);
                ps.setInt(3, cap);
                try (ResultSet rs = ps.executeQuery()) {
                    List<OutboxRecord> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(mapOutbox(rs));
                    }
                    return List.copyOf(rows);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public List<OutboxRecord> listOutbox() {
        synchronized (lock) {
            try (Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery("""
                         SELECT id, transaction_id, event_type, payload, status, attempts, next_attempt_at, last_error
                         FROM webhook_outbox ORDER BY id ASC
                         """)) {
                List<OutboxRecord> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mapOutbox(rs));
                }
                return List.copyOf(rows);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public void markOutboxDelivered(long id) {
        updateOutbox(id, OutboxRecord.DELIVERED, -1, 0, null);
    }

    public void markOutboxFailed(long id, String error) {
        updateOutbox(id, OutboxRecord.FAILED, -1, 0, error);
    }

    public void markOutboxRetry(long id, int attempts, long nextAttemptAtMillis, String error) {
        updateOutbox(id, OutboxRecord.PENDING, attempts, nextAttemptAtMillis, error);
    }

    private void updateOutbox(long id, String status, int attempts, long nextAttemptAtMillis, String error) {
        synchronized (lock) {
            try {
                if (attempts < 0) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE webhook_outbox SET status = ?, last_error = ? WHERE id = ?")) {
                        ps.setString(1, status);
                        ps.setString(2, error);
                        ps.setLong(3, id);
                        ps.executeUpdate();
                    }
                } else {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE webhook_outbox SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ? WHERE id = ?")) {
                        ps.setString(1, status);
                        ps.setInt(2, attempts);
                        ps.setLong(3, nextAttemptAtMillis);
                        ps.setString(4, error);
                        ps.setLong(5, id);
                        ps.executeUpdate();
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static OutboxRecord mapOutbox(ResultSet rs) throws SQLException {
        return new OutboxRecord(
                rs.getLong(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4),
                rs.getString(5),
                rs.getInt(6),
                rs.getLong(7),
                rs.getString(8)
        );
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
        String refundOf = rs.getString("refund_of");
        if (refundOf != null && refundOf.isBlank()) {
            refundOf = null;
        }
        String memo = rs.getString("memo");
        if (memo == null) {
            memo = "";
        }
        return new PostedTransaction(
                rs.getString("transaction_id"),
                rs.getString("account_from"),
                rs.getString("account_to"),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                rs.getInt("success") != 0,
                rs.getString("message"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getLong("created_at"),
                refundOf,
                memo
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
        if (request.getMemo().length() > MAX_MEMO_LENGTH) {
            throw new InvalidTransactionException("memo must be at most " + MAX_MEMO_LENGTH + " characters");
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
