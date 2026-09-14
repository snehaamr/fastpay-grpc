package fastpay.ledger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

final class SqlDialect {
    private final boolean postgres;

    private SqlDialect(boolean postgres) {
        this.postgres = postgres;
    }

    static SqlDialect of(String jdbcUrl) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(Locale.US);
        return new SqlDialect(url.startsWith("jdbc:postgresql"));
    }

    boolean postgres() {
        return postgres;
    }

    void configure(Connection conn) throws SQLException {
        if (postgres) {
            return;
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("PRAGMA foreign_keys = ON");
            statement.execute("PRAGMA busy_timeout = 5000");
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("PRAGMA journal_mode = WAL");
        } catch (SQLException ignored) {
            // in-memory databases may not support WAL
        }
    }

    String autoIdColumn() {
        return postgres ? "id BIGSERIAL PRIMARY KEY" : "id INTEGER PRIMARY KEY AUTOINCREMENT";
    }

    boolean uniqueViolation(SQLException e) {
        return "23505".equals(e.getSQLState())
                || String.valueOf(e.getMessage()).toUpperCase(Locale.US).contains("UNIQUE");
    }

    boolean hasColumn(Connection conn, String table, String column) throws SQLException {
        if (postgres) {
            try (PreparedStatement ps = conn.prepareStatement(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = ?
                      AND column_name = ?
                    """)) {
                ps.setString(1, table.toLowerCase(Locale.US));
                ps.setString(2, column.toLowerCase(Locale.US));
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }
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

    /**
     * Postgres INTEGER is 32-bit (epoch millis overflow). SQLite INTEGER is already
     * 64-bit, so this is a no-op there.
     */
    void widenInt64(Connection conn, String table, String column) throws SQLException {
        if (!postgres) {
            return;
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("ALTER TABLE " + table + " ALTER COLUMN " + column + " TYPE BIGINT");
        }
    }
}
