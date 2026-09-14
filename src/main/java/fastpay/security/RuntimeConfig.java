package fastpay.security;

import java.nio.file.Path;

public record RuntimeConfig(
        boolean tls,
        Path cert,
        Path key,
        Path trustCert,
        Path db,
        String webhookUrl,
        String webhookSecret,
        String paymentsToken,
        String adminToken,
        double rateLimitQps,
        int rateLimitBurst
) {
    public static final double DEFAULT_RATE_LIMIT_QPS = 20.0;
    public static final int DEFAULT_RATE_LIMIT_BURST = 40;

    public static RuntimeConfig fromEnv() {
        boolean tls = Boolean.parseBoolean(env("FASTPAY_TLS", "false"));
        Path cert = Path.of(env("FASTPAY_CERT", "certs/server.crt"));
        Path key = Path.of(env("FASTPAY_KEY", "certs/server.key"));
        Path trust = Path.of(env("FASTPAY_TRUST_CERT", env("FASTPAY_CERT", "certs/ca.crt")));
        Path db = Path.of(env("FASTPAY_DB", "data/fastpay.db"));
        String webhookUrl = env("FASTPAY_WEBHOOK_URL", "");
        String webhookSecret = env("FASTPAY_WEBHOOK_SECRET", "");
        String pay = env("FASTPAY_PAY_TOKEN", Auth.PAYMENTS_TOKEN);
        String admin = env("FASTPAY_ADMIN_TOKEN", Auth.ADMIN_TOKEN);
        return new RuntimeConfig(
                tls, cert, key, trust, db, webhookUrl, webhookSecret, pay, admin,
                envDouble("FASTPAY_RATE_LIMIT_QPS", DEFAULT_RATE_LIMIT_QPS),
                envInt("FASTPAY_RATE_LIMIT_BURST", DEFAULT_RATE_LIMIT_BURST)
        );
    }

    public static RuntimeConfig plaintext() {
        return new RuntimeConfig(
                false,
                Path.of("certs/server.crt"),
                Path.of("certs/server.key"),
                Path.of("certs/ca.crt"),
                Path.of("data/fastpay.db"),
                "",
                "",
                Auth.PAYMENTS_TOKEN,
                Auth.ADMIN_TOKEN,
                DEFAULT_RATE_LIMIT_QPS,
                DEFAULT_RATE_LIMIT_BURST
        );
    }

    public String authToken() {
        return paymentsToken;
    }

    public RuntimeConfig withDb(Path db) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, webhookUrl, webhookSecret,
                paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
    }

    public RuntimeConfig withWebhook(String webhookUrl, String webhookSecret) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, webhookUrl, webhookSecret,
                paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
    }

    public RuntimeConfig withRateLimit(double qps, int burst) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, webhookUrl, webhookSecret,
                paymentsToken, adminToken, qps, burst);
    }

    public RuntimeConfig withTls(boolean tls, Path cert, Path key, Path trustCert) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, webhookUrl, webhookSecret,
                paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static double envDouble(String name, double fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
