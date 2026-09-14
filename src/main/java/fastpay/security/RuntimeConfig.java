package fastpay.security;

import java.nio.file.Path;

public record RuntimeConfig(
        boolean tls,
        Path cert,
        Path key,
        Path trustCert,
        Path db,
        String paymentsToken,
        String adminToken,
        double rateLimitQps,
        int rateLimitBurst,
        int metricsPort
) {
    public static final double DEFAULT_RATE_LIMIT_QPS = 20.0;
    public static final int DEFAULT_RATE_LIMIT_BURST = 40;
    public static final int DEFAULT_METRICS_PORT = 6566;

    public static RuntimeConfig fromEnv() {
        boolean tls = Boolean.parseBoolean(env("FASTPAY_TLS", "false"));
        Path cert = Path.of(env("FASTPAY_CERT", "certs/server.crt"));
        Path key = Path.of(env("FASTPAY_KEY", "certs/server.key"));
        Path trust = Path.of(env("FASTPAY_TRUST_CERT", env("FASTPAY_CERT", "certs/ca.crt")));
        Path db = Path.of(env("FASTPAY_DB", "data/fastpay.db"));
        String pay = env("FASTPAY_PAY_TOKEN", Auth.PAYMENTS_TOKEN);
        String admin = env("FASTPAY_ADMIN_TOKEN", Auth.ADMIN_TOKEN);
        return new RuntimeConfig(
                tls, cert, key, trust, db, pay, admin,
                envDouble("FASTPAY_RATE_LIMIT_QPS", DEFAULT_RATE_LIMIT_QPS),
                envInt("FASTPAY_RATE_LIMIT_BURST", DEFAULT_RATE_LIMIT_BURST),
                envInt("FASTPAY_METRICS_PORT", DEFAULT_METRICS_PORT)
        );
    }

    public static RuntimeConfig plaintext() {
        return new RuntimeConfig(
                false,
                Path.of("certs/server.crt"),
                Path.of("certs/server.key"),
                Path.of("certs/ca.crt"),
                Path.of("data/fastpay.db"),
                Auth.PAYMENTS_TOKEN,
                Auth.ADMIN_TOKEN,
                DEFAULT_RATE_LIMIT_QPS,
                DEFAULT_RATE_LIMIT_BURST,
                0
        );
    }

    public String authToken() {
        return paymentsToken;
    }

    public RuntimeConfig withDb(Path db) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, paymentsToken, adminToken, rateLimitQps, rateLimitBurst, metricsPort);
    }

    public RuntimeConfig withRateLimit(double qps, int burst) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, paymentsToken, adminToken, qps, burst, metricsPort);
    }

    public RuntimeConfig withMetricsPort(int metricsPort) {
        return new RuntimeConfig(
                tls, cert, key, trustCert, db, paymentsToken, adminToken, rateLimitQps, rateLimitBurst, metricsPort);
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
