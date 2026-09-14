package fastpay.security;

import java.nio.file.Path;

public record RuntimeConfig(
        boolean tls,
        boolean mtls,
        Path cert,
        Path key,
        Path trustCert,
        Path clientCert,
        Path clientKey,
        Path db,
        String paymentsToken,
        String adminToken,
        double rateLimitQps,
        int rateLimitBurst
) {
    public static final double DEFAULT_RATE_LIMIT_QPS = 20.0;
    public static final int DEFAULT_RATE_LIMIT_BURST = 40;

    public RuntimeConfig {
        if (mtls) {
            tls = true;
        }
    }

    public static RuntimeConfig fromEnv() {
        boolean mtls = Boolean.parseBoolean(env("FASTPAY_MTLS", "false"));
        boolean tls = Boolean.parseBoolean(env("FASTPAY_TLS", "false")) || mtls;
        Path cert = Path.of(env("FASTPAY_CERT", "certs/server.crt"));
        Path key = Path.of(env("FASTPAY_KEY", "certs/server.key"));
        Path trust = Path.of(env("FASTPAY_TRUST_CERT", env("FASTPAY_CERT", "certs/ca.crt")));
        Path clientCert = Path.of(env("FASTPAY_CLIENT_CERT", "certs/client.crt"));
        Path clientKey = Path.of(env("FASTPAY_CLIENT_KEY", "certs/client.key"));
        Path db = Path.of(env("FASTPAY_DB", "data/fastpay.db"));
        String pay = env("FASTPAY_PAY_TOKEN", Auth.PAYMENTS_TOKEN);
        String admin = env("FASTPAY_ADMIN_TOKEN", Auth.ADMIN_TOKEN);
        return new RuntimeConfig(
                tls, mtls, cert, key, trust, clientCert, clientKey, db, pay, admin,
                envDouble("FASTPAY_RATE_LIMIT_QPS", DEFAULT_RATE_LIMIT_QPS),
                envInt("FASTPAY_RATE_LIMIT_BURST", DEFAULT_RATE_LIMIT_BURST)
        );
    }

    public static RuntimeConfig plaintext() {
        return new RuntimeConfig(
                false,
                false,
                Path.of("certs/server.crt"),
                Path.of("certs/server.key"),
                Path.of("certs/ca.crt"),
                Path.of("certs/client.crt"),
                Path.of("certs/client.key"),
                Path.of("data/fastpay.db"),
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
                tls, mtls, cert, key, trustCert, clientCert, clientKey,
                db, paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
    }

    public RuntimeConfig withRateLimit(double qps, int burst) {
        return new RuntimeConfig(
                tls, mtls, cert, key, trustCert, clientCert, clientKey,
                db, paymentsToken, adminToken, qps, burst);
    }

    public RuntimeConfig withTls(boolean tls, Path cert, Path key, Path trustCert) {
        return new RuntimeConfig(
                tls, mtls, cert, key, trustCert, clientCert, clientKey,
                db, paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
    }

    public RuntimeConfig withMtls(Path clientCert, Path clientKey) {
        return new RuntimeConfig(
                true, true, cert, key, trustCert, clientCert, clientKey,
                db, paymentsToken, adminToken, rateLimitQps, rateLimitBurst);
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
