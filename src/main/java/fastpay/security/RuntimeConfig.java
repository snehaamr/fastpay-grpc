package fastpay.security;

import java.nio.file.Path;

public record RuntimeConfig(
        boolean tls,
        Path cert,
        Path key,
        Path trustCert,
        Path db,
        String paymentsToken,
        String adminToken
) {
    public static RuntimeConfig fromEnv() {
        boolean tls = Boolean.parseBoolean(env("FASTPAY_TLS", "false"));
        Path cert = Path.of(env("FASTPAY_CERT", "certs/server.crt"));
        Path key = Path.of(env("FASTPAY_KEY", "certs/server.key"));
        Path trust = Path.of(env("FASTPAY_TRUST_CERT", env("FASTPAY_CERT", "certs/ca.crt")));
        Path db = Path.of(env("FASTPAY_DB", "data/fastpay.db"));
        String pay = env("FASTPAY_PAY_TOKEN", Auth.PAYMENTS_TOKEN);
        String admin = env("FASTPAY_ADMIN_TOKEN", Auth.ADMIN_TOKEN);
        return new RuntimeConfig(tls, cert, key, trust, db, pay, admin);
    }

    public static RuntimeConfig plaintext() {
        return new RuntimeConfig(
                false,
                Path.of("certs/server.crt"),
                Path.of("certs/server.key"),
                Path.of("certs/ca.crt"),
                Path.of("data/fastpay.db"),
                Auth.PAYMENTS_TOKEN,
                Auth.ADMIN_TOKEN
        );
    }

    public String authToken() {
        return paymentsToken;
    }

    public RuntimeConfig withDb(Path db) {
        return new RuntimeConfig(tls, cert, key, trustCert, db, paymentsToken, adminToken);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
