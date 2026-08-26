package fastpay.security;

import java.nio.file.Path;

public record RuntimeConfig(boolean tls, Path cert, Path key, Path trustCert, String authToken) {
    public static RuntimeConfig fromEnv() {
        boolean tls = Boolean.parseBoolean(env("FASTPAY_TLS", "false"));
        Path cert = Path.of(env("FASTPAY_CERT", "certs/server.crt"));
        Path key = Path.of(env("FASTPAY_KEY", "certs/server.key"));
        Path trust = Path.of(env("FASTPAY_TRUST_CERT", env("FASTPAY_CERT", "certs/server.crt")));
        String token = env("FASTPAY_AUTH_TOKEN", Auth.DEFAULT_TOKEN);
        return new RuntimeConfig(tls, cert, key, trust, token);
    }

    public static RuntimeConfig plaintext() {
        return new RuntimeConfig(false, Path.of("certs/server.crt"), Path.of("certs/server.key"),
                Path.of("certs/ca.crt"), Auth.DEFAULT_TOKEN);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
