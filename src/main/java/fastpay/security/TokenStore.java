package fastpay.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class TokenStore {
    private final ConcurrentHashMap<String, Role> rolesByHash = new ConcurrentHashMap<>();

    public static TokenStore seeded(String paymentsToken, String adminToken) {
        TokenStore store = new TokenStore();
        store.put(paymentsToken, Role.PAYMENTS);
        store.put(adminToken, Role.ADMIN);
        return store;
    }

    public void put(String rawToken, Role role) {
        if (rawToken == null || rawToken.isBlank()) {
            return;
        }
        putHash(sha256(rawToken), role);
    }

    public void putHash(String tokenHash, Role role) {
        if (tokenHash == null || tokenHash.isBlank() || role == null) {
            return;
        }
        rolesByHash.put(tokenHash, role);
    }

    public void remove(String rawToken) {
        if (rawToken == null || rawToken.isBlank()) {
            return;
        }
        removeHash(sha256(rawToken));
    }

    public void removeHash(String tokenHash) {
        if (tokenHash == null || tokenHash.isBlank()) {
            return;
        }
        rolesByHash.remove(tokenHash);
    }

    public Optional<Role> authenticate(String authorizationHeader) {
        String token = unwrap(authorizationHeader);
        if (token == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(rolesByHash.get(sha256(token)));
    }

    static String unwrap(String headerValue) {
        if (headerValue == null || headerValue.isBlank()) {
            return null;
        }
        String value = headerValue.trim();
        if (value.toLowerCase(Locale.ROOT).startsWith("bearer ")) {
            return value.substring(7).trim();
        }
        return value;
    }

    public static String sha256(String rawToken) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(rawToken.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
