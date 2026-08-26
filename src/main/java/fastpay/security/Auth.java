package fastpay.security;

import io.grpc.Metadata;

public final class Auth {
    public static final String DEFAULT_TOKEN = "demo-token";
    public static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private Auth() {
    }

    public static String bearer(String token) {
        return "Bearer " + token;
    }

    public static Metadata metadata(String token) {
        Metadata headers = new Metadata();
        headers.put(AUTHORIZATION, bearer(token));
        return headers;
    }

    public static boolean matches(String headerValue, String expectedToken) {
        if (headerValue == null || expectedToken == null || expectedToken.isBlank()) {
            return false;
        }
        return bearer(expectedToken).equals(headerValue) || expectedToken.equals(headerValue);
    }
}
