package fastpay.security;

import io.grpc.Metadata;

public final class Auth {
    public static final String PAYMENTS_TOKEN = "pay-token";
    public static final String ADMIN_TOKEN = "admin-token";
    /** Default client token (payments role). */
    public static final String DEFAULT_TOKEN = PAYMENTS_TOKEN;
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
}
