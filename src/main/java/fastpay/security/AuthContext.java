package fastpay.security;

import io.grpc.Context;

public final class AuthContext {
    public static final Context.Key<Role> ROLE = Context.key("fastpay-role");

    private AuthContext() {
    }

    public static Role currentRole() {
        return ROLE.get();
    }
}
