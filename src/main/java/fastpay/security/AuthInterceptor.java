package fastpay.security;

import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class AuthInterceptor implements ServerInterceptor {
    private final TokenStore tokens;

    public AuthInterceptor(TokenStore tokens) {
        this.tokens = tokens;
    }

    public AuthInterceptor(String paymentsToken) {
        this(TokenStore.seeded(paymentsToken, Auth.ADMIN_TOKEN));
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        String value = headers.get(Auth.AUTHORIZATION);
        return tokens.authenticate(value)
                .map(role -> {
                    var ctx = io.grpc.Context.current().withValue(AuthContext.ROLE, role);
                    return Contexts.interceptCall(ctx, call, headers, next);
                })
                .orElseGet(() -> {
                    call.close(Status.UNAUTHENTICATED.withDescription(
                            "missing or invalid authorization (Bearer pay-token or admin-token)"
                    ), new Metadata());
                    return new ServerCall.Listener<>() {};
                });
    }
}
