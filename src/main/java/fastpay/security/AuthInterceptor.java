package fastpay.security;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class AuthInterceptor implements ServerInterceptor {
    private final String expectedToken;

    public AuthInterceptor(String expectedToken) {
        this.expectedToken = expectedToken;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        if (expectedToken == null || expectedToken.isBlank()) {
            return next.startCall(call, headers);
        }
        String value = headers.get(Auth.AUTHORIZATION);
        if (!Auth.matches(value, expectedToken)) {
            call.close(Status.UNAUTHENTICATED.withDescription(
                    "missing or invalid authorization (expected Bearer " + expectedToken + ")"
            ), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
