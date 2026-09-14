package fastpay.metrics;

import fastpay.security.AuthInterceptor;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class MetricsInterceptor implements ServerInterceptor {
    private final FastPayMetrics metrics;

    public MetricsInterceptor(FastPayMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        if (AuthInterceptor.isPublicService(call.getMethodDescriptor().getServiceName())) {
            return next.startCall(call, headers);
        }
        String method = call.getMethodDescriptor().getBareMethodName();
        ServerCall<ReqT, RespT> counted = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            private boolean recorded;

            @Override
            public void close(Status status, Metadata trailers) {
                if (!recorded) {
                    recorded = true;
                    metrics.recordRpc(method, status.getCode());
                }
                super.close(status, trailers);
            }
        };
        return next.startCall(counted, headers);
    }
}
