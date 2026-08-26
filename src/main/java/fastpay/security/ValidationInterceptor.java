package fastpay.security;

import fastpay.ledger.InMemoryLedger;
import fastpay.ledger.InvalidTransactionException;
import fastpay.proto.AccountQuery;
import fastpay.proto.TransactionRequest;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class ValidationInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            private boolean closed;

            @Override
            public void onMessage(ReqT message) {
                try {
                    if (message instanceof TransactionRequest request) {
                        InMemoryLedger.validate(request);
                    } else if (message instanceof AccountQuery query && query.getAccountId().isBlank()) {
                        throw new InvalidTransactionException("account_id is required");
                    }
                    super.onMessage(message);
                } catch (InvalidTransactionException e) {
                    if (!closed) {
                        closed = true;
                        call.close(Status.INVALID_ARGUMENT.withDescription(e.getMessage()), new Metadata());
                    }
                }
            }
        };
    }
}
