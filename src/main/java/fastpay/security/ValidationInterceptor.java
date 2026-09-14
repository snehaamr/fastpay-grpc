package fastpay.security;

import fastpay.ledger.Ledger;
import fastpay.ledger.InvalidTransactionException;
import fastpay.proto.AccountQuery;
import fastpay.proto.CreateApiKeyRequest;
import fastpay.proto.OpenAccountRequest;
import fastpay.proto.RefundRequest;
import fastpay.proto.RevokeApiKeyRequest;
import fastpay.proto.TransactionQuery;
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
                        Ledger.validate(request);
                    } else if (message instanceof AccountQuery query && query.getAccountId().isBlank()) {
                        throw new InvalidTransactionException("account_id is required");
                    } else if (message instanceof TransactionQuery query && query.getTransactionId().isBlank()) {
                        throw new InvalidTransactionException("transaction_id is required");
                    } else if (message instanceof OpenAccountRequest request && request.getAccountId().isBlank()) {
                        throw new InvalidTransactionException("account_id is required");
                    } else if (message instanceof RefundRequest request && request.getTransactionId().isBlank()) {
                        throw new InvalidTransactionException("transaction_id is required");
                    } else if (message instanceof CreateApiKeyRequest request && request.getLabel().isBlank()) {
                        throw new InvalidTransactionException("label is required");
                    } else if (message instanceof RevokeApiKeyRequest request
                            && request.getToken().isBlank() && request.getLabel().isBlank()) {
                        throw new InvalidTransactionException("token or label is required");
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
