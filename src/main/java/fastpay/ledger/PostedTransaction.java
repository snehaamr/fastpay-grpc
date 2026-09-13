package fastpay.ledger;

import fastpay.proto.PaymentStatus;

public record PostedTransaction(
        String transactionId,
        String accountFrom,
        String accountTo,
        long amountCents,
        String currency,
        boolean success,
        String message,
        PaymentStatus status,
        long createdAtMillis,
        String refundOf
) {
    public PostedTransaction(
            String transactionId,
            String accountFrom,
            String accountTo,
            long amountCents,
            String currency,
            boolean success,
            String message,
            PaymentStatus status
    ) {
        this(
                transactionId,
                accountFrom,
                accountTo,
                amountCents,
                currency,
                success,
                message,
                status,
                System.currentTimeMillis(),
                null
        );
    }

    public boolean isRefund() {
        return refundOf != null && !refundOf.isBlank();
    }
}
