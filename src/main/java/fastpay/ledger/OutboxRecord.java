package fastpay.ledger;

public record OutboxRecord(
        long id,
        String transactionId,
        String eventType,
        String payload,
        String status,
        int attempts,
        long nextAttemptAtMillis,
        String lastError
) {
    public static final String PENDING = "pending";
    public static final String DELIVERED = "delivered";
    public static final String FAILED = "failed";
}
