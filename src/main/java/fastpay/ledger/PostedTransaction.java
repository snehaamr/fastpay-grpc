package fastpay.ledger;

public record PostedTransaction(
        String transactionId,
        String accountFrom,
        String accountTo,
        long amountCents,
        String currency,
        boolean success,
        String message
) {
}
