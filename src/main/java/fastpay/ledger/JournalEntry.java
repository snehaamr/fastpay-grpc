package fastpay.ledger;

/** Signed cents: positive credit, negative debit. */
public record JournalEntry(
        String transactionId,
        String accountId,
        long deltaCents,
        String currency
) {
}
