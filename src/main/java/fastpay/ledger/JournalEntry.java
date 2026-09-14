package fastpay.ledger;

/** Signed cents: positive credit, negative debit. */
public record JournalEntry(
        long id,
        String transactionId,
        String accountId,
        long deltaCents,
        String currency
) {
}
