package fastpay.ledger;

public record AccountSnapshot(String accountId, long balanceCents, String currency) {
}
