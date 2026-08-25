package fastpay.ledger;

public record SubmitResult(PostedTransaction transaction, boolean replayed) {
}
