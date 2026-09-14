package fastpay.webhook;

import fastpay.ledger.PostedTransaction;
import fastpay.proto.PaymentStatus;

import java.util.Optional;

public final class PaymentEvents {
    public static final String SETTLED = "payment.settled";
    public static final String FAILED = "payment.failed";
    public static final String FLAGGED = "payment.flagged";

    private PaymentEvents() {
    }

    public static Optional<String> eventType(PaymentStatus status) {
        if (status == null) {
            return Optional.empty();
        }
        return switch (status) {
            case SETTLED -> Optional.of(SETTLED);
            case FAILED -> Optional.of(FAILED);
            case FLAGGED -> Optional.of(FLAGGED);
            default -> Optional.empty();
        };
    }

    public static String payload(PostedTransaction posted, String eventType) {
        String refund = posted.refundOf() == null ? "" : posted.refundOf();
        return "{"
                + "\"event\":\"" + escape(eventType) + "\","
                + "\"transaction_id\":\"" + escape(posted.transactionId()) + "\","
                + "\"account_from\":\"" + escape(posted.accountFrom()) + "\","
                + "\"account_to\":\"" + escape(posted.accountTo()) + "\","
                + "\"amount_cents\":" + posted.amountCents() + ","
                + "\"currency\":\"" + escape(posted.currency()) + "\","
                + "\"success\":" + posted.success() + ","
                + "\"status\":\"" + posted.status().name() + "\","
                + "\"message\":\"" + escape(posted.message()) + "\","
                + "\"refund_of\":" + (refund.isBlank() ? "null" : "\"" + escape(refund) + "\"") + ","
                + "\"memo\":\"" + escape(posted.memo()) + "\","
                + "\"created_at_millis\":" + posted.createdAtMillis()
                + "}";
    }

    static String escape(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}
