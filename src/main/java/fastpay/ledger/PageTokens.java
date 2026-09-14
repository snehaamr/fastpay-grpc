package fastpay.ledger;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Opaque keyset cursors for list RPCs. */
public final class PageTokens {
    private static final String ACCOUNT = "a";
    private static final String PAYMENT = "p";
    private static final String JOURNAL = "j";
    private static final String SEP = "\n";

    private PageTokens() {
    }

    public static String encodeAccount(String accountId) {
        return encode(ACCOUNT, accountId);
    }

    public static String decodeAccount(String token) {
        return decode(token, ACCOUNT, 2)[1];
    }

    public static String encodePayment(long createdAtMillis, String transactionId) {
        return encode(PAYMENT, Long.toString(createdAtMillis), transactionId);
    }

    public static PaymentCursor decodePayment(String token) {
        String[] parts = decode(token, PAYMENT, 3);
        try {
            return new PaymentCursor(Long.parseLong(parts[1]), parts[2]);
        } catch (NumberFormatException e) {
            throw new InvalidTransactionException("invalid page_token");
        }
    }

    public static String encodeJournal(long journalId) {
        return encode(JOURNAL, Long.toString(journalId));
    }

    public static long decodeJournal(String token) {
        try {
            return Long.parseLong(decode(token, JOURNAL, 2)[1]);
        } catch (NumberFormatException e) {
            throw new InvalidTransactionException("invalid page_token");
        }
    }

    public record PaymentCursor(long createdAtMillis, String transactionId) {
    }

    private static String encode(String... parts) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(String.join(SEP, parts).getBytes(StandardCharsets.UTF_8));
    }

    private static String[] decode(String token, String kind, int expectedParts) {
        if (token == null || token.isBlank()) {
            throw new InvalidTransactionException("invalid page_token");
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            String[] parts = raw.split(SEP, -1);
            if (parts.length != expectedParts || !kind.equals(parts[0]) || parts[1].isBlank()) {
                throw new InvalidTransactionException("invalid page_token");
            }
            return parts;
        } catch (IllegalArgumentException e) {
            throw new InvalidTransactionException("invalid page_token");
        }
    }
}
