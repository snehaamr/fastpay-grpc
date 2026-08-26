package fastpay.fraud;

import fastpay.proto.TransactionRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudGuardTest {
    @Test
    void flagsAmountOverThreshold() {
        FraudGuard guard = new FraudGuard(100, 50, 10_000);
        assertTrue(guard.evaluate(tx("ACC-111", 101)).isPresent());
        assertTrue(guard.evaluate(tx("ACC-111", 100)).isEmpty());
    }

    @Test
    void flagsVelocityAfterMaxPayments() {
        FraudGuard guard = new FraudGuard(1_000_000, 2, 60_000);
        assertTrue(guard.evaluate(tx("ACC-111", 1)).isEmpty());
        guard.recordLivePayment("ACC-111");
        assertTrue(guard.evaluate(tx("ACC-111", 1)).isEmpty());
        guard.recordLivePayment("ACC-111");
        assertTrue(guard.evaluate(tx("ACC-111", 1)).isPresent());
    }

    private static TransactionRequest tx(String from, long cents) {
        return TransactionRequest.newBuilder()
                .setTransactionId("t")
                .setAccountFrom(from)
                .setAccountTo("ACC-222")
                .setAmountCents(cents)
                .setCurrency("USD")
                .build();
    }
}
