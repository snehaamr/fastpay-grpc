package fastpay.ledger;

import fastpay.proto.TransactionRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "FASTPAY_TEST_JDBC_URL", matches = ".+")
class LedgerPostgresTest {
    @Test
    void postgresPostsPaymentAndOutbox() {
        String url = System.getenv("FASTPAY_TEST_JDBC_URL");
        Ledger.dropTables(url);
        try (Ledger ledger = new Ledger(url, "pay-token", "admin-token")) {
            SubmitResult result = ledger.submit(TransactionRequest.newBuilder()
                    .setTransactionId("pg-1")
                    .setAccountFrom("ACC-111")
                    .setAccountTo("ACC-222")
                    .setAmountCents(2500)
                    .setCurrency("USD")
                    .build());
            assertTrue(result.transaction().success());
            assertEquals(Ledger.DEFAULT_OPENING_CENTS - 2500, ledger.balanceCents("ACC-111"));
            assertEquals(1, ledger.listOutbox().size());
            assertEquals("payment.settled", ledger.listOutbox().get(0).eventType());
        }
    }
}
