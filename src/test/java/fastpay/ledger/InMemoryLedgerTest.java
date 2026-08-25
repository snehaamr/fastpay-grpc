package fastpay.ledger;

import fastpay.proto.TransactionRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryLedgerTest {
    private InMemoryLedger ledger;

    @BeforeEach
    void setUp() {
        ledger = new InMemoryLedger();
    }

    @Test
    void postsDebitAndCredit() {
        long fromBefore = ledger.balanceCents("ACC-111");
        long toBefore = ledger.balanceCents("ACC-222");

        PostedTransaction posted = ledger.submit(request("txn-ledger-1", "ACC-111", "ACC-222", 250.75));

        assertTrue(posted.success());
        assertEquals(fromBefore - 25075, ledger.balanceCents("ACC-111"));
        assertEquals(toBefore + 25075, ledger.balanceCents("ACC-222"));
        assertTrue(posted.message().contains("250.75"));
    }

    @Test
    void duplicateTransactionIdDoesNotDoublePost() {
        long fromBefore = ledger.balanceCents("ACC-111");
        PostedTransaction first = ledger.submit(request("txn-dup", "ACC-111", "ACC-222", 10.00));
        PostedTransaction second = ledger.submit(request("txn-dup", "ACC-111", "ACC-222", 10.00));

        assertTrue(first.success());
        assertEquals(first, second);
        assertEquals(fromBefore - 1000, ledger.balanceCents("ACC-111"));
    }

    @Test
    void insufficientFundsIsIdempotentFailure() {
        PostedTransaction first = ledger.submit(request("txn-poor", "ACC-POOR", "ACC-222", 5.00));
        long poorAfter = ledger.balanceCents("ACC-POOR");
        long destAfter = ledger.balanceCents("ACC-222");
        PostedTransaction second = ledger.submit(request("txn-poor", "ACC-POOR", "ACC-222", 5.00));

        assertFalse(first.success());
        assertEquals(first, second);
        assertEquals(poorAfter, ledger.balanceCents("ACC-POOR"));
        assertEquals(destAfter, ledger.balanceCents("ACC-222"));
        assertEquals(InMemoryLedger.POOR_OPENING_CENTS, poorAfter);
    }

    @Test
    void rejectsUnknownAccount() {
        InvalidTransactionException ex = assertThrows(
                InvalidTransactionException.class,
                () -> ledger.submit(request("txn-unknown", "ACC-MISSING", "ACC-222", 1.00))
        );
        assertTrue(ex.getMessage().contains("unknown account"));
        assertTrue(ledger.find("txn-unknown").isEmpty());
    }

    @Test
    void rejectsSameAccountTransfer() {
        assertThrows(
                InvalidTransactionException.class,
                () -> ledger.submit(request("txn-self", "ACC-111", "ACC-111", 1.00))
        );
    }

    private static TransactionRequest request(String id, String from, String to, double amount) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom(from)
                .setAccountTo(to)
                .setAmount(amount)
                .setCurrency("USD")
                .build();
    }
}
