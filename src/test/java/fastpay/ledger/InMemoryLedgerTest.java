package fastpay.ledger;

import fastpay.proto.TransactionRequest;
import fastpay.security.Auth;
import fastpay.security.Role;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

    @AfterEach
    void tearDown() {
        if (ledger != null) {
            ledger.close();
        }
    }

    @Test
    void postsDebitAndCredit() {
        long fromBefore = ledger.balanceCents("ACC-111");
        long toBefore = ledger.balanceCents("ACC-222");

        SubmitResult result = ledger.submit(request("txn-ledger-1", "ACC-111", "ACC-222", 25075));

        assertTrue(result.transaction().success());
        assertFalse(result.replayed());
        assertEquals(fromBefore - 25075, ledger.balanceCents("ACC-111"));
        assertEquals(toBefore + 25075, ledger.balanceCents("ACC-222"));
        assertTrue(result.transaction().message().contains("250.75"));
        assertJournalBalances("ACC-111");
        assertJournalBalances("ACC-222");
    }

    @Test
    void duplicateTransactionIdDoesNotDoublePost() {
        long fromBefore = ledger.balanceCents("ACC-111");
        SubmitResult first = ledger.submit(request("txn-dup", "ACC-111", "ACC-222", 1000));
        SubmitResult second = ledger.submit(request("txn-dup", "ACC-111", "ACC-222", 1000));

        assertTrue(first.transaction().success());
        assertFalse(first.replayed());
        assertTrue(second.replayed());
        assertEquals(first.transaction(), second.transaction());
        assertEquals(fromBefore - 1000, ledger.balanceCents("ACC-111"));
        assertEquals(1, ledger.listPayments("ACC-111", 10).size());
        assertJournalBalances("ACC-111");
    }

    @Test
    void insufficientFundsIsIdempotentFailureAndSkipsJournal() {
        int journalBefore = ledger.journalEntries("ACC-POOR").size();
        SubmitResult first = ledger.submit(request("txn-poor", "ACC-POOR", "ACC-222", 500));
        long poorAfter = ledger.balanceCents("ACC-POOR");
        long destAfter = ledger.balanceCents("ACC-222");
        SubmitResult second = ledger.submit(request("txn-poor", "ACC-POOR", "ACC-222", 500));

        assertFalse(first.transaction().success());
        assertTrue(second.replayed());
        assertEquals(first.transaction(), second.transaction());
        assertEquals(poorAfter, ledger.balanceCents("ACC-POOR"));
        assertEquals(destAfter, ledger.balanceCents("ACC-222"));
        assertEquals(InMemoryLedger.POOR_OPENING_CENTS, poorAfter);
        assertEquals(journalBefore, ledger.journalEntries("ACC-POOR").size());
        assertEquals(1, ledger.listPayments("ACC-POOR", 10).size());
    }

    @Test
    void rejectsUnknownAccount() {
        InvalidTransactionException ex = assertThrows(
                InvalidTransactionException.class,
                () -> ledger.submit(request("txn-unknown", "ACC-MISSING", "ACC-222", 100))
        );
        assertTrue(ex.getMessage().contains("unknown account"));
        assertTrue(ledger.find("txn-unknown").isEmpty());
    }

    @Test
    void rejectsSameAccountTransfer() {
        assertThrows(
                InvalidTransactionException.class,
                () -> ledger.submit(request("txn-self", "ACC-111", "ACC-111", 100))
        );
    }

    @Test
    void concurrentDuplicateIdPostsOnce() throws Exception {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        List<Future<SubmitResult>> futures = IntStream.range(0, threads)
                .mapToObj(i -> pool.submit(() -> {
                    start.await();
                    try {
                        return ledger.submit(request("txn-race", "ACC-111", "ACC-222", 2500));
                    } finally {
                        done.countDown();
                    }
                }))
                .toList();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        List<SubmitResult> results = futures.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();
        long originals = results.stream().filter(result -> !result.replayed()).count();
        long replays = results.stream().filter(SubmitResult::replayed).count();
        assertEquals(1, originals);
        assertEquals(threads - 1, replays);
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 2500, ledger.balanceCents("ACC-111"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS + 2500, ledger.balanceCents("ACC-222"));
        assertJournalBalances("ACC-111");
        assertJournalBalances("ACC-222");
    }

    @Test
    void concurrentDistinctTransfersPreserveBalances() throws Exception {
        int transfers = 50;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<SubmitResult>> futures = IntStream.range(0, transfers)
                .mapToObj(i -> pool.submit(() ->
                        ledger.submit(request("txn-par-" + i, "ACC-111", "ACC-222", 100))))
                .toList();
        for (Future<SubmitResult> future : futures) {
            assertTrue(future.get(10, TimeUnit.SECONDS).transaction().success());
        }
        pool.shutdown();
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 5000, ledger.balanceCents("ACC-111"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS + 5000, ledger.balanceCents("ACC-222"));
        assertEquals(transfers, ledger.listPayments("ACC-111", 100).size());
        assertJournalBalances("ACC-111");
        assertJournalBalances("ACC-222");
    }

    @Test
    void persistsAcrossReopen() throws Exception {
        Path db = Files.createTempFile("fastpay", ".db");
        try {
            try (InMemoryLedger first = new InMemoryLedger(db)) {
                first.submit(request("txn-durable", "ACC-111", "ACC-222", 2500));
                assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 2500, first.balanceCents("ACC-111"));
            }
            try (InMemoryLedger second = new InMemoryLedger(db)) {
                assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS - 2500, second.balanceCents("ACC-111"));
                assertTrue(second.find("txn-durable").isPresent());
                assertTrue(second.find("txn-durable").get().success());
            }
        } finally {
            Files.deleteIfExists(db);
            Files.deleteIfExists(Path.of(db.toString() + "-wal"));
            Files.deleteIfExists(Path.of(db.toString() + "-shm"));
        }
    }

    @Test
    void refundReversesBalancesAndIsIdempotent() {
        ledger.submit(request("txn-refund", "ACC-111", "ACC-222", 2500));
        SubmitResult first = ledger.refund("txn-refund", "");
        SubmitResult second = ledger.refund("txn-refund", "");

        assertTrue(first.transaction().success());
        assertFalse(first.replayed());
        assertTrue(second.replayed());
        assertEquals(first.transaction(), second.transaction());
        assertEquals("txn-refund", first.transaction().refundOf());
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS, ledger.balanceCents("ACC-111"));
        assertEquals(InMemoryLedger.DEFAULT_OPENING_CENTS, ledger.balanceCents("ACC-222"));
        assertJournalBalances("ACC-111");
        assertJournalBalances("ACC-222");
    }

    @Test
    void refundRejectsFailedPayment() {
        ledger.submit(request("txn-poor-refund", "ACC-POOR", "ACC-222", 500));
        InvalidTransactionException ex = assertThrows(
                InvalidTransactionException.class,
                () -> ledger.refund("txn-poor-refund", "")
        );
        assertTrue(ex.getMessage().contains("only settled"));
    }

    @Test
    void currencyMismatchIsRejected() {
        ledger.openAccount("ACC-EUR", 50_000, "EUR");
        InvalidTransactionException ex = assertThrows(
                InvalidTransactionException.class,
                () -> ledger.submit(request("txn-fx", "ACC-111", "ACC-EUR", 100))
        );
        assertTrue(ex.getMessage().contains("currency mismatch"));
        assertTrue(ledger.find("txn-fx").isEmpty());
    }

    @Test
    void openAccountPersistsAcrossReopen() throws Exception {
        Path db = Files.createTempFile("fastpay-open", ".db");
        try {
            try (InMemoryLedger first = new InMemoryLedger(db)) {
                first.openAccount("ACC-NEW", 12_34, "USD");
                first.submit(request("txn-new", "ACC-NEW", "ACC-111", 34));
            }
            try (InMemoryLedger second = new InMemoryLedger(db)) {
                assertEquals(1200, second.balanceCents("ACC-NEW"));
                SubmitResult refund = second.refund("txn-new", "refund-custom");
                assertTrue(refund.transaction().success());
                assertEquals("refund-custom", refund.transaction().transactionId());
                assertEquals(1234, second.balanceCents("ACC-NEW"));
            }
        } finally {
            Files.deleteIfExists(db);
            Files.deleteIfExists(Path.of(db.toString() + "-wal"));
            Files.deleteIfExists(Path.of(db.toString() + "-shm"));
        }
    }

    @Test
    void listAccountsAndPaymentsUseKeysetPages() {
        ledger.openAccount("ACC-ZZZ", 100, "USD");
        Page<AccountSnapshot> firstAccounts = ledger.listAccounts(2, null);
        assertEquals(2, firstAccounts.items().size());
        assertTrue(firstAccounts.hasNextPage());
        Page<AccountSnapshot> rest = ledger.listAccounts(10, firstAccounts.nextPageToken());
        assertTrue(rest.items().stream().noneMatch(account ->
                account.accountId().equals(firstAccounts.items().get(0).accountId())
                        || account.accountId().equals(firstAccounts.items().get(1).accountId())));

        ledger.submit(request("txn-p1", "ACC-111", "ACC-222", 100));
        ledger.submit(request("txn-p2", "ACC-111", "ACC-222", 200));
        ledger.submit(request("txn-p3", "ACC-111", "ACC-222", 300));
        Page<PostedTransaction> page1 = ledger.listPayments("ACC-111", 2, null);
        assertEquals(2, page1.items().size());
        assertEquals("txn-p3", page1.items().get(0).transactionId());
        Page<PostedTransaction> page2 = ledger.listPayments("ACC-111", 2, page1.nextPageToken());
        assertEquals("txn-p1", page2.items().get(0).transactionId());
        assertFalse(page2.hasNextPage());

        assertThrows(InvalidTransactionException.class, () -> ledger.listAccounts(2, "%%%"));
    }

    @Test
    void storesMemoOnPayment() {
        SubmitResult result = ledger.submit(request("txn-memo", "ACC-111", "ACC-222", 100)
                .toBuilder().setMemo("rent").build());
        assertEquals("rent", result.transaction().memo());
        assertEquals("rent", ledger.find("txn-memo").orElseThrow().memo());
    }

    @Test
    void createAndRevokeApiKey() {
        CreatedApiKey created = ledger.createApiKey("ops", Role.PAYMENTS);
        assertTrue(created.token().startsWith("fpk_"));
        assertEquals(Role.PAYMENTS, ledger.tokenStore().authenticate(Auth.bearer(created.token())).orElseThrow());

        String label = ledger.revokeApiKey(created.token(), "");
        assertEquals("ops", label);
        assertTrue(ledger.tokenStore().authenticate(Auth.bearer(created.token())).isEmpty());
        assertThrows(InvalidTransactionException.class, () -> ledger.revokeApiKey("", "admin"));
    }

    private void assertJournalBalances(String accountId) {
        long journalSum = ledger.journalEntries(accountId).stream()
                .mapToLong(JournalEntry::deltaCents)
                .sum();
        assertEquals(ledger.balanceCents(accountId), journalSum);
    }

    private static TransactionRequest request(String id, String from, String to, long amountCents) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom(from)
                .setAccountTo(to)
                .setAmountCents(amountCents)
                .setCurrency("USD")
                .build();
    }
}
