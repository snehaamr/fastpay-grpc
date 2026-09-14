package fastpay.webhook;

import fastpay.ledger.Ledger;
import fastpay.ledger.OutboxRecord;
import fastpay.proto.TransactionRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebhookDispatcherTest {
    private Ledger ledger;
    private HttpServer http;

    @AfterEach
    void tearDown() {
        if (http != null) {
            http.stop(0);
        }
        if (ledger != null) {
            ledger.close();
        }
    }

    @Test
    void postsOutboxAndMarksDelivered() throws Exception {
        ledger = new Ledger();
        ledger.submit(tx("hook-1", 100));
        CountDownLatch latch = new CountDownLatch(1);
        List<String> bodies = new ArrayList<>();
        List<String> events = new ArrayList<>();
        List<String> signatures = new ArrayList<>();
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/hook", exchange -> {
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            events.add(exchange.getRequestHeaders().getFirst("X-FastPay-Event"));
            signatures.add(exchange.getRequestHeaders().getFirst("X-FastPay-Signature"));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            latch.countDown();
        });
        http.start();
        String url = "http://127.0.0.1:" + http.getAddress().getPort() + "/hook";
        WebhookDispatcher dispatcher = new WebhookDispatcher(ledger, url, "s3cret");
        assertEquals(1, dispatcher.tick());
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(PaymentEvents.SETTLED, events.get(0));
        assertTrue(bodies.get(0).contains("\"transaction_id\":\"hook-1\""));
        assertEquals("sha256=" + WebhookDispatcher.hmac(bodies.get(0), "s3cret"), signatures.get(0));
        assertEquals(OutboxRecord.DELIVERED, ledger.listOutbox().get(0).status());
        assertEquals(0, dispatcher.tick());
    }

    @Test
    void retriesOnHttpError() {
        ledger = new Ledger();
        ledger.submit(tx("hook-fail", 100));
        WebhookDispatcher dispatcher = new WebhookDispatcher(ledger, "http://127.0.0.1:1/missing", "");
        assertEquals(0, dispatcher.tick());
        OutboxRecord row = ledger.listOutbox().get(0);
        assertEquals(OutboxRecord.PENDING, row.status());
        assertEquals(1, row.attempts());
        assertTrue(row.nextAttemptAtMillis() > System.currentTimeMillis());
    }

    private static TransactionRequest tx(String id, long cents) {
        return TransactionRequest.newBuilder()
                .setTransactionId(id)
                .setAccountFrom("ACC-111")
                .setAccountTo("ACC-222")
                .setAmountCents(cents)
                .setCurrency("USD")
                .build();
    }
}
