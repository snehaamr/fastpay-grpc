package fastpay.webhook;

import fastpay.ledger.Ledger;
import fastpay.ledger.OutboxRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Polls the transactional outbox and POSTs JSON to {@code FASTPAY_WEBHOOK_URL}.
 */
public final class WebhookDispatcher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);
    public static final int MAX_ATTEMPTS = 8;

    private final Ledger ledger;
    private final String webhookUrl;
    private final String secret;
    private final HttpClient http;
    private ScheduledFuture<?> poll;

    public WebhookDispatcher(Ledger ledger, String webhookUrl, String secret) {
        this.ledger = ledger;
        this.webhookUrl = webhookUrl == null ? "" : webhookUrl.trim();
        this.secret = secret == null ? "" : secret;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    }

    public boolean enabled() {
        return !webhookUrl.isBlank();
    }

    public void start(ScheduledExecutorService pool) {
        if (!enabled() || poll != null) {
            return;
        }
        poll = pool.scheduleWithFixedDelay(this::tickSafe, 200, 200, TimeUnit.MILLISECONDS);
    }

    public int tick() {
        if (!enabled()) {
            return 0;
        }
        int delivered = 0;
        List<OutboxRecord> pending = ledger.pendingOutbox(20);
        for (OutboxRecord row : pending) {
            if (deliver(row)) {
                delivered++;
            }
        }
        return delivered;
    }

    private void tickSafe() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("webhook poll failed", e);
        }
    }

    private boolean deliver(OutboxRecord row) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(webhookUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("X-FastPay-Event", row.eventType())
                    .POST(HttpRequest.BodyPublishers.ofString(row.payload()));
            if (!secret.isBlank()) {
                builder.header("X-FastPay-Signature", "sha256=" + hmac(row.payload(), secret));
            }
            HttpResponse<Void> response = http.send(builder.build(), HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                ledger.markOutboxDelivered(row.id());
                return true;
            }
            retry(row, "HTTP " + response.statusCode());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retry(row, "interrupted");
            return false;
        } catch (Exception e) {
            retry(row, e.getClass().getSimpleName() + ": " + e.getMessage());
            return false;
        }
    }

    private void retry(OutboxRecord row, String error) {
        int attempts = row.attempts() + 1;
        if (attempts >= MAX_ATTEMPTS) {
            ledger.markOutboxFailed(row.id(), error);
            return;
        }
        long delayMs = Math.min(60_000L, 1_000L * (1L << Math.min(attempts, 6)));
        ledger.markOutboxRetry(row.id(), attempts, System.currentTimeMillis() + delayMs, error);
    }

    static String hmac(String body, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("hmac", e);
        }
    }

    @Override
    public void close() {
        if (poll != null) {
            poll.cancel(false);
            poll = null;
        }
    }
}
