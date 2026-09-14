package fastpay.metrics;

import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FastPayMetricsTest {
    @Test
    void recordsRpcLedgerFraudAndRateLimit() {
        FastPayMetrics metrics = new FastPayMetrics();
        metrics.recordRpc("ProcessTransaction", Status.Code.OK);
        metrics.recordRpc("ProcessTransaction", Status.Code.RESOURCE_EXHAUSTED);
        metrics.recordFraud("amount");
        metrics.recordFraud("velocity");
        metrics.recordRateLimitReject();
        String result = metrics.timeLedger(() -> "ok");

        assertEquals("ok", result);
        assertEquals(1.0d, metrics.rpcCount("ProcessTransaction", Status.Code.OK));
        assertEquals(1.0d, metrics.rpcCount("ProcessTransaction", Status.Code.RESOURCE_EXHAUSTED));
        assertEquals(1.0d, metrics.fraudCount("amount"));
        assertEquals(1.0d, metrics.fraudCount("velocity"));
        assertEquals(1.0d, metrics.rateLimitRejectCount());
        assertEquals(1L, metrics.ledgerCount());
        assertEquals(-1, metrics.httpPort());
    }

    @Test
    void fraudReasonMapsVelocityAndAmount() {
        assertEquals("velocity", FastPayMetrics.fraudReason("flagged: velocity exceeded for ACC-111"));
        assertEquals("amount", FastPayMetrics.fraudReason("flagged: amount_cents 100001 exceeds threshold"));
        assertEquals("amount", FastPayMetrics.fraudReason(null));
    }
}
