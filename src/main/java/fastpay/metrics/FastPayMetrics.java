package fastpay.metrics;

import io.grpc.Status;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Prometheus metrics for FastPay. HTTP scrape is optional ({@code FASTPAY_METRICS_PORT}).
 */
public final class FastPayMetrics implements AutoCloseable {
    public static final int DEFAULT_PORT = 6566;

    private static final FastPayMetrics NOOP = new FastPayMetrics(new PrometheusRegistry());

    private final PrometheusRegistry registry;
    private final Counter rpcRequests;
    private final Histogram ledgerDuration;
    private final Counter fraudFlags;
    private final Counter rateLimitRejects;
    private HTTPServer http;

    public FastPayMetrics() {
        this(new PrometheusRegistry());
    }

    public FastPayMetrics(PrometheusRegistry registry) {
        this.registry = registry;
        this.rpcRequests = Counter.builder()
                .name("fastpay_rpc_requests")
                .help("Completed FastPay gRPC requests")
                .labelNames("method", "code")
                .register(registry);
        this.ledgerDuration = Histogram.builder()
                .name("fastpay_ledger_duration_seconds")
                .help("Ledger submit, reject, and refund duration")
                .classicOnly()
                .classicUpperBounds(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)
                .register(registry);
        this.fraudFlags = Counter.builder()
                .name("fastpay_fraud_flags")
                .help("Live-stream payments flagged by fraud rules")
                .labelNames("reason")
                .register(registry);
        this.rateLimitRejects = Counter.builder()
                .name("fastpay_rate_limit_rejects")
                .help("Authenticated RPCs rejected by the per-token rate limiter")
                .register(registry);
    }

    public static FastPayMetrics noop() {
        return NOOP;
    }

    public void startHttp(int port) throws IOException {
        if (port < 0) {
            throw new IllegalArgumentException("metrics port must be >= 0");
        }
        if (http != null) {
            throw new IllegalStateException("metrics HTTP already started");
        }
        this.http = HTTPServer.builder()
                .port(port)
                .registry(registry)
                .buildAndStart();
    }

    public int httpPort() {
        return http == null ? -1 : http.getPort();
    }

    public void recordRpc(String method, Status.Code code) {
        String name = (method == null || method.isBlank()) ? "unknown" : method;
        String status = code == null ? "UNKNOWN" : code.name();
        rpcRequests.labelValues(name, status).inc();
    }

    public <T> T timeLedger(Supplier<T> action) {
        long start = System.nanoTime();
        try {
            return action.get();
        } finally {
            ledgerDuration.observe((System.nanoTime() - start) / 1_000_000_000.0d);
        }
    }

    public void recordFraud(String reason) {
        fraudFlags.labelValues(reason == null || reason.isBlank() ? "unknown" : reason).inc();
    }

    public static String fraudReason(String message) {
        if (message != null && message.contains("velocity")) {
            return "velocity";
        }
        return "amount";
    }

    public void recordRateLimitReject() {
        rateLimitRejects.inc();
    }

    public double rpcCount(String method, Status.Code code) {
        return rpcRequests.labelValues(method, code.name()).get();
    }

    public double fraudCount(String reason) {
        return fraudFlags.labelValues(reason).get();
    }

    public double rateLimitRejectCount() {
        return rateLimitRejects.get();
    }

    public long ledgerCount() {
        HistogramSnapshot snapshot = ledgerDuration.collect();
        long total = 0;
        for (HistogramSnapshot.HistogramDataPointSnapshot point : snapshot.getDataPoints()) {
            if (point.hasCount()) {
                total += point.getCount();
            }
        }
        return total;
    }

    @Override
    public void close() {
        if (http != null) {
            http.close();
            http = null;
        }
    }
}
