package fastpay.fraud;

import fastpay.proto.TransactionRequest;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Live-stream fraud checks: hard amount cap and per-account velocity.
 */
public final class FraudGuard {
    public static final long DEFAULT_AMOUNT_THRESHOLD_CENTS = 100_000L; // $1,000.00
    public static final int DEFAULT_MAX_PAYMENTS = 8;
    public static final long DEFAULT_WINDOW_MS = 10_000L;

    private final long amountThresholdCents;
    private final int maxPayments;
    private final long windowMs;
    private final ConcurrentHashMap<String, Deque<Long>> liveTimestamps = new ConcurrentHashMap<>();

    public FraudGuard() {
        this(DEFAULT_AMOUNT_THRESHOLD_CENTS, DEFAULT_MAX_PAYMENTS, DEFAULT_WINDOW_MS);
    }

    public FraudGuard(long amountThresholdCents, int maxPayments, long windowMs) {
        this.amountThresholdCents = amountThresholdCents;
        this.maxPayments = maxPayments;
        this.windowMs = windowMs;
    }

    public Optional<String> evaluate(TransactionRequest request) {
        if (request.getAmountCents() > amountThresholdCents) {
            return Optional.of("flagged: amount_cents " + request.getAmountCents()
                    + " exceeds threshold " + amountThresholdCents);
        }
        if (velocityExceeded(request.getAccountFrom())) {
            return Optional.of("flagged: velocity exceeded for " + request.getAccountFrom()
                    + " (max " + maxPayments + " live payments / " + windowMs + "ms)");
        }
        return Optional.empty();
    }

    public void recordLivePayment(String accountFrom) {
        long now = System.currentTimeMillis();
        Deque<Long> stamps = liveTimestamps.computeIfAbsent(accountFrom, id -> new ArrayDeque<>());
        synchronized (stamps) {
            prune(stamps, now);
            stamps.addLast(now);
        }
    }

    private boolean velocityExceeded(String accountFrom) {
        Deque<Long> stamps = liveTimestamps.get(accountFrom);
        if (stamps == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        synchronized (stamps) {
            prune(stamps, now);
            return stamps.size() >= maxPayments;
        }
    }

    private void prune(Deque<Long> stamps, long now) {
        while (!stamps.isEmpty() && now - stamps.peekFirst() > windowMs) {
            stamps.removeFirst();
        }
    }
}
