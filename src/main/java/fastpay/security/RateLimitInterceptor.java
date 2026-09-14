package fastpay.security;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Token-bucket limiter keyed on the hashed bearer token. Health and reflection
 * are skipped. {@code qps <= 0} disables limiting.
 */
public final class RateLimitInterceptor implements ServerInterceptor {
    static final Metadata.Key<String> RETRY_PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    private final double qps;
    private final double burst;
    private final LongSupplier nanoTime;
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public RateLimitInterceptor(double qps, int burst) {
        this(qps, burst, System::nanoTime);
    }

    RateLimitInterceptor(double qps, int burst, LongSupplier nanoTime) {
        this.qps = qps;
        this.burst = Math.max(1, burst);
        this.nanoTime = nanoTime;
    }

    public boolean enabled() {
        return qps > 0;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        if (!enabled() || AuthInterceptor.isPublicService(call.getMethodDescriptor().getServiceName())) {
            return next.startCall(call, headers);
        }
        String key = bucketKey(headers.get(Auth.AUTHORIZATION));
        if (call.getMethodDescriptor().getType().clientSendsOneMessage()) {
            if (!tryAcquire(key)) {
                reject(call);
                return new ServerCall.Listener<>() {};
            }
            return next.startCall(call, headers);
        }
        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            private boolean closed;

            @Override
            public void onMessage(ReqT message) {
                if (closed) {
                    return;
                }
                if (!tryAcquire(key)) {
                    closed = true;
                    reject(call);
                    return;
                }
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!closed) {
                    super.onHalfClose();
                }
            }
        };
    }

    boolean tryAcquire(String bucketKey) {
        if (!enabled()) {
            return true;
        }
        Bucket bucket = buckets.computeIfAbsent(bucketKey, ignored -> new Bucket(burst, nanoTime.getAsLong()));
        return bucket.tryAcquire(qps, burst, nanoTime.getAsLong());
    }

    private <ReqT, RespT> void reject(ServerCall<ReqT, RespT> call) {
        Metadata trailers = new Metadata();
        trailers.put(RETRY_PUSHBACK_MS, String.valueOf(pushbackMillis()));
        call.close(Status.RESOURCE_EXHAUSTED.withDescription(
                "rate limit exceeded (" + formatQps() + " requests/sec per token)"
        ), trailers);
    }

    private long pushbackMillis() {
        return Math.max(1L, Math.round(1000.0 / qps));
    }

    private String formatQps() {
        if (qps == Math.rint(qps)) {
            return String.valueOf((long) qps);
        }
        return String.valueOf(qps);
    }

    static String bucketKey(String authorizationHeader) {
        String token = TokenStore.unwrap(authorizationHeader);
        if (token == null) {
            return "anonymous";
        }
        return TokenStore.sha256(token);
    }

    static final class Bucket {
        private double tokens;
        private long lastNanos;

        Bucket(double initialTokens, long nowNanos) {
            this.tokens = initialTokens;
            this.lastNanos = nowNanos;
        }

        synchronized boolean tryAcquire(double qps, double burst, long nowNanos) {
            if (nowNanos > lastNanos) {
                double elapsedSeconds = (nowNanos - lastNanos) / 1_000_000_000.0d;
                tokens = Math.min(burst, tokens + elapsedSeconds * qps);
                lastNanos = nowNanos;
            }
            if (tokens >= 1.0d) {
                tokens -= 1.0d;
                return true;
            }
            return false;
        }
    }
}
