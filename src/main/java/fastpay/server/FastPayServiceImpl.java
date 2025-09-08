package fastpay.server;


import fastpay.proto.FastPayGrpc;
import fastpay.proto.TransactionRequest;
import fastpay.proto.TransactionResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ScheduledExecutorService;

public class FastPayServiceImpl extends FastPayGrpc.FastPayImplBase {
    private final ScheduledExecutorService workerPool;

    public FastPayServiceImpl(ScheduledExecutorService workerPool) {
        this.workerPool = workerPool;
    }

    private long nowNanos() {
        return System.nanoTime();
    }

    @Override
    public void processTransaction(TransactionRequest req, StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        workerPool.execute(() -> {
            TransactionResponse resp = TransactionResponse.newBuilder()
                    .setTransactionId(req.getTransactionId())
                    .setSuccess(true)
                    .setMessage("Processed " + req.getAmount() + " " + req.getCurrency()
                            + " from " + req.getAccountFrom() + " to " + req.getAccountTo())
                    .setServerTimestampNanos(nowNanos())
                    .setProcessingNanos(nowNanos() - start)
                    .build();
            respObs.onNext(resp);
            respObs.onCompleted();
        });
    }

    @Override
    public StreamObserver<TransactionRequest> uploadTransactions(StreamObserver<TransactionResponse> respObs) {
        long start = nowNanos();
        return new StreamObserver<>() {
            int count = 0;
            double total = 0.0;

            @Override
            public void onNext(TransactionRequest req) {
                count++;
                total += req.getAmount();
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                TransactionResponse resp = TransactionResponse.newBuilder()
                        .setTransactionId("bulk-upload")
                        .setSuccess(true)
                        .setMessage("Processed " + count + " transactions, total amount=" + total)
                        .setServerTimestampNanos(nowNanos())
                        .setProcessingNanos(nowNanos() - start)
                        .build();
                respObs.onNext(resp);
                respObs.onCompleted();
            }
        };
    }

    @Override
    public void transactionStatus(TransactionRequest req, StreamObserver<TransactionResponse> respObs) {
        for (int i = 1; i <= 3; i++) {
            int step = i;
            workerPool.execute(() -> {
                TransactionResponse resp = TransactionResponse.newBuilder()
                        .setTransactionId(req.getTransactionId())
                        .setSuccess(true)
                        .setMessage("Status update " + step + " for transaction " + req.getTransactionId())
                        .setServerTimestampNanos(nowNanos())
                        .setProcessingNanos(1000L * step)
                        .build();
                respObs.onNext(resp);
                if (step == 3) {
                    respObs.onCompleted();
                }
            });
        }
    }

    @Override
    public StreamObserver<TransactionRequest> liveTransactions(StreamObserver<TransactionResponse> respObs) {
        return new StreamObserver<>() {
            @Override
            public void onNext(TransactionRequest req) {
                TransactionResponse resp = TransactionResponse.newBuilder()
                        .setTransactionId(req.getTransactionId())
                        .setSuccess(true)
                        .setMessage("Live processed " + req.getAmount() + " " + req.getCurrency())
                        .setServerTimestampNanos(nowNanos())
                        .setProcessingNanos(500)
                        .build();
                respObs.onNext(resp);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                respObs.onCompleted();
            }
        };
    }
}
