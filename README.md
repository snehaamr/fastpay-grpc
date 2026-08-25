FastPay gRPC – Real-Time Financial Transactions API

FastPay is a high-throughput, low-latency gRPC service that simulates a real-time payments system.
It demonstrates how financial platforms like Zelle, UPI, VisaNet, or ACH could implement scalable APIs for instant money transfers, bulk uploads, and live transaction streaming using gRPC + Protobuf in Java.

Features

Unary RPC – Process a single transaction
Low-latency, request/response flow
Ideal for one-off payments (e.g., a customer paying a merchant)

Client streaming – Bulk transaction upload
Upload batches of payments in one connection
Useful for payroll systems or batch settlements

Server streaming – Transaction status updates
Receive multiple status updates for a single payment
Example: initiated → authorized → settled → confirmed

Bidirectional streaming – Live transactions
Continuous two-way stream between client and server
Perfect for trading platforms, fraud monitoring, or high-frequency payments

gRPC over HTTP/2 for multiplexed streams and low-latency communication
Protobuf serialization for compact, fast, binary payloads
Netty transport (with optional Epoll on Linux) for high-performance networking
Async, non-blocking handlers so the server can scale to thousands of requests/sec


./gradlew build
./gradlew run
./gradlew run -PmainClass=fastpay.client.FastPayClient

TransactionRequest {
  transaction_id: "txn-123",
  account_from: "ACC-111",
  account_to: "ACC-222",
  amount: 250.75,
  currency: "USD"
}

TransactionResponse {
  transaction_id: "txn-123",
  success: true,
  message: "Processed 250.75 USD from ACC-111 to ACC-222",
  processing_nanos: 53412
}

ghz --insecure \
    --proto src/main/proto/fastpay.proto \
    --call fastpay.FastPay.ProcessTransaction \
    -d '{"transaction_id":"x","account_from":"A","account_to":"B","amount":10.0,"currency":"USD"}' \
    -c 200 -n 100000 127.0.0.1:6565

 -c 200 → 200 concurrent clients
-n 100000 → 100k total requests

Monitor latency percentiles (p50, p95, p99) and throughput (QPS).

Use native Netty Epoll on Linux for lower syscalls overhead
Enable connection pooling & keep-alives on the client
Offload blocking IO (DB, external APIs) to async worker pool
Tune maxInboundMessageSize and HTTP/2 flow control for large payloads
Use TLS/mTLS for securing sensitive financial transactions

Real-World Use Cases

Instant money transfers (like Zelle, Venmo, UPI)
Payroll bulk uploads (corporates paying employees)
Real-time trading & settlements
Fraud detection pipelines with streaming APIs
Banking API integration (ACH, SEPA, SWIFT gateways)

With FastPay, you have a fintech-grade blueprint for building low-latency, high-throughput APIs in Java using gRPC + Protobuf.
