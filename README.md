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

In-memory ledger – debit the source account and credit the destination
Amounts are `int64 amount_cents` on the wire (no floating-point money)
`PaymentStatus`: PENDING → AUTHORIZED → SETTLED, or FAILED / FLAGGED

Idempotency – `transaction_id` is unique; a retry sets `replayed=true` and
does not post a second transfer (including insufficient-funds and fraud flags)

Auth interceptor – `authorization: Bearer demo-token` (override with `FASTPAY_AUTH_TOKEN`)
Validation interceptor – required fields, positive `amount_cents`, distinct accounts
Client deadlines – 5s unary / 15s streaming; server honors cancellation

TLS – set `FASTPAY_TLS=true` and point at `certs/server.crt` + `certs/server.key`
(demo certs are self-signed for localhost; regenerate with `scripts/gen-certs.sh`)

Live-stream fraud – amount above $1,000.00 (`100000` cents) or more than 8
live payments from the same account in 10 seconds is `FLAGGED` and not posted
(unary and bulk uploads skip these rules so payroll batches still work)

gRPC over HTTP/2 for multiplexed streams and low-latency communication
Protobuf serialization for compact, fast, binary payloads
Netty transport (with optional Epoll on Linux) for high-performance networking
Async, non-blocking handlers so the server can scale to thousands of requests/sec

Build (use Gradle, not a raw `javac` / IDE compiler)

Stubs in `fastpay.proto` are generated from `src/main/proto/fastpay.proto` by
`./gradlew generateProto` (this runs automatically before `compileJava`).

```bash
./gradlew build
./gradlew runClient
```

`runClient` (and `runDemo`) connect to `127.0.0.1:6565`. If nothing is listening,
they start a temporary server, run the unary + live sample, then stop it.

To keep a server up for `ghz` / `grpcurl` or a second terminal:

```bash
./gradlew run                 # server on 127.0.0.1:6565 (blocks)
./gradlew runClient           # uses that server and leaves it running
FASTPAY_TLS=true ./gradlew run
FASTPAY_TLS=true ./gradlew runClient
```

Docker (no Gradle on the host):

```bash
docker build -t fastpay .
docker run --rm -p 6565:6565 fastpay
# TLS in the container:
docker run --rm -p 6565:6565 -e FASTPAY_TLS=true fastpay
```

Default auth token is `demo-token`. ghz must send it:

```bash
ghz --insecure \
    --proto src/main/proto/fastpay.proto \
    --call fastpay.FastPay.ProcessTransaction \
    -m '{"authorization":"Bearer demo-token"}' \
    -d '{"transaction_id":"x","account_from":"ACC-111","account_to":"ACC-222","amount_cents":1000,"currency":"USD"}' \
    -c 200 -n 100000 127.0.0.1:6565
```

If you see `package fastpay.proto does not exist` or `NettyServerBuilder`:
you are compiling `main` from before this change, or IntelliJ is compiling
without Gradle. Re-import the project as a Gradle project and set
**Build and run using: Gradle** (Settings → Build, Execution, Deployment →
Build Tools → Gradle). Then run `./gradlew clean generateProto build`.


TransactionRequest {
  transaction_id: "txn-123",
  account_from: "ACC-111",
  account_to: "ACC-222",
  amount_cents: 25075,
  currency: "USD"
}

TransactionResponse {
  transaction_id: "txn-123",
  success: true,
  status: SETTLED,
  amount_cents: 25075,
  message: "Processed 250.75 USD from ACC-111 to ACC-222",
  processing_nanos: 53412
}

 -c 200 → 200 concurrent clients
-n 100000 → 100k total requests
-m → metadata; required `authorization: Bearer demo-token`

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
