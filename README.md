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
Look up a payment by `transaction_id` only (`TransactionQuery`)
Example: initiated → authorized → settled → confirmed

Bidirectional streaming – Live transactions
Continuous two-way stream between client and server
Perfect for trading platforms, fraud monitoring, or high-frequency payments

SQLite ledger (`Ledger`, formerly `InMemoryLedger`) – debit the source account and credit the destination
Amounts are `int64 amount_cents` on the wire (no floating-point money)
`PaymentStatus`: PENDING → AUTHORIZED → SETTLED, or FAILED / FLAGGED
Request `currency` must match both accounts (seeded accounts are `USD`)

Idempotency – `transaction_id` is unique; a retry sets `replayed=true` and
does not post a second transfer (including insufficient-funds and fraud flags)

Durable ledger – balances and payments survive process restart (`FASTPAY_DB`
or `FASTPAY_JDBC_URL`). Seeded demo accounts: ACC-111, ACC-222, ACC-AAA,
ACC-BBB ($10,000.00) and ACC-POOR ($1.00)

OpenAccount – create additional accounts (`opening_cents` of `0` means $10,000.00)

Memo – optional `memo` on `TransactionRequest` / `PaymentRecord` (max 280 characters)

RefundTransaction – reverse a settled payment back to the source account.
Idempotent: default refund id is `refund:{transaction_id}`. Failed (NSF / flagged)
payments cannot be refunded. A refund cannot be refunded.

GetPayment / ListTransactions – inspect a payment or recent history (newest first),
including `created_at_millis` and `refund_of`

ListAccounts – paginated directory of accounts (id order)

ListJournal – ADMIN-only double-entry audit trail (signed `delta_cents`)

Pagination – `ListAccounts`, `ListTransactions`, and `ListJournal` take
`page_token` and return `next_page_token` (opaque keyset cursor). Empty
`next_page_token` means the last page. Invalid tokens are `INVALID_ARGUMENT`.

Auth – hashed API keys in the ledger with two roles:
- `pay-token` (PAYMENTS): transfers, refunds, status, GetAccount, OpenAccount, ListAccounts, list one account
- `admin-token` (ADMIN): also list all payments and the journal, plus `CreateApiKey` / `RevokeApiKey`
Override with `FASTPAY_PAY_TOKEN` / `FASTPAY_ADMIN_TOKEN`

CreateApiKey returns the plaintext secret once (`fpk_…`). Revoke by `token` or unique
`label`. The last ADMIN key cannot be revoked. Seeded labels are `payments` and `admin`.

Health and reflection – `grpc.health.v1.Health` and server reflection do **not**
require a bearer token, so `grpc_health_probe` / `grpcurl` work without `-H`.
The health status is set to `NOT_SERVING` before a graceful shutdown.

Rate limit – token-bucket per hashed API key (default 20 requests/sec, burst 40).
Unary and server-streaming RPCs count as one request; bulk/live streams count
each message. Health and reflection are unlimited. Returns `RESOURCE_EXHAUSTED`
with `grpc-retry-pushback-ms`. Override with `FASTPAY_RATE_LIMIT_QPS` /
`FASTPAY_RATE_LIMIT_BURST` (`0` disables the limiter).

Webhooks – a transactional outbox row is written in the same commit when a
payment is `SETTLED`, `FAILED`, or `FLAGGED` (`payment.settled` /
`payment.failed` / `payment.flagged`). Replays do not enqueue a second event.
Set `FASTPAY_WEBHOOK_URL` to POST JSON (optional `FASTPAY_WEBHOOK_SECRET`
adds `X-FastPay-Signature: sha256=…`). Empty URL keeps rows pending.

Postgres – SQLite remains the demo default. For HA:

```bash
docker compose -f docker-compose.yml -f docker-compose.postgres.yml up -d --build
```

`FASTPAY_JDBC_URL=jdbc:postgresql://host:5432/fastpay?user=fastpay&password=fastpay`

TLS – private keys are **not** committed. `FASTPAY_TLS=true` generates localhost
certs via openssl if `certs/server.key` is missing (`scripts/gen-certs.sh`).

CI – GitHub Actions runs `./gradlew test` (including Postgres when the
service is available) and `docker build` on pushes and PRs to `main`.

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
they start a temporary server, run unary + refund + bulk + live samples, then stop it.

To keep a server up for `ghz` / `grpcurl` or a second terminal:

```bash
./gradlew run                 # server on 127.0.0.1:6565 (blocks)
./gradlew runClient           # uses that server and leaves it running
FASTPAY_TLS=true ./gradlew run
FASTPAY_TLS=true ./gradlew runClient
```

`grpcurl` can use reflection (no `--proto` file) because the server exposes it:

```bash
grpcurl -plaintext 127.0.0.1:6565 list
grpcurl -plaintext -H 'authorization: Bearer pay-token' \
  -d '{"transaction_id":"txn-curl","account_from":"ACC-111","account_to":"ACC-222","amount_cents":1000,"currency":"USD"}' \
  127.0.0.1:6565 fastpay.FastPay/ProcessTransaction
```

Docker (Docker Desktop must be running first: `docker info` should succeed)

Build creates an **image** named `fastpay` (Images tab). A **container** only
appears after `docker run` / `docker compose up` (Containers tab). Use a name and `-d` so it stays
listed; `--rm` deletes the container as soon as it exits.

```bash
docker compose up -d --build
docker compose logs -f
docker compose down
```

Postgres (HA) instead of the SQLite volume:

```bash
docker compose -f docker-compose.yml -f docker-compose.postgres.yml up -d --build
```

Or without Compose:

```bash
docker build -t fastpay .
docker run -d --name fastpay-grpc -p 6565:6565 -v fastpay-data:/data fastpay
docker logs -f fastpay-grpc    # should print "FastPay gRPC server started on port 6565"
docker stop fastpay-grpc && docker rm fastpay-grpc
```

TLS in the container:

```bash
docker run -d --name fastpay-grpc -p 6565:6565 -e FASTPAY_TLS=true fastpay
```

Default payments token is `pay-token` (admin is `admin-token`). ghz must send it.
Disable the rate limiter (or raise it) for load tests:

```bash
FASTPAY_RATE_LIMIT_QPS=0 ./gradlew run
ghz --insecure \
    --proto src/main/proto/fastpay.proto \
    --call fastpay.FastPay.ProcessTransaction \
    -m '{"authorization":"Bearer pay-token"}' \
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
-m → metadata; required `authorization: Bearer pay-token`

Monitor latency percentiles (p50, p95, p99) and throughput (QPS).

Use native Netty Epoll on Linux for lower syscalls overhead
Enable connection pooling & keep-alives on the client
Offload blocking IO (DB, external APIs) to async worker pool
Tune maxInboundMessageSize and HTTP/2 flow control for large payloads
Use TLS/mTLS for securing sensitive financial transactions

Real-World Use Cases

Instant money transfers (like Zelle, Venmo, UPI)
Payroll bulk uploads (corporates paying employees)
Refunds and account opening for onboarding
Real-time trading & settlements
Fraud detection pipelines with streaming APIs
Banking API integration (ACH, SEPA, SWIFT gateways)

With FastPay, you have a fintech-grade blueprint for building low-latency, high-throughput APIs in Java using gRPC + Protobuf.

This is still a demo: SQLite is the default store, Postgres is a single-node HA
step (not a multi-region payments fabric), the TLS certs are localhost self-signed, and API keys are hashed bearer tokens—not a bank-grade IAM or PCI program.
