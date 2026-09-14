TLS material for local/dev only.

Private keys (`*.key`) are gitignored and generated on demand:

    scripts/gen-certs.sh
    FASTPAY_TLS=true ./gradlew run
    FASTPAY_MTLS=true ./gradlew run

`scripts/gen-certs.sh` writes a dev CA, a localhost server cert, and a client
cert. mTLS requires the client cert (`FASTPAY_CLIENT_CERT` / `FASTPAY_CLIENT_KEY`)
in addition to the existing bearer token.

Do not commit production certificates. Use a real CA (or your company’s PKI) outside this repo.
