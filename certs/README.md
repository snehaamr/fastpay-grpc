TLS material for local/dev only.

Private keys (`*.key`) are gitignored and generated on demand:

    scripts/gen-certs.sh
    FASTPAY_TLS=true ./gradlew run

Do not commit production certificates. Use a real CA (or your company’s PKI) outside this repo.
