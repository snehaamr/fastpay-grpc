#!/bin/sh
set -eu
mkdir -p /data /opt/fastpay/certs
CERT="${FASTPAY_CERT:-/opt/fastpay/certs/server.crt}"
KEY="${FASTPAY_KEY:-/opt/fastpay/certs/server.key}"
TRUST="${FASTPAY_TRUST_CERT:-/opt/fastpay/certs/ca.crt}"
if [ "${FASTPAY_TLS:-false}" = "true" ] && [ ! -f "$KEY" ]; then
  openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
    -keyout "$KEY" -out "$CERT" \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
  cp "$CERT" "$TRUST"
fi
exec /opt/fastpay/bin/fastpay-grpc
