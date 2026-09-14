#!/bin/sh
set -eu
mkdir -p /data /opt/fastpay/certs
CERT="${FASTPAY_CERT:-/opt/fastpay/certs/server.crt}"
KEY="${FASTPAY_KEY:-/opt/fastpay/certs/server.key}"
TRUST="${FASTPAY_TRUST_CERT:-/opt/fastpay/certs/ca.crt}"
CLIENT_CERT="${FASTPAY_CLIENT_CERT:-/opt/fastpay/certs/client.crt}"
CLIENT_KEY="${FASTPAY_CLIENT_KEY:-/opt/fastpay/certs/client.key}"
if [ "${FASTPAY_MTLS:-false}" = "true" ] || [ "${FASTPAY_TLS:-false}" = "true" ]; then
  if [ ! -f "$KEY" ]; then
    openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
      -keyout "$KEY" -out "$CERT" \
      -subj "/CN=localhost" \
      -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
    cp "$CERT" "$TRUST"
  fi
  if [ "${FASTPAY_MTLS:-false}" = "true" ] && [ ! -f "$CLIENT_KEY" ]; then
    SIGNING_KEY="$KEY"
    if [ -f "${TRUST%.*}.key" ]; then
      SIGNING_KEY="${TRUST%.*}.key"
    fi
    CSR="$(mktemp)"
    EXT="$(mktemp)"
    printf 'extendedKeyUsage=clientAuth\n' > "$EXT"
    openssl req -new -newkey rsa:2048 -nodes \
      -keyout "$CLIENT_KEY" -out "$CSR" -subj "/CN=fastpay-client"
    openssl x509 -req -in "$CSR" -CA "$TRUST" -CAkey "$SIGNING_KEY" \
      -CAcreateserial -out "$CLIENT_CERT" -days 365 -sha256 -extfile "$EXT"
    rm -f "$CSR" "$EXT"
  fi
fi
exec /opt/fastpay/bin/fastpay-grpc
