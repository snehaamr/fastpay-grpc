#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS="$ROOT/certs"
mkdir -p "$CERTS"

if [[ ! -f "$CERTS/ca.key" || ! -f "$CERTS/ca.crt" ]]; then
  openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -keyout "$CERTS/ca.key" \
    -out "$CERTS/ca.crt" \
    -subj "/CN=FastPay Dev CA"
fi

if [[ ! -f "$CERTS/server.key" || ! -f "$CERTS/server.crt" ]]; then
  openssl req -new -newkey rsa:2048 -nodes \
    -keyout "$CERTS/server.key" \
    -out "$CERTS/server.csr" \
    -subj "/CN=localhost"
  openssl x509 -req -in "$CERTS/server.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
    -CAcreateserial -out "$CERTS/server.crt" -days 365 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
  rm -f "$CERTS/server.csr"
fi

if [[ ! -f "$CERTS/client.key" || ! -f "$CERTS/client.crt" ]]; then
  openssl req -new -newkey rsa:2048 -nodes \
    -keyout "$CERTS/client.key" \
    -out "$CERTS/client.csr" \
    -subj "/CN=fastpay-client"
  openssl x509 -req -in "$CERTS/client.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
    -CAcreateserial -out "$CERTS/client.crt" -days 365 -sha256 \
    -extfile <(printf "extendedKeyUsage=clientAuth\n")
  rm -f "$CERTS/client.csr"
fi

chmod 644 "$CERTS"/*.key "$CERTS"/*.crt
echo "Wrote demo certs under $CERTS (localhost CA + server + client; not for production)."
