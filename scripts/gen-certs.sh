#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/certs"
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout "$ROOT/certs/server.key" \
  -out "$ROOT/certs/server.crt" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
cp "$ROOT/certs/server.crt" "$ROOT/certs/ca.crt"
chmod 644 "$ROOT/certs/server.key" "$ROOT/certs/server.crt" "$ROOT/certs/ca.crt"
echo "Wrote demo certs under $ROOT/certs (localhost only; not for production)."
