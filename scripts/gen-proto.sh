#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROTO_DIR="$ROOT/api/proto"
OUT_DIR="$ROOT/api/gen"

mkdir -p "$OUT_DIR"
PROTOC_BIN=$(command -v protoc || true)
KITEX_BIN=$(command -v kitex || true)
if [ -z "$PROTOC_BIN" ] || [ -z "$KITEX_BIN" ]; then
  echo "protoc and kitex v0.16.3 or later must be installed and on PATH" >&2
  exit 1
fi

if [ -n "${PROTOC_INCLUDE:-}" ]; then
  WELL_KNOWN_INCLUDE=$PROTOC_INCLUDE
else
  PROTOC_PREFIX=$(CDPATH= cd -- "$(dirname -- "$PROTOC_BIN")/.." && pwd)
  WELL_KNOWN_INCLUDE=""
  for candidate in \
    "$PROTOC_PREFIX/include" \
    "/usr/local/include" \
    "/opt/homebrew/include" \
    "/usr/include"; do
    if [ -f "$candidate/google/protobuf/timestamp.proto" ]; then
      WELL_KNOWN_INCLUDE=$candidate
      break
    fi
  done
fi
if [ ! -f "$WELL_KNOWN_INCLUDE/google/protobuf/timestamp.proto" ]; then
  echo "google/protobuf/timestamp.proto not found; set PROTOC_INCLUDE to its include root" >&2
  exit 1
fi

rm -rf "$OUT_DIR/userservice"
rm -f "$OUT_DIR/user.pb.go" "$OUT_DIR/user_grpc.pb.go"

cd "$ROOT"
"$KITEX_BIN" \
  -module github.com/luck/go-learning \
  -type protobuf \
  -gen-path api/gen \
  -I "$PROTO_DIR" \
  -I "$WELL_KNOWN_INCLUDE" \
  "$PROTO_DIR/user.proto"
