#!/usr/bin/env bash
# Generate Kotlin + Swift bindings for the `stream` core into target/bindings/.
#
# Usage: ./generate-bindings.sh [language]   (language: kotlin | swift | both)
# The bindings land in core/target/bindings/<language>/ and are git-ignored
# (generated artifacts, regenerated per platform by the app build).
set -euo pipefail

cd "$(dirname "$0")"

LANG="${1:-both}"
cd ..

cargo build -p stream-ffi

LIB="target/debug/libstream_ffi.so"
BINDGEN="cargo run -p stream-ffi --bin uniffi-bindgen --"

mkdir -p "target/bindings"

if [[ "$LANG" == "kotlin" || "$LANG" == "both" ]]; then
  echo ">> Kotlin"
  $BINDGEN generate --language kotlin --no-format \
    --out-dir "target/bindings/kotlin" --library "$LIB"
fi

if [[ "$LANG" == "swift" || "$LANG" == "both" ]]; then
  echo ">> Swift"
  $BINDGEN generate --language swift --no-format \
    --out-dir "target/bindings/swift" --library "$LIB"
fi

echo ">> done: core/target/bindings/"
