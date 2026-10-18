#!/bin/sh
# The showcase: builds the library, then shows what a read of kept state costs, per kind,
# against the memory store and the local Postgres test database (Docker container es-sdk-test-pg).
#   sh bench/showcase.sh
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
node bench/showcase.mjs
