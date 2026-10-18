#!/bin/sh
# Persisted fold vs. fold on read, cold and warm, on the local Postgres test database (Docker
# container es-sdk-test-pg). Builds first. The numbers go into bench/RESULTS.md by hand, with the
# command that produced them.
#   sh bench/persisted-fold.sh                                  10 000 events of 1 KB, 11 processes per cell
#   sh bench/persisted-fold.sh <events> <payloadBytes> [reps]   one size
#   sh bench/persisted-fold.sh matrix                           every size of the matrix (synthesis §7)
#                                                               this kind has; 200 KB × 100 000 is never run
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
if [ "${1:-}" = "matrix" ]; then
  status=0
  for size in "100 1024" "10000 1024" "100000 1024" "100 204800" "10000 204800"; do
    # shellcheck disable=SC2086 # two words on purpose
    node bench/persisted-fold-bench.mjs $size "${2:-11}" || status=1
    echo ""
  done
  exit $status
fi
node bench/persisted-fold-bench.mjs "${1:-10000}" "${2:-1024}" "${3:-11}"
