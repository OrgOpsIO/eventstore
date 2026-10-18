#!/bin/sh
# The processor's cursor read when only 1 % of the events match, with EXPLAIN (synthesis §7, decision 56), on the
# local Postgres test database (Docker container es-sdk-test-pg). Builds first. The numbers go into bench/RESULTS.md
# by hand, with the command that produced them.
#   sh bench/cursor.sh                    100 000 and 1 000 000 events, 3 catch-ups per cell
#   sh bench/cursor.sh <events> [reps]
# The 1 000 000-event table takes ~1.2 GB while it exists; it is dropped at the end.
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
status=0
if [ $# -ge 1 ]; then
  node bench/cursor-bench.mjs "$1" "${2:-3}" || status=1
else
  for n in 100000 1000000; do
    node bench/cursor-bench.mjs "$n" 3 || status=1
    echo ""
  done
fi
exit $status
