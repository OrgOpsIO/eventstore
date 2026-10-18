#!/bin/sh
# The persisted read model vs. the live read model vs. fold on read: the matrix cells, list,
# rebuild and resume, steady state, takeover and lock slots, on the local Postgres test database
# (Docker container es-sdk-test-pg). Builds first. The numbers go into bench/RESULTS.md by hand,
# with the command that produced them.
#   sh bench/persisted-model.sh                               every cell below
#   sh bench/persisted-model.sh matrix                        every size of the matrix (synthesis §7);
#                                                             200 KB × 100 000 is never run (the human's limit)
#   sh bench/persisted-model.sh matrix <events> <payloadBytes> [reps]
#   sh bench/persisted-model.sh list [rows] [reps]            1 000, 10 000 and 100 000 rows without arguments
#   sh bench/persisted-model.sh rebuild [events] [reps]      100 000 events, 3 runs per cell without arguments;
#                                                             1000000 is the stage-7 cell (decision 56)
#   sh bench/persisted-model.sh steady [appends]
#   sh bench/persisted-model.sh takeover [reps]
#   sh bench/persisted-model.sh locks
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
status=0
matrix() {
  for size in "100 1024" "10000 1024" "100000 1024" "100 204800" "10000 204800"; do
    # shellcheck disable=SC2086 # two words on purpose
    node bench/persisted-model-bench.mjs matrix $size "${1:-11}" || status=1
    echo ""
  done
}
lists() {
  for rows in 1000 10000 100000; do
    node bench/persisted-model-bench.mjs list "$rows" "${1:-5}" || status=1
    echo ""
  done
}
case "${1:-all}" in
  matrix)
    if [ $# -ge 3 ]; then node bench/persisted-model-bench.mjs matrix "$2" "$3" "${4:-11}" || status=1; else matrix "${2:-11}"; fi ;;
  list)
    if [ $# -ge 2 ]; then node bench/persisted-model-bench.mjs list "$2" "${3:-5}" || status=1; else lists; fi ;;
  rebuild)
    node bench/persisted-model-bench.mjs rebuild "${2:-100000}" "${3:-3}" || status=1 ;;
  steady)
    node bench/persisted-model-bench.mjs steady "${2:-200}" || status=1 ;;
  takeover)
    node bench/persisted-model-bench.mjs takeover "${2:-5}" || status=1 ;;
  locks)
    node bench/persisted-model-bench.mjs locks || status=1 ;;
  all)
    matrix 11
    lists 5
    node bench/persisted-model-bench.mjs rebuild 100000 3 || status=1
    echo ""
    node bench/persisted-model-bench.mjs steady 200 || status=1
    echo ""
    node bench/persisted-model-bench.mjs takeover 5 || status=1
    echo ""
    node bench/persisted-model-bench.mjs locks || status=1 ;;
  *)
    echo "usage: sh bench/persisted-model.sh [all | matrix [<events> <payloadBytes>] [reps] | list [rows] [reps] | rebuild [events] [reps] | steady [appends] | takeover [reps] | locks]" >&2
    exit 2 ;;
esac
exit $status
