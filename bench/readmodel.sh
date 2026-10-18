#!/bin/sh
# The live read model vs. fold on read: the matrix cells, list, watch and heap, on the local
# Postgres test database (Docker container es-sdk-test-pg). Builds first. The numbers go into
# bench/RESULTS.md by hand, with the command that produced them.
#   sh bench/readmodel.sh                               every cell below (about 20 minutes; 18 min on an M5 Pro)
#   sh bench/readmodel.sh matrix                        every size of the matrix (synthesis §7) this kind has;
#                                                       200 KB × 100 000 is never run (the human's limit)
#   sh bench/readmodel.sh matrix <events> <payloadBytes> [reps]
#   sh bench/readmodel.sh list [keys] [reps]            1 000, 10 000 and 100 000 keys without arguments
#   sh bench/readmodel.sh watch [samples]
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
status=0
matrix() {
  for size in "100 1024" "10000 1024" "100000 1024" "100 204800" "10000 204800"; do
    # shellcheck disable=SC2086 # two words on purpose
    node bench/readmodel-bench.mjs matrix $size "${1:-11}" || status=1
    echo ""
  done
}
lists() {
  for keys in 1000 10000 100000; do
    node bench/readmodel-bench.mjs list "$keys" "${1:-5}" || status=1
    echo ""
  done
}
case "${1:-all}" in
  matrix)
    if [ $# -ge 3 ]; then node bench/readmodel-bench.mjs matrix "$2" "$3" "${4:-11}" || status=1; else matrix "${2:-11}"; fi ;;
  list)
    if [ $# -ge 2 ]; then node bench/readmodel-bench.mjs list "$2" "${3:-5}" || status=1; else lists; fi ;;
  watch)
    node bench/readmodel-bench.mjs watch "${2:-30}" || status=1 ;;
  all)
    matrix 11
    lists 5
    node bench/readmodel-bench.mjs watch 30 || status=1 ;;
  *)
    echo "usage: sh bench/readmodel.sh [all | matrix [<events> <payloadBytes>] [reps] | list [keys] [reps] | watch [samples]]" >&2
    exit 2 ;;
esac
exit $status
