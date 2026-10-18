#!/bin/sh
# Versions and rebuild (stage 5): the rebuild beside the old version, the live model's start: "persisted"
# (a row of the matrix), the persisted fold after a bump (paged), the retire recipe, and the fingerprint's
# cost, on the local Postgres test database (Docker container es-sdk-test-pg). Builds first. The numbers go
# into bench/RESULTS.md by hand, with the command that produced them.
#   sh bench/versions.sh                                every cell below
#   sh bench/versions.sh beside [K] [reps]              K = 1 000 and 100 000 without K
#   sh bench/versions.sh live [<events> <payloadBytes>] [reps]   every size of the matrix without a size;
#                                                       200 KB × 100 000 is never run (the human's limit)
#   sh bench/versions.sh paged [reps]
#   sh bench/versions.sh retire [reps]
#   sh bench/versions.sh fingerprint [definitions] [reps]   (no database; on the sources through vite-node)
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
status=0
live() {
  for size in "100 1024" "10000 1024" "100000 1024" "100 204800" "10000 204800"; do
    # shellcheck disable=SC2086 # two words on purpose
    node bench/versions-bench.mjs live $size "${1:-11}" || status=1
    echo ""
  done
}
beside() {
  for k in 1000 100000; do
    node bench/versions-bench.mjs beside "$k" "${1:-3}" || status=1
    echo ""
  done
}
case "${1:-all}" in
  beside)
    if [ $# -ge 2 ]; then node bench/versions-bench.mjs beside "$2" "${3:-3}" || status=1; else beside 3; fi ;;
  live)
    if [ $# -ge 3 ]; then node bench/versions-bench.mjs live "$2" "$3" "${4:-11}" || status=1; else live "${2:-11}"; fi ;;
  paged)
    node bench/versions-bench.mjs paged "${2:-3}" || status=1 ;;
  retire)
    node bench/versions-bench.mjs retire "${2:-3}" || status=1 ;;
  fingerprint)
    node_modules/.bin/vite-node bench/fingerprint-bench.mjs "${2:-2000}" "${3:-5}" || status=1 ;;
  all)
    beside 3
    live 11
    node bench/versions-bench.mjs paged 3 || status=1
    echo ""
    node bench/versions-bench.mjs retire 3 || status=1
    echo ""
    node_modules/.bin/vite-node bench/fingerprint-bench.mjs 2000 5 || status=1 ;;
  *)
    echo "usage: sh bench/versions.sh [all | beside [K] [reps] | live [<events> <payloadBytes>] [reps] | paged [reps] | retire [reps] | fingerprint [definitions] [reps]]" >&2
    exit 2 ;;
esac
exit $status
