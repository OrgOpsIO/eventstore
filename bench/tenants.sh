#!/bin/sh
# Tenants, rls and erasure (stage 6): statements per append with one processor vs per-tenant handles, the processor's
# page time vs tenants per page, the rebuild across tenants, the erasure recipe's time, invalidate(), and a reader of a
# processor-written model at 100 000 events against fold on read — all under rls, in a schema and roles of their own,
# on the local Postgres test database (Docker container es-sdk-test-pg). Builds first. The numbers go into
# bench/RESULTS.md by hand, with the command that produced them.
#   sh bench/tenants.sh                              every cell below
#   sh bench/tenants.sh fanout [tenants] [appends]   1, 10, 100 and 1 000 tenants without a number
#   sh bench/tenants.sh page [tenants] [runs] [fresh|analyzed]   1, 10, 100 and 1 000 tenants per page, both, without a number
#   sh bench/tenants.sh rebuild [runs] [fresh|analyzed]          both without the third argument
#   sh bench/tenants.sh erasure [events] [runs]      1 000, 10 000 and 100 000 events without a number
#   sh bench/tenants.sh invalidate [runs]
#   sh bench/tenants.sh read [runs]
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
status=0
each() {
  cell="$1"
  sizes="$2"
  reps="$3"
  stats="${4:-}"
  for n in $sizes; do
    # shellcheck disable=SC2086 # no fourth argument when empty
    node bench/tenants-bench.mjs "$cell" "$n" "$reps" $stats || status=1
    echo ""
  done
}
case "${1:-all}" in
  fanout)
    if [ $# -ge 2 ]; then node bench/tenants-bench.mjs fanout "$2" "${3:-7}" || status=1; else each fanout "1 10 100 1000" 7; fi ;;
  page)
    if [ $# -ge 2 ]; then node bench/tenants-bench.mjs page "$2" "${3:-3}" "${4:-fresh}" || status=1; else each page "1 10 100 1000" 3 fresh; each page "1 10 100 1000" 3 analyzed; fi ;;
  rebuild)
    if [ $# -ge 3 ]; then node bench/tenants-bench.mjs rebuild "$2" "$3" || status=1; else
      node bench/tenants-bench.mjs rebuild "${2:-3}" fresh || status=1
      node bench/tenants-bench.mjs rebuild "${2:-3}" analyzed || status=1
    fi ;;
  erasure)
    if [ $# -ge 2 ]; then node bench/tenants-bench.mjs erasure "$2" "${3:-3}" || status=1; else each erasure "1000 10000 100000" 3; fi ;;
  invalidate)
    node bench/tenants-bench.mjs invalidate "${2:-11}" || status=1 ;;
  read)
    node bench/tenants-bench.mjs read "${2:-11}" || status=1 ;;
  all)
    each fanout "1 10 100 1000" 7
    each page "1 10 100 1000" 3 fresh
    each page "1 10 100 1000" 3 analyzed
    node bench/tenants-bench.mjs rebuild 3 fresh || status=1
    echo ""
    node bench/tenants-bench.mjs rebuild 3 analyzed || status=1
    echo ""
    each erasure "1000 10000 100000" 3
    node bench/tenants-bench.mjs invalidate 11 || status=1
    echo ""
    node bench/tenants-bench.mjs read 11 || status=1 ;;
  *)
    echo "usage: sh bench/tenants.sh [all | fanout [tenants] [appends] | page [tenants] [runs] | rebuild [runs] | erasure [events] [runs] | invalidate [runs] | read [runs]]" >&2
    exit 2 ;;
esac
exit $status
