#!/bin/sh
# What the stage-2 subscription additions cost (transaction id, watermark, single-event skip,
# processor mode), on the local Postgres test database (Docker container es-sdk-test-pg). Builds
# first. The numbers go into bench/RESULTS.md by hand, with the command that produced them.
#   sh bench/subscriptions.sh                       every cell, default repetitions
#   sh bench/subscriptions.sh <cell> [reps]         one cell: append | query | poison | latency
#   ES_BENCH_BEFORE=<dist dir> sh bench/subscriptions.sh ...
#                                                   append and query also against a build from
#                                                   before the change, in alternating processes
set -eu
cd "$(dirname "$0")/.."
npm run build > /dev/null
# shellcheck disable=SC2086 # optional argument on purpose
node bench/subscriptions-bench.mjs "${1:-all}" ${2:-} ${ES_BENCH_BEFORE:+"--before=$ES_BENCH_BEFORE"}
