#!/bin/sh
# The whole benchmark matrix of synthesis §7 in one run, on one commit (decision 54): fold on read vs. the persisted
# fold vs. the live read model vs. the persisted read model, 100 / 10 000 / 100 000 events × 1 KB and 100 / 10 000
# events × 200 KB (200 KB × 100 000 is never run: the human's limit), cold and warm, 11 fresh processes per cell.
# It runs the four column commands back to back, unchanged in what they measure:
#   sh bench/persisted-fold.sh matrix      sh bench/readmodel.sh matrix
#   sh bench/persisted-model.sh matrix     sh bench/versions.sh live
# and tees each column's raw output to <outDir>/<column>.txt. About 1 h 15 min on an M5 Pro. Run it ALONE on the
# machine (no verify, no other bench): load from other processes caused the stage-5 statement timeouts.
#   sh bench/matrix.sh [outDir]            outDir defaults to a new directory under $TMPDIR
# It refuses to run when src/ has uncommitted changes: the numbers must belong to a commit. Exit non-zero when any
# column did. The numbers go into bench/RESULTS.md by hand, with this command and the commit it prints.
set -u
cd "$(dirname "$0")/.."
if [ -n "$(git status --porcelain -- src)" ]; then
  echo "bench/matrix.sh: src/ has uncommitted changes; the numbers must belong to a commit. Commit them first:" >&2
  git status --short -- src >&2
  exit 2
fi
out=${1:-$(mktemp -d "${TMPDIR:-/tmp}/es-matrix.XXXXXX")}
mkdir -p "$out"
npm run build > /dev/null || { echo "bench/matrix.sh: npm run build failed" >&2; exit 1; }
{
  echo "matrix run: commit $(git rev-parse HEAD)"
  echo "machine: $(node -e 'const os = require("os"); console.log(`${os.cpus()[0]?.model ?? "?"}, ${os.cpus().length} cores, ${Math.round(os.totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}`)')"
  echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "raw outputs: $out"
} | tee "$out/run.txt"
status=0
column() {
  name=$1
  shift
  echo "" | tee -a "$out/run.txt"
  echo "== $name: $* — uptime before: $(uptime)" | tee -a "$out/run.txt"
  start=$(date +%s)
  if "$@" > "$out/$name.txt" 2>&1; then result=ok; else result="FAILED (exit $?)"; status=1; fi
  echo "== $name: $result in $(( $(date +%s) - start )) s — uptime after: $(uptime)" | tee -a "$out/run.txt"
  echo "   raw: $out/$name.txt" | tee -a "$out/run.txt"
}
column persisted-fold sh bench/persisted-fold.sh matrix
column readmodel sh bench/readmodel.sh matrix
column persisted-model sh bench/persisted-model.sh matrix
column versions-live sh bench/versions.sh live
echo "" | tee -a "$out/run.txt"
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ); commit $(git rev-parse HEAD); $( [ $status -eq 0 ] && echo 'every column ok' || echo 'a column FAILED' )" | tee -a "$out/run.txt"
exit $status
