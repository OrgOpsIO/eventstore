#!/bin/sh
# The verify command: everything that must be green before a piece of work counts as done.
# Exit 0 only when every step is green; the first red step stops it and says which.
#   sh checks/check.sh
# Needs the local Postgres 18 test database (Docker container es-sdk-test-pg). The benchmarks are
# not part of it; they run from bench/.
set -eu

cd "$(dirname "$0")/.."
DB_URL="postgres://es:es@localhost:5432/es_sdk_test"
TMP="${TMPDIR:-/tmp}"
STEP=""

fail() {
  echo ""
  echo "check.sh: RED at step: $STEP"
  [ $# -gt 0 ] && echo "  $*"
  exit 1
}
step() {
  STEP="$1"
  echo ""
  echo "── $STEP"
}

step "typecheck (npm run typecheck)"
npm run typecheck || fail

step "memory tests (npm test)"
npm test || fail

step "build (npm run build)"
npm run build || fail

step "Postgres reachable ($DB_URL)"
node -e '
const pg = require("pg");
const c = new pg.Client({ connectionString: process.argv[1], connectionTimeoutMillis: 3000 });
c.connect().then(() => c.query("select version() as v")).then((r) => { console.log(r.rows[0].v); return c.end(); })
  .catch((e) => { console.error(e.message); process.exit(1); });
' "$DB_URL" || fail "Postgres is not reachable at $DB_URL — start the Docker container es-sdk-test-pg (docker start es-sdk-test-pg) and run again"

step "Postgres tests (npm run test:pg), none skipped"
REPORT="$TMP/es-check-test-pg-$$.json"
rm -f "$REPORT"
npm run test:pg -- --reporter=default --reporter=json --outputFile="$REPORT" || fail "npm run test:pg failed"
node -e '
const r = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
const skipped = [];
for (const file of r.testResults) for (const t of file.assertionResults) if (t.status !== "passed") skipped.push(`${t.status}: ${file.name.replace(process.cwd() + "/", "")} > ${t.fullName}`);
console.log(`test:pg: ${r.numPassedTests} passed, ${r.numFailedTests} failed, ${r.numPendingTests + r.numTodoTests} skipped of ${r.numTotalTests}`);
if (r.numFailedTests > 0 || skipped.length > 0 || r.numTotalTests === 0) { for (const s of skipped) console.log("  " + s); process.exit(1); }
' "$REPORT" || fail "a Postgres test failed or was skipped (with ES_TEST_DATABASE_URL set, nothing may skip)"
rm -f "$REPORT"

step "release untouched: package.json version is 0.6.4"
node -e 'const v = require("./package.json").version; if (v !== "0.6.4") { console.error("version is " + v); process.exit(1); } console.log("version " + v);' || fail

step "no new runtime dependency: no dependencies, peers exactly pg and zod"
node -e '
const p = require("./package.json");
const deps = Object.keys(p.dependencies || {});
const peers = Object.keys(p.peerDependencies || {}).sort().join(",");
if (deps.length > 0) { console.error("dependencies: " + deps.join(", ")); process.exit(1); }
if (peers !== "pg,zod") { console.error("peerDependencies: " + peers); process.exit(1); }
console.log("dependencies: none; peerDependencies: " + peers);
' || fail "a runtime dependency needs a decision in .ziv/decisions/ first"

step "the core stays driver-free: no pg import outside src/postgres/"
PG_IMPORTS=$(grep -rlE "from \"pg\"|from 'pg'|require\(\"pg\"\)|require\('pg'\)|import\(\"pg\"\)|import\('pg'\)" src --include='*.ts' | grep -v '^src/postgres/' || true)
[ -z "$PG_IMPORTS" ] || fail "pg is imported outside src/postgres/: $PG_IMPORTS"
echo "none"

echo ""
echo "check.sh: GREEN — typecheck, tests (memory and Postgres, none skipped), build and the fast limits"
