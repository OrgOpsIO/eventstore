# Benchmark results

Measured numbers for kept state, each with the command that produced it. **The current numbers are the stage-7
summary right below**: one commit, one matrix run, the benches of decision 56, and the limits re-measured. Everything
under "History (stages 1–6)" further down is kept as it was measured then, with its date, commit and command: the
evidence earlier findings refer to (the timeouts, F2, the stage-4/5 reds), not the current code's numbers.

## Summary (stage 7, commit b0ce19e)

Every number in this section comes from one commit, **b0ce19e** (`git status --porcelain -- src` empty), measured by the
stage-7 measurer on 2026-10-18. The matrix: **`sh bench/matrix.sh`** (decision 54), run as
`sh bench/matrix.sh .ziv/lab/stage-07-measurer/matrix` from 23:07:30Z to 00:30:40Z (83 min; raw output of every column
in that directory on the board). It ran the four column commands `sh bench/persisted-fold.sh matrix`,
`sh bench/readmodel.sh matrix`, `sh bench/persisted-model.sh matrix` and `sh bench/versions.sh live`; 11 fresh
processes per cell (+1 warm-up round, not kept). **Exit 1**: the persisted-fold column's own red verdict at
100 × 1 KB, delta 10 (0.85×, table S2 below and the long form); every other column exited 0, and **no run of any
cell failed** (column "runs failed" 0 everywhere, with the 600-s bench timeouts below). Then, one after the other, the
benches of decision 56 and the `list` cells: `sh .ziv/lab/stage-07-measurer/after-matrix.sh` (cursor, rebuild 10⁵ and
10⁶, list at 100 000 keys/rows; each "ok"), and `node bench/tenants-bench.mjs read 11` (exit 0).

- **Bench reads wait long enough to measure** (decision 54): every bench store is opened with
  `timeouts: { statementMs: 600 000 }`, every live cold `get` passes `timeoutMs: 600 000`. The library's defaults
  (`timeoutMs` 10 000, `statement_timeout` 30 000 ms) are unchanged. With them, stages 3 and 4 lost first reads of the
  live model at 10 000 × 200 KB (4 of 44 / 21 of 22 runs timed out, see "Stage 3" / "Stage 4" under History) and stage 5
  lost 2 of 12 fold-on-read baselines to a statement timeout; with the bench timeouts every one of those cells has
  11 of 11 runs here (live rebuild 9.0–35.4 s, fold on read 9.0–29.6 s, min–max over the columns).
- **The machine was not idle.** Load average 14.5 when the matrix started, 39 after the first column, **112** seen
  during the second (the 10 000 × 200 KB cells), 6.6–31 afterwards (`uptime` before and after every column in
  `run.txt`). The load came from processes outside this run that the measurer could not see (`ps` and `top` are refused
  by the sandbox). Absolute times are therefore higher than in stages 1–6 (fold on read cold at 100 000 × 1 KB:
  606.6–837.4 ms across the four columns here, 563.5–869.0 ms before). **Every factor divides two cells of the same
  run**, measured in rotating order, so the load falls on both sides; that the load was uneven within a run is not
  excluded.
- Machine: Apple M5 Pro, 15 cores, 48 GiB, darwin arm64, Node v22.22.3, Postgres 18.6 (Debian 18.6-1.pgdg13+2) in the
  Docker container `es-sdk-test-pg` on the same machine (1.5 TB free in the container's data volume, `df -h`).
  **Cold** = the first read in a fresh process (store connected, database warm); **warm** = a further read in the same
  process with nothing new. Factor = fold on read ÷ the kind, of the same run; the verdict for the 10× target is
  exactly "reached" (≥ 10.0) or "not reached".

### S1 · The 10× target at 100 000 × 1 KB, one line per kind

The headline cell (decision 55): 100 000 events × 1 KB, **K = 1** (one key holds every event, so `model.get(k)` and
`es.context()` are the same 100 000-event state), `get`; live cold = the **default** start (a rebuild,
`batchSize` 500). Cold against cold and warm against warm (O2). The 10× target applies to the live and the persisted
read model; the persisted fold has the floor "faster than fold on read" (O3). Beside each line: every other cell of
the kind at 100 000 × 1 KB, never left out.

| kind | cold: ms, factor, verdict | warm: ms, factor, verdict | beside it (100 000 × 1 KB) | command |
|---|---|---|---|---|
| **fold on read** (the baseline) | 766.0 / 837.4 / 748.3 / 606.6 ms (the four columns' own runs) | 1.22 / 1.11 / 0.99 ms | no target: it is what the others are measured against | each column's command, below |
| **persisted fold** | 766.0 → **6.04 ms, 127×** (126.86×): faster than fold on read, **reached** (the floor, O3); ≥ 10× as a fact (the 10× target does not apply, O3) | 1.22 → 1.01 ms, 1.20×: a warm hit is the context cache, the tier is not read | delta 100: 9.31 ms, 82.2×; nothing stored yet (folds and writes): 755.3 ms, 1.01× | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| **live read model** | 837.4 → 1 333.5 ms, **0.63×, "not reached"** (the default start is a rebuild of the whole source) | 1.11 ms → **0.27 µs, 4 067×, reached** | cold with `batchSize` 5 000: 862.1 ms, 0.97× "not reached" (warm 4 691× reached); **cold with `start: "persisted"`: 606.6 → 5.58 ms, 109×, reached** (rows 100 behind: 56.4× reached); K = 1 000: cold 0.01× "not reached", warm 6 255× reached; `start: "persisted"` at K = 1 000 0.39×, at K = 100 000 0.01× "not reached"; `list` at 100 000 keys: the kept result warm 5 823× reached, the first `list` after a change 0.9× "not reached", every call under 1 append / 10 ms 2 905× reached | `node bench/readmodel-bench.mjs matrix 100000 1024 11`; `node bench/versions-bench.mjs live 100000 1024 11`; `node bench/readmodel-bench.mjs list 100000 5` |
| **persisted read model** | 748.3 → **5.40 ms, 138×, reached** (0 events folded) | 0.99 → 0.82 ms, **1.21×, "not reached"** (O16: a warm `get` reads the key's delta from the store) | writer 100 behind: cold 629.0 → 4.85 ms, 130× reached (100 folded), warm 1.20× "not reached"; K = 1 000: cold 1.15×, warm 1.78× "not reached"; a reader of a cross-tenant model under `rls`: cold 569.7 → 17.7 ms 32.2× reached, warm 2.81 → 1.92 ms 1.47× "not reached"; `list` at 100 000 rows: warm first page 5.28 → 31.2 ms **0.17× "not reached"** (F2, O17), cold first page 630.8 → 38.9 ms 16.2× reached | `node bench/persisted-model-bench.mjs matrix 100000 1024 11`; `node bench/tenants-bench.mjs read 11`; `node bench/persisted-model-bench.mjs list 100000 5` |

### S2 · The matrix (synthesis §7): one row per size × kind × cold/warm

K = 1 and the default variant of each kind; the factor against the fold on read **of the same run** (each column
measures its own). Every other variant (K = 1 000 and K = 100 000, `batchSize` 5 000, the stored state 10 / 100 behind,
the writer 100 behind, the persisted fold before its first write, each column's own fold-on-read cells) is in
"Stage 7: the whole matrix, every variant" right below this section. Factors are the benches' own (they divide
unrounded medians); one the bench does not print is computed from the printed medians and marked "(computed)". The
table is generated from the raw outputs by `python3 -I .ziv/lab/stage-07-measurer/summarize.py <dir> compact`.

| events × payload | K | kind (variant) | cold / warm | median ms (min–max) | fold on read, same run, ms | factor | verdict | events folded | command |
|---|---|---|---|---|---|---|---|---|---|
| 100 × 1 KB | 1 | fold on read | cold | 6.10 (4.97–6.33) | – (the baseline) | – | – | 100 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | fold on read | warm | 1.11 (0.86–1.33) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 0 | cold | 6.07 (4.69–7.71) | 6.10 | 1.00× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.17 (0.88–1.50) | 1.11 | 0.95× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 7.18 (5.52–12.0) | 4.23 | 0.59× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.23 µs (0.18 µs–0.28 µs) | 0.76 | 3,372× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, start: "persisted" | cold | 6.90 (4.88–11.7) | 4.05 | 0.59× | not reached | 0 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model | cold | 6.59 (4.33–9.60) | 6.13 | 0.93× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model | warm | 1.10 (0.91–2.06) | 1.55 | 1.41× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | cold | 76.9 (72.7–97.1) | – (the baseline) | – | – | 10000 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | warm | 1.28 (1.02–1.42) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 0 | cold | 5.85 (4.49–7.03) | 76.9 | 13.14× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.21 (0.92–1.60) | 1.28 | 1.06× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 107.7 (100.1–134.9) | 70.0 | 0.65× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.21 µs (0.19 µs–0.23 µs) | 0.66 | 3,128× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, start: "persisted" | cold | 5.23 (4.88–8.92) | 70.9 | 13.56× | reached | 0 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model | cold | 4.09 (3.74–4.83) | 87.5 | 21.42× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model | warm | 0.65 (0.56–0.69) | 0.84 | 1.29× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | cold | 766.0 (732.2–1122.9) | – (the baseline) | – | – | 100000 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | warm | 1.22 (0.97–1.73) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 0 | cold | 6.04 (4.93–8.23) | 766.0 | 126.86× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.01 (0.87–1.66) | 1.22 | 1.20× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 1333.5 (942.1–2754.6) | 837.4 | 0.63× | not reached | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.27 µs (0.20 µs–0.34 µs) | 1.11 | 4,067× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, start: "persisted" | cold | 5.58 (4.56–11.1) | 606.6 | 109× | reached | 0 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model | cold | 5.40 (3.83–8.62) | 748.3 | 138× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model | warm | 0.82 (0.61–1.18) | 0.99 | 1.21× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 × 200 KB | 1 | fold on read | cold | 130.8 (111.4–148.3) | – (the baseline) | – | – | 100 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | fold on read | warm | 1.02 (0.76–1.18) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 0 | cold | 5.20 (4.19–7.11) | 130.8 | 25.16× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 0 | warm | 0.92 (0.70–1.64) | 1.02 | 1.11× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | live read model, rebuild, batchSize 500 | cold | 353.1 (108.6–644.3) | 165.4 | 0.47× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, batchSize 500 | warm | 0.21 µs (0.16 µs–0.27 µs) | 1.97 | 9,511× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, start: "persisted" | cold | 5.95 (4.56–6.87) | 105.3 | 17.70× | reached | 0 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model | cold | 3.79 (3.50–5.26) | 106.6 | 28.13× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model | warm | 0.71 (0.66–0.76) | 0.81 | 1.15× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | cold | 10560.3 (10178.9–29642.9) | – (the baseline) | – | – | 10000 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | warm | 0.71 (0.59–1.67) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 0 | cold | 4.77 (3.92–8.09) | 10560.3 | 2214.66× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 0 | warm | 0.76 (0.61–1.68) | 0.71 | 0.94× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, rebuild, batchSize 500 | cold | 11269.4 (9291.5–14279.0) | 10092.0 | 0.90× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, batchSize 500 | warm | 0.31 µs (0.21 µs–0.35 µs) | 0.80 | 2,602× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, start: "persisted" | cold | 6.55 (5.31–10.7) | 10881.4 | 1,662× | reached | 0 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model | cold | 6.34 (3.84–11.5) | 10685.0 | 1,686× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model | warm | 0.93 (0.59–1.88) | 0.84 | 0.91× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 100 000 × 200 KB | – | every kind (fold on read, persisted fold, live read model, persisted read model) | cold, warm | not run: the human's limit | – | – | – | – | – |

The live model's warm read with `start: "persisted"` is the same in-memory `get` as the live row's warm read (the
start only changes how the states get into memory); `bench/versions-bench.mjs` measures the cold start only. **Red in
the run** (not a limit's cell: the floor names 10 000 and 100 000 events): the persisted fold at **100 × 1 KB with a
stored state 10 behind, 6.10 → 7.19 ms, 0.85×, NOT faster** — at 100 events the read of the stored state (2.77 ms)
costs more than folding 100 small events; delta 0 there is 1.00× with overlapping ranges. Stages 1–2 measured the
same cell faster (1.09×); under this run's load it is not.

### S3 · Rebuild throughput: from zero to caught up, persisted (Postgres `jsonb` rows, the write side included) vs. live (in memory)

`sh bench/persisted-model.sh rebuild 100000 3` (exit 0, 74 s) and `sh bench/persisted-model.sh rebuild 1000000 3`
(exit 0, 481 s); 1 KB events, seeded through `append` in batches of 1 000 (2.23 s / 25.2 s), a fresh process and a
fresh model name per run, 3 runs per cell. `folded` = N in every run; every live run held exactly K keys.

| events | K (keys) | batchSize | model | to caught up, ms median (min–max) | events/s (median) | fold time, ms | live heap after, MB |
|---|---|---|---|---|---|---|---|
| 100 000 | 1 | 500 | persisted | **1 415.7** (1 376.3–1 553.2) | 70 637 | 35.9 | – |
| 100 000 | 1 | 500 | live | **895.9** (864.5–937.1) | 111 619 | 20.3 | 20.3 |
| 100 000 | 1 | 5 000 | persisted | **706.1** (680.7–726.5) | 141 620 | 27.7 | – |
| 100 000 | 1 | 5 000 | live | **596.8** (590.1–626.6) | 167 571 | 27.1 | 46.0 |
| 100 000 | 1 000 | 500 | persisted | **3 350.2** (3 083.6–4 058.9) | 29 849 | 241.2 | – |
| 100 000 | 1 000 | 500 | live | **1 003.0** (985.5–1 013.7) | 99 697 | 37.1 | 17.7 |
| 100 000 | 1 000 | 5 000 | persisted | **1 136.9** (1 115.4–1 161.5) | 87 961 | 80.0 | – |
| 100 000 | 1 000 | 5 000 | live | **624.3** (612.1–659.6) | 160 174 | 37.3 | 46.4 |
| 100 000 | 100 000 | 500 | persisted | **4 423.2** (4 376.4–4 593.0) | 22 608 | 162.6 | – |
| 100 000 | 100 000 | 500 | live | **1 020.7** (1 009.0–1 123.3) | 97 970 | 56.1 | 79.6 |
| 100 000 | 100 000 | 5 000 | persisted | **2 178.3** (2 168.4–2 391.8) | 45 907 | 205.1 | – |
| 100 000 | 100 000 | 5 000 | live | **733.9** (695.0–742.3) | 136 254 | 68.7 | 103.5 |
| 1 000 000 | 1 000 | 500 | persisted | **76 000.5** (75 178.3–77 620.8) | **13 158** | 3 635.0 | – |
| 1 000 000 | 1 000 | 500 | live | **10 669.5** (9 543.3–11 954.0) | 93 725 | 351.8 | 27.3 |
| 1 000 000 | 1 000 | 5 000 | persisted | **10 554.3** (10 398.8–10 619.7) | 94 748 | 723.7 | – |
| 1 000 000 | 1 000 | 5 000 | live | **6 256.6** (6 061.8–6 619.6) | 159 831 | 311.4 | 30.3 |
| 1 000 000 | 1 000 000 | 5 000 | persisted | **33 506.7** (31 106.4–33 832.5) | 29 845 | 1 834.1 | – |
| 1 000 000 | 1 000 000 | 5 000 | live | **7 003.2** (6 894.4–7 451.1) | 142 793 | 816.8 | 637.8 |

- **Finding: the persisted rebuild with `batchSize` 500 slows down as it goes.** K = 1 000: 3.35 s for 10⁵ events,
  **76.0 s for 10⁶** — ten times the events, 22.7 times the time (29 849 → 13 158 events/s), and its fold time 15× (241
  → 3 635 ms). With `batchSize` 5 000 the same cell is linear (1.14 → 10.6 s, 9.3×), and the live model is linear at
  both batch sizes (1.00 → 10.7 s; 0.62 → 6.26 s). So the cost per page of the persisted writer grows over a long
  rebuild at small pages. Not investigated (the tables are dropped at the end); **a guess, not measured**: the 2 000
  page transactions update the same 1 000 state rows ~1 000 times each, and the dead row versions grow faster than
  autovacuum removes them. It is not a 10× limit, but a rebuild of 10⁶ events takes over a minute with the writer's
  default `batchSize` 500 (README "Persisted read models"), and the README does not say so; it goes to the reviewer
  as a finding.
- Resume (K = 1 000, `batchSize` 500, 100 000 events built first in 3 093.5 ms): 1 000 events appended while no writer
  runs, a fresh process opens the model: 65.8 / 62.8 / 68.2 ms, **folded 1 000** each time (only the delta), epoch
  2 / 3 / 4, role writer.
- Database size: 0.02 GB before, 0.38 GB (10⁵) / **2.50 GB (10⁶)** with the events and every built model's rows,
  0.02 GB after the drop.

### S4 · The processor's cursor read when 1 % of the events match, with `EXPLAIN`

`sh bench/cursor.sh` (exit 0, 62 s; `node bench/cursor-bench.mjs 100000 3` and `… 1000000 3`). Every 100th event is
`Rare`, the rest `Common`, ~1 KB each. **Seeded by SQL** in the store's wire shape, 1 000 events per transaction
(0.86 s / 8.91 s), then `VACUUM ANALYZE`, and **read back through the library before measuring**: `statistics()`
counts 100 000 of 100 000 / 1 000 000 of 1 000 000, and a page of the 1 % source parses (`ok`). The page is the
subscriber's (`compileQuery` with `src/subscribe/index.ts`'s options); `EXPLAIN (ANALYZE, BUFFERS)` of the first, the
half-way and the empty end page per layout is printed in full in the raw output
(`.ziv/lab/stage-07-measurer/cursor.txt`). Checks: **all ok** — every subscription delivered exactly the matching
events (1 000 of 1 000, 10 000 of 10 000) and the 100 % reference every event.

| events, 1 % match | layout | first page (500): plan node, ms, rows removed by filter | half-way page: ms, removed | empty end page, ms | catch-up, `batchSize` 500, ms median (min–max) | catch-up, `batchSize` 5 000, ms median (min–max) | vs. A (5 000) |
|---|---|---|---|---|---|---|---|
| 100 000 | **A** default (the library's install: `transaction_id, sequence_number`) | Seq Scan, 11.9, 99 000 | 7.73, 33 167 | 0.01 | **31.0** (29.3–37.0) | **23.4** (19.2–23.6) | 1.00× |
| 100 000 | **B** `postgres.typeIndex: true` (adds `event_type, sequence_number`) | Index Scan `type_seq`, 1.40, 0 | 0.71, 500 | 0.01 | **12.6** (11.6–13.8) | **10.6** (8.88–11.3) | 2.22× |
| 100 000 | C `(event_type, transaction_id, sequence_number)` — **not in the library** | Index Scan `type_xid_seq`, 1.17, 0 | 0.34, 0 | 0.01 | **11.7** (10.6–12.2) | **9.82** (9.26–10.3) | 2.38× |
| 100 000 | reference: the 100 % source, layout A | Index Scan `xid_seq`, 0.20, 0 | | | **774** (755–805) | **520** (475–529) | 192 208 events/s |
| 1 000 000 | **A** default | Index Scan `xid_seq`, 20.7, 49 500 | 13.1, 49 500 | 0.01 | **328** (319–341) | **199** (185–205) | 1.00× |
| 1 000 000 | **B** `postgres.typeIndex: true` | Index Scan `type_seq`, 19.5, 0 | 5.72, 5 000 | 0.01 | **184** (184–192) | **65.8** (65.6–78.0) | 3.02× |
| 1 000 000 | C — **not in the library** | Index Scan `xid_seq`, 14.6, 49 500 | 3.58, 0 | 0.01 | **175** (167–196) | **70.6** (65.7–72.0) | 2.82× |
| 1 000 000 | reference: the 100 % source, layout A | Index Scan `xid_seq`, 0.39, 0 | | | **7 881** (7 790–8 547) | **4 922** (4 906–5 199) | 203 181 events/s |

- **Not a trap**, as the scout's lab found (facts §51): with the default indexes a processor over 1 % of 10⁶ events
  catches up in 199 ms (`batchSize` 5 000), against 4 922 ms for the whole source; the planner walks `xid_seq` and
  filters (49 500 rows removed for a page of 500), or at 10⁵ scans the table.
- **`typeIndex: true` (an existing option) is 2.2× (10⁵) to 3.0× (10⁶) faster** for such a processor, confirming the
  lab's ~3×. The candidate index C is no better than B at 10⁶ (the planner chose `xid_seq` for its first page) and is
  not added (decision 56 §3).

### S5 · The limits (constitution, "The highest good"), re-measured in stage 7

| Limit | Measured (stage 7, b0ce19e) | Verdict | Command |
|---|---|---|---|
| `typecheck`, `test`, `test:pg` green; `test:pg` really ran | typecheck ok; memory 345 passed, 269 skipped (614); **test:pg 614 passed, 0 failed, 0 skipped** on PostgreSQL 18.6; build ok; 4 min 31 s, load 8.5 → 21.5. Again after the docs changes of this stage (README, CHANGELOG, this file): 614 passed, 0 failed, 0 skipped, GREEN, 5 min 14 s | **reached** (GREEN, exit 0, twice) | `sh checks/check.sh` (`.ziv/logs/stage-07/4-measurer-check.txt`, `…-check-final.txt`) |
| Equivalence: each kind of kept state vs. `context(source ∧ key = k, fold)`, memory and Postgres | property tests 0 mismatches: live model 1 290 and 1 080 (memory, two runs), 204 (Postgres); persisted model 2 604 / 2 646 (memory), 476 (Postgres); cross-tenant 2 568 / 2 388 (memory), 726 (Postgres), 600 (`rls`); persisted fold on Postgres and live `start: "persisted"` on Postgres passed (no count printed). Bench: states equal and `model.get(k)` equal to `es.context()` in every process of the 15 blocks of the first three columns, and no state differing from fold on read in the `start: "persisted"` column (it reds one, `bench/versions-bench.mjs:462`; exit 0 at every size); `list` pages different from fold on read + list: 0 (live and persisted, 100 000); reader states equal 12 of 12 rounds | **reached**, 0 mismatches | `sh checks/check.sh`; `sh bench/matrix.sh`; `node bench/readmodel-bench.mjs list 100000 5`; `node bench/persisted-model-bench.mjs list 100000 5` |
| Events folded by a warm read when nothing is new | 0 in every warm read of every cell: persisted fold "max warm events per cell" 0 (5 sizes × 4 cells × 11 processes × 20); live and persisted "events folded per warm read" 0 (5 sizes × 2 layouts); 1 000 warm `list` calls 0 | **reached** | `sh bench/matrix.sh`; `node bench/readmodel-bench.mjs list 100000 5` |
| Events folded by a cold read with a persisted fold or model: only the delta | persisted fold `events` = 0 / 10 / 100 exactly where the stored state is 0 / 10 / 100 behind, every size; persisted model cold `get` 0 (row current) and exactly 100 (writer 100 behind), every size and layout; live `start: "persisted"` 0 and 100; a fresh writer after 1 000 new events folded 1 000 (3 of 3); the cross-tenant reader 0 | **reached** | `sh bench/matrix.sh`; `sh bench/persisted-model.sh rebuild 100000 3`; `node bench/tenants-bench.mjs read 11` |
| 10× at 100 000 events: live read model vs. fold on read, **warm/warm** | 1.11 ms → 0.27 µs (K = 1, `batchSize` 500) | **reached, 4 067×** | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 10× at 100 000 events: live read model vs. fold on read, **cold/cold** | the default start (rebuild): 837.4 → 1 333.5 ms | **not reached, 0.63×** (with `start: "persisted"`: 606.6 → 5.58 ms, reached, 109×) | same; `node bench/versions-bench.mjs live 100000 1024 11` |
| 10× at 100 000 events: persisted read model vs. fold on read, **cold/cold** | 748.3 → 5.40 ms, 0 events folded | **reached, 138×** | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 10× at 100 000 events: persisted read model vs. fold on read, **warm/warm** | 0.99 → 0.82 ms (O16) | **not reached, 1.21×** | same |
| Cold read at 10 000 events: persisted fold faster than fold on read | 1 KB: 76.9 → 5.85 ms (delta 0), → 8.28 ms (delta 100); every persisted run below every fold-on-read run (max 10.2 < min 72.7). 200 KB: 10 560.3 → 4.77 / 119.7 ms | **reached**, 13.1× / 9.28× (1 KB); 2 215× / 88.3× (200 KB) | `node bench/persisted-fold-bench.mjs 10000 1024 11`; `… 10000 204800 11` |
| Cold read at 100 000 events: persisted fold faster than fold on read | 766.0 → 6.04 ms (delta 0), → 9.31 ms (delta 100); max 11.5 < min 732.2 | **reached**, 127× / 82.2× | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| The benchmark matrix: every cell of an existing kind has a number with its command | every cell of S2 and of the long form: 5 sizes × 4 kinds × cold/warm (live `start: "persisted"` cold only), 11 of 11 runs each; 100 000 × 200 KB "not run: the human's limit" | **reached** | `sh bench/matrix.sh` |
| No new runtime dependency | `dependencies` none; `peerDependencies` exactly `pg`, `zod` | green | `sh checks/check.sh` |
| The core stays driver-free: `pg` imports in `src/` outside `src/postgres/` | none | green | `sh checks/check.sh` |
| Release untouched: `package.json` `version`; git tags | 0.6.4; 13 tags (`v0.1.0` … `v0.6.4`), the newest `v0.6.4` at b6d572e; none on HEAD (`git for-each-ref refs/tags`; `git for-each-ref --points-at HEAD refs/tags` prints nothing) | green | `sh checks/check.sh`; `git for-each-ref refs/tags`; `git for-each-ref --points-at HEAD refs/tags` |

## Stage 7: the whole matrix, every variant (long form)

Every cell of the four column runs of `sh bench/matrix.sh` on b0ce19e (see the summary above for the run, the load and
the timeouts), one row per size × layout K × kind and variant × cold/warm, with the fold on read **of the same run**
beside it. Factors and verdicts are the benches' own lines; "(computed)" marks one computed from the printed medians.
Generated from the raw outputs (`.ziv/lab/stage-07-measurer/matrix/*.txt`) by
`python3 -I .ziv/lab/stage-07-measurer/summarize.py .ziv/lab/stage-07-measurer/matrix`. Observation, not explained:
at 100 × 200 KB in the persisted-model column, "writer 100 behind", the cells "persisted, K = 1" and "fold on read
(N + 100), K = 1 000" print the same median and range, 106.9 (102.1–125.3); the bench keeps each cell's samples apart
(`bench/persisted-model-bench.mjs:509-562`), and stage 4's run shows four distinct values there.

| events × payload | K | kind (variant) | cold / warm | median ms (min–max) | fold on read, same run, ms | factor | verdict | events folded | command |
|---|---|---|---|---|---|---|---|---|---|
| 100 × 1 KB | 1 | fold on read | cold | 6.10 (4.97–6.33) | – (the baseline) | – | – | 100 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | fold on read | warm | 1.11 (0.86–1.33) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 0 | cold | 6.07 (4.69–7.71) | 6.10 | 1.00× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.17 (0.88–1.50) | 1.11 | 0.95× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 10 | cold | 7.19 (6.47–8.87) | 6.10 | 0.85× | **NOT faster (red)** (floor: faster, O3) | 10 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, delta 10 | warm | 1.23 (1.06–1.46) | 1.11 | 0.90× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, nothing stored (writes) | cold | 7.59 (6.12–8.59) | 6.10 | 0.80× (computed) | – (folds from zero and writes the state: what the next process saves) | 100 | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | persisted fold, nothing stored (writes) | warm | 1.29 (0.88–1.52) | 1.11 | 0.86× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 1024 11` |
| 100 × 1 KB | 1 | fold on read | cold | 4.23 (3.61–8.26) | – (the baseline) | – | – | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | fold on read | warm | 0.76 (0.59–3.18) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 7.18 (5.52–12.0) | 4.23 | 0.59× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.23 µs (0.18 µs–0.28 µs) | 0.76 | 3,372× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, rebuild, batchSize 5000 | cold | 6.91 (6.30–12.8) | 4.23 | 0.61× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | live read model, batchSize 5000 | warm | 0.20 µs (0.18 µs–0.35 µs) | 0.76 | 3,880× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | fold on read | cold | 2.40 (2.18–4.97) | – (the baseline) | – | – | 1 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | fold on read | warm | 0.69 (0.57–2.48) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, rebuild, batchSize 500 | cold | 7.13 (5.54–18.3) | 2.40 | 0.34× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, batchSize 500 | warm | 0.21 µs (0.18 µs–0.47 µs) | 0.69 | 3,202× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, rebuild, batchSize 5000 | cold | 7.38 (5.76–18.7) | 2.40 | 0.33× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, batchSize 5000 | warm | 0.19 µs (0.18 µs–0.36 µs) | 0.69 | 3,545× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | fold on read (this run) | cold | 6.13 (5.53–8.80) | – (the baseline) | – | – | 100 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | fold on read (this run) | warm | 1.55 (1.25–2.16) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model | cold | 6.59 (4.33–9.60) | 6.13 | 0.93× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model | warm | 1.10 (0.91–2.06) | 1.55 | 1.41× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 5.34 (4.92–7.24) | 5.91 | 1.11× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | persisted read model, writer 100 behind | warm | 0.74 (0.67–0.95) | 0.90 | 1.22× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | fold on read (this run) | cold | 4.41 (3.06–5.13) | – (the baseline) | – | – | 1 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | fold on read (this run) | warm | 1.49 (0.78–1.85) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | persisted read model | cold | 6.18 (4.42–8.91) | 4.41 | 0.71× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | persisted read model | warm | 1.24 (0.88–1.74) | 1.49 | 1.19× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 5.14 (4.59–6.01) | 4.43 | 0.86× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1000 | persisted read model, writer 100 behind | warm | 0.71 (0.68–0.85) | 0.87 | 1.22× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 1024 11` |
| 100 × 1 KB | 1 | fold on read (this run) | cold | 4.05 (3.64–6.22) | – (the baseline) | – | – | 100 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1 | live read model, start: "persisted" | cold | 6.90 (4.88–11.7) | 4.05 | 0.59× | not reached | 0 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 9.82 (8.50–14.7) | 5.46 | 0.56× | not reached | 100 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1000 | fold on read (this run) | cold | 2.54 (2.19–4.28) | – (the baseline) | – | – | 1 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, start: "persisted" | cold | 6.92 (5.70–15.0) | 2.54 | 0.37× | not reached | 0 | `node bench/versions-bench.mjs live 100 1024 11` |
| 100 × 1 KB | 1000 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 10.5 (8.97–16.8) | 4.04 | 0.39× | not reached | 100 | `node bench/versions-bench.mjs live 100 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | cold | 76.9 (72.7–97.1) | – (the baseline) | – | – | 10000 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | warm | 1.28 (1.02–1.42) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 0 | cold | 5.85 (4.49–7.03) | 76.9 | 13.14× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.21 (0.92–1.60) | 1.28 | 1.06× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 100 | cold | 8.28 (6.31–10.2) | 76.9 | 9.28× | faster (floor: faster, O3) | 100 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, delta 100 | warm | 1.19 (0.91–1.31) | 1.28 | 1.08× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, nothing stored (writes) | cold | 81.8 (76.0–114.8) | 76.9 | 0.94× (computed) | – (folds from zero and writes the state: what the next process saves) | 10000 | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted fold, nothing stored (writes) | warm | 1.22 (0.95–1.51) | 1.28 | 1.05× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | cold | 70.0 (63.4–92.3) | – (the baseline) | – | – | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read | warm | 0.66 (0.62–1.41) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 107.7 (100.1–134.9) | 70.0 | 0.65× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.21 µs (0.19 µs–0.23 µs) | 0.66 | 3,128× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, rebuild, batchSize 5000 | cold | 86.7 (77.1–103.3) | 70.0 | 0.81× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, batchSize 5000 | warm | 0.18 µs (0.16 µs–0.23 µs) | 0.66 | 3,626× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | fold on read | cold | 2.86 (2.65–4.41) | – (the baseline) | – | – | 10 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | fold on read | warm | 0.87 (0.79–1.17) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, rebuild, batchSize 500 | cold | 111.4 (105.3–123.0) | 2.86 | 0.03× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, batchSize 500 | warm | 0.20 µs (0.19 µs–0.26 µs) | 0.87 | 4,291× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, rebuild, batchSize 5000 | cold | 87.4 (77.8–97.9) | 2.86 | 0.03× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, batchSize 5000 | warm | 0.17 µs (0.12 µs–0.24 µs) | 0.87 | 5,059× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read (this run) | cold | 87.5 (73.3–93.2) | – (the baseline) | – | – | 10000 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read (this run) | warm | 0.84 (0.71–1.66) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model | cold | 4.09 (3.74–4.83) | 87.5 | 21.42× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model | warm | 0.65 (0.56–0.69) | 0.84 | 1.29× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 4.97 (4.74–11.0) | 73.7 | 14.85× | reached | 100 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | persisted read model, writer 100 behind | warm | 0.65 (0.61–1.03) | 0.78 | 1.21× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | fold on read (this run) | cold | 3.16 (2.76–4.28) | – (the baseline) | – | – | 10 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | fold on read (this run) | warm | 1.09 (0.95–2.20) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | persisted read model | cold | 3.86 (3.41–4.67) | 3.16 | 0.82× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | persisted read model | warm | 0.65 (0.58–0.72) | 1.09 | 1.67× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 4.91 (4.24–8.13) | 4.21 | 0.86× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1000 | persisted read model, writer 100 behind | warm | 0.68 (0.63–0.94) | 0.82 | 1.21× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 1024 11` |
| 10 000 × 1 KB | 1 | fold on read (this run) | cold | 70.9 (65.3–86.7) | – (the baseline) | – | – | 10000 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, start: "persisted" | cold | 5.23 (4.88–8.92) | 70.9 | 13.56× | reached | 0 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 9.74 (8.13–14.2) | 69.2 | 7.10× | not reached | 100 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1000 | fold on read (this run) | cold | 2.85 (2.69–3.80) | – (the baseline) | – | – | 10 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, start: "persisted" | cold | 11.5 (10.2–13.2) | 2.85 | 0.25× | not reached | 0 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 10 000 × 1 KB | 1000 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 14.7 (13.4–19.0) | 4.13 | 0.28× | not reached | 100 | `node bench/versions-bench.mjs live 10000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | cold | 766.0 (732.2–1122.9) | – (the baseline) | – | – | 100000 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | warm | 1.22 (0.97–1.73) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 0 | cold | 6.04 (4.93–8.23) | 766.0 | 126.86× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 0 | warm | 1.01 (0.87–1.66) | 1.22 | 1.20× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 100 | cold | 9.31 (7.96–11.5) | 766.0 | 82.24× | faster (floor: faster, O3) | 100 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, delta 100 | warm | 1.24 (0.94–1.60) | 1.22 | 0.98× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, nothing stored (writes) | cold | 755.3 (690.6–875.7) | 766.0 | 1.01× (computed) | – (folds from zero and writes the state: what the next process saves) | 100000 | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted fold, nothing stored (writes) | warm | 1.15 (0.91–1.73) | 1.22 | 1.06× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | cold | 837.4 (604.2–2813.3) | – (the baseline) | – | – | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read | warm | 1.11 (0.67–3.37) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, rebuild, batchSize 500 | cold | 1333.5 (942.1–2754.6) | 837.4 | 0.63× | not reached | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, batchSize 500 | warm | 0.27 µs (0.20 µs–0.34 µs) | 1.11 | 4,067× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, rebuild, batchSize 5000 | cold | 862.1 (629.5–3392.3) | 837.4 | 0.97× | not reached | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, batchSize 5000 | warm | 0.24 µs (0.19 µs–0.35 µs) | 1.11 | 4,691× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | fold on read | cold | 6.96 (3.94–33.5) | – (the baseline) | – | – | 100 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | fold on read | warm | 1.58 (0.82–8.07) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, rebuild, batchSize 500 | cold | 1304.4 (938.9–2876.9) | 6.96 | 0.01× | not reached | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, batchSize 500 | warm | 0.25 µs (0.19 µs–0.30 µs) | 1.58 | 6,255× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, rebuild, batchSize 5000 | cold | 865.0 (636.9–2912.9) | 6.96 | 0.01× | not reached | 100000 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, batchSize 5000 | warm | 0.24 µs (0.12 µs–0.33 µs) | 1.58 | 6,478× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read (this run) | cold | 748.3 (619.2–863.8) | – (the baseline) | – | – | 100000 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read (this run) | warm | 0.99 (0.77–2.07) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model | cold | 5.40 (3.83–8.62) | 748.3 | 138× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model | warm | 0.82 (0.61–1.18) | 0.99 | 1.21× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 4.85 (4.70–5.79) | 629.0 | 130× | reached | 100 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | persisted read model, writer 100 behind | warm | 0.66 (0.64–0.68) | 0.79 | 1.20× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | fold on read (this run) | cold | 6.07 (4.40–8.74) | – (the baseline) | – | – | 100 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | fold on read (this run) | warm | 1.37 (0.99–2.44) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | persisted read model | cold | 5.29 (3.50–8.03) | 6.07 | 1.15× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | persisted read model | warm | 0.77 (0.65–0.97) | 1.37 | 1.78× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 4.86 (4.63–6.46) | 5.72 | 1.18× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1000 | persisted read model, writer 100 behind | warm | 0.68 (0.66–0.70) | 0.80 | 1.18× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 100 000 × 1 KB | 1 | fold on read (this run) | cold | 606.6 (569.6–755.0) | – (the baseline) | – | – | 100000 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, start: "persisted" | cold | 5.58 (4.56–11.1) | 606.6 | 109× | reached | 0 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 10.9 (8.43–16.6) | 613.9 | 56.42× | reached | 100 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1000 | fold on read (this run) | cold | 4.45 (4.16–7.72) | – (the baseline) | – | – | 100 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, start: "persisted" | cold | 11.4 (10.3–17.1) | 4.45 | 0.39× | not reached | 0 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 1000 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 14.5 (13.4–17.1) | 6.05 | 0.42× | not reached | 100 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 100000 | fold on read (this run) | cold | 2.42 (2.23–2.58) | – (the baseline) | – | – | 1 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 000 × 1 KB | 100000 | live read model, start: "persisted" | cold | 319.5 (308.8–398.9) | 2.42 | 0.01× | not reached | 0 | `node bench/versions-bench.mjs live 100000 1024 11` |
| 100 × 200 KB | 1 | fold on read | cold | 130.8 (111.4–148.3) | – (the baseline) | – | – | 100 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | fold on read | warm | 1.02 (0.76–1.18) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 0 | cold | 5.20 (4.19–7.11) | 130.8 | 25.16× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 0 | warm | 0.92 (0.70–1.64) | 1.02 | 1.11× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 10 | cold | 19.4 (16.5–23.6) | 130.8 | 6.74× | faster (floor: faster, O3) | 10 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, delta 10 | warm | 1.01 (0.71–1.44) | 1.02 | 1.01× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, nothing stored (writes) | cold | 125.3 (112.0–150.7) | 130.8 | 1.04× (computed) | – (folds from zero and writes the state: what the next process saves) | 100 | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | persisted fold, nothing stored (writes) | warm | 1.03 (0.88–1.46) | 1.02 | 0.99× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 100 204800 11` |
| 100 × 200 KB | 1 | fold on read | cold | 165.4 (93.4–1135.8) | – (the baseline) | – | – | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | fold on read | warm | 1.97 (0.82–5.67) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, rebuild, batchSize 500 | cold | 353.1 (108.6–644.3) | 165.4 | 0.47× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, batchSize 500 | warm | 0.21 µs (0.16 µs–0.27 µs) | 1.97 | 9,511× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, rebuild, batchSize 5000 | cold | 189.9 (98.7–567.6) | 165.4 | 0.87× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | live read model, batchSize 5000 | warm | 0.23 µs (0.19 µs–0.26 µs) | 1.97 | 8,540× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | fold on read | cold | 18.9 (17.2–29.6) | – (the baseline) | – | – | 1 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | fold on read | warm | 7.88 (5.90–9.24) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, rebuild, batchSize 500 | cold | 222.3 (114.1–1086.5) | 18.9 | 0.09× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, batchSize 500 | warm | 0.25 µs (0.16 µs–0.37 µs) | 7.88 | 31,607× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, rebuild, batchSize 5000 | cold | 233.7 (86.9–877.6) | 18.9 | 0.08× | not reached | 100 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, batchSize 5000 | warm | 0.24 µs (0.16 µs–0.38 µs) | 7.88 | 32,285× | reached | 0 | `node bench/readmodel-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | fold on read (this run) | cold | 106.6 (102.4–113.7) | – (the baseline) | – | – | 100 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | fold on read (this run) | warm | 0.81 (0.71–1.03) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model | cold | 3.79 (3.50–5.26) | 106.6 | 28.13× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model | warm | 0.71 (0.66–0.76) | 0.81 | 1.15× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 106.9 (102.1–125.3) | 204.9 | 1.92× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | persisted read model, writer 100 behind | warm | 0.72 (0.69–0.75) | 0.78 | 1.09× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | fold on read (this run) | cold | 16.9 (15.5–18.4) | – (the baseline) | – | – | 1 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | fold on read (this run) | warm | 6.01 (5.45–6.94) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | persisted read model | cold | 10.5 (10.0–11.5) | 16.9 | 1.61× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | persisted read model | warm | 3.58 (3.53–3.91) | 6.01 | 1.68× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 110.1 (98.1–135.7) | 106.9 | 0.97× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1000 | persisted read model, writer 100 behind | warm | 0.70 (0.68–0.73) | 0.78 | 1.12× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 100 204800 11` |
| 100 × 200 KB | 1 | fold on read (this run) | cold | 105.3 (99.7–112.9) | – (the baseline) | – | – | 100 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1 | live read model, start: "persisted" | cold | 5.95 (4.56–6.87) | 105.3 | 17.70× | reached | 0 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 106.1 (97.4–130.9) | 194.6 | 1.83× | not reached | 100 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1000 | fold on read (this run) | cold | 15.6 (15.3–19.8) | – (the baseline) | – | – | 1 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, start: "persisted" | cold | 6.82 (5.50–11.6) | 15.6 | 2.29× | not reached | 0 | `node bench/versions-bench.mjs live 100 204800 11` |
| 100 × 200 KB | 1000 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 105.2 (96.9–110.0) | 112.4 | 1.07× | not reached | 100 | `node bench/versions-bench.mjs live 100 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | cold | 10560.3 (10178.9–29642.9) | – (the baseline) | – | – | 10000 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | warm | 0.71 (0.59–1.67) | – (the baseline) | – | – | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 0 | cold | 4.77 (3.92–8.09) | 10560.3 | 2214.66× | faster (floor: faster, O3) | 0 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 0 | warm | 0.76 (0.61–1.68) | 0.71 | 0.94× | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 100 | cold | 119.7 (108.0–167.7) | 10560.3 | 88.25× | faster (floor: faster, O3) | 100 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, delta 100 | warm | 0.78 (0.65–1.72) | 0.71 | 0.91× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, nothing stored (writes) | cold | 11057.1 (10575.1–23654.5) | 10560.3 | 0.96× (computed) | – (folds from zero and writes the state: what the next process saves) | 10000 | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted fold, nothing stored (writes) | warm | 0.88 (0.68–1.01) | 0.71 | 0.81× (computed) | – (a warm hit is the cache, the tier is not read) | 0 (cache) | `node bench/persisted-fold-bench.mjs 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | cold | 10092.0 (9183.0–15117.1) | – (the baseline) | – | – | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read | warm | 0.80 (0.62–1.73) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, rebuild, batchSize 500 | cold | 11269.4 (9291.5–14279.0) | 10092.0 | 0.90× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, batchSize 500 | warm | 0.31 µs (0.21 µs–0.35 µs) | 0.80 | 2,602× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, rebuild, batchSize 5000 | cold | 11572.1 (9316.9–12405.1) | 10092.0 | 0.87× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, batchSize 5000 | warm | 0.21 µs (0.17 µs–0.40 µs) | 0.80 | 3,760× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | fold on read | cold | 17.1 (14.0–26.5) | – (the baseline) | – | – | 10 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | fold on read | warm | 0.82 (0.61–1.65) | – (the baseline) | – | – | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, rebuild, batchSize 500 | cold | 11658.1 (9286.4–35363.5) | 17.1 | 0.00× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, batchSize 500 | warm | 0.29 µs (0.24 µs–0.47 µs) | 0.82 | 2,818× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, rebuild, batchSize 5000 | cold | 12483.2 (9288.9–13852.3) | 17.1 | 0.00× | not reached | 10000 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, batchSize 5000 | warm | 0.19 µs (0.16 µs–0.28 µs) | 0.82 | 4,239× | reached | 0 | `node bench/readmodel-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read (this run) | cold | 10685.0 (9128.3–27186.2) | – (the baseline) | – | – | 10000 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read (this run) | warm | 0.84 (0.73–2.10) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model | cold | 6.34 (3.84–11.5) | 10685.0 | 1,686× | reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model | warm | 0.93 (0.59–1.88) | 0.84 | 0.91× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 103.3 (97.2–122.2) | 9427.2 | 91.30× | reached | 100 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | persisted read model, writer 100 behind | warm | 0.69 (0.60–0.89) | 0.83 | 1.21× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | fold on read (this run) | cold | 16.2 (14.4–24.4) | – (the baseline) | – | – | 10 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | fold on read (this run) | warm | 0.80 (0.72–2.45) | – (the baseline) | – | – | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | persisted read model | cold | 6.16 (3.80–10.5) | 16.2 | 2.63× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | persisted read model | warm | 1.00 (0.62–2.70) | 0.80 | 0.80× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | persisted read model, writer 100 behind (vs. fold on read of N + 100) | cold | 103.2 (94.2–111.8) | 108.8 | 1.05× | not reached | 100 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1000 | persisted read model, writer 100 behind | warm | 0.68 (0.65–0.79) | 0.78 | 1.15× | not reached | 0 | `node bench/persisted-model-bench.mjs matrix 10000 204800 11` |
| 10 000 × 200 KB | 1 | fold on read (this run) | cold | 10881.4 (9035.7–15217.1) | – (the baseline) | – | – | 10000 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, start: "persisted" | cold | 6.55 (5.31–10.7) | 10881.4 | 1,662× | reached | 0 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 103.1 (88.3–126.3) | 10142.9 | 98.39× | reached | 100 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1000 | fold on read (this run) | cold | 14.7 (13.7–23.9) | – (the baseline) | – | – | 10 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, start: "persisted" | cold | 14.1 (10.7–21.7) | 14.7 | 1.05× | not reached | 0 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 10 000 × 200 KB | 1000 | live read model, start: "persisted", rows 100 behind (vs. fold on read of N + 100) | cold | 117.2 (97.6–181.1) | 114.8 | 0.98× | not reached | 100 | `node bench/versions-bench.mjs live 10000 204800 11` |
| 100 000 × 200 KB | – | every kind (fold on read, persisted fold, live read model, persisted read model) | cold, warm | not run: the human's limit | – | – | – | – | – |

## History (stages 1–6)

The sections below are the stage-1 to stage-6 measurements, unchanged except that every heading moved one level
down and the old headline matrix and limits table are labelled "Superseded".

### Superseded: the matrix assembled from stage 2–6 runs (was "The matrix (synthesis §7): one context, cold and warm, times in ms")

Re-run on the current code (stage-2 fixer, 2026-10-17: after the stage-1 fixes R1.1/R1.3 and the
stage-2 changes; the fold path is the same as at the stage-1 fixer's re-measurement). The first
run's numbers (stage-1 measurer, commit 36e9c78, before those fixes) are kept further down under
"Superseded", labelled; they are not the current code's.

Median of 11 fresh `node` processes per cell; min–max in the detail tables below. **Cold** = the
first load in a fresh process (the store installed and connected before the clock runs, the
database itself warm). **Warm** = a further load in the same process with nothing new (median of
20 per process, then the median over the processes). "×" = fold on read ÷ persisted fold, cold.

| events × payload | fold on read, cold | persisted fold, cold, delta 0 | persisted fold, cold, small delta | cold: persisted fold vs. fold on read | fold on read, warm | persisted fold, warm | live read model | persisted read model |
|---|---|---|---|---|---|---|---|---|
| 100 × 1 KB | 3.81 | 2.93 | 3.49 (delta 10) | 1.30× / 1.09×, faster by the median; min–max ranges overlap (delta 10) | 0.66 | 0.62 | warm 0.19 µs: **3 795×, reached**; cold 7.14: **0.55×, not reached**; cold with `start: "persisted"` 11.0: **0.66×, not reached** (stage 5) | warm 0.68: **1.11×, not reached**; cold 3.49: **1.12×, not reached** |
| 10 000 × 1 KB | 62.9 | 2.93 | 4.55 (delta 100) | **21.5× / 13.8×, faster** | 0.64 | 0.59 | warm 0.19 µs: **3 743×, reached**; cold 112.7: **0.64×, not reached**; cold with `start: "persisted"` 11.4: **10.1×, reached** (stage 5) | warm 0.68: **1.15×, not reached**; cold 3.61: **21.3×, reached** |
| 100 000 × 1 KB | 563.5 | 2.95 | 4.55 (delta 100) | **191× / 124×, faster** | 0.66 | 0.60 | warm 0.22 µs: **4 116×, reached**; cold 1 072.2: **0.81×, not reached**; cold with `start: "persisted"` 8.71: **91.9×, reached** (stage 5) | warm 0.68: **1.17×, not reached**; cold 3.47: **195×, reached** |
| 100 × 200 KB | 99.1 | 2.84 | 14.9 (delta 10) | 34.9× / 6.65×, faster | 0.60 | 0.62 | warm 0.17 µs: **3 814×, reached**; cold 88.2: **1.08×, not reached**; cold with `start: "persisted"` 6.89: **15.6×, reached** (stage 5) | warm 0.72: **1.10×, not reached**; cold 3.43: **34.4×, reached** |
| 10 000 × 200 KB | 9 785.9 | 3.95 | 107.5 (delta 100) | 2 477× / 91.0×, faster | 0.91 | 0.75 | warm 0.21 µs: **3 763×, reached**; cold 8 120.1: **1.00×, not reached** (10 of 11 runs: one failed, see "Stage 3"); cold with `start: "persisted"` 11.4: **2 280×, reached** (stage 5; its fold-on-read baseline is **red**: 2 of 12 runs failed with a statement timeout, and the median of the other 10, 25 928.6 ms, is 2.6× this row's 9 785.9 — against 9 785.9 the factor is 858×, still reached; see "Stage 5") | warm 0.73: **1.20×, not reached**; cold 7.41: **1 490×, reached** |
| 100 000 × 200 KB | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit |

Command for every row: `sh bench/persisted-fold.sh matrix` (builds, then runs
`node bench/persisted-fold-bench.mjs <events> <payloadBytes> 11` per row; exit 0, 6 min 11 s for all
five). Full output on the board: `.ziv/lab/stage-02-fixer/matrix-after.txt`.

The column **live read model** (stage-3 measurer) has its own command, `sh bench/readmodel.sh all`
(exit 0, 18 min 18 s; output `.ziv/lab/stage-03-measurer/bench-run2.txt`): K = 1 (one key holds
every event, so `model.get(k)` and `es.context()` are the same state), the default `batchSize` 500,
median of 11 fresh processes. Each factor is fold on read ÷ live **of that run** (its own
fold-on-read cells, in "Stage 3" below), not of the columns to the left. **Warm** = an awaited
`model.get(k)` vs. a warm `es.context()`; **cold** = a fresh process, the model's rebuild to its
first `get` vs. the first `es.context()`. The cold cell is a rebuild of the whole source and is
not held to 10× (FOUNDING.md); it is written "not reached" all the same. The addition **cold with
`start: "persisted"`** (stage-5 measurer, `sh bench/versions.sh live`, exit 1 from three red
baseline runs at 10 000 × 200 KB, see "Stage 5") is a live model started from the persisted rows
of the same definition, K = 1, its factor against the fold-on-read cells **of that run** (in "Stage 5"
below), not of this table.

The column **persisted read model** (stage-4 measurer) has its own command, `sh bench/persisted-model.sh all`
(exit 1 — red from the live model's 200 KB timeouts, see "Stage 4"; 27 min 34 s; output
`.ziv/lab/stage-04-measurer/bench-run1.txt`): K = 1, the model built beforehand by a process of
its own, median of 11 fresh processes. Each factor is fold on read ÷ persisted **of that run**.
**Warm** = an awaited `model.get(k)` with its in-process copy present and nothing new (one store
query, 0 events folded) vs. a warm `es.context()`; **cold** = a fresh process, the first
`get(k)` (the stored row plus the key's delta, here 0) vs. the first `es.context()`. The warm
cell cannot reach 10× while `get` reads the store's delta (O16: ~1×, by construction).

### Superseded: the limits as measured in stages 1–6 (was "The limits (constitution, 'The highest good')"; stage 7: table S5 above)

| Limit | Measured | Verdict | Command |
|---|---|---|---|
| Cold read at 10 000 events: persisted fold faster than fold on read | 62.9 → 2.93 ms (delta 0), 62.9 → 4.55 ms (delta 100), 1 KB; every persisted run below every fold-on-read run | **reached**, 21.5× / 13.8× | `node bench/persisted-fold-bench.mjs 10000 1024 11` |
| Cold read at 100 000 events: persisted fold faster than fold on read | 563.5 → 2.95 ms (delta 0), 563.5 → 4.55 ms (delta 100), 1 KB; every persisted run below every fold-on-read run | **reached**, 191× / 124× | `node bench/persisted-fold-bench.mjs 100000 1024 11` |
| Events folded by a warm read when nothing is new | 0 in every warm load of every cell (20 per process × 11 processes × 4 cells × 5 sizes) | **reached** | `sh bench/persisted-fold.sh matrix` |
| Events folded by a cold read with a persisted fold: only the delta | `events` = 0 / 10 / 100 exactly where the stored state is 0 / 10 / 100 behind, at every size | **reached** | `sh bench/persisted-fold.sh matrix` |
| Equivalence: the persisted fold equals the fold from zero | every state of every cell and round deep-equal to the first (bench); property test 0 mismatches (memory 60 runs, Postgres 15 runs) | **reached** | `sh bench/persisted-fold.sh matrix`; `sh checks/check.sh` |
| 10× target at 100 000 events: live read model vs. fold on read, **warm/warm** | 0.92 ms → 0.22 µs (K = 1, batchSize 500); 1.28 ms → 0.21 µs (K = 1 000); 1 KB; every live process below every fold-on-read process | **reached**, 4 116× (K = 1), 6 030× (K = 1 000) | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| 10× target at 100 000 events: live read model vs. fold on read, **cold/cold** (the rebuild to the first `get` vs. the first `es.context()`) | K = 1: 869.0 → 1 072.2 ms (batchSize 500), → 856.2 ms (batchSize 5 000); K = 1 000: 4.89 ms (one key's 100 events) → 1 072.0 / 950.3 ms (the whole source) | **not reached**: 0.81× / 1.01× (K = 1), 0.005× / 0.005× (K = 1 000) | `node bench/readmodel-bench.mjs matrix 100000 1024 11` |
| Events folded by a warm read of the live model | 0 in every warm read (20 000 awaited `get` per process × 11 processes × 4 cells × 5 sizes) and over 1 000 warm `list` calls at each of 1 000 / 10 000 / 100 000 keys (`status.folded` before and after) | **reached** | `sh bench/readmodel.sh all` |
| Equivalence: the live model equals fold on read | bench: `model.get(k)` deep-equal to `es.context()` in every live process, every state equal across cells and rounds; `list` pages vs. fold on read + list: 0 differences; property test 0 mismatches (memory 60 runs, Postgres 12 runs with 3 writers) | **reached** | `sh bench/readmodel.sh all`; `sh checks/check.sh` |
| 10× target at 100 000 events: persisted read model vs. fold on read, **warm/warm** | 0.79 → 0.68 ms (K = 1), 0.99 → 0.68 ms (K = 1 000); 1 KB; a warm `get` is one store query for the key's delta (O16) | **not reached**: 1.17× (K = 1), 1.46× (K = 1 000) | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| 10× target at 100 000 events: persisted read model vs. fold on read, **cold/cold** (the first `get` of a fresh process vs. the first `es.context()`) | K = 1: 675.9 → 3.47 ms (0 events folded); K = 1 000: 4.42 ms (one key's 100 events) → 3.36 ms; with the writer 100 events behind: 676.8 → 4.59 ms (K = 1, 100 folded), 5.76 → 4.69 ms (K = 1 000) | **reached** at K = 1: **195×** (147× with the writer 100 behind); **not reached** at K = 1 000: 1.31× (1.23× behind) | `node bench/persisted-model-bench.mjs matrix 100000 1024 11` |
| Events folded by a warm read of the persisted model | 0 in every warm `get` (2 000 per process × 11 processes × 4 persisted cells × 5 sizes; `status().readFolded` before and after) and in the second `get` of every process | **reached** | `sh bench/persisted-model.sh all` |
| Events folded by a cold read with a persisted model: only the delta | the first `get` of a fresh process folded 0 where the row was current and exactly 100 where the writer was 100 behind, at every size; a fresh writer after 1 000 new events folded 1 000 (3 of 3) | **reached** | `sh bench/persisted-model.sh all` |
| Equivalence: the persisted model equals fold on read | bench: `model.get(k)` deep-equal to `es.context()` in every persisted process, every state equal across cells and rounds; list pages vs. fold on read + list: 0 differences; property test 0 mismatches (memory 2 646 and 2 814 comparisons in the two suites of one verify run, Postgres 504; stage-4 measurer, `check-run1.txt`) | **reached** | `sh bench/persisted-model.sh all`; `sh checks/check.sh` |
| 10× target at 100 000 events: live read model with `start: "persisted"` vs. fold on read, **cold/cold** (stage 5: a fresh process, one snapshot of the persisted rows + the delta, to the first `get` vs. the first `es.context()`) | K = 1: 800.3 → 8.71 ms (0 events folded); with the rows 100 behind: 661.7 → 8.92 ms (100 folded); K = 1 000: 6.66 → 17.9 ms; K = 100 000: 4.23 → 411.9 ms (100 000 rows loaded) | **reached** at K = 1: **91.9×** (74.2× 100 behind); **not reached** at K = 1 000: 0.37×, at K = 100 000: 0.01× | `node bench/versions-bench.mjs live 100000 1024 11` |
| Events folded by a cold live start from a persisted state: only the delta | `status.folded` = 0 where the rows were current and exactly 100 where they were 100 behind, in every process of every size (5 sizes × 2–3 layouts × 11 processes) | **reached** | `sh bench/versions.sh live` |
| Equivalence after a version change: v2 vs. fold on read | bench: v2's `get` after the switch equal to fold on read (v2's fold) for 3 of 3 keys in 12 of 12 rebuild runs; every live `start: "persisted"` state equal to fold on read in the same process; every paged and bumped persisted-fold state equal to a fold from zero (24 of 24 steps) | **reached** | `sh bench/versions.sh all` |
| 10× target at 100 000 events: a **reader of a cross-tenant (processor-written) model** vs. fold on read, under `rls` (stage 6), cold/cold and warm/warm | K = 1: cold 677.8 → 20.4 ms, warm 3.65 → 2.71 ms; 0 events folded by the reader | **reached** cold, **33.2×**; **not reached** warm, **1.35×** (O16) | `node bench/tenants-bench.mjs read 11` |
| Events folded by a reader of a cross-tenant model, cold and warm (stage 6) | `readFolded` 0 after the cold and the warm `get` in 11 of 11 processes (the processor's row current) | **reached** | `node bench/tenants-bench.mjs read 11` |
| Equivalence and isolation, cross-tenant processor under `rls` (stage 6) | bench: 0 mismatches of 306 reader `get`s vs. `es.context()` at 1–1 000 tenants per page, 12 of 12 read rounds, 11 of 11 `invalidate()` rebuilds; erasure: 0 rows of the erased tenant left in 18 of 18 runs; property tests 0 mismatches (cross-tenant 2 616 memory, 2 700 / 666 / 480 Postgres) | **reached** | `sh bench/tenants.sh`; `sh checks/check.sh` |
| `typecheck`, `test`, `test:pg` green, `test:pg` really ran | stage 6, final run (stage-6 measurer, with its one new test): typecheck and memory green (336 passed, 268 skipped); **test:pg 600 passed, 4 failed, 0 skipped** — the installer test in `tenants-rls.test.ts` failed on the order of a warning (no `ORDER BY` at src/postgres/store.ts:785), and the three erasure tests after it failed because it left the policies on another role; the same file passed 4 of 4 runs alone. The run before (on b62fbc7): 602 passed, 0 failed, 0 skipped | **red** (intermittent) | `sh checks/check.sh` (`.ziv/logs/stage-06/4-measurer-check-final.txt`) |
| No new runtime dependency; no `pg` import outside `src/postgres/`; version 0.6.4 | none; none; 0.6.4 | green | `sh checks/check.sh` |

### Detail: persisted fold vs. fold on read

The four cells per size, as `bench/persisted-fold-bench.mjs` prints them. `readMs` is the store
read of the delta, `foldMs` the fold, `stored.readMs` the fold-state row read and decode,
`stored.writeMs` the storability check, encode and write (all medians, from `contextCache.onLoad`).
"nothing stored (writes)" is the first cold load of a defined fold on an empty fold-state table: it
folds from zero and writes once the 500 events of `contextCache.persist.after` are reached. The
fold keeps running statistics per category in a `Map` of 40 (the showcase's fold, some work per
event). Payloads are pseudo-random text, so Postgres cannot compress them.

**100 × 1 KB** — `node bench/persisted-fold-bench.mjs 100 1024 11` (stored payload 1060 bytes as JSON, table 216 kB)

| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |
|---|---|---|---|---|---|---|---|---|---|
| fold on read | initial | 100 | **3.81** (3.47–4.47) | 2.95 | 0.39 | – | – (not written) | 0.66 (0.61–0.68) | 0 (cache) |
| persisted fold, delta 0 | stored | 0 | **2.93** (2.85–3.27) | 1.57 | 0.05 | 0.80 | – (not written) | 0.62 (0.57–0.66) | 0 (cache) |
| persisted fold, delta 10 | stored | 10 | **3.49** (3.35–5.46) | 2.02 | 0.18 | 0.81 | – (not written) | 0.62 (0.58–1.10) | 0 (cache) |
| persisted fold, nothing stored (writes) | initial | 100 | **4.39** (4.19–6.81) | 2.88 | 0.40 | 0.64 | – (not written) | 0.64 (0.60–0.70) | 0 (cache) |

**10 000 × 1 KB** — `node bench/persisted-fold-bench.mjs 10000 1024 11` (stored payload 1060 bytes as JSON, table 12 MB)

| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |
|---|---|---|---|---|---|---|---|---|---|
| fold on read | initial | 10000 | **62.9** (61.4–69.3) | 56.1 | 6.36 | – | – (not written) | 0.64 (0.59–0.65) | 0 (cache) |
| persisted fold, delta 0 | stored | 0 | **2.93** (2.79–3.05) | 1.55 | 0.05 | 0.78 | – (not written) | 0.59 (0.57–0.64) | 0 (cache) |
| persisted fold, delta 100 | stored | 100 | **4.55** (4.36–4.73) | 2.88 | 0.38 | 0.81 | – (not written) | 0.61 (0.58–0.69) | 0 (cache) |
| persisted fold, nothing stored (writes) | initial | 10000 | **66.7** (64.4–69.4) | 56.8 | 5.65 | 0.64 | 2.19 (written 11/11) | 0.58 (0.53–0.60) | 0 (cache) |

**100 000 × 1 KB** — `node bench/persisted-fold-bench.mjs 100000 1024 11` (stored payload 1060 bytes as JSON, table 120 MB)

| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |
|---|---|---|---|---|---|---|---|---|---|
| fold on read | initial | 100000 | **563.5** (536.5–683.3) | 531.8 | 30.4 | – | – (not written) | 0.66 (0.60–0.92) | 0 (cache) |
| persisted fold, delta 0 | stored | 0 | **2.95** (2.77–5.18) | 1.59 | 0.04 | 0.82 | – (not written) | 0.60 (0.58–1.06) | 0 (cache) |
| persisted fold, delta 100 | stored | 100 | **4.55** (4.30–5.63) | 2.91 | 0.37 | 0.83 | – (not written) | 0.63 (0.59–0.93) | 0 (cache) |
| persisted fold, nothing stored (writes) | initial | 100000 | **574.8** (537.9–638.0) | 542.2 | 30.1 | 0.71 | 2.09 (written 11/11) | 0.62 (0.56–0.77) | 0 (cache) |

**100 × 200 KB** — `node bench/persisted-fold-bench.mjs 100 204800 11` (stored payload 204836 bytes as JSON, table 20 MB)

| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |
|---|---|---|---|---|---|---|---|---|---|
| fold on read | initial | 100 | **99.1** (93.6–113.8) | 87.6 | 11.1 | – | – (not written) | 0.60 (0.57–1.17) | 0 (cache) |
| persisted fold, delta 0 | stored | 0 | **2.84** (2.70–3.56) | 1.51 | 0.04 | 0.80 | – (not written) | 0.62 (0.57–0.73) | 0 (cache) |
| persisted fold, delta 10 | stored | 10 | **14.9** (14.1–18.1) | 10.2 | 3.32 | 0.83 | – (not written) | 0.64 (0.57–0.74) | 0 (cache) |
| persisted fold, nothing stored (writes) | initial | 100 | **101.1** (98.2–108.8) | 90.1 | 10.00 | 0.69 | – (not written) | 0.61 (0.57–0.73) | 0 (cache) |

**10 000 × 200 KB** — `node bench/persisted-fold-bench.mjs 10000 204800 11` (stored payload 204836 bytes as JSON, table 2037 MB)

| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |
|---|---|---|---|---|---|---|---|---|---|
| fold on read | initial | 10000 | **9785.9** (8885.5–12008.3) | 9173.6 | 607.4 | – | – (not written) | 0.91 (0.64–1.24) | 0 (cache) |
| persisted fold, delta 0 | stored | 0 | **3.95** (3.03–5.95) | 2.09 | 0.05 | 1.29 | – (not written) | 0.75 (0.62–1.61) | 0 (cache) |
| persisted fold, delta 100 | stored | 100 | **107.5** (97.6–131.7) | 95.5 | 10.9 | 1.19 | – (not written) | 0.83 (0.57–1.12) | 0 (cache) |
| persisted fold, nothing stored (writes) | initial | 10000 | **9830.6** (8994.2–12207.5) | 9200.6 | 616.9 | 0.67 | 3.58 (written 11/11) | 0.68 (0.62–1.57) | 0 (cache) |

### What the tier itself costs

All from the current matrix above and the state-size probe below, both run by the stage-2 fixer
on the current code.

- **The first cold load writes**: +2.1–3.6 ms (`stored.writeMs`) on a ~4.6 KB state, against 63 ms
  (10 000 events) to 9.8 s (10 000 × 200 KB) for the fold it saves the next process.
- **Below `persist.after` (500 events) a context is never stored**, and every cold load still
  asks the tier: at 100 × 1 KB the cold load on an empty table takes 4.39 ms against 3.81 ms
  without the tier (+0.6 ms, `stored.readMs` 0.64 — a miss).
- **Warm loads**: no difference beyond noise (persisted fold 0.59–0.75 ms against fold on read
  0.60–0.91 ms); the tier is not asked on a warm hit.
- **The floor of a cold load from a stored state** is two round trips: the fold-state row
  (~0.8 ms) and the delta read beyond its cursor (~1.6 ms with nothing new), 2.9 ms in all.
- **The size of the state.** `stored.writeMs` and `stored.readMs` by state size, from a fresh
  runtime that reads the stored state, folds 1 event and writes the whole state again (median of
  9; `node .ziv/lab/stage-01-measurer/state-size.mjs 9` after `npm run build`, a lab script on the
  board, not part of the package; output `.ziv/lab/stage-02-fixer/state-size-after.txt`):

  | backend | state bytes (v8) | stored.writeMs median (min–max) | stored.readMs median |
  |---|---|---|---|
  | memory | 5 252 | 0.13 (0.12–0.14) | 0.03 |
  | memory | 52 903 | 0.78 (0.73–0.89) | 0.28 |
  | memory | 537 927 | 4.47 (4.19–5.21) | 1.73 |
  | memory | 5 479 157 | 47.31 (45.47–74.31) | 27.10 |
  | Postgres | 5 595 | 0.72 (0.66–0.88) | 0.54 |
  | Postgres | 56 396 | 1.49 (1.35–3.14) | 1.12 |
  | Postgres | 568 896 | 10.23 (9.65–14.29) | 7.61 |
  | Postgres | 5 738 897 | 95.97 (91.95–98.96) | 78.02 |

  A stored state of ~5.7 MB takes 78 ms to read and decode, plus ~5 ms to size it on entering the
  cache (R1.3, `node .ziv/lab/stage-01-fixer/sizing-cost.mjs dist/index.js`), more than folding
  10 000 events of 1 KB from zero (63 ms in the matrix). The persisted fold pays off when its state is small
  against the events it folded; a state that grows like the events it folds does not get cheaper
  to load by being stored.

### How it is measured, and what it does not cover

- One context (`ledgerId = "bench"`), one events table of its own (`bench_pf_events` and
  `bench_pf_events_fold_state`), dropped at the start and the end. The stored states are written
  by the library itself (`persist.after: 1` in a setup process), kept as bytes, and put back by
  plain SQL before each cell, so each cell meets exactly the stored state it is named for.
- Before the timed rounds: `VACUUM ANALYZE` and one untimed full read (hint bits, buffers), then
  one warm-up round of all four cells that is not kept. The order of the cells rotates per round.
- **Cold means a cold process, not a cold database**: Postgres buffers and the VM's page cache
  are warm. A database restart before every load is not measured. At 10 000 × 200 KB the table
  (2 GB) is far larger than `shared_buffers` (128 MB), so those reads come from the page cache.
- Database and client on one machine: no network latency. Over a network every round trip adds
  its latency to both kinds, and the delta read adds transfer time in proportion to the events.
- An earlier run of the matrix (same day) had a broken payload generator whose text Postgres
  compressed (100 × 200 KB was 1.7 MB on disk instead of 20 MB). Its 200 KB rows are superseded
  by the numbers above; the output is kept on the board in
  `.ziv/lab/stage-01-measurer/matrix-run1-compressible-200k.txt`. The 1 KB rows were not
  affected (below the TOAST threshold) and agree within noise.

### Superseded: the first matrix run (stage-1 measurer, commit 36e9c78)

Measured before the stage-1 fixes R1.1 (storability check walks descriptors) and R1.3 (a stored
state is sized when it enters the cache) changed the measured paths. **Not the current code's
numbers**; kept so earlier logs that quote them (24.4× / 207×) can be traced. Command then:
`sh bench/persisted-fold.sh matrix`; output `.ziv/lab/stage-01-measurer/matrix-run2.txt`.

| events × payload | fold on read, cold | persisted fold, cold, delta 0 | persisted fold, cold, small delta | cold: persisted fold vs. fold on read | fold on read, warm | persisted fold, warm | live read model | persisted read model |
|---|---|---|---|---|---|---|---|---|
| 100 × 1 KB | 3.80 | 2.87 | 3.65 (delta 10) | 1.32× / 1.04×, faster by the median, ranges overlap | 0.63 | 0.61 | not measurable yet: stage 3 | not measurable yet: stage 4 |
| 10 000 × 1 KB | 68.4 | 2.81 | 4.60 (delta 100) | **24.4× / 14.9×, faster** | 0.62 | 0.59 | not measurable yet: stage 3 | not measurable yet: stage 4 |
| 100 000 × 1 KB | 579.1 | 2.80 | 4.57 (delta 100) | **207× / 127×, faster** | 0.62 | 0.59 | not measurable yet: stage 3 | not measurable yet: stage 4 |
| 100 × 200 KB | 106.7 | 2.79 | 14.3 (delta 10) | 38.3× / 7.5×, faster | 0.60 | 0.59 | not measurable yet: stage 3 | not measurable yet: stage 4 |
| 10 000 × 200 KB | 9 217.2 | 3.39 | 112.1 (delta 100) | 2 717× / 82×, faster | 0.65 | 0.60 | not measurable yet: stage 3 | not measurable yet: stage 4 |
| 100 000 × 200 KB | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit |

### Re-measured after the stage-1 fixes (2026-10-17)

Two fixer changes touch the measured paths:
- R1.3: a state read from the persisted fold is sized when it enters the cache. `sizeOf` now
  also runs on a cold load from a stored state with an empty delta.
- R1.1: the storability check before a write walks property descriptors.

So the 1 KB cells were run again. (Then, the matrix at the top still held the first run; since the
stage-2 fixer it holds a full re-run on the current code, and the first run is under
"Superseded" above. This section stays as the stage-1 fixer's record.)

| events × payload | fold on read, cold | persisted fold, cold, delta 0 | persisted fold, cold, delta 100 | cold: persisted fold vs. fold on read | stored.writeMs (nothing stored) | warm, both |
|---|---|---|---|---|---|---|
| 10 000 × 1 KB | 69.5 (60.4–78.2) | 3.16 (2.86–4.38) | 4.67 (4.39–6.22) | **22.0× / 14.9×, faster** | 2.37 | 0.65–0.71, 0 events |
| 100 000 × 1 KB | 597.2 (550.9–775.3) | 3.15 (2.77–5.48) | 4.64 (4.34–7.75) | **190× / 129×, faster** | 2.88 | 0.62–0.70, 0 events |

Commands: `sh bench/persisted-fold.sh 10000 1024 11` and `node bench/persisted-fold-bench.mjs
100000 1024 11`. Each cell is the median of 11 fresh processes, min–max in brackets; the full
output is on the board in `.ziv/lab/stage-01-fixer/bench-after.txt`. States were equal across all
cells and rounds.

The delta-0 median is 0.35 ms above the first run (2.81 → 3.16 ms). The added sizing does not
explain it: `estimateSize` of this state (a Map of 40 statistics entries) takes 8 µs. The shift is
within the spread between runs.

What the two fixes cost by state size, with the same state shape as "What the tier itself
costs" (a Map of key → `{ label, n, at, tags }`):

| keys | state bytes (v8) | added to a cold load from a stored state: `estimateSize` (R1.3) | stored.writeMs, memory: before → after (R1.1) | stored.writeMs, Postgres: before → after (R1.1) |
|---|---|---|---|---|
| 50 | ~5.5 KB | 0.04 ms | 0.09 → 0.12 | 0.67 → 0.73 |
| 500 | ~55 KB | 0.07 ms | 0.26 → 0.78 | 1.50 → 1.72 |
| 5 000 | ~560 KB | 0.72 ms | 3.46 → 4.64 | 9.44 → 11.28 |
| 50 000 | ~5.6 MB | 5.0 ms | 35.1 → 46.7 | 81.7 → 96.3 |

Commands:
- `node .ziv/lab/stage-01-fixer/sizing-cost.mjs dist/index.js` (median of 9);
- `node .ziv/lab/stage-01-measurer/state-size.mjs 9` after `npm run build`. "Before" is the
  measurer's run `.ziv/lab/stage-01-measurer/state-size-run1.txt`; "after" is
  `.ziv/lab/stage-01-fixer/state-size-after.txt`.

The descriptor walk makes a write of a large state up to about a third dearer on memory and
about a fifth dearer on Postgres. A write happens at most once per `persist.after` (500) settled
events. The sizing adds about 6% to the 83 ms cold read of a 5.7 MB stored state.

### Stage 2: what the subscription additions cost (2026-10-17)

Stage 2 adds no kind of kept state and changes no fold path (the matrix above was re-run by its fixer only to replace pre-fix numbers, R2.3): the live and the persisted
read model stay "not measurable yet" (stages 3 and 4). These cells measure what the four library
gaps cost: the transaction id on an append, the watermark on a read, the single-event skip, and
read-your-writes. Everything was run by one command, `sh bench/subscriptions.sh all` (it builds,
then runs `node bench/subscriptions-bench.mjs all`; tables `bench_sub_*`, dropped at the start and
the end), with `ES_BENCH_BEFORE` set to a build of commit 0f04d25 (the library before stage 2).
Exit 0, every check held, 1 min 14 s. The full output is on the board in
`.ziv/lab/stage-02-measurer/bench-run1.txt`.

**Before / after.** The append and query cells run the same code against two builds of the
library: "before" is 0f04d25, built from `git show` into a temporary directory, and "after" is
`dist/`. They run in alternating fresh processes (a warm-up process first, the order flipped every
round). Their absolute times are not comparable with the scout's lab (E1: 1.268 / 1.281 ms), which
went through a hooked `pg` client in another harness. The comparison is within one harness.

| cell | before (0f04d25) | after (stage 2) | difference | verdict | command |
|---|---|---|---|---|---|
| single-event append, + `pg_current_xact_id_if_assigned()::text` (decision 10); median of 300 per process, 9 processes per build | 0.438 ms (0.421–0.501) | 0.431 ms (0.419–0.506) | −0.007 ms (−1.6 %) | **no measurable cost**: ranges overlap. Every one of 2 700 appends after carried a `transactionId` | `node bench/subscriptions-bench.mjs append 9 --before=<dist of 0f04d25>` |
| the subscriber's empty settled page at the head, + `ctx.xmin::text` (decision 11); median of 300 per process | 0.360 ms (0.351–0.379) | 0.366 ms (0.353–0.391) | +0.006 ms (+1.7 %) | **no measurable cost**: ranges overlap. 3 600 of 3 600 reads after carried a watermark | `node bench/subscriptions-bench.mjs query 9 --before=…` |
| a full 500-event settled page (1 KB events), same column; median of 100 per process | 2.607 ms (2.533–2.767) | 2.683 ms (2.586–3.009) | +0.076 ms (+2.9 %) | **within noise**: ranges overlap | same |

(Values in brackets are the min–max of the per-process medians.)

**One poison event in a page of 500** (decision 12, through the built `subscribe`, `onError` →
`"skip"`). The time runs from the first handler call until the page is fully committed: the last
cursor is saved (memory) or the last transaction has committed (Postgres). Median of 11 runs per
row (min–max). Memory uses the default mode with `memoryCursors()` and an in-process handler that
applies a part only when it holds no poison event (all or nothing). Postgres uses processor mode
(decision 13): one transaction per handler call, in which the handler moves the checkpoint, writes
the rows, and throws while it applies a poison event, so the transaction rolls back. `target.skip`
writes the checkpoint and a dead letter in one transaction.

| store, mode | poison at | page committed after, ms | handler calls | `onError` calls | transactions | events applied | dead letters |
|---|---|---|---|---|---|---|---|
| memory, default mode | none | 0.044 (0.032–0.082) | 1 | 0 | – | 500 of 500 | 0 |
| memory, default mode | 0 | 0.071 (0.056–0.213) | 19 | 2 | – | 499 of 500 | 1 |
| memory, default mode | 250 | 0.051 (0.044–0.058) | 19 | 2 | – | 499 of 500 | 1 |
| memory, default mode | 499 | 0.042 (0.041–0.055) | 17 | 2 | – | 499 of 500 | 1 |
| Postgres, processor mode | none | 1.97 (1.81–2.35) | 1 | 0 | 1 | 500 of 500 | 0 |
| Postgres, processor mode | 0 | 24.8 (24.3–28.2) | 19 | 2 | 20 | 499 of 500 | 1 |
| Postgres, processor mode | 250 | 27.5 (24.2–28.6) | 19 | 2 | 20 | 499 of 500 | 1 |
| Postgres, processor mode | 499 | 25.3 (22.6–35.7) | 17 | 2 | 18 | 499 of 500 | 1 |

Command: `node bench/subscriptions-bench.mjs poison 11`. Beside it, for comparison, the scout's lab
E3 (`.ziv/lab/stage-02-subscriptions/RESULTS.txt`, a hand-written bisection in another harness,
median of 5) measured 43.2 / 19.9 / 26.0 ms for "S2 bisect" and 0 of 499 applied for the old
whole-batch skip. In every run, every healthy neighbour was applied and the subscription ended at
the page's last cursor. On Postgres, a poison event costs about 23–26 ms more than a clean page, or
about 12–14× the clean page. That is the price of 17–19 extra handler calls, each a transaction.
On memory the isolation adds a few hundredths of a millisecond.

**Re-measured after the stage-2 fix (R2.1, decision 14).** The Postgres target above wrote the
checkpoint under the end-of-page guard, which double-applies a longer redelivery (the finding
below). The bench's target now follows the corrected README pattern: it locks its checkpoint row
(`INSERT … ON CONFLICT DO NOTHING`, `SELECT … FOR UPDATE`), writes only the events beyond it, then
moves it, which is two more statements per transaction. The same machine, the old target (the
bench as of commit 9efc3ab) and the new one in alternating runs, median of 11 runs per row:

| Postgres, processor mode, page of 500 | old target, run 1 / run 2 | new target, run 1 / run 2 | new target, a single run before the pairs |
|---|---|---|---|
| no poison | 2.82 / 2.37 ms | 2.72 / 2.03 ms | 3.20 ms |
| poison at 0 | 35.1 / 26.9 ms | 33.9 / 29.0 ms | 38.5 ms |
| poison at 250 | 36.9 / 29.5 ms | 38.1 / 26.4 ms | 39.1 ms |
| poison at 499 | 40.1 / 27.1 ms | 34.8 / 25.7 ms | 34.3 ms |

**No measurable cost of the lock**: the two targets differ less than two runs of the same target
(both drifted ~10 ms between the first and the second pair). Every run: 499 applied, 1 dead letter,
the same 17–19 handler calls. A poison event in a page of 500 costs 24–36 ms more than a clean page
in these runs (the first table's 23–26 ms lies inside that). Commands: `node bench/subscriptions-bench.mjs poison 11`
for the new target, `node .ziv/lab/stage-02-fixer/old/subscriptions-bench.mjs poison 11` for the
old one (a copy of the bench at 9efc3ab on the board, run against the same `dist/`); output
`.ziv/lab/stage-02-fixer/poison-after.txt` and `poison-paired-{old,new}-{1,2}.txt`.

**Read-your-writes latency** (decision 11). Another store with its own pool appends, and the clock
runs from the append's return until `sub.hasPassed(token)` is true. `hasPassed` is checked on
every turn of the event loop. The database is idle, and each write comes after a random pause of
0–600 ms, so it falls at a random point of the poll cycle. 30 writes per row. Default options:
without the doorbell the subscriber polls every 500 ms; with `postgres: { live: true }` it polls
every 5 000 ms and wakes on the commit doorbell after a 25 ms coalescing window.

| doorbell | write | passed by | median ms | p90 ms | min–max ms | passed |
|---|---|---|---|---|---|---|
| off | not matched by the subscriber's query (type B) | the watermark | 159.6 | 480.1 | 0.67–487.2 | 30 of 30 |
| off | matched (type A) | the cursor | 374.1 | 477.2 | 4.59–495.1 | 30 of 30 |
| on (`live: true`) | not matched (type B) | the watermark | 26.9 | 27.4 | 25.9–27.8 | 30 of 30 |
| on (`live: true`) | matched (type A) | the cursor | 26.9 | 27.7 | 26.1–30.0 | 30 of 30 |

Command: `node bench/subscriptions-bench.mjs latency 30`. Without the doorbell the latency is
bounded by the poll interval (every write passed within 495 ms of a 500 ms poll). The two medians
(160 / 374 ms) differ only by where the writes fell in the cycle, and 30 samples of a
near-uniform wait do not pin a median down. With the doorbell it is about 27 ms, of which 25 ms is
the coalescing window (`wakeCoalesceMs`). A write the query does not match is passed by the
watermark as fast as a matched one is by the cursor. Not covered: a busy database, where an older
open transaction holds the watermark back until it ends (tested for correctness in
`test/subscribe-postgres.test.ts`, not timed).

**Processor mode with a longer redelivery: measured, a finding, not a speed cell.** The guard
that README "Processor mode" and decision 13 §6 showed was "move the checkpoint if it is behind,
then write every row of the page". It applies events twice whenever a redelivered page is longer
than the one already committed:
- one subscriber, a lost COMMIT acknowledgement, 2 new events before the retry: 3 events applied
  twice, in 5 of 5 rounds;
- two concurrent subscribers on the same target (separate stores and pools, in one Node process —
  the probe's "two processes" label; no second OS process was run) while appends stream in: 27–52
  extra rows per round, in 5 of 5 rounds.

A target that locks its checkpoint and applies only the events beyond it was exactly once in all
10 rounds. Command: `node .ziv/lab/stage-02-measurer/longer-redelivery.mjs 5` (after
`npm run build`; tables `lab_m2_*`, dropped), output
`.ziv/lab/stage-02-measurer/longer-redelivery.txt`.

**Fixed (stage-2 fixer, decision 14, O12's assumption (a)):** README "Processor mode", the
showcase and this bench now use the locking target, and the tests pin it: the longer redelivery in
`test/subscribe-processor.test.ts` (memory) and `test/subscribe-postgres.test.ts` (d), the
two-subscriber run in `test/subscribe-failures-postgres.test.ts`. Each of them was red with the
end-of-page guard (fixer log, R2.1).

### Stage 3: the live read model (2026-10-17)

One command for every cell: `sh bench/readmodel.sh all` (it builds, then runs
`node bench/readmodel-bench.mjs matrix|list|watch …`; tables `bench_rm_*` of its own, dropped at
the start and the end). Run on the stage-3 code (builder commit 5a037a1, product unchanged since):
exit 0, 18 min 18 s, load average 7.1 at the start (the machine is shared). Full output:
`.ziv/lab/stage-03-measurer/bench-run2.txt`. An earlier run of the same cells (attempt 1, same
product code, only the bench's output format differed) is `.ziv/lab/stage-03-measurer/bench-run1.txt`;
its 100 000-key `list` cell crashed (below) and it is quoted only as a second sample of the noise.

#### The 10× target, one line per size (1 KB; K = 1 and K = 1 000; batchSize 500 / 5 000)

| events × payload | warm: live vs. fold on read | cold: live vs. fold on read |
|---|---|---|
| 100 × 1 KB | **reached**: 3 795× / 3 773× (K = 1), 4 002× / 3 872× (K = 1 000) | **"not reached"**: 0.55× / 0.52× (K = 1), 0.31× / 0.33× (K = 1 000) |
| 10 000 × 1 KB | **reached**: 3 743× / 3 755×, 5 108× / 4 913× | **"not reached"**: 0.64× / 0.92×, 0.03× / 0.03× |
| **100 000 × 1 KB** (the constitution's cell) | **reached**: 4 116× / 4 083×, 6 030× / 5 980× | **"not reached"**: 0.81× / 1.01×, 0.005× / 0.005× |
| 100 × 200 KB | **reached**: 3 814× / 3 794×, 27 361× / 27 290× | **"not reached"**: 1.08× / 1.04×, 0.18× / 0.18× |
| 10 000 × 200 KB | **reached**: 3 763× / 5 107×, 3 367× / 4 126× | **"not reached"**: 1.00× / 1.00×, 0.00× / 0.00× (14.9 → 8 148 ms) |
| 100 000 × 200 KB | not run: the human's limit | not run: the human's limit |

Warm is an awaited `model.get(k)` (the shape users call; median of 20 segments of 1 000 gets per
process) against a warm `es.context()` with nothing new (median of 20). Cold is a fresh process:
`readModel(def)` → the first awaited `get(k)` returns, i.e. the rebuild of the whole source,
against the first `es.context()` of the key. At K = 1 both fold the same 100 000 events, and the
model's rebuild is about as fast as fold on read with pages of 5 000 and slower with pages of 500
(one page read per 500 events). At K = 1 000 fold on read folds one key's events (100 at 100 000)
while the rebuild folds all of them, so the cold factor is a comparison of 100 events against
100 000; it is written as measured. **The live model's cold start does not reach 10× and cannot:
it is a rebuild** (FOUNDING.md: reported, not held to 10×).

#### Detail: the matrix cells (ms; median of 11 fresh processes, min–max in brackets)

**100 000 × 1 KB** — `node bench/readmodel-bench.mjs matrix 100000 1024 11` (stored payload 1 081 bytes as JSON, table 142 MB)

| K | cell | cold | events folded cold | warm (min–max of the per-process medians) | events folded per warm read | keys held | runs failed |
|---|---|---|---|---|---|---|---|
| 1 | fold on read | **869.0** (542.8–2 179.6) | 100 000 | **0.92** (0.62–1.78) | 0 | – | 0 |
| 1 | live, batchSize 500 | **1 072.2** (867.4–2 839.3) | 100 000 | **0.22 µs** (0.18–0.26 µs) | 0 | 1 | 0 |
| 1 | live, batchSize 5000 | **856.2** (564.1–2 619.1) | 100 000 | **0.22 µs** (0.14–0.33 µs) | 0 | 1 | 0 |
| 1 000 | fold on read | **4.89** (3.93–10.8) | 100 | **1.28** (0.79–2.65) | 0 | – | 0 |
| 1 000 | live, batchSize 500 | **1 072.0** (933.6–3 283.5) | 100 000 | **0.21 µs** (0.14–0.43 µs) | 0 | 1 000 | 0 |
| 1 000 | live, batchSize 5000 | **950.3** (601.7–2 665.9) | 100 000 | **0.21 µs** (0.13–0.25 µs) | 0 | 1 000 | 0 |

The cold ranges are wide in this run (a max of 2–3 s in every cell: the shared machine). The
earlier run (bench-run1.txt) had the same order with narrow ranges: fold on read 496.0
(481.8–611.0), live 500 864.5 (837.4–928.6), live 5000 547.7 (534.9–571.2) at K = 1, i.e. cold
0.57× / 0.91×; warm 1 503× / 3 193×. Neither run reaches 10× cold; both reach it warm.

**10 000 × 1 KB** — `node bench/readmodel-bench.mjs matrix 10000 1024 11` (1 081 bytes, table 14 MB)

| K | cell | cold | events folded cold | warm | events folded per warm read | keys held | runs failed |
|---|---|---|---|---|---|---|---|
| 1 | fold on read | **72.5** (66.7–82.0) | 10 000 | **0.73** (0.69–0.90) | 0 | – | 0 |
| 1 | live, batchSize 500 | **112.7** (105.6–119.8) | 10 000 | **0.19 µs** (0.17–0.23 µs) | 0 | 1 | 0 |
| 1 | live, batchSize 5000 | **78.6** (73.9–91.9) | 10 000 | **0.19 µs** (0.16–0.21 µs) | 0 | 1 | 0 |
| 1 000 | fold on read | **2.87** (2.72–3.72) | 10 | **0.98** (0.89–1.19) | 0 | – | 0 |
| 1 000 | live, batchSize 500 | **111.7** (107.0–124.1) | 10 000 | **0.19 µs** (0.16–0.22 µs) | 0 | 1 000 | 0 |
| 1 000 | live, batchSize 5000 | **88.8** (77.1–99.8) | 10 000 | **0.20 µs** (0.19–0.23 µs) | 0 | 1 000 | 0 |

**100 × 1 KB** — `node bench/readmodel-bench.mjs matrix 100 1024 11` (1 080 bytes, table 248 kB)

| K | cell | cold | events folded cold | warm | events folded per warm read | keys held | runs failed |
|---|---|---|---|---|---|---|---|
| 1 | fold on read | **3.90** (3.56–6.07) | 100 | **0.72** (0.66–0.98) | 0 | – | 0 |
| 1 | live, batchSize 500 | **7.14** (6.16–12.4) | 100 | **0.19 µs** (0.17–0.20 µs) | 0 | 1 | 0 |
| 1 | live, batchSize 5000 | **7.57** (5.91–8.86) | 100 | **0.19 µs** (0.18–0.23 µs) | 0 | 1 | 0 |
| 1 000 | fold on read | **2.40** (2.28–3.23) | 1 | **0.76** (0.65–1.14) | 0 | – | 0 |
| 1 000 | live, batchSize 500 | **7.74** (6.60–9.43) | 100 | **0.19 µs** (0.18–0.20 µs) | 0 | 100 | 0 |
| 1 000 | live, batchSize 5000 | **7.33** (6.85–8.21) | 100 | **0.20 µs** (0.18–0.23 µs) | 0 | 100 | 0 |

**100 × 200 KB** — `node bench/readmodel-bench.mjs matrix 100 204800 11` (204 856 bytes, table 21 MB)

| K | cell | cold | events folded cold | warm | events folded per warm read | keys held | runs failed |
|---|---|---|---|---|---|---|---|
| 1 | fold on read | **95.2** (87.5–112.5) | 100 | **0.66** (0.56–0.75) | 0 | – | 0 |
| 1 | live, batchSize 500 | **88.2** (80.8–123.1) | 100 | **0.17 µs** (0.15–0.20 µs) | 0 | 1 | 0 |
| 1 | live, batchSize 5000 | **91.4** (83.6–103.0) | 100 | **0.17 µs** (0.16–0.20 µs) | 0 | 1 | 0 |
| 1 000 | fold on read | **16.0** (15.3–19.3) | 1 | **5.68** (5.35–6.44) | 0 | – | 0 |
| 1 000 | live, batchSize 500 | **90.5** (83.8–117.3) | 100 | **0.21 µs** (0.16–0.26 µs) | 0 | 100 | 0 |
| 1 000 | live, batchSize 5000 | **90.2** (81.6–100.1) | 100 | **0.21 µs** (0.21–0.25 µs) | 0 | 100 | 0 |

(An observation, not a limit: the warm fold on read at K = 1 000 here is 5.68 ms against ~0.7 ms
in every other cell — run 1: 5.36 ms. It folds 0 events, so the time is the store's delta read for
`accountId = a7`; why that read is slower on this 21 MB table was not investigated.)

**10 000 × 200 KB** — `node bench/readmodel-bench.mjs matrix 10000 204800 11` (204 857 bytes, table 2 037 MB)

| K | cell | cold | events folded cold | warm | events folded per warm read | keys held | runs failed |
|---|---|---|---|---|---|---|---|
| 1 | fold on read | **8 152.6** (7 979.5–8 964.0) | 10 000 | **0.78** (0.63–1.12) | 0 | – | 0 |
| 1 | live, batchSize 500 | **8 120.1** (7 944.0–8 419.9) | 10 000 | **0.21 µs** (0.19–0.22 µs) | 0 | 1 | **1** (below) |
| 1 | live, batchSize 5000 | **8 124.7** (7 924.7–9 269.7) | 10 000 | **0.15 µs** (0.12–0.19 µs) | 0 | 1 | 0 |
| 1 000 | fold on read | **14.9** (13.6–19.0) | 10 | **0.69** (0.64–0.77) | 0 | – | 0 |
| 1 000 | live, batchSize 500 | **8 147.9** (7 980.9–8 454.6) | 10 000 | **0.21 µs** (0.19–0.22 µs) | 0 | 1 000 | 0 |
| 1 000 | live, batchSize 5000 | **8 219.3** (7 998.3–8 532.2) | 10 000 | **0.17 µs** (0.15–0.18 µs) | 0 | 1 000 | 0 |

**Red: first reads that failed with `read_model_timeout`.** In run 2 one process of "live,
batchSize 500, K = 1" failed, so that cell has 10 runs instead of 11. In that run the bench kept
only the last three lines of the child's stderr, so the failure did not make the run red (exit 0).
That was a defect of the bench. I fixed it: a failed run now makes the run red, and its whole
stderr goes into a file. Then I ran the row once more to see the error:
`sh bench/readmodel.sh matrix 10000 204800 11`, output
`.ziv/lab/stage-03-measurer/bench-run3-200k.txt`, stderr `run3-failed-1…4.txt`. That run was
**red, exit 1**, with load average 5.4 at the start and 13.4 at the end.

- 4 of the 72 processes failed (2 layouts × 3 modes × 12 rounds: 11 measured and 1 warm-up;
  corrected by the stage-3 fixer, R3.4 — this line first said 78). All 4 were measured live runs:
  4 of the 44 measured live processes (4 of 48 with the warm-up round); no fold-on-read process
  failed. All with
  `TransientError: read model "bench-stats" v1 has not finished its rebuild within 10000 ms (timeoutMs)`:
  - 2 in "live, batchSize 500, K = 1";
  - 1 in "live, batchSize 500, K = 1 000";
  - 1 in "live, batchSize 5000, K = 1 000".
- The processes that finished show the same picture as run 2:

| K | fold on read cold | live 500 cold | live 5000 cold | warm, fold on read → live 500 / live 5000 |
|---|---|---|---|---|
| 1 | 8 178.8 (7 971.7–9 404.0) | 8 317.4 (8 022.8–8 724.5), 9 runs | 8 466.3 (8 122.0–9 779.4) | 0.74 ms → 0.20 µs / 0.16 µs: 3 693× / 4 750×, reached |
| 1 000 | 14.6 (12.9–79.5) | 8 196.3 (8 004.7–8 735.4), 10 runs | 8 133.5 (8 038.7–8 663.1), 10 runs | 0.66 ms → 0.21 µs / 0.16 µs: 3 113× / 4 048×, reached |

- The cold factors are 0.98× / 0.97× (K = 1), "not reached".
- The bench's line "states equal … : NO" in that output came from a second defect. The line read
  the shared exit code, and the only red messages were the four timeouts: no state differed. It
  now counts unequal states on their own.

What this measures: a rebuild that reads 2 GB takes 8.0–9.8 s on this machine. That is close to
the default `timeoutMs` of 10 000 ms (decision 18). A shared or slower machine pushes some first
reads past it. They throw `TransientError` (HTTP 503, "retry") until the rebuild is done. This
is how README "Live read models" says it behaves ("the wait — the first rebuild included — is
bounded by `timeoutMs`"). It is a finding about the default, not a broken guarantee: no read
returned a wrong state.

#### `list` (Postgres, the reader with the doorbell; K keys, one ~1 KB `OfferMade` each, 25 % open)

`list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 50 })` against
fold on read + list: `es.context()` of the whole source into one `Map` key → state, then the same
filter, sort and page in JavaScript (what an app without a model does). Five reps per size, each
in a fresh runtime. "Under load" = one append every 10 ms from a second runtime for 3 s, `list`
called back to back (1 ms apart), then the same for fold on read + list.
Command: `node bench/readmodel-bench.mjs list <keys> 5`.

| keys | fold on read + list, warm | live, warm at a stable revision (the kept result) | factor | the first live list after a change (scan + sort) | factor | live rebuild to the first list | fold on read + list, cold |
|---|---|---|---|---|---|---|---|
| 1 000 | 0.67 | 2.21 µs | **302×, reached** | 0.12 | **5.6×, "not reached"** | 27.1 | 13.3 |
| 10 000 | 0.87 | 0.75 µs | **1 157×, reached** | 0.44 | **2.0×, "not reached"** | 102.6 | 67.0 |
| 100 000 | 4.61 | 2.23 µs | **2 066×, reached** | 5.55 | **0.8×, "not reached"** | 980.7 | 553.8 |

| keys | under load: live list, every call (median, p90) | under load: fold on read + list, every call | factor | under load: the live calls that met a new revision | factor | calls that met a new revision / all live calls; writes |
|---|---|---|---|---|---|---|
| 1 000 | 7.21 µs, p90 0.0175 | 0.81, p90 1.16 | **113×, reached** | 0.16, p90 0.20 | **5.2×, "not reached"** | 445 / 11 815; 2 665 |
| 10 000 | 8.25 µs, p90 0.0204 | 1.74, p90 4.75 | **211×, reached** | 1.49, p90 1.93 | **1.2×, "not reached"** | 448 / 11 350; 2 617 |
| 100 000 | 6.29 µs, p90 0.0180 | 12.3, p90 14.3 | **1 952×, reached** | 10.3, p90 13.3 | **1.2×, "not reached"** | 387 / 8 975; 2 127 |

Events folded by 1 000 warm `list` calls per size: 0. Pages different from fold on read + list
(after the rebuild, after the 30 changes): 0. **The first `list` after a change is a scan and sort
of every key** (decision 19 keeps the result per shape until the revision changes), so it costs
about what fold on read + list costs warm: at 100 000 keys 5.55 ms against 4.61 ms. Decision 19's
"an index per field waits for the measurer's list-under-writes cell": this is that cell. The
10× target names reads of a model, not `list` after a change; it is written "not reached" here
and left to the architect of a later stage.

#### Memory: heap per key (`--expose-gc`, `heapUsed` after gc, the caught-up model held vs. before it)

| keys | heap held by the model | per key |
|---|---|---|
| 1 000 | 0.55 MiB | 575 bytes |
| 10 000 | 1.66 MiB | 174 bytes |
| 100 000 | 11.69 MiB | 123 bytes |

State `{ status, amount, revisions }` per key; the per-key figure is the rebuilt model after one
`get`: its states, its own maps and the runtime's overhead. It does **not** include a kept list:
the heap process (`heapChild`) never calls `list`, so its kept-list map is empty (corrected by the
stage-3 fixer, R3.4 — this line first said "and the kept list"). A kept list adds one sorted key
array per `where` and `sort` (at most 16); it is not measured here. Command: part of
`node bench/readmodel-bench.mjs list <keys> 5`.

#### Push on change: `model.watch` latency, fan-out, read-your-writes

Command: `node bench/readmodel-bench.mjs watch 30`. Latency from the append's return (in the
writer) to the watcher's callback, wall clock in ms; appends 0–600 ms apart (a random point of
the poll cycle).

| path | median | p90 | min–max | n | missed |
|---|---|---|---|---|---|
| memory store, same process | **1.50** | 1.69 | 0.21–1.94 | 30 | 0 |
| Postgres, same process, doorbell (`live: true`) | **28.9** | 30.2 | 27.4–30.2 | 30 | 0 |
| Postgres, **an append in a second OS process**, doorbell | **30.4** | 31.8 | 28.0–32.7 | 30 | 0 (writer exit 0) |
| Postgres, same process, no doorbell (poll 500 ms) | **316.8** | 462.5 | 39.3–467.8 | 20 | 0 |
| Postgres, `get(k, { atLeast })` right after the append returned, doorbell | **5.66** | 6.42 | 3.48–8.59 | 30 | – |
| Postgres, `get(k, { atLeast })` right after the append returned, no doorbell | **5.81** | 6.57 | 3.69–7.21 | 30 | – |

The ~29 ms of the doorbell path is subscribe's `wakeCoalesceMs` (default 25 ms,
`src/subscribe/index.ts:141`: doorbells within that window share one read) plus one page read;
`atLeast` asks the feed to read at once (`whenCaughtUp`), hence ~6 ms with or without the doorbell.

| watchers on one key (Postgres, doorbell, one process) | fold calls per change | last delivery after the append's return, median | p90 | changes |
|---|---|---|---|---|
| 1 | 1 (every change) | **29.5** | 30.2 | 20 |
| 100 | 1 (every change) | **29.2** | 30.2 | 20 |
| 1 000 | 1 (every change) | **29.2** | 30.6 | 20 |

**Folded once per change, never once per watcher**: 1 fold call per change at 1, 100 and 1 000
watchers, and the last of 1 000 deliveries within the same ~29 ms.

`get(k, { atLeast })` behind an older open transaction (held 300 ms, the builder's assumption 3:
a 1 → 50 ms pause between `whenCaughtUp()` rounds): from the older transaction's COMMIT to the
read's return, doorbell 7.20 ms median (3.44–32.7), no doorbell 7.65 ms (3.54–59.8), n = 10 each;
the read never resolved while the older transaction was open.

#### How it is measured, and what it does not cover (stage 3)

**Processes and the database.**
- Cold means a fresh `node` process: the store is installed and connected before the clock runs,
  and the database is warm, as in the stage-1 cells.
- Before the timed rounds: `VACUUM ANALYZE`, one untimed full read, and one warm-up round that is
  not kept. The order of the cells rotates per round.
- Database and client run on one machine, so there is no network latency. Over a network a
  rebuild pays one round trip per page (`batchSize`).

**What the warm cells mean.**
- The live warm cell is a lookup in RAM behind an awaited promise. Its ~0.2 µs is the cost of
  `await` plus a `Map` read.
- The 10× compares that with a warm fold on read, which already folds 0 events: about 0.7–1 ms is
  the store's delta read, which a live `get` does not make. This freshness choice is O14 (an open
  question to the human): a `get` that adds the store delta would not reach 10× warm (facts §16).

**The `list` cells.**
- The fold-on-read comparison reads the whole source into one `Map` with `contextCache.maxEvents`
  raised to 1 000 000. The default of 100 000 makes `es.context()` throw `ContextTooLargeError` at
  100 000 keys plus the load's revisions.
- That error is what crashed attempt 1's 100 000-key cell (bench-run1.txt). It is a limit of the
  comparison, not of the model.

**Not measured here.**
- Several tenants at once (one feed per tenant).
- A model with a memory budget (there is none, by decision 22).
- `watchList` latency. It shares the delivery path of `watch`, but was not timed on its own.
- The persisted read model: "not measurable yet: stage 4". (Superseded by "Stage 4" below.)

### Stage 4: the persisted read model (2026-10-17)

One command for every cell: `sh bench/persisted-model.sh all` (it builds, then runs
`node bench/persisted-model-bench.mjs matrix|list|rebuild|steady|takeover|locks …`; tables
`bench_pm_*` with their `_read_model` and `_read_model_state`, dropped at the start and the end).
Run on the stage-4 builder's code (commit 317288d, product unchanged since): **exit 1**, 27 min
34 s; the only red is the live model's 200 KB timeouts below. Full output:
`.ziv/lab/stage-04-measurer/bench-run1.txt`. Every matrix cell compares within one run: its own
fold-on-read cells, its own live cells (`batchSize` 500), its own persisted cells.

#### The 10× target, one line per size (persisted vs. fold on read; K = 1 / K = 1 000)

| events × payload | warm: persisted vs. fold on read | cold: persisted vs. fold on read (row current) | cold with the writer 100 events behind |
|---|---|---|---|
| 100 × 1 KB | **"not reached"**: 1.11× / 1.07× | **"not reached"**: 1.12× / 0.74× | **"not reached"**: 1.20× / 0.91× |
| 10 000 × 1 KB | **"not reached"**: 1.15× / 1.49× | **reached** 21.3× / **"not reached"** 0.87× | **reached** 15.7× / **"not reached"** 0.91× |
| **100 000 × 1 KB** (the constitution's cell) | **"not reached"**: 1.17× (0.79 → 0.68 ms) / 1.46× (0.99 → 0.68 ms) | **reached 195×** (675.9 → 3.47 ms) / **"not reached"** 1.31× (4.42 → 3.36 ms) | **reached 147×** (676.8 → 4.59 ms) / **"not reached"** 1.23× (5.76 → 4.69 ms) |
| 100 × 200 KB | **"not reached"**: 1.10× / 1.70× | **reached** 34.4× / **"not reached"** 1.66× | **"not reached"**: 1.95× / 1.08× |
| 10 000 × 200 KB | **"not reached"**: 1.20× / 1.38× | **reached** 1 490× (11 046 → 7.41 ms) / **"not reached"** 2.47× | **reached** 94.5× / **"not reached"** 1.14× |
| 100 000 × 200 KB | not run: the human's limit | not run: the human's limit | not run: the human's limit |

- **Warm never reaches 10×, at any size** (1.07–1.70×): a warm persisted `get` is one store query
  for the key's delta (Marten `FetchLatest`, INTENT decision 5), ~0.68–0.73 ms, about what a warm
  `es.context()` costs. This is O16's assumption, measured; the live model's warm `get` reaches
  3 704–25 337× in the same runs (it reads RAM only).
- **Cold reaches 10× where the key's history is long** (K = 1: 21× at 10 000, 195× at 100 000,
  1 490× at 10 000 × 200 KB): the row replaces the history, the read is ~3.5 ms (7.4 ms at 200 KB
  per event; the state row is still small). Where the key's history is short (K = 1 000: 1–100
  events per key) fold on read is already ~3–18 ms and the persisted `get` is 0.7–2.5× of it, not 10×.
- **With the writer 100 events behind**, the first `get` folds exactly those 100 from the store
  (≈ 1 ms per 100 × 1 KB, ≈ 110 ms per 100 × 200 KB — the delta's payloads), the second folds 0
  (the copy moved forward).

#### Detail: the matrix cells (ms; median of 11 fresh processes, min–max in brackets)

Cold = the first read in a fresh process (store installed and connected before the clock).
2nd = the next `get` in the same process. Warm = awaited reads with nothing new (persisted: 20
segments of 100 `get`s; live: 20 × 1 000; fold on read: 20 loads), the median of the per-process
medians. In the main cells the fresh process's handle claims the writer lock beside its first
read (its role right after that read was "standby" in every row but one); in the lagging cells
the process holds the model's writer lock itself through the store's internal capability (as
`bench/showcase-persisted.mjs` does), so its handle stays a standby and the row stays 100 behind.

| events × payload | K | fold on read cold / warm | live cold / warm | persisted cold (events folded) / 2nd / warm | fold on read (N + 100) cold | persisted, writer 100 behind: cold (events folded) / 2nd / warm |
|---|---|---|---|---|---|---|
| 100 × 1 KB | 1 | 3.92 / 0.76 | 7.28 / 0.20 µs | 3.49 (0) / see raw / 0.68 | 5.54 | 4.62 (100) / see raw |
| 100 × 1 KB | 1 000 | 2.48 / 0.78 | 7.25 / 0.21 µs | 3.34 (0) / see raw / 0.72 | 4.13 | 4.56 (100) / see raw |
| 10 000 × 1 KB | 1 | 76.7 (73.5–97.2) / 0.78 | 109.1 / 0.19 µs | 3.61 (3.16–5.20) (0) / 0.96 / 0.68 (0.63–0.70) | 76.8 | 4.89 (4.37–6.50) (100) / 0.84 / 0.68 |
| 10 000 × 1 KB | 1 000 | 2.90 (2.78–4.62) / 1.01 | 115.8 / 0.22 µs | 3.34 (3.19–5.34) (0) / 0.91 / 0.68 | 4.19 | 4.62 (4.36–5.05) (100) / 0.86 / 0.69 |
| 100 000 × 1 KB | 1 | 675.9 (644.8–717.6) / 0.79 (0.73–1.21) | 964.0 / 0.21 µs | 3.47 (3.29–3.85) (0) / 0.96 / 0.68 (0.66–0.72) | 676.8 | 4.59 (4.48–4.81) (100) / 0.84 / 0.68 |
| 100 000 × 1 KB | 1 000 | 4.42 (4.18–4.58) / 0.99 | 995.8 / 0.21 µs | 3.36 (3.13–5.13) (0) / 0.99 / 0.68 (0.64–0.69) | 5.76 | 4.69 (4.33–6.75) (100) / 0.92 / 0.69 |
| 100 × 200 KB | 1 | 118.2 (111.7–148.8) / 0.79 | 111.9 / 0.19 µs | 3.43 (3.29–5.50) (0) / 0.93 / 0.72 | 224.0 | 114.7 (109.3–131.9) (100) / 0.94 / 0.72 |
| 100 × 200 KB | 1 000 | 16.7 (16.2–17.8) / 5.89 | 108.9 / 0.23 µs | 10.1 (9.62–15.8) (0) / 3.58 / 3.46 | 120.9 | 111.6 (106.1–118.8) (100) / 0.94 / 0.73 |
| 10 000 × 200 KB | 1 | 11 045.8 (9 976.4–11 957.1) / 0.88 | **red: 10 of 11 runs `read_model_timeout`**; 1 run 9 973.8 / 0.24 µs | 7.41 (5.17–24.3) (0) / 1.51 / 0.73 | 11 357.5 | 120.2 (113.5–131.2) (100) / 1.23 / 0.70 |
| 10 000 × 200 KB | 1 000 | 17.9 (15.6–21.8) / 0.98 | **red: 11 of 11 runs `read_model_timeout`** | 7.25 (4.01–11.4) (0) / 1.26 / 0.71 | 122.5 | 107.7 (94.7–235.8) (100) / 1.07 / 0.72 |

The 100 × 200 KB, K = 1 000 main row's key holds one event; its fold-on-read warm 5.89 ms and
persisted warm 3.46 ms are written as measured and were not investigated further.
Builds before the cells (a process of its own, `batchSize` 5 000, from zero to caught up):
100 000 × 1 KB 778 ms (K = 1) / 1 109 ms (K = 1 000); 10 000 × 200 KB 10 321 / 9 966 ms.

**Red: the live model at 10 000 × 200 KB.** 21 of the 22 measured live runs (11 per layout:
10 at K = 1, 11 at K = 1 000) threw `TransientError` `read_model_timeout`; the 2 warm-up runs (one
per layout, not kept) did not, so it is 21 of 24 counting them (`round = -1 … 10` in
`bench/persisted-model-bench.mjs`; no failure in the raw output is marked `(warm-up round)`). The
live model's rebuild of ~2 GB of payload (~10 s here, 8.1 s in the stage-3 run) does not fit the
first `get`'s default `timeoutMs` of 10 000 ms. The stage-3 run saw 4 of its 44 measured live
processes time out (4 of 48 with its warm-up round, of 72 child processes in all; see the stage-3
200 KB section above and the O14 note). It is the documented 503-to-retry, not a wrong state; both
runs are left red and were not repeated. (Denominators corrected by the stage-4 fixer, R4.8.)

#### `list` (Postgres; R rows, one ~1 KB `OfferMade` each, 25 % open)

`list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 50 })`, first page
and last page (offset `total − 50`) with `total`. Persisted: 5 fresh processes with the first page
as the first call and 5 with the last page first (cold), then 50 calls of each page (warm). Fold on
read + list: `es.context()` of the whole source into a `Map`, filter, sort, page in JavaScript.
Live: the model's kept list. Command: `node bench/persisted-model-bench.mjs list <rows> 5`.

| rows | persisted cold, first / last page | persisted warm, first / last page | fold on read + list cold / warm (first, last) | live kept list, warm | warm: persisted vs. fold on read + list | cold: persisted vs. fold on read + list |
|---|---|---|---|---|---|---|
| 1 000 | 3.02 / 2.73 | 0.94 / 0.96 | 11.8 / 0.72, 0.72 | 2.17 µs | **"not reached"**: 0.76× (slower) | **"not reached"**: 3.92× |
| 10 000 | 6.94 / 7.16 | 4.66 / 4.80 | 80.0 / 1.21, 1.19 | 1.46 µs | **"not reached"**: 0.26× (slower) | **reached**: 11.5× |
| 100 000 | 36.7 / 33.6 | **27.9 / 29.8** | 651.0 / **5.48, 5.54** | 1.75 µs | **"not reached"**: **0.20× (5× slower)** | **reached**: 17.7× |

Pages different from fold on read + list (every persisted and live page compared): **0**.

**Red finding: a warm persisted `list` is slower than fold on read + list**, 5× at 100 000 rows.
Why (`node .ziv/lab/stage-04-measurer/list-explain.mjs 100000`: `EXPLAIN (ANALYZE, BUFFERS)` of the
statement the library sends; output `list-explain.txt`): every call sorts **all** matching rows by
the rank expression `CASE jsonb_typeof(doc->'amount') …` (25 000 rows: ~19 ms, two workers) and
counts them (`count(*)` 7.5 ms); the GIN index finds the `where` rows (0.9 ms) but nothing serves
the order, so the first page costs what the last costs. Without a `where` (100 000 rows) the sort
spills to disk (`external sort`, 4.3 MB) and a list takes 82 ms. **The README's advice does not
help**: with `CREATE INDEX ON <table>_read_model_state (tenant, name, version, (doc->'amount'))`,
created literally as README "Persisted read models" suggests, the plans are unchanged (26.5 ms /
80.8 ms; `node .ziv/lab/stage-04-measurer/list-explain.mjs 100000 readme-index`, output
`list-explain-readme-index.txt`): the index expression is not the `ORDER BY` expression. (The
stage-4 fixer withdrew that advice from the README, R4.6; the cells above are unchanged and stay red.)
O17's assumption (a) (list in SQL, "~1× warm") is measured here at 0.20–0.76×.

#### Rebuild and resume (100 000 × 1 KB; a fresh process and a fresh model name per run, 3 runs)

Command: `node bench/persisted-model-bench.mjs rebuild 3`.

| K (keys) | batchSize | from zero to caught up, ms median (min–max) | events/s | writer's fold time (ms) | pages (revision) |
|---|---|---|---|---|---|
| 1 | 500 | 1 644.9 (1 644.5–1 671.6) | 60 795 | 37.8 | 200 |
| 1 | 5 000 | **833.2** (832.5–864.4) | 120 016 | 31.8 | 20 |
| 1 000 | 500 | 3 587.7 (3 519.6–3 784.7) | 27 873 | 263.3 | 200 |
| 1 000 | 5 000 | **1 281.0** (1 257.7–1 303.3) | 78 061 | 91.8 | 20 |
| 100 000 | 500 | 4 128.8 (4 050.3–4 161.4) | 24 220 | 168.9 | 200 |
| 100 000 | 5 000 | **2 484.6** (2 383.5–2 491.0) | 40 248 | 213.5 | 20 |

The fold is 2–9 % of a rebuild; the rest is reading pages and writing rows. Pages of 5 000 are
1.7–2.8× faster than the default 500. **Resume** (K = 1 000, `batchSize` 500): 1 000 events
appended while no writer runs, then a fresh process opens the model: caught up in **70.3 / 54.9 /
57.2 ms**, `folded` = 1 000 each time (3 of 3), epoch +1 each time.

#### Steady state: one append every 10 ms (200 appends per path)

Command: `node bench/persisted-model-bench.mjs steady 200`. The writer and `watch("o1")` in this
process. Page transaction = the store capability's `page()` timed in the writer's process (lock the
model row, check the epoch, read rows, fold, write rows and checkpoint, commit). A watcher gets one
delivery per page with the newest state; an append counts as delivered by the first delivery that
holds it. In one process the loop also awaits a `get` and two `status()` after each append, so its
appends are more than 10 ms apart.

| path | page transaction, median (p90) | pages | append → delivered, median (p90, max) | never delivered | `get` right after the append, median (p90); events folded per such `get` | `get` = `es.context()` |
|---|---|---|---|---|---|---|
| memory store, one process | 0.029 (0.052) | 200 | **1.43** (1.61, 1.96) | 0 | 0.022 (0.050); 1.00 | yes |
| Postgres, one process, doorbell | **3.14** (4.74) | 165 | **14.4** (31.9, 34.5) | 0 | **0.80** (1.24); 1.00 | yes |
| Postgres, appends from a second OS process, doorbell | **3.63** (4.68) | 85 | **17.9** (31.3, 33.3) | 0 | – | yes |

A `get` right after an append folds that one event from the store (current without waiting); the
watcher's 14–18 ms median sits within subscribe's `wakeCoalesceMs` window (25 ms) plus one page.

#### Takeover, lock slots

`node bench/persisted-model-bench.mjs takeover 5` (default tick 1 s, `live: true`): the writer
process `SIGKILL`ed, this process holds a standby handle, one event appended right after the kill.
Kill → the standby is writer: **1 009.1 / 1 008.1 / 1 011.4 / 19.6 / 1 012.0 ms**; kill → that
event applied (`list({ atLeast })` returned): 1 014.1 / 1 014.6 / 1 016.6 / 19.1 / 1 015.5 ms
(median 1 014.6). The takeover waits for the next standby tick (1 s; in run 4 the tick fell right
after the kill). The event was applied exactly once in 5 of 5; epoch +1 each time.

`node bench/persisted-model-bench.mjs locks` (server: `max_locks_per_transaction` 64,
`max_connections` 100): N tenant handles of one model in one process, all writers.

| N | advisory locks held | sessions holding them | backends of the database | all writers after |
|---|---|---|---|---|
| 10 | 0 → **10** | 1 | 1 → 5 | 81.8 ms |
| 100 | 0 → **100** | 1 | 1 → 5 | 349.4 ms |

One lock slot per writer handle, all on one session per store (the backends: pool 3, the lock
session, the bench's admin). The cap of 1 000 writers per store and process is tested, not
benched: `test/persisted-model-failures.test.ts` (1 001 handles → exactly 1 000 writers, one
session). Both runs print a `pg` `DeprecationWarning` ("Calling client.query() when the client is
already executing a query") from `WriterLocks.tryLock` (`node --trace-deprecation`): concurrent
standby tries queue on the one lock session — allowed by `pg` 8 (the peer range is `< 9`),
removed in `pg` 9.

#### How it is measured, and what it does not cover (stage 4)

- `VACUUM ANALYZE` of the events and both model tables after seeding and after each build
  (facts §30); `ANALYZE` again after the 100 lagging events.
- The machine is shared and its load was not controlled; compare cells within one run.
- Not measured: a partitioned writer host (keepalives, ~25 s computed); the persisted model under
  `rls` with many tenants (stage 6); the GIN index's write cost per page alone (it is inside the
  page time above); 200 KB states in rows (here the states are small; the 200 KB is in the
  events); `list` under a steady write load; `watchList` latency on its own.

### Stage 5: versions and rebuild (2026-10-17)

One command for every cell: `sh bench/versions.sh all` (it builds, then runs
`node bench/versions-bench.mjs beside|live|paged|retire …` and
`node_modules/.bin/vite-node bench/fingerprint-bench.mjs`; tables `bench_v_*` with their
`_read_model`, `_read_model_state` and `_fold_state`, dropped at the start and the end). Run on the
stage-5 builder's code (commit d799985, product unchanged since) by the stage-5 measurer: **exit 1**,
37 min 59 s; the only red is three 10 000 × 200 KB runs of the *baselines* (fold on read and the
live rebuild) that hit the store's `statement_timeout`, below. Full output:
`.ziv/lab/stage-05-measurer/bench-run1.txt`. Load average during the run 9–27 (printed per
command): the machine was shared and not quiet, so compare cells within one command.

#### The 10× target, live model with `start: "persisted"`, cold (one line per size)

A fresh process: `readModel(def, { start: "persisted" })` → the first awaited `get(key)` (one
`REPEATABLE READ` snapshot of the model row and every state row, then its own feed from the
checkpoint), against the first `es.context()` of the key and the stage-3 rebuild (`readModel(def)`
→ the first `get`, the whole source). The persisted model (same name, version and definition,
`batchSize` 500) was built beforehand by a process of its own, which ended. Median of 11 fresh
processes after one warm-up round; factor = fold on read ÷ start from persisted, cold against cold.
Then 100 events for the measured key were appended while no writer ran: the start folds exactly
those (the "behind" columns). Times in ms, median (min–max where given; every cell's min–max is in
the raw output).

| events × payload | K | fold on read, cold (events) | live rebuild, cold (events) | **start: "persisted", cold** (events) | vs. fold on read | vs. rebuild | behind 100: fold on read (events) / start (events) | behind 100: vs. fold on read |
|---|---|---|---|---|---|---|---|---|
| 100 × 1 KB | 1 | 7.30 (6.57–13.5) (100) | 10.8 (100) | 11.0 (9.58–15.7) (0) | **"not reached"**: 0.66× | 0.98× | 9.49 (200) / 18.0 (100) | **"not reached"**: 0.53× |
| 100 × 1 KB | 1 000 | 4.69 (1) | 11.1 (100) | 12.0 (0) | **"not reached"**: 0.39× | 0.92× | 7.30 (101) / 19.5 (100) | **"not reached"**: 0.38× |
| 10 000 × 1 KB | 1 | 115.6 (89.6–313.9) (10 000) | 233.3 (10 000) | 11.4 (6.12–16.6) (0) | **reached**: 10.1× | 20.5× | 90.8 (10 100) / 14.1 (100) | **"not reached"**: 6.44× |
| 10 000 × 1 KB | 1 000 | 5.76 (10) | 238.8 (10 000) | 21.9 (0) | **"not reached"**: 0.26× | 10.9× | 5.91 (110) / 20.8 (100) | **"not reached"**: 0.28× |
| **100 000 × 1 KB** (the constitution's cell) | 1 | 800.3 (679.7–2 813.5) (100 000) | 1 154.3 (100 000) | **8.71 (6.30–11.4) (0)** | **reached: 91.9×** | 132× | 661.7 (100 100) / 8.92 (100) | **reached**: 74.2× |
| 100 000 × 1 KB | 1 000 | 6.66 (100) | 1 154.8 (100 000) | 17.9 (12.7–25.0) (0) | **"not reached"**: 0.37× | 64.5× | 6.09 (200) / 16.0 (100) | **"not reached"**: 0.38× |
| 100 000 × 1 KB | 100 000 | 4.23 (1) | 1 315.2 (100 000) | 411.9 (338.1–477.3) (0) | **"not reached"**: 0.01× | 3.19× | not measured (no delta at K = 100 000) | – |
| 100 × 200 KB | 1 | 107.7 (100) | 110.6 (100) | 6.89 (0) | **reached**: 15.6× | 16.1× | 215.5 (200) / 108.8 (100) | **"not reached"**: 1.98× |
| 100 × 200 KB | 1 000 | 17.1 (1) | 102.9 (100) | 7.18 (0) | **"not reached"**: 2.39× | 14.3× | 113.7 (101) / 112.7 (100) | **"not reached"**: 1.01× |
| 10 000 × 200 KB | 1 | 25 928.6 (10 103.3–34 230.9) (10 000) — **red: 2 runs failed** | 24 727.7 (10 000) — **red: 1 run failed** | 11.4 (7.02–21.4) (0) | **reached**: 2 280× | 2 175× | 11 466.2 (10 100) / 114.6 (100) | **reached**: 100× |
| 10 000 × 200 KB | 1 000 | 18.3 (10) | 25 276.9 (10 000) | 17.0 (0) | **"not reached"**: 1.08× | 1 491× | 122.7 (110) / 125.0 (100) | **"not reached"**: 0.98× |
| 100 000 × 200 KB | – | not run: the human's limit | not run: the human's limit | not run: the human's limit | not run: the human's limit | – | not run: the human's limit | – |

Commands: `node bench/versions-bench.mjs live <events> <payloadBytes> 11` per size (`sh bench/versions.sh live`).

- **The cold live model reaches 10× where one key holds a long history** (K = 1: 91.9× at
  100 000 × 1 KB, 2 280× at 10 000 × 200 KB — against a red baseline, see the last bullet; 858×
  against the matrix's 9 785.9 ms fold on read): the start reads one row and folds 0 events, where
  the rebuild folds the whole source. Stages 3 and 4 had the cold live cell at 0.81× / 1.00× "not
  reached" (the rebuild); `start: "persisted"` is what moves it. 10 000 × 1 KB, K = 1 is 10.1× by
  the median, and its fold-on-read range (89.6–313.9 ms) shows the load: read it as "about 10×",
  not as a margin.
- **Where keys are many and short it does not**: a live model loads **every** row at start
  (K = 100 000: 412 ms for 100 000 rows; K = 1 000: 18 ms), fold on read loads one small key
  (1–100 events, 4–7 ms). Against the rebuild it is still 3.2× (K = 100 000) to 1 491× faster.
- **Behind by 100 events** the start folds exactly those 100 from the store: cheap at 1 KB
  (8.9 ms at K = 1), ~110 ms at 200 KB (the delta's payloads), which is why the 200 KB "behind"
  cells drop to 1–2×.
- **Red: 3 measured runs of the 10 000 × 200 KB, K = 1 baselines** (2 of 12 fold-on-read runs, 1 of
  12 live-rebuild runs, the warm-up round included; none of the 12 `start: "persisted"` runs) failed
  with `TransientError` `57014`, "canceling statement due to statement timeout": one statement
  reading ~2 GB of payload went past the store's default `statement_timeout` of 30 000 ms
  (src/postgres/store.ts:118) at a load average of 20–27; the medians of those cells (25.9 s /
  24.7 s, of 10 and 11 runs) are 2.3× the stage-4 run's (11.0 s / ~10 s) for the same reason. Left
  red, not repeated. (Stages 3 and 4 used the live `get`'s default `timeoutMs` and recorded
  `read_model_timeout` there; this run passes `timeoutMs: 600 000` so the rebuild gives a number —
  those earlier red cells stay as recorded.)

#### Rebuild beside the old version (100 000 × 1 KB, one append every 5 ms)

Per run a fresh model name: v1 (`batchSize` 5 000) built by a process of its own; then this process
opens v1 (it was v1's writer in every run), a second process appends one 1 KB event every 5 ms
(~210/s), and this process samples `v1.get(key)` and `v1.list({ limit: 50 })` back to back with
10 ms pauses — 3 s "before", then v2 (same fold, version 2, `batchSize` 5 000) is opened, "during"
until v2's `status()` (polled every 25 ms) says `building: false`. **One process**: v2 in the same
runtime and pool (10, the default) as v1's writer and the sampler. **Two processes**: v2 in a fresh
node process of its own (the rolling-deploy shape facts §37 did not measure). `postgres: { live:
true }`. 3 runs per cell. After every switch v2's `get` equalled fold on read (v2's fold) for 3 of 3
keys, and its `list({ atLeast })` of the last append answered with every key.

| K | shape | open → switch, ms: median (min–max) | v2 `lag.events` at the switch, per run | v1 `get` p50 / p95: before → during | v1 `list` p50 / p95: before → during |
|---|---|---|---|---|---|
| 1 000 | one process | 1 634.5 (1 522.2–2 447.9) | 13 / 2 / 2 | 1.34 / 1.80 → 2.01 / **9.19** | 1.71 / 2.32 → 2.26 / 7.44 |
| 1 000 | two processes | 1 606.3 (1 602.9–2 656.8) | 3 / 2 / 2 | 1.20 / 1.59 → 1.45 / 6.09 | 1.55 / 2.07 → 1.97 / 5.26 |
| 100 000 | one process | 2 842.3 (2 787.8–3 930.6) | 4 / 2 / 2 | 1.12 / 1.43 → 1.34 / 6.28 | 32.2 / 34.2 → 33.0 / 40.3 |
| 100 000 | two processes | 2 885.0 (2 702.8–9 276.9) | 2 / 2 / 6 | 1.12 / 1.47 → 1.37 / 2.57 | 32.6 / 36.0 → 34.5 / 49.4 |

Commands: `node bench/versions-bench.mjs beside 1000 3`, `node bench/versions-bench.mjs beside 100000 3`
(`sh bench/versions.sh beside`). p50/p95 per run, then the median over the 3 runs (ms).

- **v2 switches about when its build is done**: 1.5–2.7 s at K = 1 000 and 2.7–3.9 s at
  K = 100 000 (one outlier of 9.3 s in the two-process shape, at a load average of 23), about what
  v1's own build took in the same runs (1.4–2.8 s / 2.7–3.9 s); its lag at the first poll after the
  switch was 2–13 events — the switch waits for the token T, not for lag 0 (decision 39), and under
  this stream it never waited for a quiet moment.
- **The old version pays during the build**: v1 `get` p95 1.8 → 9.2 ms with v2 in the same
  process at K = 1 000 (p50 1.3 → 2.0 ms), 1.6 → 6.1 ms with v2 in another process; at K = 100 000
  the `get` p95 rises 1.4 → 6.3 ms (one process) and 1.5 → 2.6 ms (two). Two processes cost v1
  less than one: the pool and the event loop are not shared — the database is, so it is not zero.
- v1's `list` at K = 100 000 is 32–40 ms p50 before any rebuild (stage 4's red warm-list finding,
  F2/O17, unchanged) and 33–45 ms p50 during it.

#### Persisted fold after a version bump (one key holds every event, 1 KB; every step a fresh process)

`contextCache` default (`maxEvents` 100 000, `persist.after` 500). Run A at 100 000 events; run B:
v1 first at 100 000, then 500 events appended, then the bump at 100 500 > `maxEvents`, which folds
in pages (decision 41) instead of throwing `ContextTooLargeError` as facts §39 measured it before
this stage. Every state equal to a fold from zero. Median of 3 runs (min–max), ms.

| step | load (wall) | start | events | `readMs` | `foldMs` | `stored.readMs` | `stored.writeMs` | written |
|---|---|---|---|---|---|---|---|---|
| A v1 first (100 000) | 777.5 (679.0–780.2) | initial | 100 000 | 733.3 | 36.3 | 1.23 | 5.47 | true |
| A v1 again | 5.96 (5.87–6.82) | stored | 0 | 2.93 | 0.057 | 2.63 | 0 | false |
| A v2 first, the bump (100 000) | 704.0 (702.3–828.4) | initial | 100 000 | 664.6 | 34.4 | 1.03 | 4.37 | true |
| A v2 again | 4.64 (4.54–5.39) | stored | 0 | 2.20 | 0.052 | 1.92 | 0 | false |
| B v1 first (100 000) | 721.5 (670.6–727.5) | initial | 100 000 | 679.2 | 35.2 | 1.01 | 5.01 | true |
| B v1 after 500 more (100 500) | 15.3 (14.7–17.8) | stored | 500 | 8.86 | 1.88 | 2.03 | 2.01 | true |
| **B v2 first, the bump (100 500: paged)** | **679.8 (654.1–703.1)** | initial | **100 500** | 639.2 | 30.3 | 1.09 | 1.20 | true |
| B v2 again | 4.83 (4.42–6.22) | stored | 0 | 2.45 | 0.051 | 1.87 | 0 | false |

Command: `node bench/versions-bench.mjs paged 3` (`sh bench/versions.sh paged`).

- **A bump costs one full refold per state key, once** (680–704 ms at 100 000–100 500 events, the
  same as a first load), then 4.6–4.8 ms from the stored state with 0 events — and above
  `maxEvents` it now loads (100 500 events in two pages, written along the way) where stage 1 threw
  `ContextTooLargeError` on every load (facts §39, steps 6–7).
- The fold is ~5 % of a first load; reading 100 000 events is the rest.

#### Retiring an old version: the README recipe as printed

v1 and v2 (`batchSize` 5 000) built by processes of their own (v2 switches), both ended; then the
recipe of README "Retiring an old version" — parsed from the README, table names replaced — as the
owner, in one transaction (`UPDATE … status = 'retired'` + `DELETE` of the state rows), timed from
`BEGIN` to `COMMIT`. 3 runs per size, a fresh name each.

| rows per version | recipe, ms: median (min–max) | v1 rows before → after | v2 rows before → after | v1 status after |
|---|---|---|---|---|
| 1 000 | 3.97 (3.84–6.01) | 1 000 → 0 | 1 000 → 1 000 | retired |
| 100 000 | 30.7 (27.4–56.9) | 100 000 → 0 | 100 000 → 100 000 | retired |

Command: `node bench/versions-bench.mjs retire 3` (`sh bench/versions.sh retire`).

#### The fingerprint: cost per definition, computed once

On the sources (the fingerprint is internal, not in `dist/`), through vite-node — its esbuild
transform makes the handler text differ from a tsup build, the cost not. 2 000 fresh definitions
per rep (new objects, the same text), 5 reps after one warm-up; the defs are three event types with
zod schemas, merged as the runtime merges them (src/runtime.ts:184), so the first call also renders
their JSON schemas. "Hashes" is the internal counter `fingerprintsComputed()` before and after.

| definition | first call (hashes), µs per definition | again (memo), µs | hashes per definition: first / again | the same print for the same text |
|---|---|---|---|---|
| read model (`$fold`, 3 handlers) | 36.6 (34.0–39.7) | 0.13 | 1 / 0 | yes |
| persisted fold (`defineFold` over a plain fold) | 41.7 (36.0–43.6) | 0.65 | 1 / 0 | yes |

Command: `node_modules/.bin/vite-node bench/fingerprint-bench.mjs 2000 5` (`sh bench/versions.sh
fingerprint`). A single earlier run of the same command at a lower load gave 23.0 / 21.2 µs.

#### How it is measured, and what it does not cover (stage 5)

- `VACUUM ANALYZE` of the events table after seeding and of all tables after each build; `ANALYZE`
  after the appended delta.
- Not measured: blue-green and the live start under `rls` with many tenants (stage 6); a v2 whose
  fold is slower than v1's; a separate pool for the new version's writer in one process (the
  two-process shape is the closest); `watch`/`watchList` delays across the switch; the live start
  with large *states* (here the states are small, the 200 KB is in the events).

### Stage 6: tenants, rls and erasure (2026-10-17)

One command for every cell: `sh bench/tenants.sh` (it builds, then runs
`node bench/tenants-bench.mjs fanout|page|rebuild|erasure|invalidate|read …`). **Everything runs under
row-level security**: a schema of its own (`bench_t6`) and three roles of its own, created at the start
and dropped at the end of every command (`bench_t6_owner` installs, no superuser, bound by FORCE;
`bench_t6_app` is the application; `bench_t6_proc` is the processor role, `postgres.processorRole`).
Every event 1 KB. Run on the stage-6 builder's code (commit b62fbc7, product unchanged since) by the
stage-6 measurer: **exit 0**, 5 min 20 s, load average 9–14 (printed per command; the machine was
shared, so compare cells within one command). Full output: `.ziv/logs/stage-06/4-measurer-bench.txt`
(the same as `.ziv/lab/stage-06-measurer/bench-run3.txt`).

Statements are counted in the bench process by wrapping `pg.Client.prototype.query`, per
`application_name`: the application's handles and the processor; the appender and the observing
superuser are not counted, nor the heartbeat `SELECT 1`. The processor's page transactions are
recognised by their shared erasure lock and timed statement by statement (`BEGIN` → `COMMIT` on the
client; the fold's JavaScript between statements included).

#### One processor vs. a writer per tenant: statements and latency per append

N tenants, 4 events each. "Per tenant" = every tenant's `es.forTenant(t).persistedReadModel(def)`
without `processor` (a writer each, stage 4); "processor" = one `processor: "cross-tenant"` processor
on the processor api's root plus a reader per tenant. 7 single-event appends per cell (one more as a
warm-up, not kept), each to another tenant; "applied" = the append's return → the tenant's model row
checkpoint at or beyond the event (polled every 5 ms by the superuser). Statements are those of the
window append → applied + 300 ms. Median (min–max).

| tenants | per tenant: statements per append | per tenant: append → applied (ms) | per tenant: idle statements per 6 s | per tenant: writer locks | processor: statements per append (readers: 0) | processor: append → applied (ms) | processor: idle per 6 s | processor: writer locks |
|---|---|---|---|---|---|---|---|---|
| 1 | 20 | 43.5 (41.2–45.3) | 8 | 1 | 15 | 44.0 (41.0–49.5) | 2 | 1 |
| 10 | 92 | 42.5 (40.6–50.3) | 80 | 10 | 15 | 44.9 (42.6–46.3) | 2 | 1 |
| 100 | 812 | 79.9 (71.6–123.0) | 800 | 100 | 15 | 45.5 (44.2–50.3) | 2 | 1 |
| 1 000 | 8 012 | 323.5 (316.6–356.3) | 8 357 | 1 000 | 15 | 43.8 (37.9–50.4) | 2 | 1 |

Command: `sh bench/tenants.sh fanout` (`node bench/tenants-bench.mjs fanout <N> 7`). The per-tenant
count includes the idle polling that falls into the window; minus the idle rate it is 19.5 / 87.4 /
761 / 7 143. The processor's count is the same at every N (min = max in every cell); the readers run
none. First build of N tenants: per tenant 45 / 72 / 298 / 2 601 ms, processor 22 / 49 / 87 / 612 ms.
These are client-level statements under `rls`; README's "18 statements" comes from the builder's test
(`tenants-postgres.test.ts`, no `rls`, counting `pool.query` and the client's `query`), a different count.

#### The processor's page vs. tenants per page

10 000 events over T tenants, appended so that every 1 000 consecutive events hold every tenant; a
processor with `batchSize` 1 000 builds from zero; a fresh model name per run, 3 runs × 10 pages, the
first page of each run (it inserts the tenant rows) apart. "fresh" = the kept tables carry the
statistics of their empty state (VACUUM ANALYZE after seeding the events), as on a fresh install before
autovacuum's first ANALYZE; "analyzed" = one build first, then VACUUM ANALYZE before every run. After
each command, 20 tenants' reader `get` vs. `es.context()`: **0 mismatches** (3 / 30 / 60 / 60 gets per
T, both variants). Median (min–max) in ms; the share of the page in brackets.

| T | statistics | page | erasure lock | count check | notify | state rows read | state rows upsert | between statements (fold, client) | first page |
|---|---|---|---|---|---|---|---|---|---|
| 1 | fresh | 13.0 (10.8–20.9) | 0.71 (5.3 %) | 1.29 (10.0 %) | 0.74 (5.6 %) | 1.06 | 1.21 | 1.65 | 13.3 |
| 10 | fresh | 17.5 (9.82–24.1) | 0.72 (4.2 %) | 1.36 (7.6 %) | 0.77 (4.6 %) | 1.66 | 3.07 | 2.36 | 19.3 |
| 100 | fresh | 36.9 (22.8–49.4) | 0.48 (1.7 %) | 0.86 (3.0 %) | 0.52 (1.7 %) | 3.92 | 11.1 | 5.12 | 30.6 |
| 1 000 | fresh | **247.3 (27.8–508.8)** | 0.44 (0.18 %) | 0.94 (0.42 %) | 1.24 (0.53 %) | **211.5** | 11.1 | 6.31 | 39.7 |
| 1 | analyzed | 10.5 (7.35–14.3) | 0.59 (5.4 %) | 1.09 (10.3 %) | 0.52 (5.1 %) | 0.85 | 1.01 | 1.36 | 12.1 |
| 10 | analyzed | 12.6 (8.89–29.3) | 0.50 (4.2 %) | 0.94 (7.7 %) | 0.46 (3.7 %) | 1.32 | 2.37 | 1.95 | 14.9 |
| 100 | analyzed | 34.4 (31.4–40.3) | 0.66 (1.9 %) | 1.15 (3.3 %) | 0.76 (2.2 %) | 3.89 | 13.7 | 6.62 | 31.6 |
| 1 000 | analyzed | **178.9 (28.7–502.4)** | 0.56 (0.37 %) | 1.03 (0.69 %) | 1.33 (0.83 %) | **138.7** | 11.1 | 6.31 | 42.4 |

Command: `sh bench/tenants.sh page` (`node bench/tenants-bench.mjs page <T> 3 fresh|analyzed`). Every
page has 12 statements. The erasure fence (lock + count check) costs ~1–2 ms per page whatever T.
**Red, for the reviewer: at 1 000 tenants per page the page's "state rows read"** (`… WHERE (tenant,
key_hash) IN (SELECT * FROM unnest(…)) AND name = $3 AND version = $4`, src/postgres/models.ts, the
cross-tenant `page`) takes most of the page. Its plan, measured on a table of its own
(`node .ziv/lab/stage-06-measurer/state-read-plan.mjs`, output `state-read-plan.txt`, 30 000 state
rows): with the statistics of the empty table, a nested loop with a join filter, **776 ms for 1 000
pairs** (72 ms for 100); after ANALYZE of the filled table, a hash join, 2.2 ms; **when the model's rows
are newer than the last ANALYZE (a new model name or version, the table otherwise analyzed), the same
nested loop again, 650 ms** for 1 000 pairs (0.36 ms for 100). So a cross-tenant processor's first
build, and every new version's build, is quadratic in tenants per page until autovacuum analyzes the
table. A `LATERAL` form of the same lookup is flattened by the planner into the same plan (801 ms, lab
only). No limit names this cost; it stands here as measured.

Fixer (R6.3), two more forms on the same table copy plus a case D (20 tenants × 2 000 keys per model,
the queried model never analyzed): `node .ziv/lab/stage-06-fixer/state-read-plan.mjs` (output and
summary in `.ziv/lab/stage-06-fixer/state-read-plan*.txt`). A `LATERAL` subquery fenced with `OFFSET 0`
(never pulled up) and one with `ORDER BY key_hash LIMIT 1` take 1.9–2.5 ms for 1 000 pairs in A and C
(the product: 760 / 691 ms), but in D the planner looks each pair up through `(tenant, name, version,
revision)` with `key_hash` as a filter: 87 / 39 ms for 1 000 pairs, against 7.9 ms for the product's
statement. Neither form is independent of the statistics, so the product's statement is unchanged; the
README ("One processor for every tenant") states both costs and the advice to `ANALYZE` the state
table once a build has written its first pages.

#### Rebuild across tenants

100 000 × 1 KB over 100 tenants × 10 keys (appended interleaved: 1 000 rounds of 10 events per
tenant), default `batchSize` (500), a fresh model name per run, 3 runs each, the order alternated.

| statistics | one processor: zero → caught up | events/s | 100 per-tenant writers: zero → caught up | events/s | processor vs. per tenant |
|---|---|---|---|---|---|
| fresh | 5.38 s (4.52–6.43) | 18 592 | 0.96 s (0.94–0.98) | 103 847 | the processor takes **5.6×** as long |
| analyzed | 4.16 s (3.88–4.78) | 24 054 | 1.03 s (1.01–1.14) | 97 370 | the processor takes **4.0×** as long |

Command: `sh bench/tenants.sh rebuild` (`node bench/tenants-bench.mjs rebuild 3 fresh|analyzed`). The
per-tenant writers fold in parallel (one feed each, pool of 10); the processor is one feed, one page at
a time, each page spanning ~50 tenants. No limit names the rebuild; the processor's gain is per append
(above), not per rebuild.

#### Erasure: the README recipe, as printed

A tenant X with N × 1 KB events on 10 keys, beside 9 tenants × 1 000; a cross-tenant model built over
all and X's persisted folds stored (10 keys). The recipe "Erasing a tenant" read from README.md (schema
and table substituted), run as the owner (no superuser, bound by FORCE), the step-1 count
(`statistics()`) checked before `COMMIT`; timed from the lock statement to `COMMIT`. "running" = the
processor runs and an appender writes one event every 5 ms to the 9 other tenants. 3 runs each, a fresh
X per run. Every run: count check ok; deleted N events, 10 fold states, 10 state rows, 1 model row;
**0 rows of X left** in the events table and the three kept tables (read as the superuser; "running":
again after one more processor page), 18 of 18 runs.

| N | processor stopped | processor running | processor page during the recipe | processor page, stream alone (2 s right after) |
|---|---|---|---|---|
| 1 000 | 8.43 ms (7.33–8.75) | 5.65 ms (3.97–9.13) | 6.97 ms (5.66–19.1), 5 pages, 0 rolled back | 4.54 ms (3.19–11.3), 842 pages |
| 10 000 | 14.1 ms (9.49–17.5) | 12.7 ms (12.5–17.3) | 16.6 ms (5.40–19.0), 4 pages, 0 rolled back | 4.64 ms (3.14–59.7), 759 pages |
| 100 000 | 148.1 ms (127.1–158.3) | 106.6 ms (95.6–193.3) | 96.4 ms (5.93–192.8), 5 pages, 0 rolled back | 4.65 ms (3.63–99.8), 706 pages |

Command: `sh bench/tenants.sh erasure` (`node bench/tenants-bench.mjs erasure <N> 3`). A page that
overlaps the recipe waits for its exclusive lock, so its time grows with the recipe's; none was rolled
back by the count check in these runs (the interleavings that roll back are the builder's tests).

#### `invalidate()`, and a reader at 100 000 events

- **`invalidate()`**: one tenant with 10 000 × 1 KB on 10 keys, a live model of it (application role,
  `rls`): `invalidate()` → the first `get` after it **122.4 ms (117.2–148.7)**, 10 000 events folded per
  rebuild, 11 of 11 answers equal to `es.context()`; the model's first build 128.1 ms. Command:
  `node bench/tenants-bench.mjs invalidate 11`.
- **A reader of a processor-written model, 100 000 × 1 KB, K = 1** (one tenant, built by the processor,
  `batchSize` 5 000, which then closes; 11 fresh processes per kind after a warm-up round, the order
  rotated; a plain fold, so fold on read has no persisted fold):

| | cold (the first call of a fresh process) | warm (the next call) |
|---|---|---|
| fold on read (`es.forTenant(t).context`) | 677.8 ms (647.2–845.6) | 3.65 ms (3.01–5.15) |
| reader `get` (`es.forTenant(t).persistedReadModel(def).get`) | 20.4 ms (19.1–36.5) | 2.71 ms (1.91–4.84) |
| factor, 10× target | **33.2×, reached** | **1.35×, not reached** |

Command: `node bench/tenants-bench.mjs read 11`. The reader's two `get`s folded 0 events (`readFolded`)
in every process; the states equal fold on read in 12 of 12 rounds. The warm cell is O16's (a warm
`get` reads the key's delta from the store, ~1× by construction); under `rls` that read is a
transaction with `set_config`, so the stage-4 cells without `rls` (3.47 ms cold, 0.68 ms warm) are not
comparable to these. An earlier run of this command (`.ziv/lab/stage-06-measurer/bench-run1.txt`)
passed a fresh `initial` per call, so its warm fold on read missed the context cache (668 ms); that run
is superseded and not used.

#### How it is measured, and what it does not cover (stage 6)

- `VACUUM ANALYZE` of every table after seeding (and, "analyzed", before every run); the "fresh" cells
  keep the statistics of the empty kept tables on purpose.
- The rls setup is the README's "Roles for RLS": the processor role has the three policies of
  `processorRole` and the grants listed there; the application role is not a member of it.
- Not measured: the processor's page with large states or 200 KB events; more than one cross-tenant
  model per processor api; the processor's `status().lag` across tenants; a user-owned pool that
  switches roles per connection (decision 46's documented limit); the residual of decision 48 (a root
  fold load in flight without `rls`).
