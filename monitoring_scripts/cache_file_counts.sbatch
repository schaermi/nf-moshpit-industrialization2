#!/bin/bash
#SBATCH --job-name=cache-file-counts
#SBATCH --time=5-00:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --output=log_files/%x_%j.out
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=schaererm@ethz.ch
#
# Show which artifact types use the files in a QIIME 2 cache, without walking the whole cache:
# keys are grouped by type (the run ID, sample or batch suffix removed), a few artifacts per type are
# counted (all files, and those under provenance/), and the type total is estimated from the mean.
# Also counts artifacts that no key or pool refers to (left behind by killed jobs).
#
# Usage: mkdir -p log_files && sbatch monitoring_scripts/cache_file_counts.sbatch [CACHE_DIR] [SAMPLES_PER_TYPE]
# Submit from outside /cluster/work: results go to the submit directory, and the quota there is full.
# Writes cache_file_counts_<jobid>.txt to the sbatch submit directory (and prints it to the job log).
# The notification email only says the job finished; the results are in that file.

CACHE_DIR="${1:-/cluster/work/bokulich/schaererm/industrialization2/output/caches/main}"
SAMPLES="${2:-3}"
CACHE_DIR="${CACHE_DIR%/}"

OUT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
JOB="${SLURM_JOB_ID:-local}"
REPORT="$OUT_DIR/cache_file_counts_${JOB}.txt"
TMP="$OUT_DIR/cache_file_counts_${JOB}.parts"

case "$OUT_DIR" in
  /cluster/work/*)
    echo "Submit from outside /cluster/work (the quota there is full): $OUT_DIR" >&2
    exit 1 ;;
esac
if [ ! -d "$CACHE_DIR/keys" ] || [ ! -d "$CACHE_DIR/data" ]; then
  echo "Not a QIIME 2 cache (no keys/ or data/): $CACHE_DIR" >&2
  exit 1
fi
mkdir -p "$TMP" || exit 1
cd "$CACHE_DIR" || exit 1
shopt -s nullglob

# Value of a top-level "data:" or "pool:" field in a key file
key_field() {
  awk -F':' -v f="$1" '$0 ~ "^[[:space:]]*" f "[[:space:]]*:" { gsub(/[ "'"'"']/, "", $2); print $2; exit }' "$2"
}

# Prints "<files> <provenance files>" for one artifact uuid
count_artifact() {
  printf '%s %s\n' "$(find "data/$1" -type f 2>/dev/null | wc -l)" \
                   "$(find "data/$1/provenance" -type f 2>/dev/null | wc -l)"
}

# One line per key: key <TAB> type <TAB> data|pool <TAB> uuid-or-pool-name
for k in keys/*; do
  k=${k#keys/}
  type=$(printf '%s' "$k" | sed -E 's/_([SED]RR[0-9]+|NA|[0-9]+)$//; s/_(group|batch|partition)$//')
  u=$(key_field data "keys/$k")
  if [ -n "$u" ]; then
    printf '%s\t%s\tdata\t%s\n' "$k" "$type" "$u"
  else
    printf '%s\t%s\tpool\t%s\n' "$k" "$type" "$(key_field pool "keys/$k")"
  fi
done > "$TMP/keys.tsv"

# Artifacts that a key or pool refers to, versus everything in data/
{
  awk -F'\t' '$3 == "data" { print $4 }' "$TMP/keys.tsv"
  for p in pools/*/; do [ -d "$p" ] && ls "$p"; done
} | sort -u > "$TMP/referenced.txt"
ls data | sort -u > "$TMP/all_artifacts.txt"
comm -23 "$TMP/all_artifacts.txt" "$TMP/referenced.txt" > "$TMP/unreferenced.txt"

# Sample a few keys per type. A pool key counts all artifacts in its pool.
: > "$TMP/samples.tsv"
cut -f2 "$TMP/keys.tsv" | sort -u | while IFS= read -r type; do
  awk -F'\t' -v t="$type" '$2 == t' "$TMP/keys.tsv" | head -n "$SAMPLES" \
  | while IFS=$'\t' read -r k _t kind ref; do
      if [ "$kind" = "data" ]; then
        read -r files prov <<< "$(count_artifact "$ref")"
      else
        files=0; prov=0
        for u in $(ls "pools/$ref" 2>/dev/null); do
          read -r f p <<< "$(count_artifact "$u")"
          files=$((files + f)); prov=$((prov + p))
        done
      fi
      printf '%s\t%s\t%s\t%s\t%s\n' "$type" "$k" "$kind" "$files" "$prov"
    done >> "$TMP/samples.tsv"
done

# Spot-check of unreferenced artifacts
head -n 5 "$TMP/unreferenced.txt" | while read -r u; do
  printf '%s\t%s\n' "$u" "$(count_artifact "$u")"
done > "$TMP/unreferenced_samples.tsv"

FMT='function c(n,  x, o) { x = sprintf("%d", n); o = ""
  while (length(x) > 3) { o = "," substr(x, length(x) - 2) o; x = substr(x, 1, length(x) - 3) }
  return x o }'

{
  echo "QIIME 2 CACHE FILE COUNTS: $CACHE_DIR"
  echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
  echo
  echo "Keys                 : $(wc -l < "$TMP/keys.tsv" | tr -d ' ')"
  echo "Artifacts in data/   : $(wc -l < "$TMP/all_artifacts.txt" | tr -d ' ')"
  echo "Unreferenced artifacts: $(wc -l < "$TMP/unreferenced.txt" | tr -d ' ')  (no key or pool points to them)"
  echo "Pools                : $(ls pools 2>/dev/null | wc -l | tr -d ' ')"
  echo "processes/ entries   : $(ls processes 2>/dev/null | wc -l | tr -d ' ')"
  echo

  echo "== ARTIFACT TYPES, BY ESTIMATED FILES =="
  echo "Estimated total = keys x mean files of the sampled keys ($SAMPLES per type)."
  awk -F'\t' "$FMT"'
    FNR == NR { nkeys[$2]++; next }
    { n[$1]++; f[$1] += $4; p[$1] += $5 }
    END {
      for (t in n) {
        mean = f[t] / n[t]
        printf "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", nkeys[t] * mean, c(nkeys[t] * mean), c(nkeys[t]), c(mean),
          (f[t] > 0 ? sprintf("%.0f%%", 100 * p[t] / f[t]) : "-"), t, ""
      }
    }' "$TMP/keys.tsv" "$TMP/samples.tsv" \
  | sort -t$'\t' -k1,1nr \
  | awk -F'\t' 'BEGIN { printf "%14s  %6s  %12s  %10s  %s\n", "est. files", "keys", "files/key", "provenance", "type" }
                { printf "%14s  %6s  %12s  %10s  %s\n", $2, $3, $4, $5, $6 }'
  awk -F'\t' "$FMT"'
    FNR == NR { nkeys[$2]++; next }
    { n[$1]++; f[$1] += $4 }
    END { for (t in n) s += nkeys[t] * f[t] / n[t]; printf "Estimated total in keyed artifacts: %s files\n", c(s) }' \
    "$TMP/keys.tsv" "$TMP/samples.tsv"
  echo

  echo "== SAMPLED KEYS =="
  awk -F'\t' "$FMT"'
    BEGIN { printf "%12s  %12s  %s\n", "files", "provenance", "key" }
    { printf "%12s  %12s  %s%s\n", c($4), c($5), $2, ($3 == "pool" ? " (pool)" : "") }' "$TMP/samples.tsv"
  echo

  echo "== UNREFERENCED ARTIFACTS (first 5) =="
  if [ -s "$TMP/unreferenced_samples.tsv" ]; then
    awk -F'\t' "$FMT"'
      BEGIN { printf "%12s  %12s  %s\n", "files", "provenance", "uuid" }
      { split($2, a, " "); printf "%12s  %12s  %s\n", c(a[1]), c(a[2]), $1 }' "$TMP/unreferenced_samples.tsv"
    awk -F'\t' -v total="$(wc -l < "$TMP/unreferenced.txt")" "$FMT"'
      { split($2, a, " "); f += a[1]; n++ }
      END { printf "Estimated files in all unreferenced artifacts: %s (from the mean of these %d)\n", c(total * f / n), n }' \
      "$TMP/unreferenced_samples.tsv"
  else
    echo "None"
  fi
  echo
  echo "Full lists: $TMP"
} > "$REPORT"

cat "$REPORT"
