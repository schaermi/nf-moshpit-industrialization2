#!/bin/bash
#SBATCH --job-name=inode-usage
#SBATCH --time=5-00:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --output=log_files/%x_%j.out
#SBATCH --signal=B:USR1@900
#
# Report where the file-count (inode) quota of the group's work share is being used: quota status,
# your directories (subtotals plus the biggest subfolders), then the other top-level group folders.
# Your own directories are scanned first. If the time limit approaches, a report marked PARTIAL is
# written from whatever has finished.
#
# Usage: mkdir -p log_files && sbatch monitoring_scripts/inode_usage.sbatch [MY_DIR] [GROUP_DIR]
# Submit from outside /cluster/work: results go to the submit directory, and the quota there is full.
# Writes inode_usage_<jobid>.txt to the sbatch submit directory (and prints it to the job log).
#
# Counts are of files as seen by each du process, so a file hard-linked into two scanned folders is
# counted twice here but only once by the quota.

MY_DIR="${1:-/cluster/work/bokulich/schaererm}"
GROUP_DIR="${2:-/cluster/work/bokulich}"
MY_DIR="${MY_DIR%/}"
GROUP_DIR="${GROUP_DIR%/}"
# Used only if lquota is not available on the compute node
DEFAULT_SOFT=46250000
DEFAULT_HARD=50875000

CPUS="${SLURM_CPUS_PER_TASK:-4}"
OUT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
JOB="${SLURM_JOB_ID:-local}"
REPORT="$OUT_DIR/inode_usage_${JOB}.txt"
WORK="$OUT_DIR/inode_usage_${JOB}.parts"

case "$OUT_DIR" in
  /cluster/work/*)
    echo "Submit from outside /cluster/work (the quota there is full): $OUT_DIR" >&2
    exit 1 ;;
esac
if [ ! -d "$MY_DIR" ] || [ ! -d "$GROUP_DIR" ]; then
  echo "Directory not found: $MY_DIR or $GROUP_DIR" >&2
  exit 1
fi
mkdir -p "$WORK" || exit 1

MY_TSV="$WORK/my_dirs.tsv"
DIRECT_TSV="$WORK/my_direct_files.tsv"
GROUP_TSV="$WORK/group_dirs.tsv"
ERRORS="$WORK/errors.txt"
: > "$MY_TSV"; : > "$DIRECT_TSV"; : > "$GROUP_TSV"; : > "$ERRORS"

STATUS="RUNNING"
START=$(date '+%Y-%m-%d %H:%M:%S')
START_S=$(date +%s)

read_quota() {
  # Prints "used soft hard" for the files quota, from a table row: | path | files | used | soft | hard |
  lquota "$GROUP_DIR" 2>/dev/null | awk -F'|' '$3 ~ /files/ {
    gsub(/ /, "", $4); gsub(/ /, "", $5); gsub(/ /, "", $6); print $4, $5, $6; exit }'
}

# NUL-separated directories on stdin; each is scanned by its own du, CPUS at a time
scan() {
  xargs -0 -n 1 -P "$CPUS" sh -c 'du --inodes '"$1"' "$3" >> "$1" 2>> "$2"' sh "$2" "$ERRORS"
}

render() {
  local line used soft hard
  line=$(read_quota)
  if [ -n "$line" ]; then
    read -r used soft hard <<< "$line"
  else
    used=0; soft=$DEFAULT_SOFT; hard=$DEFAULT_HARD
  fi

  {
    echo "INODE USAGE REPORT: $STATUS"
    echo "Started   : $START"
    echo "Updated   : $(date '+%Y-%m-%d %H:%M:%S') ($(( ($(date +%s) - START_S) / 60 )) min elapsed)"
    echo "Scanned   : $MY_DIR, then the rest of $GROUP_DIR"
    echo

    echo "== QUOTA (lquota, as of the update time) =="
    if [ -n "$line" ]; then
      lquota "$GROUP_DIR" 2>&1
    else
      echo "lquota unavailable on this node: using the soft and hard limits set in the script"
    fi
    awk -v u="$used" -v s="$soft" -v h="$hard" '
      function c(n,  x, o) { x = sprintf("%d", n); o = ""
        while (length(x) > 3) { o = "," substr(x, length(x) - 2) o; x = substr(x, 1, length(x) - 3) }
        return x o }
      BEGIN {
        if (u == 0) exit
        printf "%-34s %s\n", "Left before the hard limit:", c(h - u)
        printf "%-34s %s\n", "To delete to get below soft limit:", (u > s ? c(u - s) : "0 (already below)")
      }'
    echo

    awk -F'\t' -v root="$MY_DIR" -v used="$used" -v rollup="$WORK/rollup.tsv" -v deep="$WORK/deep.tsv" '
      function add(rel, n,  d, p, i, key) {
        total += n
        if (rel == "") return
        d = split(rel, p, "/")
        key = ""
        for (i = 1; i <= d; i++) { key = (i == 1 ? p[1] : key "/" p[i]); sum[key] += n }
      }
      {
        rel = (length($2) > length(root) ? substr($2, length(root) + 2) : "")
        if (FILENAME == ARGV[2]) { add(rel, $1); next }
        if (split(rel, q, "/") == 3) add(rel, $1)
        else print $1 "\t" rel > deep
      }
      END {
        printf "" > deep
        for (k in sum) print sum[k] "\t" k > rollup
        print total + 0
      }' "$MY_TSV" "$DIRECT_TSV" > "$WORK/my_total.txt"
    my_total=$(cat "$WORK/my_total.txt")

    format='function c(n,  x, o) { x = sprintf("%d", n); o = ""
        while (length(x) > 3) { o = "," substr(x, length(x) - 2) o; x = substr(x, 1, length(x) - 3) }
        return x o }
      BEGIN { FS = "\t"; printf "%14s  %7s  %s\n", "files", "% used", "path" }
      { pct = (t > 0 ? sprintf("%.1f%%", 100 * $1 / t) : "-"); printf "%14s  %7s  %s\n", c($1), pct, $2 }'
    denom=$used
    [ "$denom" -gt 0 ] || denom=$my_total

    echo "== YOUR DIRECTORIES: $MY_DIR ($(awk -v n="$my_total" 'BEGIN { printf "%.2fM", n / 1e6 }') files so far) =="
    echo "Subtotals down to 3 levels, largest first. Parents include their children, so do not add rows up."
    sort -t$'\t' -k1,1nr "$WORK/rollup.tsv" 2>/dev/null | head -30 | awk -v t="$denom" "$format"
    echo

    echo "== BIGGEST INDIVIDUAL SUBFOLDERS (4 levels down) =="
    sort -t$'\t' -k1,1nr "$WORK/deep.tsv" 2>/dev/null | head -40 | awk -v t="$denom" "$format"
    echo

    echo "== OTHER GROUP FOLDERS: $GROUP_DIR =="
    if [ -s "$GROUP_TSV" ]; then
      awk -F'\t' -v root="$GROUP_DIR/" -v me="$my_total" -v my="${MY_DIR#"$GROUP_DIR"/}" '
        BEGIN { print me "\t" my " (your directories)" }
        { p = $2; sub("^" root, "", p); print $1 "\t" p }' "$GROUP_TSV" \
        | sort -t$'\t' -k1,1nr | awk -v t="$denom" "$format"
    else
      echo "Not scanned yet"
    fi
    echo

    n_err=$(wc -l < "$ERRORS" | tr -d ' ')
    echo "Scan errors: $n_err (usually permission denied; folders behind them are undercounted)"
    if [ "$n_err" -gt 0 ]; then echo "See: $ERRORS"; fi
  } > "$REPORT.tmp"
  mv "$REPORT.tmp" "$REPORT"
}

on_time_limit() {
  STATUS="PARTIAL (time limit reached)"
  kill $(jobs -p) 2>/dev/null
  render
  cat "$REPORT"
  exit 0
}
trap on_time_limit USR1

# Scans run in the background so that the time-limit signal is handled straight away.
# Depth 3 keeps work/<xx> and each QIIME 2 cache as separate units, so they can scan in parallel.
{
  find "$MY_DIR" -maxdepth 2 -type d | while IFS= read -r d; do
    printf '%s\t%s\n' "$(find "$d" -mindepth 1 -maxdepth 1 ! -type d | wc -l | tr -d ' ')" "$d"
  done > "$DIRECT_TSV"
} &
wait $!

find "$MY_DIR" -mindepth 3 -maxdepth 3 -type d -print0 2>> "$ERRORS" | scan "-d 1" "$MY_TSV" &
wait $!
render

find "$GROUP_DIR" -mindepth 1 -maxdepth 1 -type d ! -path "$MY_DIR" -print0 2>> "$ERRORS" \
  | scan "-s" "$GROUP_TSV" &
wait $!

STATUS="COMPLETE"
render
cat "$REPORT"
