#!/bin/bash
#SBATCH --job-name=cache-cleanup
#SBATCH --time=5-00:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=4G
#SBATCH --output=log_files/%x_%j.out
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=schaererm@ethz.ch
#
# Remove the per-sample contig abundance tables ("<runId>_contigs_ft_partitioned_<sample>") from the main
# QIIME 2 cache with QIIME 2's own tools, and delete their Nextflow storeDir markers. Each table holds
# ~49k files, almost all provenance copied from the collated contig lengths.
#
# Keys are removed one by one with "qiime tools cache-remove". QIIME 2 locks the cache while it changes
# it, and its garbage collection deletes only artifacts that no key or pool still refers to. A key's
# storeDir marker is deleted only after QIIME 2 has removed that key. A final
# "qiime tools cache-garbage-collection" makes sure everything the removed keys pointed to is gone.
#
# Usage: mkdir -p log_files && sbatch monitoring_scripts/cache_cleanup.sbatch   # dry run: plan only
#        sbatch monitoring_scripts/cache_cleanup.sbatch --run                    # remove
# The dry run is quick and can also be run on a login node: bash cache_cleanup.sbatch
# Paths can be changed with: sbatch --export=ALL,OUTPUT_DIR=...,RUN_ID=... monitoring_scripts/cache_cleanup.sbatch
# Submit from outside /cluster/work: the report goes to the submit directory, and the quota there is full.

eval "$(conda shell.bash hook)"
conda activate rachis-qiime2-2026.7

OUTPUT_DIR="${OUTPUT_DIR:-/cluster/work/bokulich/schaererm/industrialization2/output}"
RUN_ID="${RUN_ID:-industrialization2}"
CACHE="$OUTPUT_DIR/caches/main"
MARKERS="$OUTPUT_DIR/keys"
PREFIX="${RUN_ID}_contigs_ft_partitioned_"

DO_RUN=0
for a in "$@"; do
  case "$a" in
    --run) DO_RUN=1 ;;
    *) echo "Unknown argument: $a (the only option is --run)" >&2; exit 1 ;;
  esac
done

OUT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
JOB="${SLURM_JOB_ID:-local}"
WORK="$OUT_DIR/cache_cleanup_${JOB}.parts"
REPORT="$OUT_DIR/cache_cleanup_${JOB}.txt"

case "$OUT_DIR" in
  /cluster/work/*)
    echo "Submit from outside /cluster/work (the quota there is full): $OUT_DIR" >&2
    exit 1 ;;
esac
if ! command -v qiime > /dev/null || ! qiime tools cache-remove --help > /dev/null 2>&1; then
  echo "qiime (with 'tools cache-remove') not found: did 'conda activate rachis-qiime2-2026.7' work?" >&2
  exit 1
fi
if [ ! -d "$CACHE/keys" ] || [ ! -d "$CACHE/data" ]; then
  echo "Not a QIIME 2 cache (no keys/ or data/): $CACHE" >&2
  exit 1
fi
mkdir -p "$WORK" || exit 1
exec > >(tee "$REPORT") 2>&1
shopt -s nullglob

files_quota() {
  command -v lquota > /dev/null || { echo "lquota not available on this node"; return; }
  lquota "$(dirname "$(dirname "$OUTPUT_DIR")")" 2>&1 | grep -E 'files|Quota type'
}
n_artifacts() { ls "$CACHE/data" | wc -l | tr -d ' '; }

echo "CACHE CLEANUP: $([ $DO_RUN = 1 ] && echo REMOVING || echo 'DRY RUN (nothing is removed; add --run)')"
echo "Started : $(date '+%Y-%m-%d %H:%M:%S')"
echo "QIIME 2 : $(command -v qiime) (conda env: ${CONDA_DEFAULT_ENV:-?})"
echo "Cache   : $CACHE"
echo "Markers : $MARKERS"
echo "Keys    : ${PREFIX}*"
echo

# 1. Nothing may write to the cache while keys are removed
busy=$(squeue -h -u "$USER" -o '%i %j %T' 2>/dev/null | awk -v me="$JOB" '$1 != me && $2 ~ /^nf-/')
if [ -n "$busy" ]; then
  echo "Nextflow jobs are still queued or running ($(echo "$busy" | wc -l | tr -d ' ')), for example:"
  echo "$busy" | head -5
  if [ $DO_RUN = 1 ]; then
    echo "Stopping without removing anything. Cancel them first (scancel) and resubmit."
    exit 1
  fi
  echo "(Dry run continues, but --run would stop here.)"
  echo
fi

# 2. Plan: the cache keys with this prefix, and the storeDir markers with this prefix
(cd "$CACHE/keys" && for k in "$PREFIX"*; do echo "$k"; done) > "$WORK/keys.txt"
(cd "$MARKERS" && for m in "$PREFIX"*; do echo "$m"; done) > "$WORK/markers.txt"
comm -13 "$WORK/keys.txt" "$WORK/markers.txt" > "$WORK/markers_without_key.txt"

echo "== PLAN =="
echo "Cache keys to remove                     : $(wc -l < "$WORK/keys.txt" | tr -d ' ')"
echo "  with a storeDir marker                 : $(comm -12 "$WORK/keys.txt" "$WORK/markers.txt" | wc -l | tr -d ' ')"
echo "storeDir markers with no cache key       : $(wc -l < "$WORK/markers_without_key.txt" | tr -d ' ') (also deleted)"
echo "Artifacts in the cache now               : $(n_artifacts)"
echo "Lists: $WORK/keys.txt and $WORK/markers.txt"
echo
echo "Files quota before:"
files_quota
echo

if [ $DO_RUN = 0 ]; then
  echo "Dry run: nothing removed."
  exit 0
fi

# 3. Remove each key with QIIME 2; delete its marker only once QIIME 2 has removed the key
echo "== REMOVING =="
: > "$WORK/removed.txt"; : > "$WORK/failed.txt"
total=$(wc -l < "$WORK/keys.txt" | tr -d ' ')
i=0
while IFS= read -r k; do
  i=$((i + 1))
  if qiime tools cache-remove --cache "$CACHE" --key "$k" >> "$WORK/qiime.log" 2>&1 \
     && [ ! -e "$CACHE/keys/$k" ]; then
    rm -f "$MARKERS/$k"
    echo "$k" >> "$WORK/removed.txt"
  else
    echo "$k" >> "$WORK/failed.txt"
    echo "FAILED: $k (marker kept; details in $WORK/qiime.log)"
  fi
  [ $((i % 10)) -eq 0 ] && echo "$(date '+%H:%M:%S') $i of $total keys processed"
done < "$WORK/keys.txt"

# Markers whose table never reached the cache: nothing to remove in QIIME 2
while IFS= read -r m; do
  rm -f "$MARKERS/$m"
done < "$WORK/markers_without_key.txt"

# 4. Final garbage collection: deletes any artifact that no key or pool refers to any more
echo "$(date '+%H:%M:%S') running qiime tools cache-garbage-collection"
if ! qiime tools cache-garbage-collection --cache "$CACHE" >> "$WORK/qiime.log" 2>&1; then
  echo "Garbage collection failed (details in $WORK/qiime.log)"
fi

echo
echo "== RESULT =="
echo "Keys removed                 : $(wc -l < "$WORK/removed.txt" | tr -d ' ') of $total"
echo "Keys failed (markers kept)   : $(wc -l < "$WORK/failed.txt" | tr -d ' ')"
echo "Markers without key deleted  : $(wc -l < "$WORK/markers_without_key.txt" | tr -d ' ')"
echo "Artifacts in the cache now   : $(n_artifacts)"
echo
echo "Files quota after:"
files_quota
echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
[ -s "$WORK/failed.txt" ] && exit 1
exit 0
