#!/bin/bash
#SBATCH --job-name=check-fastp-empty
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --output=log_files/%x_%j.out
#
# For every PROCESS_READS_FASTP task that exited with 125 (ignored by the pipeline), check whether
# the fastp input in the sample's QIIME 2 cache is an empty "xxx_*" placeholder (i.e. the download
# silently produced nothing) or real reads.
#
# Usage: sbatch monitoring_scripts/check_fastp_empty_inputs.sbatch [NEXTFLOW_LOG] [CACHES_DIR] [RUN_ID]
# Prints a summary (placeholder vs. real, per read type) and writes the id list and a per-sample TSV to the sbatch submit directory.

NF_LOG="${1:-${SLURM_SUBMIT_DIR:-.}/.nextflow.log.1}"
CACHES_DIR="${2:-/cluster/work/bokulich/schaererm/industrialization2/output/caches}"
RUN_ID="${3:-industrialization2}"

OUT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
IDS_FILE="$OUT_DIR/fastp125_ids_${SLURM_JOB_ID:-local}.txt"
RESULT_TSV="$OUT_DIR/fastp125_inputs_${SLURM_JOB_ID:-local}.tsv"

if [ ! -f "$NF_LOG" ]; then
  echo "Nextflow log not found: $NF_LOG" >&2
  exit 1
fi
cd "$CACHES_DIR" || exit 1

grep -o 'PROCESS_READS_FASTP ([A-Z0-9]*)[^(]*(125)' "$NF_LOG" \
  | grep -o '([A-Z]*[0-9]*)' | tr -d '()' | sort -u > "$IDS_FILE"
echo "Samples with fastp exit 125: $(wc -l < "$IDS_FILE")"

while read -r id; do
  for t in paired single; do
    k="keys/${RUN_ID}_reads_${t}_${id}"
    [ -f "$id/$k" ] || continue
    u=$(awk -F':' '/^[[:space:]]*data[[:space:]]*:/ {gsub(/[ "'"'"']/,"",$2); print $2; exit}' "$id/$k")
    echo -e "${id}\t${t}\t$(ls "$id/data/$u/data" 2>/dev/null | head -1)"
  done
done < "$IDS_FILE" > "$RESULT_TSV"

echo "Per-sample results: $(realpath "$RESULT_TSV")"
echo
echo "count  class  read_type"
awk '{print ($3 ~ /^xxx_/ ? "placeholder" : "real"), $2}' "$RESULT_TSV" | sort | uniq -c
