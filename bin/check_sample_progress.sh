#!/bin/bash
#SBATCH --job-name=check_sample_progress
#SBATCH --output=check_sample_progress_%j.log
#SBATCH --time=4:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#
# check_sample_progress.sh - Report how many samples have passed each stage
# of the moshpit pipeline, by checking for the actual QIIME 2 cache key files
# on disk (not by parsing logs).
#
# Every stage in this pipeline writes its output as a QIIME 2 cache "key" at
#   <outputDir>/caches/<sample_id>/keys/<key_name>
# so "did sample X finish step Y" is answered by checking whether that key
# file exists.
#
# Usage:
#   sbatch check_sample_progress.sh <outputDir> <runId> [reportFile]
#   ./bin/check_sample_progress.sh <outputDir> <runId> [reportFile]
#
# Arguments:
#   outputDir   The pipeline's --outputDir (contains caches/, keys/, etc.)
#   runId       The --runId used for the run (cache keys are prefixed with it)
#   reportFile  Optional path for the text report
#               (default: <outputDir>/pipeline_info/sample_progress_<timestamp>.txt)
#
# Notes / limitations:
#   - Samples are auto-discovered as the subdirectories of <outputDir>/caches/
#     (excluding "main", the shared cache, and "mags", the dereplicated-MAG
#     batch cache, which are not per-sample).
#   - Steps that were disabled for this run (e.g. host removal, BUSCO,
#     classification) will simply show 0/N samples passed - that does not
#     necessarily mean anything failed.
#   - If params.retain.* was set to false, or params.archive was enabled,
#     intermediate cache keys may have been deleted/archived after being
#     consumed downstream. Archived samples (zipped under <outputDir>/archives)
#     will show as "missing" for every step here; restore them first with
#     bin/restore_cache.sh if you need to check their progress.

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <outputDir> <runId> [reportFile]" >&2
    exit 1
fi

OUTPUT_DIR="$1"
RUN_ID="$2"
CACHES_DIR="${OUTPUT_DIR}/caches"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT_FILE="${3:-${OUTPUT_DIR}/pipeline_info/sample_progress_${TIMESTAMP}.txt}"

if [ ! -d "$CACHES_DIR" ]; then
    echo "ERROR: Caches directory not found: $CACHES_DIR" >&2
    exit 1
fi

mkdir -p "$(dirname "$REPORT_FILE")"

# Discover samples: any per-sample cache dir under caches/, excluding the
# shared "main" cache and the "mags" (derep MAG batch) cache.
SAMPLES=()
for d in "$CACHES_DIR"/*/; do
    [ -d "$d" ] || continue
    sample="$(basename "$d")"
    [ "$sample" = "main" ] && continue
    [ "$sample" = "mags" ] && continue
    SAMPLES+=("$sample")
done

if [ ${#SAMPLES[@]} -eq 0 ]; then
    echo "ERROR: No per-sample cache directories found under $CACHES_DIR" >&2
    exit 1
fi

# Returns 0 (true) if any file in <dir>/keys matches any of the given glob
# patterns.
has_any() {
    local dir="$1"; shift
    local pattern
    for pattern in "$@"; do
        if compgen -G "${dir}/keys/${pattern}" > /dev/null 2>&1; then
            return 0
        fi
    done
    return 1
}

# Each step is "label:::pattern1|pattern2|...", patterns use SAMPLE as the
# placeholder for the sample id and are checked with/without a busco-lineage
# or binner token where the underlying module varies by config.
STEPS=(
    "Reads imported/simulated/fetched:::${RUN_ID}_reads_SAMPLE|${RUN_ID}_reads_partitioned_SAMPLE|${RUN_ID}_reads_paired_SAMPLE|${RUN_ID}_reads_single_SAMPLE"
    "Reads subsampled:::${RUN_ID}_reads_subsampled_*_SAMPLE"
    "Reads QC'd (fastp):::${RUN_ID}_reads_fastp_SAMPLE"
    "Host reads removed:::${RUN_ID}_reads_no_host_partitioned_SAMPLE"
    "Low-count samples filtered:::${RUN_ID}_reads_filtered_SAMPLE"
    "Reads classified (Kraken2):::${RUN_ID}_kraken_reports_reads_partitioned_SAMPLE"
    "Reads classified (Kaiju):::${RUN_ID}_kaiju_ft_reads_partitioned_SAMPLE"
    "Reads profiled (HUMAnN3):::${RUN_ID}_humann_gene_families_reads_partitioned_SAMPLE"
    "Contigs assembled:::${RUN_ID}_contigs_partitioned_SAMPLE"
    "Contigs length-filtered:::${RUN_ID}_contigs_filtered_partitioned_SAMPLE"
    "Contigs indexed:::${RUN_ID}_contigs_index_partitioned_SAMPLE"
    "Reads mapped to contigs:::${RUN_ID}_reads_to_contigs_partitioned_SAMPLE"
    "Contigs classified (Kraken2):::${RUN_ID}_kraken_reports_contigs_partitioned_SAMPLE"
    "Contigs classified (Kaiju):::${RUN_ID}_kaiju_ft_contigs_partitioned_SAMPLE"
    "Contigs annotated (eggNOG orthologs):::${RUN_ID}_eggnog_orthologs_contigs_partitioned_SAMPLE"
    "Contigs annotated (eggNOG annotations):::${RUN_ID}_eggnog_annotations_contigs_partitioned_SAMPLE"
    "Contigs binned into MAGs:::${RUN_ID}_mags_*_partitioned_SAMPLE"
    "MAGs evaluated (BUSCO):::${RUN_ID}_busco_results_*_partitioned_*_SAMPLE"
    "MAGs QC-filtered:::${RUN_ID}_mags_*_filtered_*_SAMPLE"
    "MAGs classified (Kraken2):::${RUN_ID}_kraken_reports_mags_*_partitioned_SAMPLE|${RUN_ID}_kraken_reports_mags_*_partitioned_*_SAMPLE"
    "MAGs annotated (eggNOG orthologs):::${RUN_ID}_eggnog_orthologs_mags_*_partitioned_SAMPLE|${RUN_ID}_eggnog_orthologs_mags_*_partitioned_*_SAMPLE"
    "MAGs annotated (eggNOG annotations):::${RUN_ID}_eggnog_annotations_mags_*_partitioned_SAMPLE|${RUN_ID}_eggnog_annotations_mags_*_partitioned_*_SAMPLE"
)

TOTAL=${#SAMPLES[@]}

{
    echo "======== Sample Progress Report ========"
    echo "Output dir : ${OUTPUT_DIR}"
    echo "Run ID     : ${RUN_ID}"
    echo "Generated  : $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Samples    : ${TOTAL} (auto-discovered from ${CACHES_DIR})"
    echo "=========================================="
    echo ""
    echo "--- Summary: samples passed per step ---"
} > "$REPORT_FILE"

# Plain indexed arrays only (no associative arrays), so this also runs under
# the older bash (3.2) shipped on macOS, not just the bash 4+ typically found
# on Linux clusters. MATRIX_ROWS is kept parallel to SAMPLES and grows one
# "\tX" / "\t." column per step as steps are processed.
STEP_LABELS=()
MATRIX_ROWS=("${SAMPLES[@]}")

for step in "${STEPS[@]}"; do
    label="${step%%:::*}"
    pattern_field="${step#*:::}"
    IFS='|' read -r -a raw_patterns <<< "$pattern_field"

    STEP_LABELS+=("$label")
    passed=0
    for i in "${!SAMPLES[@]}"; do
        sample="${SAMPLES[$i]}"
        patterns=()
        for p in "${raw_patterns[@]}"; do
            patterns+=("${p//SAMPLE/$sample}")
        done
        if has_any "${CACHES_DIR}/${sample}" "${patterns[@]}"; then
            passed=$((passed + 1))
            MATRIX_ROWS[$i]="${MATRIX_ROWS[$i]}	X"
        else
            MATRIX_ROWS[$i]="${MATRIX_ROWS[$i]}	."
        fi
    done
    printf "%-42s %4d / %-4d\n" "$label" "$passed" "$TOTAL" >> "$REPORT_FILE"
done

{
    echo ""
    echo "--- Per-sample matrix (X = key present, . = missing) ---"
    header="sample_id"
    for label in "${STEP_LABELS[@]}"; do
        header="${header}	${label}"
    done
    echo "$header"
    for row in "${MATRIX_ROWS[@]}"; do
        echo "$row"
    done
} >> "$REPORT_FILE"

echo "Report written to: $REPORT_FILE"
