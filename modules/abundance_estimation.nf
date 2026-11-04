process INDEX_DEREP_MAGS {
    label "indexing"
    storeDir params.storeDir
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }
    maxRetries 3

    input:
    path(mags_derep_file)
    path q2Cache

    output:
    path(key)

    script:
    if (params.binning.qc.busco.enabled) {
      key = "${params.runId}_mags_${params.binning.primary}_derep_index_${params.binning.qc.busco.selectLineage}"
    } else {
      key = "${params.runId}_mags_${params.binning.primary}_derep_index"
    }
    """
    qiime assembly index-derep-mags \
      --verbose \
      --p-seed 42 \
      --p-threads ${task.cpus} \
      --i-mags ${params.q2cacheDir}:${mags_derep_file} \
      --o-index ${params.q2cacheDir}:${key} \
    && touch ${key}
    """
}

process MAP_READS_TO_DEREP_MAGS {
    label "readMapping"
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }
    maxRetries 4
    storeDir params.storeDir
    tag "${_id}"

    input:
    tuple val(_id), path(reads_file), path(index_file)
    path q2Cache

    output:
    tuple val(_id), path(key)

    script:
    q2cacheDir = "${params.q2TemporaryCachesDir}/${_id}"
    if (params.binning.qc.busco.enabled) {
      key = "${params.runId}_reads_to_derep_mags_${params.binning.primary}_partitioned_${params.binning.qc.busco.selectLineage}_${_id}"
    } else {
      key = "${params.runId}_reads_to_derep_mags_${params.binning.primary}_partitioned_${_id}"
    }
    """
    echo Processing sample ${_id}
    qiime assembly map-reads \
      --verbose \
      --p-seed 42 \
      --p-threads ${task.cpus} \
      --i-index ${params.q2cacheDir}:${index_file} \
      --i-reads ${q2cacheDir}:${reads_file} \
      --o-alignment-maps "${q2cacheDir}:${key}" \
    && touch ${key}
    """
}

process GET_GENOME_LENGTHS {
    cpus 1
    memory { 16.GB * Math.pow(2, task.attempt - 1) }
    time { 4.h * task.attempt }
    maxRetries 3
    storeDir params.storeDir
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }

    input:
    path mags_derep_file
    path q2Cache
    val input_type

    output:
    path key

    script:
    if (input_type == "mags_derep") {
      if (params.binning.qc.busco.enabled) {
        key = "${params.runId}_mags_${params.binning.primary}_derep_lengths_${params.binning.qc.busco.selectLineage}"
      } else {
        key = "${params.runId}_mags_${params.binning.primary}_derep_lengths"
      }
    } else if (params.binning.qc.busco.enabled) {
      key = "${params.runId}_${input_type}_lengths_${params.binning.qc.busco.selectLineage}"
    } else {
      key = "${params.runId}_${input_type}_lengths"
    }
    """
    qiime mag get-feature-lengths \
      --verbose \
      --i-features ${params.q2cacheDir}:${mags_derep_file} \
      --o-lengths ${params.q2cacheDir}:${key} \
    && touch ${key}
    """
}

process ESTIMATE_ABUNDANCE {
    label "abundanceEstimation"
    storeDir params.storeDir
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }
    maxRetries 3

    input:
    path mags_derep_index_file
    path mags_derep_lengths_file
    val input_type
    path q2Cache

    output:
    path key

    script:
    if (input_type == "mags_derep") {
      if (params.binning.qc.busco.enabled) {
        key = "${params.runId}_mags_${params.binning.primary}_derep_ft_${params.binning.qc.busco.selectLineage}"
      } else {
        key = "${params.runId}_mags_${params.binning.primary}_derep_ft"
      }
    } else {
      key = "${params.runId}_${input_type}_ft"
    }
    """
    qiime mag estimate-abundance \
      --verbose \
      --p-metric ${params.abundance_estimation.metric} \
      --p-min-mapq ${params.abundance_estimation.min_mapq} \
      --p-min-query-len ${params.abundance_estimation.min_query_len} \
      --p-min-base-quality ${params.abundance_estimation.min_base_quality} \
      --p-threads ${task.cpus} \
      --i-alignment-maps ${params.q2cacheDir}:${mags_derep_index_file} \
      --i-feature-lengths ${params.q2cacheDir}:${mags_derep_lengths_file} \
      --o-abundances ${params.q2cacheDir}:${key} \
    && touch ${key}
    """
}

// estimate-abundance pivots to a dense samples x contigs table, which cannot fit in memory across
// all samples. Each contig belongs to one sample and tpm/rpkm normalise per sample, so running one
// sample per job gives the same values.
process ESTIMATE_SAMPLE_ABUNDANCE {
    label "abundanceEstimation"
    tag "${sample_id}"
    storeDir params.storeDir
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }
    maxRetries 3

    input:
    tuple val(sample_id), path(alignment_maps)
    path lengths_file
    path q2Cache

    output:
    path key

    script:
    key = "${params.runId}_contigs_ft_partitioned_${sample_id}"
    """
    qiime mag estimate-abundance \
      --verbose \
      --p-metric ${params.abundance_estimation.metric} \
      --p-min-mapq ${params.abundance_estimation.min_mapq} \
      --p-min-query-len ${params.abundance_estimation.min_query_len} \
      --p-min-base-quality ${params.abundance_estimation.min_base_quality} \
      --p-threads ${task.cpus} \
      --i-alignment-maps ${params.q2TemporaryCachesDir}/${sample_id}:${alignment_maps} \
      --i-feature-lengths ${params.q2cacheDir}:${lengths_file} \
      --o-abundances ${params.q2cacheDir}:${key} \
    && touch ${key}
    """
}

process MERGE_FEATURE_TABLES {
    cpus 1
    // biom pads every input table to all features, so cost grows with tables x total features:
    // merge in groups first rather than passing hundreds of tables with distinct features at once
    memory { [128.GB * params.mem_intensive_task_multiplier * Math.pow(2, task.attempt - 1), 1900.GB].min() }
    time { 8.h * task.attempt }
    maxRetries 3
    storeDir params.storeDir
    errorStrategy = { Retry.withDelay('retry', params.retryDelaySeconds) }

    input:
    val tables
    val key

    output:
    path "${key}"

    script:
    def inputFlags = tables.collect { "--i-tables ${params.q2cacheDir}:${new File(it.toString()).getName()}" }.sort().join(' ')
    """
    qiime feature-table merge \
      --verbose \
      ${inputFlags} \
      --o-merged-table "${params.q2cacheDir}:${key}" \
    && touch ${key}
    """
}
