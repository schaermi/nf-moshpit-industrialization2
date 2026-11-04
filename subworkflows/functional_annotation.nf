include { SEARCH_ORTHOLOGS_EGGNOG as SEARCH_ORTHOLOGS_MAGS_DEREP; SEARCH_ORTHOLOGS_EGGNOG as SEARCH_ORTHOLOGS_CONTIGS; SEARCH_ORTHOLOGS_EGGNOG as SEARCH_ORTHOLOGS_MAGS } from '../modules/functional_annotation'
include { ANNOTATE_EGGNOG as ANNOTATE_MAGS_DEREP; ANNOTATE_EGGNOG as ANNOTATE_CONTIGS; ANNOTATE_EGGNOG as ANNOTATE_MAGS } from '../modules/functional_annotation'
include { COLLATE_PARTITIONS as COLLATE_ANNOTATIONS_CONTIGS; COLLATE_PARTITIONS as COLLATE_ANNOTATIONS_MAGS; COLLATE_PARTITIONS_DEREP as COLLATE_ANNOTATIONS_MAGS_DEREP} from '../modules/data_prep'
include { COLLATE_PARTITIONS as COLLATE_HITS_CONTIGS; COLLATE_PARTITIONS as COLLATE_HITS_MAGS; COLLATE_PARTITIONS_DEREP as COLLATE_HITS_MAGS_DEREP } from '../modules/data_prep'
include { COLLATE_PARTITIONS as COLLATE_ANNOTATION_BATCHES } from '../modules/data_prep'
include { EXTRACT_ANNOTATIONS; MULTIPLY_TABLES; DRAW_ANNOTATION_BARPLOT } from '../modules/functional_annotation'
include { EXTRACT_ANNOTATIONS as EXTRACT_ANNOTATIONS_BATCH; MERGE_EXTRACTED_ANNOTATIONS; COMPARE_EXTRACTED_ANNOTATIONS } from '../modules/functional_annotation'
include { FETCH_ARTIFACT as FETCH_MULTIPLIED_TABLE } from '../modules/data_prep'
include { FETCH_ARTIFACT as FETCH_ARTIFACT_ORTHOLOGS } from '../modules/data_prep'
include { FETCH_ARTIFACT as FETCH_ARTIFACT_ANNOTATIONS } from '../modules/data_prep'
include { FETCH_ARTIFACT as FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS } from '../modules/data_prep'
include { FETCH_ARTIFACT as FETCH_ARTIFACT_EXTRACTED_ANNOTATION_MAP } from '../modules/data_prep'
include { FETCH_ARTIFACT as FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS_PER_CONTIG } from '../modules/data_prep'
include { CLEAN_UP_CACHES } from '../modules/data_prep'
include { FIX_CACHE_PERMISSIONS } from '../modules/data_prep'

workflow ANNOTATE_EGGNOG_MAGS {
    take:
        mags
        diamond_db
        eggnog_db
    main:
        SEARCH_ORTHOLOGS_MAGS(mags, diamond_db, "mags")
        ANNOTATE_MAGS(SEARCH_ORTHOLOGS_MAGS.out.hits, eggnog_db, "mags")

        if (params.binning.qc.busco.enabled) {
            orthologs_key = "${params.runId}_eggnog_orthologs_mags_${params.binning.primary}_${params.binning.qc.busco.selectLineage}"
            annotations_key = "${params.runId}_eggnog_annotations_mags_${params.binning.primary}_${params.binning.qc.busco.selectLineage}"
        } else {
            orthologs_key = "${params.runId}_eggnog_orthologs_mags_${params.binning.primary}"
            annotations_key = "${params.runId}_eggnog_annotations_mags_${params.binning.primary}"
        }

        collated_orthologs = COLLATE_HITS_MAGS(SEARCH_ORTHOLOGS_MAGS.out.hits | collect(flat: false), orthologs_key, "types collate-orthologs", "--i-orthologs", "--o-collated-orthologs", true)
        collated_annotations = COLLATE_ANNOTATIONS_MAGS(ANNOTATE_MAGS.out.annotations | collect(flat: false), annotations_key, "types collate-ortholog-annotations", "--i-ortholog-annotations", "--o-collated-annotations", true)

        if (params.functional_annotation.ortholog_search.fetchArtifact) {
            FETCH_ARTIFACT_ORTHOLOGS(collated_orthologs)
        }
        if (params.functional_annotation.annotation.fetchArtifact) {
            FETCH_ARTIFACT_ANNOTATIONS(collated_annotations)
        }
    emit:
        collated_orthologs
}

workflow ANNOTATE_EGGNOG_MAGS_DEREP {
    take:
        mags_derep
        diamond_db
        eggnog_db
        q2_cache
    main:
        mags_derep = mags_derep.map { partition ->
            def path = java.nio.file.Paths.get(partition.toString())
            def filename = path.getFileName().toString()
            def batchId = filename.substring("${params.runId}_mags_${params.binning.primary}_derep_partitioned_".length())
            tuple("batch_${batchId}", partition)
        }
        SEARCH_ORTHOLOGS_MAGS_DEREP(mags_derep, diamond_db, "mags_derep")
        ANNOTATE_MAGS_DEREP(SEARCH_ORTHOLOGS_MAGS_DEREP.out.hits, eggnog_db, "mags_derep")

        if (params.binning.qc.busco.enabled) {
            orthologs_key = "${params.runId}_eggnog_orthologs_mags_${params.binning.primary}_derep_${params.binning.qc.busco.selectLineage}"
            annotations_key = "${params.runId}_eggnog_annotations_mags_${params.binning.primary}_derep_${params.binning.qc.busco.selectLineage}"
        } else {
            orthologs_key = "${params.runId}_eggnog_orthologs_mags_${params.binning.primary}_derep"
            annotations_key = "${params.runId}_eggnog_annotations_mags_${params.binning.primary}_derep"
        }
        collated_orthologs = COLLATE_HITS_MAGS_DEREP(SEARCH_ORTHOLOGS_MAGS_DEREP.out.hits | collect(flat: false), orthologs_key, "types collate-orthologs", "--i-orthologs", "--o-collated-orthologs", true)
        collated_annotations = COLLATE_ANNOTATIONS_MAGS_DEREP(ANNOTATE_MAGS_DEREP.out.annotations | collect(flat: false), annotations_key, "types collate-ortholog-annotations", "--i-ortholog-annotations", "--o-collated-annotations", true)


        if (params.functional_annotation.cleanUp) {
            wait_for_collation = collated_orthologs.mix(collated_annotations).collect().map { true }
            CLEAN_UP_CACHES(wait_for_collation, "${params.q2TemporaryCachesDir}/mags")
        } else {
             // If not cleaning up, fix permissions on each batch's cache
             // We can recreate the batch IDs from params.functional_annotation.partitionBatchCount.
             
             batch_ids = Channel.of(0..(params.functional_annotation.partitionBatchCount - 1))
             mags_cache_batches = batch_ids.map { batch_id -> 
                tuple("mags_batch_${batch_id}", "${params.q2TemporaryCachesDir}/mags/batch_${batch_id}", collated_annotations) 
             }
             FIX_CACHE_PERMISSIONS(mags_cache_batches)
        }

        if (params.functional_annotation.annotation.extract != "") {
            annotation_type = Channel.of(params.functional_annotation.annotation.extract.types.split(","))
            EXTRACT_ANNOTATIONS(collated_annotations, annotation_type, "mags_derep", q2_cache)
            if (params.functional_annotation.annotation.extract.fetchArtifact) {
                FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS(EXTRACT_ANNOTATIONS.out.counts_per_genome | map { _type, key -> key })
                FETCH_ARTIFACT_EXTRACTED_ANNOTATION_MAP(EXTRACT_ANNOTATIONS.out.annotation_map | map { _type, key -> key })
                FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS_PER_CONTIG(EXTRACT_ANNOTATIONS.out.counts_per_contig | map { _type, key -> key })
            }
        }

        if (params.functional_annotation.ortholog_search.fetchArtifact) {
            FETCH_ARTIFACT_ORTHOLOGS(collated_orthologs)
        }
        if (params.functional_annotation.annotation.fetchArtifact) {
            FETCH_ARTIFACT_ANNOTATIONS(collated_annotations)
        }
    emit:
        extracted_annotations = EXTRACT_ANNOTATIONS.out.counts_per_genome
        annotation_map = EXTRACT_ANNOTATIONS.out.annotation_map
        counts_per_contig = EXTRACT_ANNOTATIONS.out.counts_per_contig
}

workflow COMBINE_ANNOTATION_ABUNDANCE {
    take:
        abundance_table
        annotation_counts
        input_type
        q2_cache
    main:
        qzv_outputs = Channel.empty()
        multiplied = MULTIPLY_TABLES(abundance_table, annotation_counts, input_type, q2_cache)
        DRAW_ANNOTATION_BARPLOT(multiplied, input_type)
        qzv_outputs = qzv_outputs.mix(DRAW_ANNOTATION_BARPLOT.out.qzv)
        if (params.functional_annotation.annotation.extract.fetchArtifact) {
            FETCH_MULTIPLIED_TABLE(multiplied | map { _type, key -> key })
        }
    emit:
        feature_table = multiplied
        qzv = qzv_outputs
}

workflow ANNOTATE_EGGNOG_CONTIGS {
    take:
        contigs
        diamond_db
        eggnog_db
        q2_cache
    main:
        SEARCH_ORTHOLOGS_CONTIGS(contigs, diamond_db, "contigs")
        ANNOTATE_CONTIGS(SEARCH_ORTHOLOGS_CONTIGS.out.hits, eggnog_db, "contigs")

        collated_orthologs = COLLATE_HITS_CONTIGS(SEARCH_ORTHOLOGS_CONTIGS.out.hits | collect(flat: false), "${params.runId}_eggnog_orthologs_contigs", "types collate-orthologs", "--i-orthologs", "--o-collated-orthologs", true)
        collated_annotations = COLLATE_ANNOTATIONS_CONTIGS(ANNOTATE_CONTIGS.out.annotations | collect(flat: false), "${params.runId}_eggnog_annotations_contigs", "types collate-ortholog-annotations", "--i-ortholog-annotations", "--o-collated-annotations", true)

        if (params.functional_annotation.annotation.extract != "") {
            def extract_params = params.functional_annotation.annotation.extract
            def annotation_types = extract_params.types.split(",").toList()
            // kegg_ko and ec have too many distinct values to build the dense contig x annotation
            // table in one go (OOM at 1.9 TB, job 19360090), so extract those per batch and merge
            def batched_types = extract_params.batchedTypes.split(",").toList().findAll { it in annotation_types }
            // validateBatched: also run this single-run type through batching and compare the results
            def validate_type = extract_params.validateBatched
            if (validate_type && (validate_type in batched_types || !(validate_type in annotation_types))) {
                error "extract.validateBatched must be listed in extract.types but not in extract.batchedTypes: ${validate_type}"
            }

            EXTRACT_ANNOTATIONS(collated_annotations, Channel.fromList(annotation_types - batched_types), "contigs", q2_cache)
            counts_per_genome = EXTRACT_ANNOTATIONS.out.counts_per_genome
            annotation_map = EXTRACT_ANNOTATIONS.out.annotation_map
            counts_per_contig = EXTRACT_ANNOTATIONS.out.counts_per_contig

            def types_to_batch = batched_types + (validate_type ? [validate_type] : [])
            if (types_to_batch) {
                annotation_batches = ANNOTATE_CONTIGS.out.annotations
                    | collect(flat: false)
                    | flatMap { items ->
                        def sorted_items = items.sort(false) { it[0].toString() }
                        sorted_items.collate(extract_params.batchSize).withIndex().collect { batch, batch_id -> tuple(batch_id, batch) }
                    }
                    | multiMap { item ->
                        id_and_paths: item[1]
                        key: "${params.runId}_eggnog_annotations_contigs_batch_${item[0]}"
                    }
                COLLATE_ANNOTATION_BATCHES(annotation_batches.id_and_paths, annotation_batches.key, "types collate-ortholog-annotations", "--i-ortholog-annotations", "--o-collated-annotations", true)

                batch_inputs = COLLATE_ANNOTATION_BATCHES.out
                    | combine(Channel.fromList(types_to_batch))
                    | multiMap { item ->
                        annotation_file: item[0]
                        annotation_type: item[1]
                        input_type: "contigs_batch_" + item[0].getName().tokenize("_").last()
                    }
                EXTRACT_ANNOTATIONS_BATCH(batch_inputs.annotation_file, batch_inputs.annotation_type, batch_inputs.input_type, q2_cache)

                merge_inputs = EXTRACT_ANNOTATIONS_BATCH.out.counts_per_genome.groupTuple()
                    | join(EXTRACT_ANNOTATIONS_BATCH.out.annotation_map.groupTuple())
                    | join(EXTRACT_ANNOTATIONS_BATCH.out.counts_per_contig.groupTuple())
                    | map { type, per_genome, per_map, per_contig ->
                        tuple(type, per_genome, per_map, per_contig, type in batched_types ? "" : "_batchcheck")
                    }
                MERGE_EXTRACTED_ANNOTATIONS(merge_inputs)

                def batched_only = { it[0] in batched_types }
                counts_per_genome = counts_per_genome.mix(MERGE_EXTRACTED_ANNOTATIONS.out.counts_per_genome.filter(batched_only))
                annotation_map = annotation_map.mix(MERGE_EXTRACTED_ANNOTATIONS.out.annotation_map.filter(batched_only))
                counts_per_contig = counts_per_contig.mix(MERGE_EXTRACTED_ANNOTATIONS.out.counts_per_contig.filter(batched_only))

                if (validate_type) {
                    def validated_only = { it[0] == validate_type }
                    comparison = EXTRACT_ANNOTATIONS.out.counts_per_genome.filter(validated_only)
                        | join(EXTRACT_ANNOTATIONS.out.annotation_map.filter(validated_only))
                        | join(EXTRACT_ANNOTATIONS.out.counts_per_contig.filter(validated_only))
                        | join(MERGE_EXTRACTED_ANNOTATIONS.out.counts_per_genome.filter(validated_only))
                        | join(MERGE_EXTRACTED_ANNOTATIONS.out.annotation_map.filter(validated_only))
                        | join(MERGE_EXTRACTED_ANNOTATIONS.out.counts_per_contig.filter(validated_only))
                    COMPARE_EXTRACTED_ANNOTATIONS(comparison)
                }
            }

            if (extract_params.fetchArtifact) {
                FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS(counts_per_genome | map { _type, key -> key })
                FETCH_ARTIFACT_EXTRACTED_ANNOTATION_MAP(annotation_map | map { _type, key -> key })
                FETCH_ARTIFACT_EXTRACTED_ANNOTATIONS_PER_CONTIG(counts_per_contig | map { _type, key -> key })
            }
        }

        if (params.functional_annotation.ortholog_search.fetchArtifact) {
            FETCH_ARTIFACT_ORTHOLOGS(collated_orthologs)
        }
        if (params.functional_annotation.annotation.fetchArtifact) {
            FETCH_ARTIFACT_ANNOTATIONS(collated_annotations)
        }
    emit:
        collated_orthologs
        counts_per_contig
        annotation_map
}
