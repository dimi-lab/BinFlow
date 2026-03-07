process MARKER_RECOVERY_ANALYSIS {
    publishDir(
        path: "${params.output_dir}/marker_recovery",
        mode: "copy"
    )

    input:
    path(quant_tables)

    output:
    path("marker_recovery_artifacts"), emit: artifacts

    script:
    def excludePatterns = (params.exclude_component_patterns ?: []).join(',')
    """
    mkdir -p marker_recovery_artifacts

    marker_recovery_pipeline.py \
      --marker ${params.marker_recovery_marker} \
      --classification-col ${params.marker_recovery_classification_col} \
      --output-dir marker_recovery_artifacts \
      --seed ${params.marker_recovery_seed} \
      --test-size ${params.marker_recovery_test_size} \
      --cv-splits ${params.marker_recovery_cv_splits} \
      --n-iter-search ${params.marker_recovery_n_iter_search} \
      --outlier-contamination ${params.marker_recovery_outlier_contamination} \
      --exclude-component-patterns "${excludePatterns}" \
      ${quant_tables}

    build_html_report.py       --title "Marker recovery workflow summary"       --output marker_recovery_artifacts/marker_recovery_summary.html       --inputs marker_recovery_artifacts/*.csv marker_recovery_artifacts/*.tsv marker_recovery_artifacts/*.png marker_recovery_artifacts/*.json
    """
}

workflow marker_recovery_wf {
    take:
    tables_of_quantification

    main:
    MARKER_RECOVERY_ANALYSIS(tables_of_quantification)
}
