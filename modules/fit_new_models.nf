// Assumption: File contians a single column with multiple +/- labels, matching a single Maker column.
process GET_SINGLE_MARKER_TRAINING_DF {
    input:
    path(tables_collected)
    
    output:
    path 'training_*.tsv', emit: trainingdata, optional: true
    path('training_generation_report.html'), emit: html_report

    script:
    """
    generate_training_sets.py ${params.singleLabelColumn} "|" ${tables_collected}
    build_html_report.py --title "Generate single marker training" --output training_generation_report.html --inputs ${tables_collected} training_*.tsv
    """
}

// Accept any panel design, assume all input files have common markers
process BINARY_MODEL_TRAINING{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.html",
        mode: "copy"
    )

    input:
    path(training_df)
    
    output: 
    path("*best_model*.pkl"), emit: model, optional: true
    path("model_training_report.html"), emit: html_report
    
    script:
    """
    fit_models.py ${training_df}
    build_html_report.py --title "Binary model training" --output model_training_report.html --inputs ${training_df} *best_model*.pkl
    """
}

process PREDICTIONS_FROM_BEST_MODEL{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.html",
        mode: "copy"
    )
    
    input:
    path(best_model)
    path(original_df)
    
    output: 
    tuple val(original_df.baseName), path("*_PRED.tsv"), emit: classifications
    path("prediction_report.html"), emit: html_report
    
    script:
    """
    best_model_predictions.py ${best_model} ${original_df}
    build_html_report.py --title "Predictions from best model" --output prediction_report.html --inputs ${best_model} ${original_df} *_PRED.tsv
    """
}

process MERGE_BY_PRED_IMAGE {
    publishDir(
        path: "${params.output_dir}/merged/",
        pattern: "*_MERGED.tsv",
        mode: "copy"
    )

    input:
    tuple val(image_id), path(pred_files)

    output:
    tuple val(image_id), path("*_MERGED.tsv"), emit: merged
    path("*_merge_report.html"), emit: html_report

    script:
    """
    merge_preds.py ${image_id} ${pred_files}
    out=$(ls *_MERGED.tsv | head -n1)
    build_html_report.py --title "Merge predictions by image" --output merge_report.html --inputs ${pred_files} $out
    mv merge_report.html ${image_id}_merge_report.html
    """
}

process MERGE_TRAINING_BY_MARKER {
    input:
    tuple val(mark), path(training_files)

    output:
    path("*_all.tsv"), emit: merged
    path("training_merge_report.html"), emit: html_report

    script:
    """
    merge_training.py "${mark}_all.tsv"
    build_html_report.py --title "Merge training by marker" --output training_merge_report.html --inputs ${training_files} ${mark}_all.tsv
    """
}

process REPORT_PER_IMAGE {
    publishDir(
        path: "${params.output_dir}/per_image_reports/${image_id}",
        mode: "copy"
    )

    input:
    tuple val(image_id), path(merged_file)
    path(quant_file_inputDir)

    output:
    path("*.png"), emit: plots
    path("*.html"), emit: html
    path("image_step_report.html"), emit: step_report

    script:
    """
    generate_reports_per_image.py ${merged_file} ${image_id} ${quant_file_inputDir}
    build_html_report.py --title "Per-image report generation" --output image_step_report.html --inputs ${merged_file} ${quant_file_inputDir} *.png *.html
    """
}

workflow supervised_wf {
	take: 
    tablesOfQuantification
    input_dir
	
	main:
	trainingMk = GET_SINGLE_MARKER_TRAINING_DF(tablesOfQuantification.flatten())
	//trainingMk.flatMap{ it }.view()
	
    // Group training files by their basename (e.g., training_CD68.tsv)
    grouped_training = trainingMk.trainingdata
    .flatMap{ it }
    .map { file -> 
        def marker = file.baseName
        def extension = file.extension ? ".${file.extension}" : "" // Handle files without extension
        if (marker =~ /_([a-zA-Z0-9]{8})$/) {
            def newBaseName = marker[0..<(marker.length() - 9)] // -9 to remove '_' and 8 chars
            return tuple(newBaseName, file)
        } else {
            // If the suffix pattern is not found, return the original file
            return file
        }
    }
    .groupTuple()

    merged_training = MERGE_TRAINING_BY_MARKER(grouped_training)
	fitting = BINARY_MODEL_TRAINING(merged_training.merged)
	//fitting.view()
	
	pairs = fitting.model.combine(tablesOfQuantification.flatMap { it })
    //pairs.view()
    // pairs is a tuple of (best_model, original_df)
    predict = PREDICTIONS_FROM_BEST_MODEL(pairs.map { it[0] }, pairs.map { it[1] })

    // Group predictions by image_id (the first value in the tuple)
    merged_input = predict.classifications.groupTuple().map { id, files -> tuple(id, files instanceof List ? files : [files]) } //.distinct()
	//merged_input.view()

    merged = MERGE_BY_PRED_IMAGE(merged_input)
	//merged.view()
    
    report = REPORT_PER_IMAGE(merged.merged, input_dir)

    emit:
    merged_tables = merged.merged
    prediction_tables = predict.classifications
}


