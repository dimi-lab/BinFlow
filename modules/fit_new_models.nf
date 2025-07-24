// Assumption: File contians a single column with multiple +/- labels, matching a single Maker column.
process GET_SINGLE_MARKER_TRAINING_DF {
    input:
    path(tables_collected)
    
    output:
    path 'training_*.tsv', emit: trainingdata, optional: true

    script:
    """
    generate_training_sets.py ${params.singleLabelColumn} "|" ${tables_collected}
    """
}

// Accept any panel design, assume all input files have common markers
process BINARY_MODEL_TRAINING{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.pdf",
        mode: "copy"
    )

    input:
    path(training_df)
    
    output: 
    path("*best_model*.pkl"), emit: model, optional: true
    
    script:
    """
    fit_models.py ${training_df}
    """
}

process PREDICTIONS_FROM_BEST_MODEL{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.pdf",
        mode: "copy"
    )
    
    input:
    path(best_model)
    path(original_df)
    
    output: 
    tuple val(original_df.baseName), path("*_PRED.tsv"), emit: classifications
    
    script:
    """
    best_model_predictions.py ${best_model} ${original_df}
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

    script:
    """
    merge_preds.py ${image_id} ${pred_files}
    """
}

process MERGE_TRAINING_BY_MARKER {
    input:
    tuple val(mark), path(training_files)

    output:
    path("${mark}_all.tsv"), emit: merged

    script:
    """
    merge_training.py ${mark}_all.tsv
    """
}

process REPORT_PER_IMAGE {
    publishDir(
        path: "${params.output_dir}/per_image_reports/${image_id}",
        mode: "copy"
    )

    input:
    tuple val(image_id), path(merged_file)

    output:
    path("*.png"), emit: plots
    path("*.html"), emit: html

    script:
    """
    generate_reports_per_image.py ${merged_file} ${image_id}
    """
}

workflow supervised_wf {
	take: 
    tablesOfQuantification
	
	main:
	trainingMk = GET_SINGLE_MARKER_TRAINING_DF(tablesOfQuantification.flatten())
	//trainingMk.flatMap{ it }.view()
	
    // Group training files by their basename (e.g., training_CD68.tsv)
    grouped_training = trainingMk
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
	fitting = BINARY_MODEL_TRAINING(merged_training)
	//fitting.view()
	
	pairs = fitting.combine(tablesOfQuantification.flatMap { it })
    //pairs.view()
    // pairs is a tuple of (best_model, original_df)
    predict = PREDICTIONS_FROM_BEST_MODEL(pairs.map { it[0] }, pairs.map { it[1] })

    // Group predictions by image_id (the first value in the tuple)
    merged_input = predict.groupTuple().map { id, files -> tuple(id, files instanceof List ? files : [files]) } //.distinct()
	//merged_input.view()

    merged = MERGE_BY_PRED_IMAGE(merged_input)
	//merged.view()
    
    report = REPORT_PER_IMAGE(merged)
}


