// Assumption: File contians a single column with multiple +/- labels, matching a single Maker column.
process GET_SINGLE_MARKER_TRAINING_DF {
    input:
    path(tables_collected)
    
    output:
    path 'training_*.tsv', emit: trainingdata

    script:
    template 'generate_training_sets.py'
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
    template 'fit_models.py'
}

process PREDICTIONS_FROM_BEST_MODEL{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.pdf",
        mode: "copy"
    )
    publishDir(
        path: "${params.output_dir}/classifications/",
        pattern: "*_PRED.tsv",
        mode: "copy"
    )

    input:
    path(best_model)
    path(original_df)
    
    output: 
    path("*_PRED.tsv"), emit: model
    
    script:
    template 'best_model_predictions.py'
}



workflow supervised_wf {
	take: 
    tablesOfQuantification
	
	main:
	trainingMk = GET_SINGLE_MARKER_TRAINING_DF(tablesOfQuantification)
	//trainingMk.view()
	
	fitting = BINARY_MODEL_TRAINING(trainingMk.flatMap { it })
	//fitting.view()
	
	pairs = fitting.combine(tablesOfQuantification.flatMap { it })
    predict = PREDICTIONS_FROM_BEST_MODEL(pairs.map { it[0] }, pairs.map { it[1] })

	
}
