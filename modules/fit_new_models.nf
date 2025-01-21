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
        pattern: "*.pdf"
    )

    input:
    path(training_df)
    
    output: 
    path("*_model.pkl"), emit: model
    
    script:
    template 'fit_models.py'

}



workflow supervised_wf {
	take: 
    tablesOfQuantification
	
	main:
	trainingMk = GET_SINGLE_MARKER_TRAINING_DF(tablesOfQuantification)
	trainingMk.view()
	
	fitting = BINARY_MODEL_TRAINING(trainingMk.flatMap { it })
	fitting.view()
	
}
