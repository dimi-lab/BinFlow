// Assumption: File contians a single column with multiple +/- labels, matching a single Maker column.
process GET_SINGLE_MARKER_TRAINING_DF {
    input:
    path(tables_collected)
    
    output:
    path 'training_*.tsv', emit: trainingdata

    script:
    template 'generate_training_sets.py'
}


workflow supervised_wf {
	take: 
    tablesOfQuantification
	
	main:
	trainingMk = GET_SINGLE_MARKER_TRAINING_DF(tablesOfQuantification)
	trainingMk.view()
	
}
