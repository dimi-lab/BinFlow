#!/usr/bin/env nextflow

// Using DSL-2
nextflow.enable.dsl=2
println "Active profile: ${workflow.profile}"


// All of the default parameters are being set in `nextflow.config`
params.input_dir = "${projectDir}/data"
params.output_dir = "${projectDir}/output"

// Import sub-workflows
// include { nimbus_wf } from './modules/nimbus_wrapper'
include { supervised_wf } from './modules/fit_new_models'

//Static Assests for beautification
params.letterhead = "${projectDir}/images/BinFlow_banner.PNG"

// Build Input List of Batches
Channel.fromPath("${params.input_dir}/*/", type: 'file')
			.ifEmpty { error "No files found in ${params.input_dir}" }
			.set { inputTables }
			

// -------------------------------------- //
// Function which prints help message text
def helpMessage() {
    log.info"""
Usage:

nextflow run main.nf <ARGUMENTS>

Required Arguments:

  Input Data:
  --input_dir        Folder containing subfolders of QuPath's Quantification Exported Measurements,
                        each dir containing Quant files belonging to a common batch of images.
""".stripIndent()
}

// Accept any panel design, assume all input files have common markers
process REPORT_PANEL_DESIGN{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.pdf"
    )

    input:
    path(tables_collected)
    
    script:
    template 'analyze_panel_design.py'

}

process ALL_LABEL_COUNTS{
    input:
    path(tables_collected)
    
    output: 
    path("label_counts.tsv"), emit: count
    
    script:
    template 'binary_counter.py'
}

process BOOST_NEGATIVE_LABELS{
    input:
    path(quant_table)
    path(counts_tsv)
    
    output: 
    path("*_mod.tsv"), emit: quant_files
    
    script:
    template 'relabel_synthetic_negatives.py'
}

process GET_ALL_LABEL_RECOUNTS{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*_table.tsv"
    )
    
    input:
    path(tables_collected)

    output: 
    path("*_table.tsv"), emit: count
        
    script:
    template 'binary_table.py'
}

// Produce Batch based normalization - boxcox
process BOXCOX_TRANSFORM {
	publishDir(
        path: "${params.output_dir}/normalization_reports",
        pattern: "*.pdf",
        mode: "copy"
    )
	
	input:
	path(quant_table)
	
	output:
    path("*_mod.tsv"), emit: quant_files
	path("boxcox_*.pdf")
	
	script:
	template 'boxcox_transformer.py'

}

process CHECK_LABEL_COUNTS {
    input:
    path(label_counts)

    output:
    val(true), emit: labels_ok

    script:
    """
    total=\$(awk 'NR>1 {sum+=\$2} END {print sum+0}' ${label_counts})
    if [[ \$total -eq 0 ]]; then
        echo 'ERROR: No labels found in any input file. Exiting workflow.'
        exit 99
    fi
    echo true
    """
}

// Main workflow
workflow {
    // Show help message if the user specifies the --help flag at runtime
    // or if any required params are not provided
    if ( params.help || params.input_dir == false ){
        // Invoke the function above which prints the help message
        helpMessage()
        // Exit out and do not run anything else
        exit 1
    } else {
        label_summary = ALL_LABEL_COUNTS(inputTables.collect())
        CHECK_LABEL_COUNTS(label_summary.count) // This will exit if no labels are found
        
        REPORT_PANEL_DESIGN(inputTables.collect())
        recount = GET_ALL_LABEL_RECOUNTS(inputTables.collect())
        modTables = BOOST_NEGATIVE_LABELS(inputTables, recount.count)
        
        if (params.use_boxcox_transformation) {
        	modTables = BOXCOX_TRANSFORM(modTables.quant_files)
        }
        
        sup = supervised_wf(modTables.quant_files.collect())
    }
    
    
}


