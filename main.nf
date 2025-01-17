#!/usr/bin/env nextflow

// Using DSL-2
nextflow.enable.dsl=2

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
process GETPANELDESIGN{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.pdf"
    )

    input:
    path(tables_collected)
    
    output:
    path 'panel_design.csv', emit: paneldesignfile

    script:
    template 'analyze_panel_design.py'

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
    
        panel = GETPANELDESIGN(inputTables.collect())
        // panel.paneldesignfile.view()
        
        sup = supervised_wf(inputTables.collect())
        

    
    }
    
    
}


