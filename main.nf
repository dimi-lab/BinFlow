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
include { marker_recovery_wf } from './modules/marker_recovery'

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
process REPORT_PANEL_DESIGN {
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.html"
    )
    input:
    path(tables_collected)

    output:
    path("*_panel_design_report.html"), emit: html_report

    script:
    """
    analyze_panel_design.py ${params.letterhead} ${tables_collected}
    build_html_report.py --title "Panel design report" --output panel_design_report.html --inputs ${tables_collected}
    mv panel_design_report.html ${tables_collected.baseName}_panel_design_report.html
    """
}

process ALL_LABEL_COUNTS{
    input:
    path(tables_collected)
    
    output: 
    path("label_counts.tsv"), emit: count
    path("label_counts_report.html"), emit: html_report
    
    script:
    """
    binary_counter.py label_counts.tsv ${params.singleLabelColumn} ${tables_collected}
    build_html_report.py --title "All label counts" --output label_counts_report.html --inputs label_counts.tsv ${tables_collected}
    """
}

process BOOST_NEGATIVE_LABELS{
    input:
    path(quant_table)
    path(counts_tsv)
    
    output: 
    path("*_mod.tsv"), emit: quant_files
    path("*_boost_report.html"), emit: html_report
    
    script:
    """
    relabel_synthetic_negatives.py \
      ${quant_table} \
      ${counts_tsv} \
      ${params.huerustic_negative_n_cells} \
      ${params.huerustic_negative_percentile} \
      ${params.huerustic_negative_add_only_missing} \
      ${params.singleLabelColumn} \
      "${params.keptContextColumns.join(',')}"
    out=$(ls *_mod.tsv | head -n1)
    build_html_report.py --title "Boost negative labels" --output boost_report.html --inputs ${quant_table} ${counts_tsv} $out
    mv boost_report.html ${quant_table.baseName}_boost_report.html
    """
}

process GET_ALL_LABEL_RECOUNTS{
    publishDir(
        path: "${params.output_dir}/reports/",
        pattern: "*.html"
    )
    
    input:
    path(tables_collected)

    output: 
    path("*_table.tsv"), emit: count
    path("recount_report.html"), emit: html_report
        
    script:
    """
    binary_table.py perlabel_table.tsv ${params.singleLabelColumn} ${tables_collected}
    build_html_report.py --title "Per-label recount" --output recount_report.html --inputs perlabel_table.tsv ${tables_collected}
    """
}

// Produce Batch based normalization - boxcox
process BOXCOX_TRANSFORM {
    publishDir(
        path: "${params.output_dir}/normalization_reports",
        pattern: "*.html",
        mode: "copy"
    )
    input:
    path(quant_table)

    output:
    path("*.tsv"), emit: quant_files
    path("boxcox_*.html"), emit: html_report

    script:
    """
    boxcox_transformer.py \
      ${quant_table} \
      ${params.qupath_object_type} \
      ${params.nucleus_marker} \
      ${params.transformation_group_by_column} \
      ${params.letterhead} \
      ${params.hasFOV}
    out_tsv=$(ls *.tsv | head -n1)
    build_html_report.py --title "BoxCox transform" --output boxcox_report.html --inputs ${quant_table} $out_tsv
    mv boxcox_report.html boxcox_${quant_table.baseName}.html
    """
}

process CHECK_LABEL_COUNTS {
    input:
    path(label_counts)

    output:
    val(true), emit: labels_ok
    path("label_check_report.html"), emit: html_report

    script:
    """
    total=\$(awk 'NR>1 {sum+=\$2} END {print sum+0}' ${label_counts})
    if [[ \$total -eq 0 ]]; then
        echo 'ERROR: No labels found in any input file. Exiting workflow.'
        exit 99
    fi
    echo true
    cat > label_check_report.html <<EOF
    <html><body><h1>Label count check</h1><p>Total labels: $total</p><p>Status: PASS</p></body></html>
    EOF
    """
}


process PREPROCESS_QUANT_TABLE {
    publishDir(
        path: "${params.output_dir}/preprocessing_reports",
        pattern: "*.html",
        mode: "copy"
    )

    input:
    path(quant_table)

    output:
    path("*_preprocessed.tsv"), emit: quant_files
    path("*_gmm_summary.csv"), emit: gmm_summary
    path("*_gmm_summary.png"), emit: gmm_plot
    path("*_preprocess_report.html"), emit: html_report

    script:
    def base = quant_table.baseName
    """
    preprocess_quant_table.py \
      ${quant_table} \
      --output-table ${base}_preprocessed.tsv \
      --summary-csv ${base}_gmm_summary.csv \
      --summary-plot ${base}_gmm_summary.png \
      --seed ${params.preprocessing_seed} \
      ${params.run_gmmgating ? '--run-gmmgating' : ''} \
      ${params.run_powertransform ? '--run-powertransform' : ''}
    build_html_report.py --title "Preprocess quant table" --output preprocess_report.html --inputs ${base}_preprocessed.tsv ${base}_gmm_summary.csv ${base}_gmm_summary.png ${quant_table}
    mv preprocess_report.html ${base}_preprocess_report.html
    """
}


process RECOMBINE_PREDICTIONS_WITH_CONTEXT {
    publishDir(
        path: "${params.output_dir}/final_merged_predictions",
        mode: "copy"
    )

    input:
    path(merged_file)
    path(context_tables)

    output:
    path("*_FINAL.tsv"), emit: merged_with_context
    path("*_final_recombine_report.html"), emit: html_report

    script:
    def base = merged_file.baseName.replace('_MERGED','')
    """
    recombine_predictions_with_context.py       ${merged_file}       --context-columns "${params.keptContextColumns.join(',')}"       --output ${base}_FINAL.tsv       ${context_tables}

    build_html_report.py       --title "Final merged predictions with context"       --output ${base}_final_recombine_report.html       --inputs ${merged_file} ${base}_FINAL.tsv ${context_tables}
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
        
        //REPORT_PANEL_DESIGN(inputTables)
        recount = GET_ALL_LABEL_RECOUNTS(inputTables.collect())
        modTables = BOOST_NEGATIVE_LABELS(inputTables, recount.count)
        modTables = modTables.flatten()
        //modTables.view()

        quantForPreprocess = modTables
        if (params.use_boxcox_transformation) {
            boxcoxTables = BOXCOX_TRANSFORM(modTables)
            quantForPreprocess = boxcoxTables.quant_files
        }

        preprocessedTables = PREPROCESS_QUANT_TABLE(quantForPreprocess)

        marker_recovery_wf(preprocessedTables.quant_files.collect())
        supervised_out = supervised_wf(preprocessedTables.quant_files, params.input_dir)

        RECOMBINE_PREDICTIONS_WITH_CONTEXT(supervised_out.merged_tables, preprocessedTables.quant_files.collect())
    }
    
}


