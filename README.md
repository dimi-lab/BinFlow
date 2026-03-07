# BinFlow: Automated Batch Analysis Pipeline

BinFlow is a Nextflow-based workflow for automated batch processing and analysis of multiplexed imaging data. It supports label counting, normalization, model training, prediction, and report generation.

## Main Features
- Batch input directory support
- Label counting and recounting
- Negative label boosting
- Optional BoxCox normalization
- Marker-specific model training and prediction
- Merged and per-image report outputs

## Usage
Run the workflow with:

```
nextflow run main.nf --input_dir <input_folder> --output_dir <output_folder> -work-dir <temp_folder> -profile slurm
```

See `nextflow.config` for parameter customization.

## Output
- Reports, merged data, and predictions are saved in the specified output directory.
- Final per-image merged prediction tables that recombine marker predictions and include configured context columns are written to `<output_dir>/final_merged_predictions/`.

For more details, see comments in `main.nf` and `modules/fit_new_models.nf`.


## Marker recovery module
- Reporting outputs across preprocessing, normalization, modeling, and marker recovery now include intermediate HTML summaries with counts/tables/checks/figures.
- Upstream optional preprocessing now runs before all downstream analyses: GMM gating (`run_gmmgating`) followed by optional Yeo-Johnson power transform (`run_powertransform`) in `main.nf`, so both marker recovery and supervised modeling consume the same transformed tables.
- The workflow now includes a marker-focused recovery analysis module that generates QC plots, clustering diagnostics, supervised model comparisons, and per-file spatial visualizations under `<output_dir>/marker_recovery/`.
- Configure all marker recovery behavior through `nextflow.config` (`marker_recovery_*` and `exclude_component_patterns`).
