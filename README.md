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
## End-to-end workflow logic

1. **Input discovery and gatekeeping (`main.nf`)**
   - The pipeline reads batch directories from `--input_dir` using `Channel.fromPath("${params.input_dir}/*/")`.
   - It then computes global label counts (`ALL_LABEL_COUNTS`) and explicitly fails if the total count is zero (`CHECK_LABEL_COUNTS`).

2. **Label preparation (`main.nf`)**
   - It recomputes a per-label table (`GET_ALL_LABEL_RECOUNTS`).
   - It applies heuristic negative-label relabeling to each quantification table (`BOOST_NEGATIVE_LABELS`).

3. **Optional normalization (`main.nf`)**
   - If `params.use_boxcox_transformation` is true, each modified table is transformed by `BOXCOX_TRANSFORM`.

4. **Modeling and reporting sub-workflow (`modules/fit_new_models.nf`)**
   - Training sets are generated from relabeled/normalized tables (`GET_SINGLE_MARKER_TRAINING_DF`).
   - Marker training files are grouped and merged by marker (`MERGE_TRAINING_BY_MARKER`).
   - Binary models are trained (`BINARY_MODEL_TRAINING`).
   - Each trained model is paired with each input table, then predictions are made (`PREDICTIONS_FROM_BEST_MODEL`).
   - Predictions are grouped per image and merged (`MERGE_BY_PRED_IMAGE`).
   - Per-image PNG/HTML reports are produced (`REPORT_PER_IMAGE`).

5. **Output layout**
   - Reports: `${output_dir}/reports/` and `${output_dir}/per_image_reports/<image_id>/`
   - Merged prediction tables: `${output_dir}/merged/`
   - Normalization PDFs: `${output_dir}/normalization_reports/`
