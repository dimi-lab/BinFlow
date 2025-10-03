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
nextflow run main.nf --input_dir <input_folder> --output_dir <output_folder> -work-dir <temp_folder> -profile -slrum
```

See `nextflow.config` for parameter customization.

## Output
- Reports, merged data, and predictions are saved in the specified output directory.

For more details, see comments in `main.nf` and `modules/fit_new_models.nf`.
