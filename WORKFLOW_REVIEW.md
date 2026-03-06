# BinFlow workflow logic and potential weaknesses

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

## Potential weakness points

1. **Parameter overriding bug risk in `main.nf`**
   - `params.input_dir` and `params.output_dir` are assigned inside `main.nf`, which can override user-provided CLI/config values depending on evaluation order.
   - This is fragile and can surprise users if custom values are ignored.

2. **Missing local profile config reference**
   - `nextflow.config` defines `local` profile with `includeConfig 'conf/local.config'`, but `conf/local.config` is absent.
   - Running with `-profile local` can fail immediately.

3. **Potential channel shape mismatch in prediction pairing**
   - `fitting.combine(tablesOfQuantification.flatMap { it })` can create broad Cartesian-style pairings if not carefully constrained.
   - Depending on number of models/tables, this can explode compute and produce logically invalid model-to-table pairs.

4. **Non-deterministic file grouping/name cleanup**
   - Grouping logic strips an 8-char hash suffix only if basename matches `/_([a-zA-Z0-9]{8})$/`.
   - If upstream naming differs, grouping may be inconsistent (mixture of tuple/file return paths), leading to fragile behavior.

5. **Optional outputs may hide silent failures**
   - Several outputs are marked `optional: true` (`GET_SINGLE_MARKER_TRAINING_DF`, `BINARY_MODEL_TRAINING`).
   - This prevents early hard failures and can cause downstream steps to run with partial/no data.

6. **Hard-coded schema assumptions**
   - Defaults like `singleLabelColumn = "Classification"`, context columns with special spacing/Unicode (`" Centroid X µm"`), and `nucleus_marker = "NA2"` assume stable export schema.
   - Any slight naming drift in source TSVs can break scripts.

7. **Typo-prone heuristic parameter names**
   - Parameter names use `huerustic_*` spelling consistently; this works internally but increases operator mistake risk when overriding values.

8. **Dependency/environment coupling**
   - Processes call Python scripts directly (no explicit conda/container declaration in workflow files).
   - Reproducibility depends on external environment setup, reducing portability.

9. **README command typo**
   - Usage example shows `-profile -slrum` (typo/order issue), which may confuse users and reduce successful first runs.

## Suggested hardening priorities

1. Move default param assignments out of `main.nf` and keep them solely in `nextflow.config`.
2. Add `conf/local.config` or remove/adjust the `local` profile reference.
3. Validate channel cardinality before `combine` to prevent combinatorial blow-up.
4. Add explicit schema validation step for required columns before modeling.
5. Add process-level software environment declarations (conda/container) and pinned versions.
