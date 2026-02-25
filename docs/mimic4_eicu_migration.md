# MIMIC-IV and eICU Migration Plan (Minimal Rewrite)

## Goal

Keep your current backend/frontend code and ML pipeline mostly unchanged by generating a **compatibility dataset** with the same schema as:

- `data/mimic-iii-clinical-database-demo-1.4/full_medical_data_clean.csv`

This lets current code continue to work while moving to stronger datasets.

## Phase 1: Keep Current System Stable

1. Continue validating reliability on current dataset.
2. Keep API contracts stable (`/api/patients`, `/api/summary`, `/api/alerts/live`).
3. Ensure alerting service and dashboard are production-like.

## Phase 2: Rebuild Training/Serving Data from MIMIC-IV

Use:

- `ml/build_mimic4_compatible_dataset.py`
- `ml/mimic4_itemids.example.json`

Command:

```bash
python ml/build_mimic4_compatible_dataset.py ^
  --mimic4-root "D:\\datasets\\mimic-iv-2.2" ^
  --mapping-json "ml/mimic4_itemids.example.json" ^
  --output-csv "data/mimic-iii-clinical-database-demo-1.4/full_medical_data_clean.csv"
```

Then rerun existing training flow:

```bash
python ml/build_ml_dataset_v2.py
python ml/train_risk_model_v2.py
```

Then restart backend and validate.

## Phase 3: Validation on MIMIC-IV

Minimum checks:

1. Class balance in `future_risk`.
2. Calibration by risk band.
3. Alert precision/recall at clinical thresholds.
4. Drift in feature ranges vs previous dataset.

## Phase 4: External Validation on eICU-CRD

Approach:

1. Build an `eicu -> compatible schema` adapter (same output columns).
2. Keep feature engineering and model inference unchanged.
3. Report external performance without refitting first (true generalization test).
4. Optionally refit on mixed MIMIC-IV + eICU and compare.

## Operational Recommendation

For real-hospital positioning:

1. Internal validation (MIMIC-IV split).
2. External validation (eICU).
3. Monitoring + retraining policy.
4. Human-in-the-loop and escalation SOPs documented.

