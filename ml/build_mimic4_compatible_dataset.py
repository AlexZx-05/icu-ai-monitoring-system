import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


EXPECTED_COLUMNS = [
    "subject_id",
    "hadm_id",
    "charttime",
    "heart_rate",
    "bp_mean",
    "spo2",
    "temp",
    "creatinine",
    "lactate",
    "wbc",
    "spo2_missing",
    "temp_missing",
    "creatinine_missing",
    "lactate_missing",
    "wbc_missing",
]


def resolve_table_path(root: Path, relative_path: str) -> Path:
    direct = root / relative_path
    if direct.exists():
        return direct
    gz_path = Path(str(direct) + ".gz")
    if gz_path.exists():
        return gz_path
    raise FileNotFoundError(f"Cannot find {relative_path}(.gz) under {root}")


def build_item_to_feature(mapping: Dict[str, List[int]], temp_f_key: str | None = None) -> Dict[int, str]:
    item_to_feature: Dict[int, str] = {}
    for feature, ids in mapping.items():
        if feature == temp_f_key:
            continue
        for itemid in ids:
            item_to_feature[int(itemid)] = feature
    return item_to_feature


def read_filtered_events(
    path: Path,
    itemids: Iterable[int],
    feature_map: Dict[int, str],
    chunksize: int,
    temp_f_itemids: set[int] | None = None,
) -> pd.DataFrame:
    columns = ["subject_id", "hadm_id", "charttime", "itemid", "valuenum"]
    frames: List[pd.DataFrame] = []
    itemid_set = {int(x) for x in itemids}
    temp_f_itemids = temp_f_itemids or set()

    for chunk in pd.read_csv(path, usecols=columns, chunksize=chunksize):
        chunk = chunk[chunk["itemid"].isin(itemid_set)]
        chunk = chunk.dropna(subset=["subject_id", "hadm_id", "charttime", "valuenum"])
        if chunk.empty:
            continue

        chunk["subject_id"] = pd.to_numeric(chunk["subject_id"], errors="coerce")
        chunk["hadm_id"] = pd.to_numeric(chunk["hadm_id"], errors="coerce")
        chunk["valuenum"] = pd.to_numeric(chunk["valuenum"], errors="coerce")
        chunk = chunk.dropna(subset=["subject_id", "hadm_id", "valuenum"])
        if chunk.empty:
            continue

        chunk["subject_id"] = chunk["subject_id"].astype(int)
        chunk["hadm_id"] = chunk["hadm_id"].astype(int)
        chunk["charttime"] = pd.to_datetime(chunk["charttime"], errors="coerce")
        chunk = chunk.dropna(subset=["charttime"])
        if chunk.empty:
            continue

        f_mask = chunk["itemid"].isin(temp_f_itemids)
        chunk.loc[f_mask, "valuenum"] = (chunk.loc[f_mask, "valuenum"] - 32) * (5.0 / 9.0)
        chunk["feature"] = chunk["itemid"].map(feature_map)
        chunk = chunk.dropna(subset=["feature"])
        if chunk.empty:
            continue

        chunk["charttime"] = chunk["charttime"].dt.floor("h")
        grouped = (
            chunk.groupby(["subject_id", "hadm_id", "charttime", "feature"], as_index=False)["valuenum"]
            .median()
        )
        frames.append(grouped)

    if not frames:
        return pd.DataFrame(columns=["subject_id", "hadm_id", "charttime"])

    long_df = pd.concat(frames, ignore_index=True)
    long_df = (
        long_df.groupby(["subject_id", "hadm_id", "charttime", "feature"], as_index=False)["valuenum"]
        .median()
    )
    wide = (
        long_df.pivot_table(
            index=["subject_id", "hadm_id", "charttime"], columns="feature", values="valuenum", aggfunc="median"
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    return wide


def merge_asof_feature(base: pd.DataFrame, feature_df: pd.DataFrame, feature_col: str, tolerance: str) -> pd.DataFrame:
    if feature_col not in feature_df.columns:
        feature_df = feature_df.assign(**{feature_col: pd.NA})
    part = feature_df[["subject_id", "hadm_id", "charttime", feature_col]].sort_values(
        ["subject_id", "hadm_id", "charttime"]
    )
    return pd.merge_asof(
        base.sort_values(["subject_id", "hadm_id", "charttime"]),
        part,
        on="charttime",
        by=["subject_id", "hadm_id"],
        direction="nearest",
        tolerance=pd.Timedelta(tolerance),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build MIMIC-IV dataset compatible with full_medical_data_clean.csv schema."
    )
    parser.add_argument(
        "--mimic4-root",
        required=True,
        help="Path to MIMIC-IV root folder containing hosp/ and icu/ subfolders.",
    )
    parser.add_argument(
        "--mapping-json",
        default="ml/mimic4_itemids.example.json",
        help="JSON file with itemid mapping for vitals/labs.",
    )
    parser.add_argument(
        "--output-csv",
        default="data/mimic-iii-clinical-database-demo-1.4/full_medical_data_clean.csv",
        help="Output file path for compatible dataset.",
    )
    parser.add_argument("--chunksize", type=int, default=1_500_000, help="CSV chunk size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.mimic4_root)
    mapping_path = Path(args.mapping_json)
    output_csv = Path(args.output_csv)

    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    vital_map = mapping["vitals"]
    lab_map = mapping["labs"]
    vital_tol = int(mapping.get("merge_tolerance_hours", {}).get("vitals", 2))
    lab_tol = int(mapping.get("merge_tolerance_hours", {}).get("labs", 12))

    temp_f_ids = set(int(x) for x in vital_map.get("temp_f", []))
    vital_feature_map = build_item_to_feature(vital_map, temp_f_key="temp_f")
    if "temp_c" in vital_map:
        for iid in vital_map["temp_c"]:
            vital_feature_map[int(iid)] = "temp"
    if "temp_f" in vital_map:
        for iid in vital_map["temp_f"]:
            vital_feature_map[int(iid)] = "temp"

    for key in ["heart_rate", "bp_mean", "spo2"]:
        for iid in vital_map.get(key, []):
            vital_feature_map[int(iid)] = key

    lab_feature_map = build_item_to_feature(lab_map)

    chartevents_path = resolve_table_path(root, "icu/chartevents.csv")
    labevents_path = resolve_table_path(root, "hosp/labevents.csv")

    vital_itemids = sorted(vital_feature_map.keys())
    lab_itemids = sorted(lab_feature_map.keys())

    print(f"Reading vitals from: {chartevents_path}")
    vitals_wide = read_filtered_events(
        chartevents_path,
        vital_itemids,
        vital_feature_map,
        chunksize=args.chunksize,
        temp_f_itemids=temp_f_ids,
    )
    if "heart_rate" not in vitals_wide.columns:
        raise RuntimeError("No heart_rate extracted. Check --mapping-json for correct MIMIC-IV itemids.")

    print(f"Reading labs from: {labevents_path}")
    labs_wide = read_filtered_events(
        labevents_path,
        lab_itemids,
        lab_feature_map,
        chunksize=args.chunksize,
    )

    base = vitals_wide[["subject_id", "hadm_id", "charttime", "heart_rate"]].copy()
    for col in ["bp_mean", "spo2", "temp"]:
        base = merge_asof_feature(base, vitals_wide, col, f"{vital_tol}h")
    for col in ["creatinine", "lactate", "wbc"]:
        base = merge_asof_feature(base, labs_wide, col, f"{lab_tol}h")

    base = base.sort_values(["subject_id", "hadm_id", "charttime"]).reset_index(drop=True)

    for col in ["spo2", "temp", "creatinine", "lactate", "wbc"]:
        base[f"{col}_missing"] = base[col].isna().astype(int)

    # Keep same filling strategy used in the existing pipeline.
    for col in ["spo2", "temp", "creatinine", "lactate", "wbc"]:
        base[col] = base.groupby("subject_id")[col].ffill()
        base[col] = base.groupby("subject_id")[col].bfill()
        if base[col].isna().all():
            base[col] = 0.0
        else:
            base[col] = base[col].fillna(base[col].median())

    for col in ["heart_rate", "bp_mean", "spo2", "temp", "creatinine", "lactate", "wbc"]:
        base[col] = pd.to_numeric(base[col], errors="coerce")

    # Reorder and enforce exact compatible schema.
    for col in EXPECTED_COLUMNS:
        if col not in base.columns:
            base[col] = pd.NA
    compatible = base[EXPECTED_COLUMNS].copy()
    compatible = compatible.dropna(subset=["subject_id", "hadm_id", "charttime", "heart_rate"])
    compatible["subject_id"] = compatible["subject_id"].astype(int)
    compatible["hadm_id"] = compatible["hadm_id"].astype(int)
    compatible["charttime"] = pd.to_datetime(compatible["charttime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    compatible.to_csv(output_csv, index=False)
    print(f"Saved compatible dataset: {output_csv}")
    print(f"Rows: {len(compatible)} | Columns: {len(compatible.columns)}")


if __name__ == "__main__":
    main()
