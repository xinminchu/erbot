#!/usr/bin/env python3
"""
er_splink.py  --  Splink probabilistic record linkage backend for ERBOT.

Called by er_baseline_splink() in R/10-baselines.R via system2().

Usage:
  python er_splink.py \
    --data      /path/data.csv     \
    --output    /path/output.csv   \
    --id_col    uid                \
    --block_col surname            \
    --fields    givenname surname suburb postcode \
    --threshold 0.9

Output CSV columns: id1, id2, match_prob

Requirements:
  pip install splink>=4.0 duckdb pandas

Splink >= 4 API used here (DuckDBAPI backend).
"""

import argparse
import sys
import os
import warnings
warnings.filterwarnings("ignore")

def log(msg):
    print(f"[splink] {msg}", flush=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data",      required=True)
    p.add_argument("--output",    required=True)
    p.add_argument("--id_col",    required=True)
    p.add_argument("--block_col", required=True)
    p.add_argument("--fields",    nargs="+", required=True)
    p.add_argument("--threshold", type=float, default=0.9)
    args = p.parse_args()

    # ── Import splink ──────────────────────────────────────────────────────────
    try:
        import splink
        from splink import DuckDBAPI, Linker, SettingsCreator, block_on
        import splink.comparison_library as cl
        SPLINK_V4 = True
    except ImportError:
        try:
            import splink.duckdb.linker as splink_duck
            SPLINK_V4 = False
        except ImportError:
            print("[splink] ERROR: splink not installed. Run: pip install splink",
                  file=sys.stderr)
            sys.exit(1)

    import pandas as pd

    log(f"Loading data from {args.data}")
    df = pd.read_csv(args.data, dtype=str).fillna("")

    if args.id_col not in df.columns:
        print(f"[splink] ERROR: id_col '{args.id_col}' not found in data.",
              file=sys.stderr)
        sys.exit(1)

    # Rename id column to 'unique_id' (Splink convention)
    df = df.rename(columns={args.id_col: "unique_id"})
    block_col = args.block_col if args.block_col in df.columns else df.columns[1]

    log(f"n={len(df)} records, blocking on '{block_col}', "
        f"fields={args.fields}, threshold={args.threshold}")

    # ── Build comparisons ─────────────────────────────────────────────────────
    comparisons = []
    for f in args.fields:
        if f not in df.columns:
            continue
        try:
            comparisons.append(cl.JaroWinklerAtThresholds(f, [0.9, 0.8, 0.7]))
        except Exception:
            try:
                comparisons.append(cl.levenshtein_at_thresholds(f, [1, 2]))
            except Exception:
                comparisons.append(cl.exact_match(f))

    if not comparisons:
        print("[splink] ERROR: no valid comparison fields.", file=sys.stderr)
        sys.exit(1)

    # ── Run Splink (v4 API) ───────────────────────────────────────────────────
    if SPLINK_V4:
        db_api = DuckDBAPI()
        settings = SettingsCreator(
            link_type="dedupe_only",
            comparisons=comparisons,
            blocking_rules_to_generate_predictions=[
                block_on(block_col)
            ],
            retain_matching_columns=False,
            retain_intermediate_calculation_columns=False,
            max_iterations=10,
            em_convergence=0.001,
        )
        linker = Linker(df, settings, db_api=db_api)
    else:
        # Splink v3 fallback
        from splink.duckdb.linker import DuckDBLinker
        settings = {
            "link_type": "dedupe_only",
            "comparisons": comparisons,
            "blocking_rules_to_generate_predictions": [
                f"l.{block_col} = r.{block_col}"
            ],
        }
        linker = DuckDBLinker(df, settings)

    log("Running EM parameter estimation...")
    try:
        linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on(block_col) if SPLINK_V4 else f"l.{block_col} = r.{block_col}"
        )
    except Exception as e:
        log(f"EM training warning: {e} — proceeding with u-only estimates")

    log("Predicting match probabilities...")
    df_pred = linker.inference.predict(threshold_match_probability=args.threshold)
    df_out  = df_pred.as_pandas_dataframe() if SPLINK_V4 else df_pred.as_pandas_dataframe()

    # Standardise output columns
    id_cols_left  = [c for c in df_out.columns if "unique_id" in c and "_l" in c]
    id_cols_right = [c for c in df_out.columns if "unique_id" in c and "_r" in c]
    prob_col      = next((c for c in df_out.columns
                          if "match_probability" in c or "match_prob" in c), None)

    if not id_cols_left or not id_cols_right or prob_col is None:
        # Try common column name patterns
        id_cols_left  = ["unique_id_l"]
        id_cols_right = ["unique_id_r"]
        prob_col      = "match_probability"

    result = pd.DataFrame({
        "id1":        df_out[id_cols_left[0]].astype(str),
        "id2":        df_out[id_cols_right[0]].astype(str),
        "match_prob": df_out[prob_col].astype(float),
    })

    result = result[result["match_prob"] >= args.threshold]
    result.to_csv(args.output, index=False)
    log(f"Wrote {len(result)} predicted pairs (threshold={args.threshold}) "
        f"to {args.output}")

if __name__ == "__main__":
    main()
