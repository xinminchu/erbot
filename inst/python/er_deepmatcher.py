#!/usr/bin/env python3
"""
er_deepmatcher.py  --  DeepMatcher deep learning EM baseline for ERBOT.

Called by er_baseline_deepmatcher() in R/10-baselines.R via system2().

Input CSV columns:
  id1, id2, label,
  left_<field1>, ..., left_<fieldN>,
  right_<field1>, ..., right_<fieldN>

Output CSV columns: id1, id2, match_prob

Usage:
  python er_deepmatcher.py \
    --input     /path/pairs.csv  \
    --output    /path/output.csv \
    --fields    givenname surname suburb postcode \
    --model     hybrid           \
    --epochs    10               \
    --threshold 0.5

Requirements:
  pip install deepmatcher torch pandas scikit-learn

DeepMatcher models: hybrid (default), rnn, attention, sif.
Falls back to a Siamese MLP if deepmatcher is not installed.
"""

import argparse
import sys
import os
import warnings
warnings.filterwarnings("ignore")

def log(msg):
    print(f"[deepmatcher] {msg}", flush=True)

# ── Siamese MLP fallback (no deepmatcher needed) ─────────────────────────────
def run_siamese_mlp(df, fields, epochs, threshold):
    """Lightweight Siamese MLP using only sklearn + numpy."""
    import numpy as np
    from sklearn.neural_network import MLPClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    log("DeepMatcher not available — using Siamese MLP fallback.")

    # Encode each record field as character n-gram hashes
    def ngram_vec(text, n=3, dim=128):
        text = str(text).lower()
        grams = [text[i:i+n] for i in range(len(text) - n + 1)]
        v = np.zeros(dim)
        for g in grams:
            v[hash(g) % dim] += 1
        norm = np.linalg.norm(v)
        return v / norm if norm > 0 else v

    def record_vec(row, prefix):
        vecs = [ngram_vec(row.get(f"{prefix}_{f}", "")) for f in fields]
        return np.concatenate(vecs)

    log(f"Building feature vectors for {len(df)} pairs...")
    X = np.array([
        np.abs(record_vec(row, "left") - record_vec(row, "right"))
        for _, row in df.iterrows()
    ])
    y = df["label"].values.astype(int)

    labeled = y >= 0
    X_lab, y_lab = X[labeled], y[labeled]

    if len(np.unique(y_lab)) < 2:
        log("WARNING: only one class in labels — returning uniform 0.5 probabilities.")
        df_out = df[["id1", "id2"]].copy()
        df_out["match_prob"] = 0.5
        return df_out

    X_tr, X_val, y_tr, y_val = train_test_split(
        X_lab, y_lab, test_size=0.2, stratify=y_lab, random_state=42)

    scaler = StandardScaler()
    X_tr  = scaler.fit_transform(X_tr)
    X_val = scaler.transform(X_val)
    X_all = scaler.transform(X)

    clf = MLPClassifier(
        hidden_layer_sizes=(256, 128, 64),
        max_iter=epochs * 10,
        early_stopping=True,
        validation_fraction=0.1,
        random_state=42,
        verbose=False
    )
    log(f"Training Siamese MLP (epochs≈{epochs})...")
    clf.fit(X_tr, y_tr)
    val_acc = (clf.predict(X_val) == y_val).mean()
    log(f"Validation accuracy: {val_acc:.3f}")

    probs = clf.predict_proba(X_all)[:, 1]
    df_out = df[["id1", "id2"]].copy()
    df_out["match_prob"] = probs
    return df_out


def run_deepmatcher(df, fields, model_type, epochs, threshold, tmp_dir):
    """Run native DeepMatcher if installed."""
    import deepmatcher as dm
    import pandas as pd
    import os

    log(f"Running DeepMatcher model='{model_type}', epochs={epochs}")

    # Split into train/test (70/30 of labeled pairs)
    labeled = df[df["label"] >= 0].copy()
    unlabeled = df[df["label"] < 0].copy()

    from sklearn.model_selection import train_test_split
    pos = labeled[labeled["label"] == 1]
    neg = labeled[labeled["label"] == 0]
    # Balance: sample equal negatives to positives (capped at 3x)
    n_pos = len(pos)
    neg_sample = neg.sample(min(len(neg), n_pos * 3), random_state=42)
    balanced = pd.concat([pos, neg_sample]).sample(frac=1, random_state=42)

    train_df, test_df = train_test_split(balanced, test_size=0.2,
                                          stratify=balanced["label"],
                                          random_state=42)
    train_path = os.path.join(tmp_dir, "dm_train.csv")
    test_path  = os.path.join(tmp_dir, "dm_test.csv")
    all_path   = os.path.join(tmp_dir, "dm_all.csv")
    train_df.reset_index(drop=True).to_csv(train_path, index=True, index_label="id")
    test_df.reset_index(drop=True).to_csv(test_path,  index=True, index_label="id")
    df.reset_index(drop=True).to_csv(all_path, index=True, index_label="id")

    left_fields  = [f"left_{f}"  for f in fields if f"left_{f}"  in df.columns]
    right_fields = [f"right_{f}" for f in fields if f"right_{f}" in df.columns]

    train_set, val_set, _ = dm.data.process(
        path=tmp_dir,
        train="dm_train.csv",
        validation="dm_test.csv",
        test="dm_all.csv",
        left_prefix="left_",
        right_prefix="right_",
        label_attr="label",
        id_attr="id",
        ignore_columns=["id1", "id2"],
    )

    model = dm.MatchingModel(attr_summarizer=model_type)
    model.run_train(train_set, val_set, best_save_path=os.path.join(tmp_dir, "dm_best.pt"),
                    epochs=epochs, batch_size=16)

    candidates, preds = model.run_prediction(_, get_token_probs=False)
    df_out = df[["id1", "id2"]].copy().reset_index(drop=True)
    df_out["match_prob"] = preds
    return df_out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",     required=True)
    p.add_argument("--output",    required=True)
    p.add_argument("--fields",    nargs="+", required=True)
    p.add_argument("--model",     default="hybrid")
    p.add_argument("--epochs",    type=int, default=10)
    p.add_argument("--threshold", type=float, default=0.5)
    args = p.parse_args()

    import pandas as pd
    tmp_dir = os.path.dirname(args.input)

    log(f"Loading {args.input}")
    df = pd.read_csv(args.input, dtype=str)
    df["label"] = pd.to_numeric(df["label"], errors="coerce").fillna(-1).astype(int)
    log(f"{len(df)} pairs loaded, fields={args.fields}")

    try:
        import deepmatcher
        df_out = run_deepmatcher(df, args.fields, args.model,
                                  args.epochs, args.threshold, tmp_dir)
    except ImportError:
        df_out = run_siamese_mlp(df, args.fields, args.epochs, args.threshold)

    df_out.to_csv(args.output, index=False)
    n_match = (df_out["match_prob"] >= args.threshold).sum()
    log(f"Wrote {len(df_out)} scored pairs ({n_match} above threshold) to {args.output}")

if __name__ == "__main__":
    main()
