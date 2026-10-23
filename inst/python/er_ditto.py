#!/usr/bin/env python3
"""
er_ditto.py  --  Ditto / BERT-based entity matching baseline for ERBOT.

Called by er_baseline_ditto() in R/10-baselines.R via system2().

Input: JSONL file, one record per line:
  {"left": "COL f1 VAL v1 ...", "right": "COL f1 VAL v1 ...",
   "label": 0/1, "id1": "...", "id2": "..."}

Output CSV columns: id1, id2, match_prob

Usage:
  python er_ditto.py \
    --input     /path/pairs.jsonl          \
    --output    /path/output.csv           \
    --lm        distilbert-base-uncased    \
    --epochs    5                          \
    --threshold 0.5

Requirements (full Ditto):
  pip install transformers torch datasets scikit-learn

Falls back to a TF-IDF + logistic regression baseline if transformers
is not available, preserving the same interface.
"""

import argparse
import sys
import json
import os
import warnings
warnings.filterwarnings("ignore")

def log(msg):
    print(f"[ditto] {msg}", flush=True)


# ── TF-IDF + Logistic Regression fallback ────────────────────────────────────
def run_tfidf_lr(records, epochs, threshold):
    """
    Fallback when transformers is not installed.
    Encodes each pair as TF-IDF of concatenated left+right text,
    trains a logistic regression classifier.
    """
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import normalize
    from scipy.sparse import hstack

    log("Transformers not available — using TF-IDF + LogReg fallback.")

    lefts  = [r["left"]  for r in records]
    rights = [r["right"] for r in records]
    labels = [r.get("label", -1) for r in records]
    id1s   = [r["id1"] for r in records]
    id2s   = [r["id2"] for r in records]

    # Joint TF-IDF vocabulary
    tfidf = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4),
                             max_features=50000, sublinear_tf=True)
    all_text = lefts + rights
    tfidf.fit(all_text)

    X_left  = tfidf.transform(lefts)
    X_right = tfidf.transform(rights)
    # Element-wise absolute difference (sparse)
    X = abs(X_left - X_right)

    y = np.array(labels)
    labeled = y >= 0
    X_lab, y_lab = X[labeled], y[labeled]

    if len(np.unique(y_lab)) < 2:
        log("WARNING: only one class — returning uniform 0.5.")
        return [{"id1": i1, "id2": i2, "match_prob": 0.5}
                for i1, i2 in zip(id1s, id2s)]

    clf = LogisticRegression(max_iter=epochs * 20, C=1.0, random_state=42,
                              class_weight="balanced")
    log(f"Training LogReg on {X_lab.shape[0]} labeled pairs...")
    clf.fit(X_lab, y_lab)
    probs = clf.predict_proba(X)[:, 1]

    return [{"id1": i1, "id2": i2, "match_prob": float(prob)}
            for i1, i2, prob in zip(id1s, id2s, probs)]


# ── Ditto / Transformers ──────────────────────────────────────────────────────
def run_ditto(records, lm, epochs, threshold, tmp_dir):
    """Fine-tune a pre-trained LM on serialised record pairs (Ditto style)."""
    import numpy as np
    import torch
    from torch.utils.data import Dataset, DataLoader
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    from torch.optim import AdamW
    from sklearn.model_selection import train_test_split

    device = "cuda" if torch.cuda.is_available() else "cpu"
    log(f"Device: {device}, model: {lm}")

    labels = [r.get("label", -1) for r in records]
    labeled_idx = [i for i, l in enumerate(labels) if l >= 0]

    if len(labeled_idx) < 10:
        log("WARNING: fewer than 10 labeled pairs — falling back to TF-IDF.")
        return run_tfidf_lr(records, epochs, threshold)

    # Tokenise: "[CLS] left [SEP] right [SEP]"
    tokenizer = AutoTokenizer.from_pretrained(lm)
    max_len   = 256

    class PairDataset(Dataset):
        def __init__(self, idxs):
            self.idxs = idxs
        def __len__(self):
            return len(self.idxs)
        def __getitem__(self, i):
            r = records[self.idxs[i]]
            enc = tokenizer(r["left"], r["right"],
                            max_length=max_len, truncation=True,
                            padding="max_length", return_tensors="pt")
            return {k: v.squeeze(0) for k, v in enc.items()}, \
                   torch.tensor(r.get("label", 0), dtype=torch.long)

    train_idx, val_idx = train_test_split(
        labeled_idx,
        test_size=0.15,
        stratify=[labels[i] for i in labeled_idx],
        random_state=42
    )
    train_ds = PairDataset(train_idx)
    val_ds   = PairDataset(val_idx)

    model = AutoModelForSequenceClassification.from_pretrained(lm, num_labels=2)
    model.to(device)
    optimiser = AdamW(model.parameters(), lr=2e-5, weight_decay=0.01)

    train_loader = DataLoader(train_ds, batch_size=16, shuffle=True)

    log(f"Fine-tuning for {epochs} epochs on {len(train_idx)} pairs...")
    for epoch in range(epochs):
        model.train()
        total_loss = 0.0
        for batch, batch_labels in train_loader:
            batch     = {k: v.to(device) for k, v in batch.items()}
            batch_labels = batch_labels.to(device)
            outputs   = model(**batch, labels=batch_labels)
            loss      = outputs.loss
            optimiser.zero_grad()
            loss.backward()
            optimiser.step()
            total_loss += loss.item()
        log(f"  Epoch {epoch+1}/{epochs}  loss={total_loss/len(train_loader):.4f}")

    # Predict on all pairs
    all_ds = PairDataset(list(range(len(records))))
    all_loader = DataLoader(all_ds, batch_size=32, shuffle=False)
    model.eval()
    all_probs = []
    with torch.no_grad():
        for batch, _ in all_loader:
            batch  = {k: v.to(device) for k, v in batch.items()}
            logits = model(**batch).logits
            probs  = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
            all_probs.extend(probs.tolist())

    return [{"id1": r["id1"], "id2": r["id2"], "match_prob": float(p)}
            for r, p in zip(records, all_probs)]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",     required=True)
    p.add_argument("--output",    required=True)
    p.add_argument("--lm",        default="distilbert-base-uncased")
    p.add_argument("--epochs",    type=int,   default=5)
    p.add_argument("--threshold", type=float, default=0.5)
    args = p.parse_args()

    import csv

    log(f"Loading {args.input}")
    records = []
    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    log(f"{len(records)} pairs loaded")

    tmp_dir = os.path.dirname(args.output)

    try:
        import transformers
        results = run_ditto(records, args.lm, args.epochs,
                             args.threshold, tmp_dir)
    except ImportError:
        results = run_tfidf_lr(records, args.epochs, args.threshold)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id1", "id2", "match_prob"])
        writer.writeheader()
        writer.writerows(results)

    n_match = sum(1 for r in results if r["match_prob"] >= args.threshold)
    log(f"Wrote {len(results)} scored pairs ({n_match} above threshold) "
        f"to {args.output}")

if __name__ == "__main__":
    main()
