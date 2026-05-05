"""
TI-832: XGBoost + SHAP feature ranking — CONVERSION target (Fangorn V2)

Mirrors TI-790's methodology (ti_790_xgboost_split_analysis.py) but:
  * Label = "this (IP, advertiser) had a conversion in F+1..F+14"  (TI-790 was visit in F+1)
  * Adds candidate conv-history features (rolling 7/14/30d backward) so SHAP can rank them
  * Three model splits:
      A — Pre-bid features only (deployment-realistic for a bid-time model)
      B — Conv-history features only (the L1-extension candidates)
      C — All combined (full picture)

Goal: produce a defensible, measured top-N to ship in the conv_log_ip Layer-1 extension
and the new Layer-2 conv_log_derived_ip model.

Inputs:  outputs/ti_832_training_data.csv (produced by queries/ti_832_training_dataset.sql)
Outputs: outputs/ti_832_importance_*.csv, ti_832_shap_*.csv, ti_832_shap_combined.png
"""

import os
import sys
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

WORKSPACE = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_832_feature_store_roas_cpa'
DATA_PATH = f'{WORKSPACE}/outputs/ti_832_training_data.csv'
OUTPUT_DIR = f'{WORKSPACE}/outputs'

LABEL = 'converted'
DROP_COLS = ['ip', 'advertiser_id', LABEL, 'n_conversions_label', 'order_amt_label',
             'n_wins_this_adv', 'n_cgs_this_adv']  # last two are deployment leakage proxies, drop


def get_source(col: str) -> str:
    """Map a feature column to its source table for grouping."""
    if col.startswith('wl_'): return 'win_logs'
    if col.startswith('ci_'): return 'cost_impression_log'
    if col.startswith('bae_'): return 'bidder_auction_events'
    if col.startswith('cv_'): return 'conv_log (IP grain)'
    if col.startswith('cvp_'): return 'conv_log (IP, adv pair)'
    return 'base'


def train_and_report(name, features, X_tr, X_te, y_tr, y_te):
    print(f"\n{'='*70}\nMODEL {name}: {len(features)} features\n{'='*70}")
    model = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=len(y_tr[y_tr == 0]) / max(len(y_tr[y_tr == 1]), 1),
        eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0,
    )
    model.fit(X_tr[features], y_tr, eval_set=[(X_te[features], y_te)], verbose=False)
    y_pred_proba = model.predict_proba(X_te[features])[:, 1]
    auc = roc_auc_score(y_te, y_pred_proba)
    print(f"Test AUC: {auc:.4f}")
    print(classification_report(y_te, model.predict(X_te[features]),
                                target_names=['no_conv', 'converted']))

    gain = model.get_booster().get_score(importance_type='gain')
    weight = model.get_booster().get_score(importance_type='weight')
    cover = model.get_booster().get_score(importance_type='cover')
    xgb_map = {f'f{i}': c for i, c in enumerate(features)}

    imp = pd.DataFrame({'feature': features})
    imp['gain']   = imp['feature'].map({xgb_map.get(k, k): v for k, v in gain.items()}).fillna(0)
    imp['weight'] = imp['feature'].map({xgb_map.get(k, k): v for k, v in weight.items()}).fillna(0)
    imp['cover']  = imp['feature'].map({xgb_map.get(k, k): v for k, v in cover.items()}).fillna(0)
    imp['gain_rank']   = imp['gain'].rank(ascending=False)
    imp['weight_rank'] = imp['weight'].rank(ascending=False)
    imp['cover_rank']  = imp['cover'].rank(ascending=False)
    imp['composite_rank'] = (imp['gain_rank'] + imp['weight_rank'] + imp['cover_rank']) / 3
    imp['source'] = imp['feature'].apply(get_source)
    imp = imp.sort_values('composite_rank')

    print(f"\nTop 15 Features:")
    print(f"{'Rank':>5} {'Feature':<28} {'Source':<26} {'Gain':>10} {'Weight':>8}")
    print("-" * 90)
    for _, row in imp.head(15).iterrows():
        print(f"{row['composite_rank']:>5.1f} {row['feature']:<28} {row['source']:<26} "
              f"{row['gain']:>10.1f} {row['weight']:>8.0f}")

    src = imp.groupby('source').agg(
        n_feats=('feature', 'count'),
        used=('gain', lambda x: (x > 0).sum()),
        avg_rank=('composite_rank', 'mean'),
        total_gain=('gain', 'sum')
    ).sort_values('avg_rank')
    print(f"\nBy Source Table:")
    print(f"{'Source':<26} {'Feats':>6} {'Used':>6} {'Avg Rank':>10} {'Total Gain':>12}")
    print("-" * 65)
    for s, r in src.iterrows():
        print(f"{s:<26} {r['n_feats']:>6.0f} {r['used']:>6.0f} {r['avg_rank']:>10.1f} {r['total_gain']:>12.1f}")
    return model, imp, auc


def main():
    if not os.path.exists(DATA_PATH):
        print(f"ERROR: training data not found at {DATA_PATH}", file=sys.stderr)
        sys.exit(1)

    print("Loading data...")
    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df):,} rows, {df[LABEL].mean():.4%} conversion rate "
          f"({int(df[LABEL].sum()):,} positives)\n")

    feature_cols = [c for c in df.columns if c not in DROP_COLS]
    pre_bid_cols    = [c for c in feature_cols if c.startswith(('wl_', 'ci_', 'bae_'))]
    conv_hist_cols  = [c for c in feature_cols if c.startswith(('cv_', 'cvp_'))]

    print(f"Pre-bid features:      {len(pre_bid_cols)}  (win_logs, cost_impression_log, bidder_auction_events)")
    print(f"Conv-history features: {len(conv_hist_cols)}  (conv_log IP-grain + (IP, adv) pair)")
    print(f"Total features:        {len(feature_cols)}\n")

    y = df[LABEL].astype(int)
    X = df[feature_cols].fillna(0)

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )
    print(f"Train: {len(X_tr):,} ({y_tr.sum():,} pos)   Test: {len(X_te):,} ({y_te.sum():,} pos)\n")

    # Three splits
    _, imp_a, auc_a = train_and_report("A — Pre-bid only",   pre_bid_cols,   X_tr, X_te, y_tr, y_te)
    imp_a.to_csv(f'{OUTPUT_DIR}/ti_832_importance_pre_bid.csv', index=False)

    _, imp_b, auc_b = train_and_report("B — Conv-history only", conv_hist_cols, X_tr, X_te, y_tr, y_te)
    imp_b.to_csv(f'{OUTPUT_DIR}/ti_832_importance_conv_history.csv', index=False)

    model_c, imp_c, auc_c = train_and_report("C — All combined", feature_cols, X_tr, X_te, y_tr, y_te)
    imp_c.to_csv(f'{OUTPUT_DIR}/ti_832_importance_all.csv', index=False)

    # SHAP on the combined model — that's the one whose rankings drive the spec.
    print(f"\n{'='*70}\nSHAP — Model C (All Features Combined)\n{'='*70}")
    try:
        import shap
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        explainer = shap.TreeExplainer(model_c)
        sample = X_te[feature_cols].sample(min(5000, len(X_te)), random_state=42)
        shap_vals = explainer.shap_values(sample)
        shap_imp = pd.DataFrame({
            'feature': feature_cols,
            'mean_abs_shap': np.abs(shap_vals).mean(axis=0),
        }).sort_values('mean_abs_shap', ascending=False)
        shap_imp['source'] = shap_imp['feature'].apply(get_source)

        print("\nTop 25 by SHAP:")
        for _, row in shap_imp.head(25).iterrows():
            print(f"  {row['mean_abs_shap']:.6f}  {row['feature']:<28} ({row['source']})")
        shap_imp.to_csv(f'{OUTPUT_DIR}/ti_832_shap_combined.csv', index=False)

        plt.figure(figsize=(12, 10))
        shap.summary_plot(shap_vals, sample, feature_names=feature_cols, max_display=25, show=False)
        plt.title("TI-832 — SHAP, conversion target, all features")
        plt.tight_layout()
        plt.savefig(f'{OUTPUT_DIR}/ti_832_shap_combined.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"\nSaved SHAP plot: {OUTPUT_DIR}/ti_832_shap_combined.png")
    except ImportError as e:
        print(f"SHAP skipped: {e}")

    # Lift table on Model C (mirrors TI-790 reporting style)
    proba_c = model_c.predict_proba(X_te[feature_cols])[:, 1]
    lift_df = pd.DataFrame({'proba': proba_c, 'label': y_te.values}).sort_values('proba', ascending=False)
    base_rate = y_te.mean()
    lift_rows = []
    for pct in [0.01, 0.05, 0.10, 0.25, 0.50]:
        n = int(len(lift_df) * pct)
        rate = lift_df.head(n)['label'].mean() if n else 0.0
        lift_rows.append({'top_pct': pct, 'n': n, 'rate': rate, 'lift_x': rate / base_rate if base_rate else 0.0})
    lift = pd.DataFrame(lift_rows)
    lift.to_csv(f'{OUTPUT_DIR}/ti_832_lift_combined.csv', index=False)
    print(f"\nLift table (Model C, base rate {base_rate:.4%}):")
    print(lift.to_string(index=False))

    print(f"\n{'='*70}\nFINAL\n{'='*70}")
    print(f"A (pre-bid only,    {len(pre_bid_cols)} feats):  AUC={auc_a:.4f}")
    print(f"B (conv-hist only,  {len(conv_hist_cols)} feats):  AUC={auc_b:.4f}")
    print(f"C (all combined,    {len(feature_cols)} feats):  AUC={auc_c:.4f}")
    print(f"Lift from conv-history: ΔAUC={auc_c - auc_a:+.4f}")
    print(f"\nFiles saved to {OUTPUT_DIR}/")


if __name__ == '__main__':
    main()
