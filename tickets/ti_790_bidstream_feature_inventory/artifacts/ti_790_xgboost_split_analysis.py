"""
TI-790: XGBoost Feature Importance — Split Analysis
Model A: Pre-visit features only (available at bid time) — real predictive signal
Model B: Feedback loop features (guid_log + conversion_log) — post-visit enrichment
Model C: All features combined — full picture

The distinction matters:
  - Pre-visit features (bidstream, impressions) → used for TARGETING decisions
  - Feedback features (guid_log, conversion_log) → used for RETRAINING, scoring returning visitors,
    enriching the feature store over time, identity resolution
"""

import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import warnings
warnings.filterwarnings('ignore')

DATA_PATH = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs/ti_790_training_data.csv'
OUTPUT_DIR = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs'

print("Loading data...")
df = pd.read_csv(DATA_PATH)
print(f"Loaded {len(df):,} rows, {df['visited'].mean():.2%} visit rate\n")

# --- Define Feature Groups ---
drop_cols = ['ip', 'visited', 'n_visits', 'n_visit_adv']

# Pre-visit: available at bid/impression time (BEFORE any site visit)
pre_visit_cols = [c for c in df.columns if c not in drop_cols and (
    c.startswith('wl_') or      # win_logs — ad engagement
    c.startswith('ci_') or      # cost_impression_log — impression enrichment
    c.startswith('al_') or      # augmentor_log — bidstream supply
    c.startswith('bae_') or     # bidder_auction_events — content/device
    c in ('n_wins', 'n_win_adv')  # base impression counts
)]

# Feedback: only available AFTER site interaction
feedback_cols = [c for c in df.columns if c not in drop_cols and (
    c.startswith('gl_') or      # guid_log — pixel fires on advertiser sites
    c.startswith('cv_')         # conversion_log — purchase data
)]

all_feature_cols = [c for c in df.columns if c not in drop_cols]

print(f"Pre-visit features: {len(pre_visit_cols)}")
print(f"  Sources: win_logs, cost_impression_log, augmentor_log, bidder_auction_events")
print(f"Feedback features:  {len(feedback_cols)}")
print(f"  Sources: guid_log, conversion_log")
print(f"Total features:     {len(all_feature_cols)}\n")

y = df['visited'].copy()
X_all = df[all_feature_cols].fillna(0)

X_train, X_test, y_train, y_test = train_test_split(
    X_all, y, test_size=0.2, random_state=42, stratify=y
)

def get_source(col):
    if col.startswith('gl_'): return 'guid_log'
    if col.startswith('wl_'): return 'win_logs'
    if col.startswith('ci_'): return 'cost_impression_log'
    if col.startswith('cv_'): return 'conversion_log'
    if col.startswith('al_'): return 'augmentor_log'
    if col.startswith('bae_'): return 'bidder_auction_events'
    return 'base'

def train_and_report(name, features, X_tr, X_te, y_tr, y_te):
    """Train XGBoost and return importance DataFrame."""
    print(f"\n{'='*70}")
    print(f"MODEL {name}: {len(features)} features")
    print(f"{'='*70}")

    model = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=len(y_tr[y_tr==0]) / max(len(y_tr[y_tr==1]), 1),
        eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0
    )
    model.fit(X_tr[features], y_tr, eval_set=[(X_te[features], y_te)], verbose=False)

    y_pred_proba = model.predict_proba(X_te[features])[:, 1]
    auc = roc_auc_score(y_te, y_pred_proba)
    print(f"Test AUC: {auc:.4f}")

    y_pred = model.predict(X_te[features])
    print(classification_report(y_te, y_pred, target_names=['no_visit', 'visited']))

    # Feature importance
    gain = model.get_booster().get_score(importance_type='gain')
    weight = model.get_booster().get_score(importance_type='weight')
    cover = model.get_booster().get_score(importance_type='cover')

    xgb_map = {f'f{i}': col for i, col in enumerate(features)}

    imp = pd.DataFrame({'feature': features})
    imp['gain'] = imp['feature'].map({xgb_map.get(k,k): v for k,v in gain.items()}).fillna(0)
    imp['weight'] = imp['feature'].map({xgb_map.get(k,k): v for k,v in weight.items()}).fillna(0)
    imp['cover'] = imp['feature'].map({xgb_map.get(k,k): v for k,v in cover.items()}).fillna(0)
    imp['gain_rank'] = imp['gain'].rank(ascending=False)
    imp['weight_rank'] = imp['weight'].rank(ascending=False)
    imp['cover_rank'] = imp['cover'].rank(ascending=False)
    imp['composite_rank'] = (imp['gain_rank'] + imp['weight_rank'] + imp['cover_rank']) / 3
    imp['source'] = imp['feature'].apply(get_source)
    imp = imp.sort_values('composite_rank')

    print(f"\nTop 15 Features:")
    print(f"{'Rank':>5} {'Feature':<30} {'Source':<25} {'Gain':>10} {'Weight':>8} {'Cover':>10}")
    print("-" * 90)
    for _, row in imp.head(15).iterrows():
        print(f"{row['composite_rank']:>5.1f} {row['feature']:<30} {row['source']:<25} {row['gain']:>10.1f} {row['weight']:>8.0f} {row['cover']:>10.1f}")

    # Source summary
    src = imp.groupby('source').agg(
        n_feats=('feature','count'),
        used=('gain', lambda x: (x>0).sum()),
        avg_rank=('composite_rank','mean'),
        total_gain=('gain','sum')
    ).sort_values('avg_rank')
    print(f"\nBy Source Table:")
    print(f"{'Source':<25} {'Feats':>6} {'Used':>6} {'Avg Rank':>10} {'Total Gain':>12}")
    print("-" * 65)
    for s, r in src.iterrows():
        print(f"{s:<25} {r['n_feats']:>6.0f} {r['used']:>6.0f} {r['avg_rank']:>10.1f} {r['total_gain']:>12.1f}")

    return model, imp, auc


# --- Model A: Pre-Visit Only (THE REAL PREDICTIVE SIGNAL) ---
model_a, imp_a, auc_a = train_and_report(
    "A — Pre-Visit (Targeting)", pre_visit_cols, X_train, X_test, y_train, y_test
)
imp_a.to_csv(f'{OUTPUT_DIR}/ti_790_importance_pre_visit.csv', index=False)

# --- Model B: Feedback Only (Post-Visit Enrichment) ---
model_b, imp_b, auc_b = train_and_report(
    "B — Feedback Loop (Post-Visit)", feedback_cols, X_train, X_test, y_train, y_test
)
imp_b.to_csv(f'{OUTPUT_DIR}/ti_790_importance_feedback.csv', index=False)

# --- Model C: All Features ---
model_c, imp_c, auc_c = train_and_report(
    "C — All Features Combined", all_feature_cols, X_train, X_test, y_train, y_test
)
imp_c.to_csv(f'{OUTPUT_DIR}/ti_790_importance_all.csv', index=False)


# --- SHAP on Model A (pre-visit — the one that matters for targeting) ---
print(f"\n{'='*70}")
print("SHAP Analysis — Model A (Pre-Visit Features)")
print(f"{'='*70}")

try:
    import shap
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    explainer = shap.TreeExplainer(model_a)
    shap_sample = X_test[pre_visit_cols].sample(min(5000, len(X_test)), random_state=42)
    shap_values = explainer.shap_values(shap_sample)

    shap_imp = pd.DataFrame({
        'feature': pre_visit_cols,
        'mean_abs_shap': np.abs(shap_values).mean(axis=0)
    }).sort_values('mean_abs_shap', ascending=False)

    print("\nTop 15 Pre-Visit Features by SHAP:")
    for _, row in shap_imp.head(15).iterrows():
        src = get_source(row['feature'])
        print(f"  {row['mean_abs_shap']:.6f}  {row['feature']} ({src})")

    shap_imp.to_csv(f'{OUTPUT_DIR}/ti_790_shap_pre_visit.csv', index=False)

    plt.figure(figsize=(12, 10))
    shap.summary_plot(shap_values, shap_sample, feature_names=pre_visit_cols,
                      max_display=20, show=False)
    plt.title("SHAP — Pre-Visit Features (Available at Bid Time)")
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/ti_790_shap_pre_visit.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved SHAP plot: {OUTPUT_DIR}/ti_790_shap_pre_visit.png")

except ImportError:
    print("shap not installed")


# --- Iterative Paring on Model A ---
print(f"\n{'='*70}")
print("Iterative Paring — Model A (Pre-Visit)")
print(f"{'='*70}")

used_features = imp_a[imp_a['gain'] > 0].sort_values('composite_rank')['feature'].tolist()
paring_results = []

for n_drop in range(0, len(used_features), 3):
    if n_drop >= len(used_features) - 3:
        break
    feats = used_features[:len(used_features) - n_drop] if n_drop > 0 else used_features
    m = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=len(y_train[y_train==0]) / max(len(y_train[y_train==1]), 1),
        eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0
    )
    m.fit(X_train[feats], y_train, eval_set=[(X_test[feats], y_test)], verbose=False)
    a = roc_auc_score(y_test, m.predict_proba(X_test[feats])[:, 1])
    paring_results.append({'n_features': len(feats), 'dropped': n_drop, 'auc': a})
    marker = " *** AUC DROP" if a < auc_a - 0.005 else ""
    print(f"  {len(feats):>3} features (dropped {n_drop:>2}): AUC = {a:.4f}{marker}")

pd.DataFrame(paring_results).to_csv(f'{OUTPUT_DIR}/ti_790_paring_pre_visit.csv', index=False)


# --- Final Comparison ---
print(f"\n{'='*70}")
print("FINAL COMPARISON")
print(f"{'='*70}")
print(f"Model A (Pre-Visit, {len(pre_visit_cols)} features):  AUC = {auc_a:.4f}  ← USE FOR TARGETING")
print(f"Model B (Feedback, {len(feedback_cols)} features):   AUC = {auc_b:.4f}  ← Post-visit enrichment")
print(f"Model C (All, {len(all_feature_cols)} features):        AUC = {auc_c:.4f}  ← Full picture")
print(f"\nLift from feedback features: {auc_c - auc_a:.4f} AUC")
print(f"\nFiles saved to {OUTPUT_DIR}/")
