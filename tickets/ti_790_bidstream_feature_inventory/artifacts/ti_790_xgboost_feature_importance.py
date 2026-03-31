"""
TI-790: XGBoost Feature Importance Analysis
Matt Brorby's methodology:
  1. Train XGBoost predicting visited (0/1) from all features
  2. Extract 3 importance metrics: gain, weight (frequency), cover
  3. Composite rank = average rank across all 3
  4. Iterative paring: drop least important, retrain, verify AUC holds
  5. SHAP values for fine-tuning

Input: ti_790_training_data.csv (117K IPs, 63 columns, from 2026-03-29)
Output: Feature importance rankings + SHAP plots
"""

import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import warnings
warnings.filterwarnings('ignore')

# --- Load Data ---
DATA_PATH = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs/ti_790_training_data.csv'
OUTPUT_DIR = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs'

print("Loading data...")
df = pd.read_csv(DATA_PATH)
print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
print(f"Visit rate: {df['visited'].mean():.4f} ({df['visited'].sum():,} / {len(df):,})")

# --- Prepare Features ---
# Drop non-feature columns
drop_cols = ['ip', 'visited', 'n_visits', 'n_visit_adv']
feature_cols = [c for c in df.columns if c not in drop_cols]

X = df[feature_cols].copy()
y = df['visited'].copy()

# Fill NaN with 0 (NULL = no data from that table = meaningful signal)
X = X.fillna(0)

print(f"\nFeatures: {len(feature_cols)}")
print(f"Non-zero feature fill rates:")
for col in feature_cols:
    fill = (X[col] != 0).mean()
    if fill < 0.5:
        print(f"  {col}: {fill:.1%} non-zero")

# --- Train/Test Split (time-based would be ideal, using random for single-day) ---
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\nTrain: {len(X_train):,} ({y_train.mean():.4f} visit rate)")
print(f"Test:  {len(X_test):,} ({y_test.mean():.4f} visit rate)")

# --- Step 1: Train XGBoost ---
print("\n" + "="*60)
print("STEP 1: Train XGBoost")
print("="*60)

model = XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=len(y_train[y_train==0]) / max(len(y_train[y_train==1]), 1),
    eval_metric='auc',
    random_state=42,
    n_jobs=-1,
    verbosity=0
)

model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=False
)

# Evaluate
y_pred_proba = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_proba)
print(f"Test AUC: {auc:.4f}")

y_pred = model.predict(X_test)
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['no_visit', 'visited']))

# --- Step 2: Extract 3 Importance Metrics ---
print("\n" + "="*60)
print("STEP 2: Feature Importance (3 Methods)")
print("="*60)

importance_gain = model.get_booster().get_score(importance_type='gain')
importance_weight = model.get_booster().get_score(importance_type='weight')
importance_cover = model.get_booster().get_score(importance_type='cover')

# Build importance DataFrame
imp_df = pd.DataFrame({
    'feature': feature_cols,
})

# Map xgboost feature names (f0, f1, ...) to our column names
xgb_to_col = {f'f{i}': col for i, col in enumerate(feature_cols)}

imp_df['gain'] = imp_df['feature'].map(
    {xgb_to_col.get(k, k): v for k, v in importance_gain.items()}
).fillna(0)
imp_df['weight'] = imp_df['feature'].map(
    {xgb_to_col.get(k, k): v for k, v in importance_weight.items()}
).fillna(0)
imp_df['cover'] = imp_df['feature'].map(
    {xgb_to_col.get(k, k): v for k, v in importance_cover.items()}
).fillna(0)

# Rank each (lower = more important)
imp_df['gain_rank'] = imp_df['gain'].rank(ascending=False)
imp_df['weight_rank'] = imp_df['weight'].rank(ascending=False)
imp_df['cover_rank'] = imp_df['cover'].rank(ascending=False)

# Composite rank (Matt's method: average rank across all 3)
imp_df['composite_rank'] = (imp_df['gain_rank'] + imp_df['weight_rank'] + imp_df['cover_rank']) / 3
imp_df = imp_df.sort_values('composite_rank')

# Add source table label
def get_source(col):
    if col.startswith('gl_'): return 'guid_log'
    if col.startswith('wl_'): return 'win_logs'
    if col.startswith('ci_'): return 'cost_impression_log'
    if col.startswith('cv_'): return 'conversion_log'
    if col.startswith('al_'): return 'augmentor_log'
    if col.startswith('bae_'): return 'bidder_auction_events'
    return 'base'

imp_df['source'] = imp_df['feature'].apply(get_source)

print("\nTop 20 Features by Composite Rank:")
print("-" * 90)
print(f"{'Rank':>4} {'Feature':<30} {'Source':<25} {'Gain':>8} {'Weight':>8} {'Cover':>8}")
print("-" * 90)
for i, row in imp_df.head(20).iterrows():
    print(f"{row['composite_rank']:>4.1f} {row['feature']:<30} {row['source']:<25} {row['gain']:>8.1f} {row['weight']:>8.0f} {row['cover']:>8.1f}")

print(f"\nFeatures with zero importance (never used in any tree):")
zero_imp = imp_df[imp_df['gain'] == 0]
if len(zero_imp) > 0:
    for _, row in zero_imp.iterrows():
        print(f"  {row['feature']} ({row['source']})")
else:
    print("  None — all features used")

# --- Step 3: Source Table Summary ---
print("\n" + "="*60)
print("STEP 3: Importance by Source Table")
print("="*60)

source_summary = imp_df.groupby('source').agg(
    n_features=('feature', 'count'),
    avg_composite_rank=('composite_rank', 'mean'),
    best_rank=('composite_rank', 'min'),
    total_gain=('gain', 'sum'),
    features_used=('gain', lambda x: (x > 0).sum())
).sort_values('avg_composite_rank')

print(f"\n{'Source':<25} {'# Feats':>8} {'Used':>6} {'Avg Rank':>10} {'Best Rank':>10} {'Total Gain':>12}")
print("-" * 75)
for src, row in source_summary.iterrows():
    print(f"{src:<25} {row['n_features']:>8.0f} {row['features_used']:>6.0f} {row['avg_composite_rank']:>10.1f} {row['best_rank']:>10.1f} {row['total_gain']:>12.1f}")

# --- Save Results ---
imp_df.to_csv(f'{OUTPUT_DIR}/ti_790_feature_importance.csv', index=False)
print(f"\nSaved feature importance to {OUTPUT_DIR}/ti_790_feature_importance.csv")

# --- Step 4: SHAP Analysis ---
print("\n" + "="*60)
print("STEP 4: SHAP Values")
print("="*60)

try:
    import shap
    explainer = shap.TreeExplainer(model)
    # Use a sample for SHAP (computationally expensive)
    shap_sample = X_test.sample(min(5000, len(X_test)), random_state=42)
    shap_values = explainer.shap_values(shap_sample)

    # SHAP importance (mean absolute SHAP value per feature)
    shap_importance = pd.DataFrame({
        'feature': feature_cols,
        'mean_abs_shap': np.abs(shap_values).mean(axis=0)
    }).sort_values('mean_abs_shap', ascending=False)

    print("\nTop 20 Features by SHAP:")
    for i, row in shap_importance.head(20).iterrows():
        src = get_source(row['feature'])
        print(f"  {row['mean_abs_shap']:.6f}  {row['feature']} ({src})")

    shap_importance.to_csv(f'{OUTPUT_DIR}/ti_790_shap_importance.csv', index=False)

    # SHAP summary plot
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    plt.figure(figsize=(12, 10))
    shap.summary_plot(shap_values, shap_sample, feature_names=feature_cols,
                      max_display=20, show=False)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/ti_790_shap_summary.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved SHAP summary plot to {OUTPUT_DIR}/ti_790_shap_summary.png")

except ImportError:
    print("shap not installed — skipping SHAP analysis")
    print("Install with: pip install shap")

# --- Step 5: Iterative Paring ---
print("\n" + "="*60)
print("STEP 5: Iterative Feature Paring")
print("="*60)

# Start with all features, progressively drop least important
results = []
current_features = imp_df[imp_df['gain'] > 0].sort_values('composite_rank')['feature'].tolist()

for n_drop in [0, 5, 10, 15, 20, 25, 30]:
    if n_drop >= len(current_features):
        break
    feats = current_features[:len(current_features) - n_drop] if n_drop > 0 else current_features

    m = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=len(y_train[y_train==0]) / max(len(y_train[y_train==1]), 1),
        eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0
    )
    m.fit(X_train[feats], y_train, eval_set=[(X_test[feats], y_test)], verbose=False)
    auc_iter = roc_auc_score(y_test, m.predict_proba(X_test[feats])[:, 1])
    results.append({'n_features': len(feats), 'n_dropped': n_drop, 'auc': auc_iter})
    print(f"  {len(feats):>3} features (dropped {n_drop:>2}): AUC = {auc_iter:.4f}")

paring_df = pd.DataFrame(results)
paring_df.to_csv(f'{OUTPUT_DIR}/ti_790_paring_results.csv', index=False)

# --- Final Summary ---
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Dataset: {len(df):,} IPs, {len(feature_cols)} features, {df['visited'].mean():.2%} visit rate")
print(f"Baseline AUC (all {len(current_features)} used features): {auc:.4f}")
print(f"\nTop 10 most important features for predicting visits:")
for i, (_, row) in enumerate(imp_df.head(10).iterrows(), 1):
    print(f"  {i:>2}. {row['feature']} ({row['source']}) — gain: {row['gain']:.1f}")
print(f"\nFiles saved to {OUTPUT_DIR}/")
