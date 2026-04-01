"""
TI-790: XGBoost Feature Importance — Campaign-Group Scoped
Each row = (IP, advertiser). Label = visited THIS advertiser.
Compare rankings to unscoped model to see which features gain/lose importance.
"""
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import warnings
warnings.filterwarnings('ignore')

DATA = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs/ti_790_training_data_scoped.csv'
OUT = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_790_bidstream_feature_inventory/outputs'

df = pd.read_csv(DATA)
print(f"Loaded {len(df):,} rows, visit rate: {df['visited'].mean():.4f} ({df['visited'].sum():,} visits)")

drop = ['ip', 'advertiser_id', 'visited', 'n_visits']
pre_visit = [c for c in df.columns if c not in drop and (
    c.startswith('wl_') or c.startswith('ci_') or c.startswith('al_') or
    c.startswith('bae_') or c in ('n_wins_this_adv', 'n_cgs_this_adv', 'wl_n_wins', 'wl_n_adv')
)]
feedback = [c for c in df.columns if c not in drop and (c.startswith('gl_') or c.startswith('cv_'))]

def get_source(col):
    if col.startswith('gl_'): return 'guid_log'
    if col.startswith('wl_'): return 'win_logs'
    if col.startswith('ci_'): return 'cost_impression_log'
    if col.startswith('cv_'): return 'conversion_log'
    if col.startswith('al_'): return 'augmentor_log'
    if col.startswith('bae_'): return 'bidder_auction_events'
    return 'base'

y = df['visited']
X = df[[c for c in df.columns if c not in drop]].fillna(0)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Pre-visit model
print(f"\n{'='*60}")
print(f"SCOPED MODEL: Pre-Visit ({len(pre_visit)} features)")
print(f"{'='*60}")

model = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.1,
    subsample=0.8, colsample_bytree=0.8,
    scale_pos_weight=len(y_train[y_train==0]) / max(len(y_train[y_train==1]), 1),
    eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0
)
model.fit(X_train[pre_visit], y_train, eval_set=[(X_test[pre_visit], y_test)], verbose=False)
auc = roc_auc_score(y_test, model.predict_proba(X_test[pre_visit])[:, 1])
print(f"Test AUC: {auc:.4f}")
print(classification_report(y_test, model.predict(X_test[pre_visit]), target_names=['no_visit', 'visited']))

# SHAP
import shap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

explainer = shap.TreeExplainer(model)
shap_sample = X_test[pre_visit].sample(min(5000, len(X_test)), random_state=42)
shap_values = explainer.shap_values(shap_sample)

shap_imp = pd.DataFrame({
    'feature': pre_visit,
    'shap': np.abs(shap_values).mean(axis=0),
    'source': [get_source(c) for c in pre_visit]
}).sort_values('shap', ascending=False)

# Also get gain
gain = model.get_booster().get_score(importance_type='gain')
xgb_map = {f'f{i}': col for i, col in enumerate(pre_visit)}
shap_imp['gain'] = shap_imp['feature'].map({xgb_map.get(k,k): v for k,v in gain.items()}).fillna(0)

shap_imp['rank'] = range(1, len(shap_imp)+1)
shap_imp.to_csv(f'{OUT}/ti_790_scoped_importance.csv', index=False)

print(f"\nTop 20 by SHAP (scoped):")
print(f"{'#':>3} {'Feature':<30} {'Source':<25} {'SHAP':>8} {'Gain':>8}")
print("-" * 78)
for _, r in shap_imp.head(20).iterrows():
    print(f"{r['rank']:>3.0f} {r['feature']:<30} {r['source']:<25} {r['shap']:>8.4f} {r['gain']:>8.1f}")

plt.figure(figsize=(12, 10))
shap.summary_plot(shap_values, shap_sample, feature_names=pre_visit, max_display=20, show=False)
plt.title("SHAP — Pre-Visit Features (Scoped to Advertiser)")
plt.tight_layout()
plt.savefig(f'{OUT}/ti_790_shap_scoped.png', dpi=150, bbox_inches='tight')
plt.close()

# Compare to unscoped
print(f"\n{'='*60}")
print("COMPARISON: Scoped vs Unscoped Rankings")
print(f"{'='*60}")

try:
    unscoped = pd.read_csv(f'{OUT}/ti_790_shap_pre_visit.csv')
    unscoped = unscoped.rename(columns={'mean_abs_shap': 'unscoped_shap'})
    unscoped['unscoped_rank'] = range(1, len(unscoped)+1)

    comp = shap_imp[['feature', 'shap', 'rank', 'source']].rename(columns={'shap': 'scoped_shap', 'rank': 'scoped_rank'})
    comp = comp.merge(unscoped[['feature', 'unscoped_shap', 'unscoped_rank']], on='feature', how='left')
    comp['rank_change'] = comp['unscoped_rank'] - comp['scoped_rank']  # positive = moved UP in scoped
    comp = comp.sort_values('scoped_rank')

    print(f"\n{'#':>3} {'Feature':<25} {'Source':<20} {'Scoped':>8} {'Unscoped':>8} {'Change':>8}")
    print("-" * 78)
    for _, r in comp.head(25).iterrows():
        change = f"+{r['rank_change']:.0f}" if r['rank_change'] > 0 else f"{r['rank_change']:.0f}"
        print(f"{r['scoped_rank']:>3.0f} {r['feature']:<25} {r['source']:<20} {r['scoped_shap']:>8.4f} {r['unscoped_shap']:>8.4f} {change:>8}")

    print(f"\nBiggest movers UP (gained importance with scoping):")
    for _, r in comp.nlargest(10, 'rank_change').iterrows():
        print(f"  {r['feature']} ({r['source']}): {r['unscoped_rank']:.0f} → {r['scoped_rank']:.0f} (+{r['rank_change']:.0f})")

    print(f"\nBiggest movers DOWN (lost importance with scoping):")
    for _, r in comp.nsmallest(5, 'rank_change').iterrows():
        print(f"  {r['feature']} ({r['source']}): {r['unscoped_rank']:.0f} → {r['scoped_rank']:.0f} ({r['rank_change']:.0f})")

    comp.to_csv(f'{OUT}/ti_790_scoped_vs_unscoped.csv', index=False)
except Exception as e:
    print(f"Could not compare: {e}")

print(f"\nFiles: {OUT}/ti_790_scoped_importance.csv, ti_790_shap_scoped.png, ti_790_scoped_vs_unscoped.csv")
