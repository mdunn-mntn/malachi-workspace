"""
TI-809: Multi-day validation of feature rankings
Runs XGBoost + SHAP on 7 date pairs to compute confidence intervals on SHAP rankings.
Confirms top 10 NEW features are stable across days.

Usage:
  1. First run the BQ query for each date pair (see run_all_queries.sh)
  2. Then run this script: python ti_809_multiday_validation.py

Output: rank stability metrics, Spearman correlations, 95% CIs per feature
"""

import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from scipy import stats
import warnings
import os
import json
warnings.filterwarnings('ignore')

TICKET_DIR = '/Users/malachi/Developer/work/mntn/workspace/tickets/ti_809_multiday_validation'
OUTPUT_DIR = f'{TICKET_DIR}/outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Date pairs: (feature_date, label_date)
DATE_PAIRS = [
    ('2026-03-22', '2026-03-23'),  # Sun→Mon
    ('2026-03-24', '2026-03-25'),  # Mon→Tue
    ('2026-03-25', '2026-03-26'),  # Tue→Wed
    ('2026-03-27', '2026-03-28'),  # Thu→Fri
    ('2026-03-28', '2026-03-29'),  # Fri→Sat (original TI-790)
    ('2026-03-29', '2026-03-30'),  # Sat→Sun
    ('2026-03-30', '2026-03-31'),  # Sun→Mon
]

# Feature groups
EXISTING_FEATURES = [
    'n_wins_this_adv', 'n_cgs_this_adv',
    'ci_pct_new', 'ci_hh_score', 'ci_adv_hh_score', 'ci_pct_rtc',
    'ci_total_cost', 'ci_n_imp', 'al_avg_segments',
]
DROP_COLS = ['ip', 'advertiser_id', 'visited', 'n_visits']


def get_source(col):
    if col.startswith('wl_'): return 'win_logs'
    if col.startswith('ci_'): return 'cost_impression_log'
    if col.startswith('al_'): return 'augmentor_log'
    if col.startswith('bae_'): return 'bidder_auction_events'
    return 'base'


def get_tag(col):
    return 'EXISTING' if col in EXISTING_FEATURES else 'NEW'


def train_and_shap(df, feature_cols, label='visited', n_shap_samples=5000):
    """Train XGBoost, compute SHAP values, return (auc, shap_importances_df)."""
    y = df[label].copy()
    X = df[feature_cols].fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1),
        eval_metric='auc', random_state=42, n_jobs=-1, verbosity=0
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_proba)

    # SHAP
    import shap
    explainer = shap.TreeExplainer(model)
    shap_sample = X_test.sample(min(n_shap_samples, len(X_test)), random_state=42)
    shap_values = explainer.shap_values(shap_sample)

    shap_imp = pd.DataFrame({
        'feature': feature_cols,
        'mean_abs_shap': np.abs(shap_values).mean(axis=0),
        'source': [get_source(c) for c in feature_cols],
        'tag': [get_tag(c) for c in feature_cols],
    }).sort_values('mean_abs_shap', ascending=False)
    shap_imp['rank'] = range(1, len(shap_imp) + 1)

    return auc, shap_imp


def run_all_days():
    """Run XGBoost + SHAP for each date pair, collect results."""
    all_results = []
    all_shap = []

    for feat_date, label_date in DATE_PAIRS:
        csv_path = f'{OUTPUT_DIR}/ti_809_training_{feat_date}.csv'
        if not os.path.exists(csv_path):
            print(f"  SKIP {feat_date} → {label_date}: CSV not found at {csv_path}")
            continue

        print(f"\n{'='*60}")
        print(f"  {feat_date} → {label_date}")
        print(f"{'='*60}")

        df = pd.read_csv(csv_path)
        visit_rate = df['visited'].mean()
        print(f"  Rows: {len(df):,}  Visit rate: {visit_rate:.2%}")

        feature_cols = [c for c in df.columns if c not in DROP_COLS]

        # All features model
        auc_all, shap_all = train_and_shap(df, feature_cols)
        shap_all['model'] = 'all'
        shap_all['feature_date'] = feat_date
        shap_all['label_date'] = label_date
        print(f"  All-features AUC: {auc_all:.4f}")

        # NEW-only model
        new_cols = [c for c in feature_cols if c not in EXISTING_FEATURES]
        auc_new, shap_new = train_and_shap(df, new_cols)
        shap_new['model'] = 'new_only'
        shap_new['feature_date'] = feat_date
        shap_new['label_date'] = label_date
        print(f"  NEW-only AUC: {auc_new:.4f}")

        all_results.append({
            'feature_date': feat_date,
            'label_date': label_date,
            'n_rows': len(df),
            'visit_rate': visit_rate,
            'auc_all': auc_all,
            'auc_new': auc_new,
        })
        all_shap.append(shap_all)
        all_shap.append(shap_new)

    return pd.DataFrame(all_results), pd.concat(all_shap, ignore_index=True)


def compute_stability(shap_df, model_type='all'):
    """Compute rank stability metrics for a given model type."""
    subset = shap_df[shap_df['model'] == model_type].copy()
    dates = subset['feature_date'].unique()

    if len(dates) < 2:
        print(f"  Need at least 2 days for stability analysis, got {len(dates)}")
        return None

    # Pivot: features x dates with SHAP values
    pivot_shap = subset.pivot_table(
        index='feature', columns='feature_date', values='mean_abs_shap'
    )
    pivot_rank = subset.pivot_table(
        index='feature', columns='feature_date', values='rank'
    )

    stability = pd.DataFrame({
        'feature': pivot_shap.index,
        'mean_shap': pivot_shap.mean(axis=1).values,
        'std_shap': pivot_shap.std(axis=1).values,
        'mean_rank': pivot_rank.mean(axis=1).values,
        'std_rank': pivot_rank.std(axis=1).values,
        'min_rank': pivot_rank.min(axis=1).values,
        'max_rank': pivot_rank.max(axis=1).values,
        'source': [get_source(f) for f in pivot_shap.index],
        'tag': [get_tag(f) for f in pivot_shap.index],
    })

    # 95% CI on mean SHAP
    n = len(dates)
    t_crit = stats.t.ppf(0.975, df=n - 1)
    stability['ci_lower'] = stability['mean_shap'] - t_crit * stability['std_shap'] / np.sqrt(n)
    stability['ci_upper'] = stability['mean_shap'] + t_crit * stability['std_shap'] / np.sqrt(n)

    # Coefficient of variation for rank
    stability['rank_cv'] = stability['std_rank'] / stability['mean_rank']

    stability = stability.sort_values('mean_shap', ascending=False).reset_index(drop=True)
    stability.insert(0, 'overall_rank', range(1, len(stability) + 1))

    return stability


def compute_spearman_matrix(shap_df, model_type='all'):
    """Compute pairwise Spearman rank correlations between days."""
    subset = shap_df[shap_df['model'] == model_type]
    dates = sorted(subset['feature_date'].unique())

    pivot_rank = subset.pivot_table(
        index='feature', columns='feature_date', values='rank'
    ).dropna()

    n = len(dates)
    corr_matrix = np.ones((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            rho, pval = stats.spearmanr(pivot_rank[dates[i]], pivot_rank[dates[j]])
            corr_matrix[i, j] = rho
            corr_matrix[j, i] = rho

    corr_df = pd.DataFrame(corr_matrix, index=dates, columns=dates)
    return corr_df


if __name__ == '__main__':
    print("TI-809: Multi-Day Feature Ranking Validation")
    print("=" * 60)

    results_df, shap_df = run_all_days()

    if len(results_df) < 2:
        print("\nNeed at least 2 days of data. Run BQ queries first.")
        print("See: queries/ti_809_training_dataset_parameterized.sql")
        exit(1)

    # Save raw results
    results_df.to_csv(f'{OUTPUT_DIR}/ti_809_daily_results.csv', index=False)
    shap_df.to_csv(f'{OUTPUT_DIR}/ti_809_all_shap_values.csv', index=False)

    # AUC summary
    print(f"\n{'='*60}")
    print("AUC Summary Across Days")
    print(f"{'='*60}")
    print(f"  All-features: {results_df['auc_all'].mean():.4f} ± {results_df['auc_all'].std():.4f}")
    print(f"  NEW-only:     {results_df['auc_new'].mean():.4f} ± {results_df['auc_new'].std():.4f}")
    print(f"  Days:         {len(results_df)}")
    for _, r in results_df.iterrows():
        print(f"    {r['feature_date']}: all={r['auc_all']:.4f}  new={r['auc_new']:.4f}  rows={r['n_rows']:,}  vr={r['visit_rate']:.2%}")

    # Stability analysis — all features
    print(f"\n{'='*60}")
    print("Rank Stability — All Features Model")
    print(f"{'='*60}")
    stability_all = compute_stability(shap_df, 'all')
    if stability_all is not None:
        stability_all.to_csv(f'{OUTPUT_DIR}/ti_809_stability_all.csv', index=False)
        print(f"\n{'Rank':>4} {'Feature':<25} {'Tag':<8} {'Mean SHAP':>10} {'95% CI':>18} {'Rank Range':>12} {'Rank CV':>8}")
        print("-" * 90)
        for _, row in stability_all.head(20).iterrows():
            ci = f"[{row['ci_lower']:.3f}, {row['ci_upper']:.3f}]"
            rng = f"{row['min_rank']:.0f}-{row['max_rank']:.0f}"
            print(f"{row['overall_rank']:>4} {row['feature']:<25} {row['tag']:<8} {row['mean_shap']:>10.3f} {ci:>18} {rng:>12} {row['rank_cv']:>8.2f}")

    # Stability analysis — NEW only
    print(f"\n{'='*60}")
    print("Rank Stability — NEW-Only Model")
    print(f"{'='*60}")
    stability_new = compute_stability(shap_df, 'new_only')
    if stability_new is not None:
        stability_new.to_csv(f'{OUTPUT_DIR}/ti_809_stability_new.csv', index=False)
        print(f"\n{'Rank':>4} {'Feature':<25} {'Mean SHAP':>10} {'95% CI':>18} {'Rank Range':>12} {'Rank CV':>8}")
        print("-" * 85)
        for _, row in stability_new.head(15).iterrows():
            ci = f"[{row['ci_lower']:.3f}, {row['ci_upper']:.3f}]"
            rng = f"{row['min_rank']:.0f}-{row['max_rank']:.0f}"
            print(f"{row['overall_rank']:>4} {row['feature']:<25} {row['mean_shap']:>10.3f} {ci:>18} {rng:>12} {row['rank_cv']:>8.2f}")

    # Spearman correlation matrix
    print(f"\n{'='*60}")
    print("Spearman Rank Correlation — All Features")
    print(f"{'='*60}")
    corr_all = compute_spearman_matrix(shap_df, 'all')
    print(corr_all.round(3).to_string())
    mean_corr = corr_all.values[np.triu_indices_from(corr_all.values, k=1)].mean()
    print(f"\nMean pairwise Spearman ρ: {mean_corr:.3f}")

    print(f"\n{'='*60}")
    print("Spearman Rank Correlation — NEW Only")
    print(f"{'='*60}")
    corr_new = compute_spearman_matrix(shap_df, 'new_only')
    print(corr_new.round(3).to_string())
    mean_corr_new = corr_new.values[np.triu_indices_from(corr_new.values, k=1)].mean()
    print(f"\nMean pairwise Spearman ρ: {mean_corr_new:.3f}")

    # Top 10 NEW stability check
    print(f"\n{'='*60}")
    print("TOP 10 NEW FEATURES — Stability Check")
    print(f"{'='*60}")
    if stability_new is not None:
        top10 = stability_new[stability_new['tag'] == 'NEW'].head(10)
        stable = top10[top10['rank_cv'] < 0.3]
        unstable = top10[top10['rank_cv'] >= 0.3]
        print(f"\n  Stable (rank CV < 0.3): {len(stable)}/{len(top10)}")
        for _, row in stable.iterrows():
            print(f"    ✓ {row['feature']:<25} rank {row['mean_rank']:.1f} ± {row['std_rank']:.1f}")
        if len(unstable) > 0:
            print(f"\n  Unstable (rank CV ≥ 0.3): {len(unstable)}/{len(top10)}")
            for _, row in unstable.iterrows():
                print(f"    ✗ {row['feature']:<25} rank {row['mean_rank']:.1f} ± {row['std_rank']:.1f}")

    print(f"\nFiles saved to {OUTPUT_DIR}/")
