"""TI-1044 — ElevenLabs CTV power analysis (two regimes: visits vs CVR).

Reuses the TI-884 Lewis-Rao MDE calculator. Establishes the headline:
  - VISIT RATE (3.07%): well-powered — ~$36k detects a 5% lift.
  - CVR (0.062%):       underpowered — ~$1.8M detects a 5% lift; at ~$1M spend, unmeasurable.

Inputs are ElevenLabs (AID 51660) actuals, matching the prefilled MDE calculator
(treated 23.1M / holdout 2.6M IPs over a 4-week flight, CPM $8.58, 4.22 imps/IP).
"""
import sys, csv, importlib.util

# import the TI-884 calculator
CALC = "/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_mde_calculator.py"
spec = importlib.util.spec_from_file_location("mde", CALC)
mde = importlib.util.module_from_spec(spec); spec.loader.exec_module(mde)

# --- ElevenLabs actuals (from prefilled calculator + Step-1 panel) ---
N_TREATED = 23.1e6      # 90% of pool, 4-week flight @ ~$1.01M/mo
N_HOLDOUT = 2.6e6       # 10% holdout
CPM = 8.58              # media CPM ($)
IMPS_PER_IP = 4.22
ACTUAL_SPEND = 1.01e6   # ~$1M/mo national
VR_STACK = 0.595        # CUPED 0.934 x ghost-ad 0.75 x stratified 0.85

REGIMES = {
    "Visit rate (IVR)": 0.0307,
    "Conversion rate (CVR)": 0.00062,
}

print(f"{'='*78}\nElevenLabs (AID 51660) — power by outcome regime\n{'='*78}")
print(f"Treated IPs {N_TREATED:,.0f} | Holdout {N_HOLDOUT:,.0f} | CPM ${CPM} | {IMPS_PER_IP} imps/IP\n")

rows = []
for name, p in REGIMES.items():
    raw_abs, raw_rel = mde.mde_binomial(N_TREATED, N_HOLDOUT, p)
    ps_abs, ps_rel = mde.mde_binomial(N_TREATED, N_HOLDOUT, p, var_reduction=VR_STACK)
    # spend to detect 2% / 5% / 8% relative lift (raw, no variance reduction)
    spend = {t: mde.spend_required(p, t, cpm=CPM, impressions_per_ip=IMPS_PER_IP)["spend_dollars"]
             for t in (0.02, 0.05, 0.08)}
    print(f"{name}  (baseline p={p*100:.3f}%)")
    print(f"  RAW MDE       : {raw_rel*100:6.2f}%  [{mde.tier_label(raw_rel)}]")
    print(f"  POST-STACK MDE: {ps_rel*100:6.2f}%  [{mde.tier_label(ps_rel)}]")
    print(f"  spend to detect  2% lift: ${spend[0.02]:>12,.0f}")
    print(f"  spend to detect  5% lift: ${spend[0.05]:>12,.0f}")
    print(f"  spend to detect  8% lift: ${spend[0.08]:>12,.0f}")
    print(f"  vs ACTUAL spend         : ${ACTUAL_SPEND:>12,.0f}  -> "
          f"{'CAN detect' if spend[0.05] <= ACTUAL_SPEND else 'CANNOT detect'} a 5% lift\n")
    rows.append({"regime": name, "baseline_pct": round(p*100, 4),
                 "mde_raw_pct": round(raw_rel*100, 2), "mde_poststack_pct": round(ps_rel*100, 2),
                 "tier_raw": mde.tier_label(raw_rel),
                 "spend_2pct": round(spend[0.02]), "spend_5pct": round(spend[0.05]),
                 "spend_8pct": round(spend[0.08]), "actual_spend": ACTUAL_SPEND})

out = "/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality/outputs/ti_1044_power_table.csv"
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print(f"saved -> {out}")
