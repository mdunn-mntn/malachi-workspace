import csv, os
BASE = "tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE, fn)) as f:
        return list(csv.DictReader(f))

ghost = {r["advertiser_id"]: r for r in load("incr_75_ghost_current_lift.csv")}
tier  = {r["advertiser_id"]: r for r in load("incr_75_final_tiered.csv")}   # 1,287 eligible
allf  = {r["advertiser_id"]: r for r in load("incr_75_all_flagged.csv")}    # 2,009 universe

gi = lambda r,k: int(r[k]) if r.get(k) not in (None,"","NaN") else 0
gf = lambda r,k: float(r[k]) if r.get(k) not in (None,"","NaN") else None

# --- merge ---
rows=[]
for aid, g in ghost.items():
    t = tier.get(aid); a = allf.get(aid)
    name = (t or a or {}).get("advertiser_name","")
    rows.append({
        "advertiser_id": aid,
        "advertiser_name": name,
        "days_present": gi(g,"days_present"),
        "ghost_ips": gi(g,"ghost_ips"),
        "treat_ips": gi(g,"treat_ips"),
        "ghost_visited": gi(g,"ghost_visited"),
        "treat_visited": gi(g,"treat_visited"),
        "abs_lift_pp": gf(g,"abs_lift_pp"),
        "rel_lift": gf(g,"rel_lift"),
        "in_eligible": aid in tier,
        "final_tier": (t or {}).get("final_tier",""),
        "value_score": (t or a or {}).get("value_score",""),
        "ivr": (t or a or {}).get("ivr",""),
        "prior_lift_pp": (t or a or {}).get("prior_lift_pp",""),
        "avg_monthly_spend": (t or a or {}).get("avg_monthly_spend",""),
    })

# write merged
outp = os.path.join(BASE,"incr_75_ghost_lift_crossref.csv")
cols = list(rows[0].keys())
with open(outp,"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(rows)

# --- summary ---
N=len(rows)
in_elig=[r for r in rows if r["in_eligible"]]
in_univ=[r for r in rows if r["advertiser_name"]]
not_in_univ=[r for r in rows if not r["advertiser_name"]]
from collections import Counter
days_dist=Counter(r["days_present"] for r in rows)
tier_dist=Counter(r["final_tier"] or "(not eligible)" for r in rows)

def treat_visited_ge(r,n): return r["treat_visited"]>=n
meas = lambda r: r["treat_visited"]>=100 and r["ghost_visited"]>=10  # crude adequacy

print(f"TABLE: enriched__dev_matthewbrorby.lift__ghost_bid_visits  (rolling 10-day window {ghost[list(ghost)[0]]['min_dt']}..{ghost[list(ghost)[0]]['max_dt']})")
print(f"Advertisers present in Matt's ghost table: {N}")
print(f"  - matched to INCR-75 universe (2,009): {len(in_univ)}")
print(f"  - in INCR-75 ELIGIBLE set (1,287):      {len(in_elig)}")
print(f"  - present in ghost table but NOT in INCR-75 universe: {len(not_in_univ)}")
print()
print("Days present distribution (max possible = 10; TTL-capped, so 30 days is unavailable):")
for d in sorted(days_dist, reverse=True):
    print(f"   {d:>2} days: {days_dist[d]:>4} advertisers")
print()
print("By INCR-75 tier (advertisers present in ghost table):")
for k in ["Top","Mid","Low","(not eligible)"]:
    print(f"   {k:>14}: {tier_dist.get(k,0)}")
print()
n_full = sum(1 for r in rows if r["days_present"]>=10)
n_meas = sum(1 for r in rows if meas(r))
print(f"Advertisers present all 10 days (max coverage): {n_full}")
print(f"Advertisers with a minimally-measurable arm (>=100 treat visits & >=10 ghost visits): {n_meas}")
print()
# Top-tier advertisers, their current-window lift
top=[r for r in rows if r["final_tier"]=="Top"]
top.sort(key=lambda r:-r["treat_visited"])
print(f"=== Top-tier INCR-75 advertisers present in ghost table ({len(top)}) — current-window lift ===")
print(f"{'AID':>6} {'name':<22} {'days':>4} {'treat_ips':>10} {'t_vis':>6} {'g_vis':>6} {'abs_pp':>10} {'rel':>8} {'prior_pp':>8}")
for r in top[:25]:
    ap = f"{r['abs_lift_pp']*100:.4f}" if r['abs_lift_pp'] is not None else "NA"
    rl = f"{r['rel_lift']*100:.1f}%" if r['rel_lift'] is not None else "NA"
    print(f"{r['advertiser_id']:>6} {r['advertiser_name'][:22]:<22} {r['days_present']:>4} {r['treat_ips']:>10} {r['treat_visited']:>6} {r['ghost_visited']:>6} {ap:>10} {rl:>8} {str(r['prior_lift_pp'])[:6]:>8}")
print(f"\nMerged file: {outp}")
