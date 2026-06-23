"""TI-1044 — ghost-WIN simulation. Turn ghost BIDS into ghost WINS by the per-bid win rate,
which frequency-weights the control (high-frequency households more likely to 'win') to match
the served winners. Then ATT = served vs ghost-wins. Compare to raw ATT (served vs all ghost).
"""
import json, csv, math
B="/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality"
d=json.load(open(f"{B}/outputs/ti_1044_ghost_win_sim.json"))
while isinstance(d,list) and d and isinstance(d[0],list): d=d[0]
rows=[r for r in d if r.get("nbid_bucket") is not None]
real_bids=int(rows[0]["real_bid_events"])
imp=sum(float(r["ctv_imps"]) for r in csv.DictReader(open(f"{B}/outputs/ti_1044_daily_ctv_panel.csv"))
        if r.get("dt") and "2026-06-13"<=r["dt"]<="2026-06-22")
w=imp/real_bids                                    # per-bid win rate
print(f"impressions(wins)={imp:,.0f}  real_bid_events={real_bids:,}  per-bid win rate w={w:.4f}")

# served (treated) actuals from the ATT-full run (same window)
S_ip,S_guid,S_conv = 3466665, 98178, 2141
served_guid, served_conv = S_guid/S_ip, S_conv/S_ip

def sim(outcome_key):
    raw_num=raw_den=ww_num=ww_den=0.0
    for r in rows:
        n=int(r["nbid_bucket"]); hh=int(r["households"]); out=int(r[outcome_key])
        pwon=1-(1-w)**n                            # P(>=1 ghost win) for an n-bid household
        raw_num+=out; raw_den+=hh                  # all-ghost (unweighted)
        ww_num+=out*pwon; ww_den+=hh*pwon          # ghost-WINS (win-prob weighted)
    return raw_num/raw_den, ww_num/ww_den, ww_den

print(f"\n{'metric':12}{'served':>10}{'all-ghost':>11}{'ghost-WINS':>12}{'ATT raw':>10}{'ATT ghost-win':>15}")
for key,label,srate in [("visitors","guid visits",served_guid),("converters","conversions",served_conv)]:
    raw,wwin,_=sim(key)
    att_raw=srate/raw-1; att_win=srate/wwin-1
    print(f"{label:12}{srate*100:>9.3f}%{raw*100:>10.3f}%{wwin*100:>11.3f}%{att_raw*100:>+9.0f}%{att_win*100:>+14.0f}%")
print(f"\nghost-win effective N (sum P_won) = {sim('visitors')[2]:,.0f}")
print("Note: ghost-WINS frequency-weights the control up toward served; the ATT shrinks accordingly.")
