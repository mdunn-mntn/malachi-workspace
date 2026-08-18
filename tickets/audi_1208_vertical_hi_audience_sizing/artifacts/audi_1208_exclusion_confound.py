"""Is the with-exclusion HI gap real, or just bigger verticals? Reads only committed outputs."""
import csv
import statistics as s
from collections import defaultdict
from math import comb

OUT = "tickets/audi_1208_vertical_hi_audience_sizing/outputs"
hi = {int(r[2]): dict(adv=int(r[0]), all_ips=int(r[3]), hi=int(r[4]))
      for r in csv.reader(open(f"{OUT}/audi_1208_hi_by_campaign_2026_08_17.csv"))}
fl = {int(r[2]): (r[7].strip().lower() == "true", r[8].strip().lower() == "true")
      for r in csv.reader(open(f"{OUT}/audi_1208_campaign_exclusion_flags_2026_08_17.csv")) if len(r) >= 9}
fu = {int(r[0]): int(r[1]) for r in csv.reader(open(f"{OUT}/audi_1208_campaign_funnel_levels.csv")) if len(r) >= 4}
av = {int(r[0]): int(r[1]) for r in csv.reader(open(f"{OUT}/audi_1208_advertiser_vertical.csv"))}
vs = {int(r[0]): int(r[4]) for r in csv.reader(open(f"{OUT}/audi_1208_vertical_sizes_2026_08_17.csv"))
      if len(r) >= 5 and r[1] == "6"}

rows = [dict(cid=k, **hi[k], excl=fl[k][1], vid=av[hi[k]["adv"]])
        for k in set(hi) & set(fl) & set(fu) if fl[k][0] and fu[k] == 1]
N = [r for r in rows if not r["excl"]]
W = [r for r in rows if r["excl"]]

for label, f in (("advertiser vertical size", lambda r: vs[r["vid"]]),
                 ("HI pool", lambda r: r["hi"]),
                 ("all scored IPs", lambda r: r["all_ips"]),
                 ("HI / own scored pool", lambda r: r["hi"] / r["all_ips"] if r["all_ips"] else 0)):
    a, b = s.median([f(r) for r in N]), s.median([f(r) for r in W])
    print(f"{label:26} no-excl {a:14,.3f}  with-excl {b:14,.3f}  {(b / a - 1) * 100:+6.1f}%")

g = defaultdict(lambda: ([], []))
for r in rows:
    g[r["vid"]][1 if r["excl"] else 0].append(r["hi"])
pairs = [(v, s.median(a), s.median(b)) for v, (a, b) in g.items() if len(a) >= 5 and len(b) >= 5]
ok = [(v, a, b) for v, a, b in pairs if a > 0]
wins = sum(1 for _, a, b in ok if b > a)
p = sum(comb(len(ok), k) for k in range(wins, len(ok) + 1)) / 2 ** len(ok)
print(f"\nwithin-vertical: with-excl higher in {wins}/{len(ok)}; "
      f"median gap {s.median([b / a - 1 for _, a, b in ok]) * 100:+.1f}%; sign-test p = {p:.2f}")
