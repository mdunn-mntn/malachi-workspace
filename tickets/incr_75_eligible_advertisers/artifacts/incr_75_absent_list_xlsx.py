"""INCR-75 — workbook of eligible advertisers missing from the ghost-bid data."""
import csv
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers"
WINDOW = "2026-06-23 to 2026-07-07"

rows = list(csv.DictReader(open(f"{T}/outputs/incr_75_ghost_absent_prospectors.csv")))
df = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "Prospecting impressions in window": int(r["beeswax_prospecting_imps_2026_06_23_to_07_07"]),
} for r in rows]).sort_values("Prospecting impressions in window", ascending=False)

wb = MntnWorkbook(
    title="Ghost Bid Coverage Gap",
    ticket="INCR-75",
    subtitle="Eligible advertisers running prospecting that never appear in the ghost-bid lift data",
    period=f"Prospecting activity measured {WINDOW}",
    generated="2026-08-25",
)
wb.table(
    "Missing advertisers", df,
    finding=f"{len(df)} eligible advertisers ran prospecting on the measured bidder in the window yet have no ghost-bid rows",
    method=f"10K+ prospecting impressions on the Beeswax bidder, {WINDOW}, and zero rows in the ghost-bid lift table at any date. 689 comparable advertisers do appear.",
    formats={"Prospecting impressions in window": FMT.INT, "Advertiser ID": "0"},
    kind="headline",
    toc="The advertisers the measurement never covers",
)
wb.cover(takeaways=[
    f"{len(df)} eligible advertisers prospect on the measured bidder but never enter the ghost-bid data.",
    "Delivery, bidder, score thresholds, and targeting setup have been ruled out as the cause.",
    "Largest by volume: 7 For All Mankind, Sur La Table, Shea Homes, Seasons 52, Aceable.",
])
path = wb.save_drive(
    "INCR-75", "Ghost Bid Coverage Gap",
    drive_root="/Users/malachi/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/Tickets/INCR",
)
print("wrote", path)
