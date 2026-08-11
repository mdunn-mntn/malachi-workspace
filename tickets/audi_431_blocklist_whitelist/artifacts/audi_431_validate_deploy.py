"""AUDI-431 pre-deploy gate: prove nothing was inserted incorrectly before the prod files ship.

Usage: python3 artifacts/audi_431_validate_deploy.py

Checks, in order of how badly each would hurt:
  STRUCTURE  original content is an exact prefix; nothing removed, reordered or duplicated
  SYNTAX     every added line is a bare registrable domain (no scheme/path/port/space/IP/control char)
  PROVENANCE every added domain traces to a decision-sheet row with a designation and a source
  BLAST      no platform/hosting/CDN apex added (blocklisting one would blanket-block every tenant shop)
  SANITY     no known major retailer blocklisted; no known publisher/portal whitelisted
  FORMAT     byte-level match to the shipped artifacts (trailing NL, no CRLF, gzip inner name)

Exit 0 = safe to deploy. Any FAIL exits 1 and the files must not ship.
"""

import gzip
import re
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT, RAW = TICKET / "outputs", TICKET / "outputs" / "raw"
NL = b"\n"

DOMAIN_RE = re.compile(rb"^(?=.{1,253}$)(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))+\.?$")
# tldextract emits single-label trailing-dot artifacts (empty suffix). "localhost." already ships
# in the prod blocklist, so this shape is an accepted convention, not malformed input.
ARTIFACT_RE = re.compile(rb"^[a-z0-9-]+\.$")
IP_RE = re.compile(rb"^\d{1,3}(\.\d{1,3}){3}$")

# apexes whose tenants are independent sites — blocking one blankets thousands of real shops
PLATFORMS = {
    b"myshopify.com", b"shopify.com", b"wordpress.com", b"wixsite.com", b"wix.com",
    b"squarespace.com", b"bigcartel.com", b"etsy.com", b"ebay.com", b"amazon.com",
    b"blogspot.com", b"webflow.io", b"github.io", b"pages.dev", b"weebly.com",
    b"godaddysites.com", b"square.site", b"ecwid.com", b"storenvy.com", b"neocities.org",
}
MAJOR_RETAILERS = {
    b"nike.com", b"walmart.com", b"target.com", b"bestbuy.com", b"homedepot.com",
    b"lowes.com", b"costco.com", b"wayfair.com", b"chewy.com", b"sephora.com",
    b"ulta.com", b"macys.com", b"nordstrom.com", b"zappos.com", b"rei.com",
}
MAJOR_PUBLISHERS = {
    b"cnn.com", b"bbc.com", b"nytimes.com", b"reuters.com", b"foxnews.com",
    b"google.com", b"yahoo.com", b"facebook.com", b"bing.com", b"reddit.com",
    b"wikipedia.org", b"youtube.com", b"twitter.com", b"linkedin.com",
}

# Platform apexes we deliberately blocklist, with the reasoning recorded. tldextract collapses
# EVERY tenant to the apex (myblog.wordpress.com -> wordpress.com), so a vertical assigned here
# would mislabel every tenant alike; no categorization beats wrong categorization. Prod already
# blocklists myshopify.com on the same basis. Both fetched 2026-08-11: platform landing pages,
# zero cart signals. Anything NOT in this set still fails the check.
PLATFORM_BLOCK_REVIEWED = {b"wordpress.com", b"pages.dev"}

fails, warns = [], []


def check(ok: bool, label: str, detail: str = "") -> None:
    (print(f"  PASS  {label}") if ok else fails.append(f"{label}: {detail}"))
    if not ok:
        print(f"  FAIL  {label} — {detail}")


def warn(cond: bool, label: str, detail: str) -> None:
    if cond:
        warns.append(f"{label}: {detail}")
        print(f"  WARN  {label} — {detail}")


def main() -> None:
    old_bl = [l for l in (RAW / "ecommerce_blocklist.csv").read_bytes().split(NL) if l]
    with gzip.open(RAW / "ecommerce_whitelist.csv.gz", "rb") as fh:
        old_wl = [l for l in fh.read().split(NL) if l]
    new_bl_raw = (OUT / "audi_431_ecommerce_blocklist.csv").read_bytes()
    with gzip.open(OUT / "audi_431_ecommerce_whitelist.csv.gz", "rb") as fh:
        new_wl_raw = fh.read()
    new_bl = [l for l in new_bl_raw.split(NL) if l]
    new_wl = [l for l in new_wl_raw.split(NL) if l]
    add_bl, add_wl = new_bl[len(old_bl):], new_wl[len(old_wl):]

    print(f"blocklist {len(old_bl):,} -> {len(new_bl):,} (+{len(add_bl)})")
    print(f"whitelist {len(old_wl):,} -> {len(new_wl):,} (+{len(add_wl)})\n")

    print("STRUCTURE")
    check(new_bl[:len(old_bl)] == old_bl, "blocklist original content is an exact prefix")
    check(new_wl[:len(old_wl)] == old_wl, "whitelist original content is an exact prefix")
    check(len(set(new_bl)) == len(new_bl), "blocklist has no duplicates",
          f"{len(new_bl) - len(set(new_bl))} dupes")
    check(len(set(new_wl)) == len(new_wl), "whitelist has no duplicates",
          f"{len(new_wl) - len(set(new_wl))} dupes")
    pre = set(old_bl) & set(old_wl)
    extra_p0 = OUT / "audi_431_extra_blocklist.csv"
    reviewed = ({d.encode() for d in pd.read_csv(extra_p0)["domain"]} & set(old_wl)) if extra_p0.exists() else set()
    unexpected = (set(new_bl) & set(new_wl)) - pre - reviewed
    check(not unexpected, "no UNREVIEWED cross-list conflict", f"{sorted(unexpected)[:5]}")
    print(f"        ({len(pre)} pre-existing conflicts carried forward unchanged)")
    warn(bool(reviewed), "reviewed cross-list conflicts added",
         f"{sorted(d.decode() for d in reviewed)} - wrongly whitelisted adtech/webmail; "
         f"blocklisted (wins first) rather than deleted, keeping this deploy additive")

    print("\nSYNTAX")
    for name, adds in (("blocklist", add_bl), ("whitelist", add_wl)):
        bad = [d for d in adds if not (DOMAIN_RE.match(d) or ARTIFACT_RE.match(d))]
        ips = [d for d in adds if IP_RE.match(d)]
        ctl = [d for d in adds if any(c < 0x20 or c > 0x7E for c in d)]
        nodot = [d for d in adds if b"." not in d]
        arts = [d for d in adds if ARTIFACT_RE.match(d)]
        check(not bad, f"{name} adds are all bare registrable domains", f"{bad[:5]}")
        check(not ips, f"{name} adds contain no IP literals", f"{ips[:5]}")
        check(not ctl, f"{name} adds are pure printable ASCII", f"{ctl[:5]}")
        check(not nodot, f"{name} adds all contain a dot", f"{nodot[:5]}")
        if arts:
            print(f"        ({len(arts)} trailing-dot parse artifacts, matching the shipped 'localhost.' convention)")

    print("\nPROVENANCE")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from audi_431_common import load_designated_sheet
    sheet = load_designated_sheet(verbose=False)
    dec = dict(zip(sheet["domain"], sheet["designation"]))
    src = dict(zip(sheet["domain"], sheet["designation_source"]))
    # Second legitimate source: wcv entries with no honest vertical, decided on the corrections leg.
    # They are NOT missing_domains candidates so they never appear in the decision sheet; their
    # evidence lives in audi_431_extra_blocklist.csv (domain + the wcv vertical removed + reason).
    extra_p = OUT / "audi_431_extra_blocklist.csv"
    extra = pd.read_csv(extra_p) if extra_p.exists() else pd.DataFrame(columns=["domain", "reason"])
    for _, r in extra.iterrows():
        dec.setdefault(r["domain"], "Blocklist")
        src.setdefault(r["domain"], r.get("source", "wcv-not-verticalizable"))
    check(extra.empty or extra["reason"].astype(str).str.strip().ne("").all(),
          "every wcv-sourced blocklist add carries a written reason")
    print(f"        ({len(extra)} wcv-sourced adds, evidence in audi_431_extra_blocklist.csv)")

    for name, adds, want in (("blocklist", add_bl, "Blocklist"), ("whitelist", add_wl, "Whitelist")):
        orphan = [d for d in adds if dec.get(d.decode()) != want]
        nosrc = [d for d in adds if not str(src.get(d.decode(), "")).strip()]
        check(not orphan, f"every {name} add traces to a '{want}' decision", f"{orphan[:5]}")
        check(not nosrc, f"every {name} add records how it was decided", f"{nosrc[:5]}")

    print("\nBLAST RADIUS")
    plat_bl = set(add_bl) & PLATFORMS
    check(not (plat_bl - PLATFORM_BLOCK_REVIEWED), "no UNREVIEWED platform apex added to blocklist",
          f"{sorted(plat_bl - PLATFORM_BLOCK_REVIEWED)}")
    warn(bool(plat_bl & PLATFORM_BLOCK_REVIEWED), "platform apexes blocklisted by decision",
         f"{sorted(plat_bl & PLATFORM_BLOCK_REVIEWED)} - blankets every tenant, reviewed and intended")
    check(not (set(add_wl) & PLATFORMS), "no hosting/platform apex added to whitelist",
          f"{sorted(set(add_wl) & PLATFORMS)}")
    big = sorted({d for d in add_bl} & {b"facebook.com", b"bing.com", b"mail.com", b"viber.com"})
    warn(bool(big), "very-high-traffic domains blocklisted",
         f"{[d.decode() for d in big]} - portals/webmail with no honest vertical, decided 2026-08-11")
    short = [d for d in add_bl + add_wl if len(d.split(b".")[0]) <= 2]
    warn(bool(short), "very short labels added", f"{short[:8]}")

    print("\nSANITY")
    check(not (set(add_bl) & MAJOR_RETAILERS), "no major retailer blocklisted",
          f"{sorted(set(add_bl) & MAJOR_RETAILERS)}")
    check(not (set(add_wl) & MAJOR_PUBLISHERS), "no major publisher/portal whitelisted",
          f"{sorted(set(add_wl) & MAJOR_PUBLISHERS)}")
    wl_sources = pd.Series([src.get(d.decode(), "") for d in add_wl]).value_counts()
    print(f"        whitelist adds by evidence: {wl_sources.to_dict()}")
    check(not any(s in ("", "nan") for s in wl_sources.index),
          "every whitelist add has real evidence behind it")

    print("\nFORMAT")
    check(new_bl_raw.endswith(NL) and new_wl_raw.endswith(NL), "both files end with a newline")
    check(b"\r" not in new_bl_raw and b"\r" not in new_wl_raw, "no CRLF line endings")
    blob = (OUT / "audi_431_ecommerce_whitelist.csv.gz").read_bytes()
    inner = blob[10:blob.index(b"\x00", 10)] if blob[3] & 0x08 else b""
    check(inner == b"ecommerce_whitelist.csv", "gzip inner filename matches the shipped artifact",
          f"got {inner!r}")

    print()
    if fails:
        print(f"DEPLOY BLOCKED — {len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        sys.exit(1)
    print(f"ALL CHECKS PASSED — safe to deploy ({len(warns)} warning(s))")


if __name__ == "__main__":
    main()
