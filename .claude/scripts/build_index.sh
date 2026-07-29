#!/usr/bin/env bash
# Regenerate every knowledge/ticket index from each doc's YAML front-matter.
# Generates: knowledge/INDEX.md, knowledge/_ROUTING.md,
#            knowledge/bq/_CATALOG_INDEX.md, knowledge/bq/_TOPICS.md, knowledge/bq/_COVERAGE.md,
#            knowledge/decisions/INDEX.md, knowledge/runbooks/INDEX.md, tickets/INDEX.md
# Idempotent: same docs in -> byte-identical indexes out (no timestamps; total-ordered sections).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

python3 - "$ROOT" <<'PY'
import os, re, sys

root = sys.argv[1]
kdir = os.path.join(root, "knowledge")


def parse_front_matter(path):
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    i = 0
    while i < len(lines) and lines[i].strip() == "":
        i += 1
    if i >= len(lines) or lines[i].strip() != "---":
        return None
    fm, i = {}, i + 1
    while i < len(lines) and lines[i].strip() != "---":
        line = lines[i]; i += 1
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        key, val = line.split(":", 1)
        key, val = key.strip(), val.strip()
        # Strip a trailing inline YAML comment ( ' #...') on unquoted scalars AND on list values.
        # (Must run BEFORE the list-detection branch, or a commented '[...]' line fails the
        #  endswith(']') test and is mis-parsed as a scalar string — corrupting _TOPICS/_ROUTING.)
        if val[:1] not in ('"', "'"):
            hpos = val.find(" #")
            if hpos != -1:
                val = val[:hpos].strip()
        if val.startswith("[") and val.endswith("]"):
            inner = val[1:-1].strip()
            val = [v.strip().strip('"').strip("'") for v in inner.split(",") if v.strip()]
        else:
            val = val.strip('"').strip("'")
            if val.lower() in ("null", ""):   # keep the meaningful scalar 'none' (e.g. partition_by: none = genuinely unpartitioned)
                val = ""
        fm[key] = val
    return fm


docs = []
for dirpath, _dirs, files in os.walk(kdir):
    _dirs[:] = [d for d in _dirs if not d.startswith(("_", "."))]   # skip _staging/ + hidden dirs so fragments never leak into an index
    for fn in sorted(files):
        if not fn.endswith(".md"):   continue
        if fn.startswith("_"):       continue   # templates + generated maps (_ROUTING/_TOPICS/_COVERAGE/_CATALOG_INDEX)
        if fn == "INDEX.md":         continue   # generated
        path = os.path.join(dirpath, fn)
        fm = parse_front_matter(path)
        if not fm or "doc_type" not in fm:  continue
        fm["_abspath"] = path
        docs.append(fm)

# Also fold on-call docs (the master runbook + any future incident docs) into the index. They live in
# on-call/ — NOT knowledge/ — so every existing reference (CLAUDE.md, memory, "read this FIRST") stays
# stable, but they carry doc_type: runbook front-matter and should be keyword-retrievable via _ROUTING.md
# and listed in runbooks/INDEX.md exactly like any other runbook.
ocdir = os.path.join(root, "on-call")
if os.path.isdir(ocdir):
    for fn in sorted(os.listdir(ocdir)):
        if not fn.endswith(".md") or fn.startswith("_") or fn == "INDEX.md":  continue
        path = os.path.join(ocdir, fn)
        fm = parse_front_matter(path)
        if not fm or "doc_type" not in fm:  continue
        fm["_abspath"] = path
        docs.append(fm)

# Also fold selected ROOT-level docs (workspace-root *.md that carry doc_type front-matter, e.g.
# improvements_backlog.md) into the index. They live at the repo ROOT — not knowledge/ — so their existing
# references (CLAUDE.md Key Paths, memory) stay stable, but their keywords should also be grep-retrievable
# via _ROUTING.md. Non-recursive + doc_type-gated, so README.md / CLAUDE.md (no front-matter) are skipped.
for fn in sorted(os.listdir(root)):
    if not fn.endswith(".md") or fn.startswith("_") or fn == "INDEX.md":  continue
    path = os.path.join(root, fn)
    if not os.path.isfile(path):  continue
    fm = parse_front_matter(path)
    if not fm or "doc_type" not in fm:  continue
    fm["_abspath"] = path
    docs.append(fm)

# Memory docs (knowledge/memory/*.md) carry `name`, not `title` — give them a title so they render in
# _ROUTING.md and the memory indexes. (build_index flattens the nested `metadata:` block, so `type` is
# already present for both the flat and nested memory schemas.)
for d in docs:
    if d.get("doc_type") == "memory" and not d.get("title"):
        d["title"] = d.get("name", os.path.splitext(os.path.basename(d["_abspath"]))[0])


def link(from_dir, doc):
    return os.path.relpath(doc["_abspath"], start=from_dir)


def g(d, k, default=""):
    """Source-order join for lists (KEEP for cluster_by — ordinal is meaningful), passthrough for scalars."""
    v = d.get(k, default)
    if isinstance(v, list):
        return ", ".join(v) if v else default
    return v or default


def write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print("wrote", os.path.relpath(path, root))


GEN = "<!-- GENERATED by scripts/build_index.sh — do not edit by hand. -->"

# ---- master INDEX.md ----
order = ["routing", "bq_table", "bq_cookbook", "bq_playbook", "glossary", "decision", "runbook"]
titles = {"routing": "Start here", "bq_table": "BigQuery tables", "bq_cookbook": "Query cookbook",
          "bq_playbook": "Optimization playbook", "glossary": "Glossary", "decision": "Decisions",
          "runbook": "Runbooks"}
by_type = {}
for d in docs:
    if d["doc_type"] == "memory":   # memory has its own _MEMORY_INDEX.md; keep INDEX.md about knowledge docs
        continue
    by_type.setdefault(d["doc_type"], []).append(d)
out = [GEN, "# Knowledge Index", "",
       "Every knowledge doc, grouped by type. Open the specific doc you need — not the whole tree.",
       "For a term or symptom, start at `_ROUTING.md`; for tables by domain, `bq/_TOPICS.md`.", ""]
for t in order + [t for t in sorted(by_type) if t not in order]:
    items = by_type.get(t)
    if not items: continue
    out.append(f"## {titles.get(t, t)}")
    for d in sorted(items, key=lambda x: g(x, "title")):
        lv = g(d, "last_verified"); lv = f"  ·  _verified {lv}_" if lv else ""
        cov = ""
        if d["doc_type"] == "bq_table" and g(d, "coverage_state"):
            cov = f"  ·  _{g(d,'coverage_state')}_"
        out.append(f"- [{g(d,'title')}]({link(kdir, d)}) — {g(d,'summary')}{cov}{lv}")
    out.append("")
write(os.path.join(kdir, "INDEX.md"), "\n".join(out).rstrip() + "\n")

# ---- routing index (_ROUTING.md): keyword -> docs, across ALL doc types ----
kw_map = {}
for d in docs:
    kws = d.get("keywords")
    if isinstance(kws, list):
        for kw in kws:
            kw_map.setdefault(kw, []).append(d)
# Fold ticket-card keywords into routing too (tickets/ lives outside knowledge/; epic children nest one level
# deeper — os.walk reaches both). A ticket's TL;DR card carries `keywords:` front-matter; this makes prior work
# keyword-retrievable, not only scannable in tickets/INDEX.md.
_troot = os.path.join(root, "tickets")
if os.path.isdir(_troot):
    for _dp, _dns, _fns in os.walk(_troot):
        _dns[:] = [x for x in _dns if not x.startswith(("_", "."))]
        if "summary.md" not in _fns:
            continue
        _tfm = parse_front_matter(os.path.join(_dp, "summary.md"))
        if not _tfm or not isinstance(_tfm.get("keywords"), list) or not _tfm.get("keywords"):
            continue
        _tfm["_abspath"] = os.path.join(_dp, "summary.md")
        _tfm.setdefault("title", os.path.basename(_dp))
        for kw in _tfm["keywords"]:
            kw_map.setdefault(kw, []).append(_tfm)
out = [GEN, "# Routing — keyword → docs", "",
       "Need something specific? grep this file for your term, then open ONLY the doc(s) it names.",
       "(Generated from every knowledge doc's AND ticket card's `keywords:` front-matter. Add a keyword, rebuild, it appears here.)", ""]
for kw in sorted(kw_map):
    links = ", ".join(f"[{g(x,'title')}]({link(kdir, x)})"
                      for x in sorted(kw_map[kw], key=lambda x: g(x, "title")))
    out.append(f"- **{kw}** — {links}")
if not kw_map:
    out.append("_(no keywords yet — add `keywords:` front-matter to docs and rebuild.)_")
write(os.path.join(kdir, "_ROUTING.md"), "\n".join(out) + "\n")

# ---- memory indexes: _MEMORY_INDEX.md (browse by domain) + _MEMORY_LIFECYCLE.md (rollup + work-queue) ----
# Memory files (doc_type: memory, under knowledge/memory/) already fold into _ROUTING.md via keywords above
# — that is the primary retrieval path. These two indexes are the browse-by-domain full list and the
# lifecycle work-queue. MEMORY.md itself carries NO doc_type → it's the native-loaded hot tier, never
# generated here. Both files are idempotent (no 'today' reference — the day-relative stale threshold lives
# in health_scorecard.py, not in committed output).
mems = [d for d in docs if d["doc_type"] == "memory"]
LIFE_RANK = {"active": 0, "superseded": 1, "archived": 2}
active = [d for d in mems if g(d, "lifecycle", "active") == "active"]
inactive = [d for d in mems if g(d, "lifecycle", "active") != "active"]

mdom = {}
for d in active:
    doms = d.get("domain")
    doms = doms if isinstance(doms, list) and doms else ["(unassigned)"]
    for dm in doms:
        mdom.setdefault(dm, []).append(d)
out = [GEN, "# Memory Index — cross-session facts by domain", "",
       "The full memory list, browse-by-domain. To retrieve: grep `_ROUTING.md` for your term → open the",
       "one `memory/<file>.md` it names. Detail lives in each file; the always-loaded hot tier is `memory/MEMORY.md`.", ""]
for dm in sorted(mdom):
    out.append(f"### {dm}")
    for d in sorted(mdom[dm], key=lambda x: g(x, "title")):
        out.append(f"- [{g(d,'title')}]({link(kdir, d)}) — {g(d,'description')}  ·  "
                   f"_{g(d,'type','reference')} · verified {g(d,'last_verified','—')}_")
    out.append("")
if inactive:
    out.append("## Archived / superseded (out of the always-loaded set; still grep-reachable)")
    for d in sorted(inactive, key=lambda x: (LIFE_RANK.get(g(x, "lifecycle"), 9), g(x, "title"))):
        out.append(f"- [{g(d,'title')}]({link(kdir, d)}) — _{g(d,'lifecycle')}_ — {g(d,'description')}")
    out.append("")
write(os.path.join(kdir, "_MEMORY_INDEX.md"), "\n".join(out).rstrip() + "\n")

lc_counts = {"active": 0, "superseded": 0, "archived": 0}
for d in mems:
    lc = g(d, "lifecycle", "active"); lc_counts[lc] = lc_counts.get(lc, 0) + 1
out = [GEN, "# Memory Lifecycle", "",
       f"Rollup: **active {lc_counts['active']} · superseded {lc_counts['superseded']} · "
       f"archived {lc_counts['archived']}**", "",
       "Active memories, oldest-verified first — the refresh/dedup work-queue. "
       "(`health_scorecard.py --memory` applies the day-relative stale threshold + overlap clusters.)", "",
       "| memory | type | last_verified | doc |",
       "|--------|------|---------------|-----|"]
for d in sorted(active, key=lambda x: (g(x, "last_verified") or "0000", g(x, "title"))):
    out.append(f"| {g(d,'title')} | {g(d,'type','reference')} | {g(d,'last_verified','—')} | [doc]({link(kdir, d)}) |")
if not active:
    out.append("| _(none)_ | | | |")
write(os.path.join(kdir, "_MEMORY_LIFECYCLE.md"), "\n".join(out) + "\n")

# ---- memory recall map (_MEMORY_RECALL.tsv): compact keyword→memory for the UserPromptSubmit recall hook ----
# This Claude Code setup has no native per-file semantic recall (memory arrives only via the whole-file
# MEMORY.md startup injection). `memory_recall.py` rebuilds recall on top of this map: one line per ACTIVE
# memory NOT already in the always-loaded hot tier (MEMORY.md wikilinks) → `stem\tdescription\tkw1|kw2|…`.
# Not a `.md` and `_`-prefixed, so the crawl never re-indexes it.
mm_path = os.path.join(kdir, "memory", "MEMORY.md")
hot = set()
if os.path.exists(mm_path):
    for _m in re.findall(r"\[\[([^\]]+)\]\]", open(mm_path, encoding="utf-8").read()):
        hot.add(_m.strip().lower().replace("-", "_"))
rec_lines = []
for d in sorted(active, key=lambda x: g(x, "title")):
    stem = os.path.splitext(os.path.basename(d["_abspath"]))[0]
    if stem.lower() in hot:                       # already always-loaded — don't re-surface
        continue
    kws = d.get("keywords") if isinstance(d.get("keywords"), list) else []
    if not kws:
        continue
    desc = g(d, "description").replace("\t", " ").replace("\n", " ")
    rec_lines.append(f"{stem}\t{desc}\t{'|'.join(kws)}")
write(os.path.join(kdir, "_MEMORY_RECALL.tsv"), "\n".join(rec_lines) + ("\n" if rec_lines else ""))

# ---- BQ catalog index ----
bqdir = os.path.join(kdir, "bq")
os.makedirs(bqdir, exist_ok=True)
tables = sorted((d for d in docs if d["doc_type"] == "bq_table"), key=lambda x: g(x, "title"))
out = [GEN, "# BQ Catalog Index", "",
       "Load this first. Find the tables you need, then open ONLY those docs.",
       "`coverage`: skeleton (schema only) · enriched (curated) · verified (adversarially checked).", "",
       "| table | coverage | grain / summary | partition | time_unit | verified | doc |",
       "|-------|----------|-----------------|-----------|-----------|----------|-----|"]
for d in tables:
    out.append(f"| `{g(d,'title')}` | {g(d,'coverage_state','skeleton')} | "
               f"{g(d,'summary') or g(d,'grain')} | {g(d,'partition_by','unknown')} | "
               f"{g(d,'time_unit','unknown')} | {g(d,'last_verified') or '—'} | [doc]({link(bqdir, d)}) |")
if not tables:
    out.append("| _(none yet — run scripts/bq_introspect.sh)_ | | | | | | |")
write(os.path.join(bqdir, "_CATALOG_INDEX.md"), "\n".join(out) + "\n")

# ---- topics (_TOPICS.md): tables grouped by domain, with an (unassigned) nudge bucket ----
dom_map, unassigned = {}, []
for d in tables:
    doms = d.get("domain")
    if isinstance(doms, list) and doms:
        for dm in doms:
            dom_map.setdefault(dm, []).append(d)
    else:
        unassigned.append(d)
out = [GEN, "# BQ Topics — tables by domain", "",
       "Grouped by each table's `domain:` front-matter. `(unassigned)` = still needs a domain.", ""]
for dm in sorted(dom_map):
    out.append(f"### {dm}")
    for d in sorted(dom_map[dm], key=lambda x: g(x, "title")):
        out.append(f"- [`{g(d,'title')}`]({link(bqdir, d)}) — {g(d,'summary') or g(d,'grain')}")
    out.append("")
if unassigned:
    out.append("### (unassigned)")
    for d in sorted(unassigned, key=lambda x: g(x, "title")):
        out.append(f"- [`{g(d,'title')}`]({link(bqdir, d)}) — {g(d,'summary') or g(d,'grain')}")
    out.append("")
write(os.path.join(bqdir, "_TOPICS.md"), "\n".join(out).rstrip() + "\n")

# ---- coverage tracker (_COVERAGE.md): depth ranking + rollup + stale = the enrichment work-queue ----
rank = {"skeleton": 0, "enriched": 1, "verified": 2}
counts = {"skeleton": 0, "enriched": 0, "verified": 0}
stale_n = 0
for d in tables:
    cs = g(d, "coverage_state", "skeleton"); counts[cs] = counts.get(cs, 0) + 1
    lv, ss = g(d, "last_verified"), g(d, "schema_synced")
    if lv and ss and ss > lv:            # stale ONLY when last_verified is set (schema moved after a real verify)
        stale_n += 1


def cov_key(d):
    return (rank.get(g(d, "coverage_state", "skeleton"), 0), g(d, "last_verified") or "0000", g(d, "title"))


out = [GEN, "# BQ Catalog Coverage", "",
       f"Rollup: **skeleton {counts['skeleton']} · enriched {counts['enriched']} · "
       f"verified {counts['verified']}** · stale {stale_n}", "",
       "Worst-first — this IS the enrichment work-queue. `stale` = schema changed after the last human verify.", "",
       "| table | coverage | schema_synced | last_verified | partition | ttl_days | stale | doc |",
       "|-------|----------|---------------|---------------|-----------|----------|-------|-----|"]
for d in sorted(tables, key=cov_key):
    lv, ss = g(d, "last_verified"), g(d, "schema_synced")
    out.append(f"| `{g(d,'title')}` | {g(d,'coverage_state','skeleton')} | {ss or '—'} | {lv or '—'} | "
               f"{g(d,'partition_by','unknown')} | {g(d,'ttl_days','—')} | "
               f"{'yes' if (lv and ss and ss > lv) else ''} | [doc]({link(bqdir, d)}) |")
if not tables:
    out.append("| _(none yet)_ | | | | | | | |")
write(os.path.join(bqdir, "_COVERAGE.md"), "\n".join(out) + "\n")

# ---- decisions index ----
ddir = os.path.join(kdir, "decisions")
if os.path.isdir(ddir):
    decs = sorted((d for d in docs if d["doc_type"] == "decision"), key=lambda x: g(x, "title"))
    out = [GEN, "# Decision Log (ADR)", "",
           "| decision | summary | status | date | doc |",
           "|----------|---------|--------|------|-----|"]
    for d in decs:
        out.append(f"| {g(d,'title')} | {g(d,'summary')} | {g(d,'status','—')} | "
                   f"{g(d,'date','—')} | [doc]({link(ddir, d)}) |")
    if not decs:
        out.append("| _(none yet)_ | | | | |")
    write(os.path.join(ddir, "INDEX.md"), "\n".join(out) + "\n")

# ---- runbooks index ----
rdir = os.path.join(kdir, "runbooks")
if os.path.isdir(rdir):
    rbs = sorted((d for d in docs if d["doc_type"] == "runbook"), key=lambda x: g(x, "title"))
    out = [GEN, "# Runbooks", "",
           "| runbook | summary | verified | doc |",
           "|---------|---------|----------|-----|"]
    for d in rbs:
        out.append(f"| {g(d,'title')} | {g(d,'summary')} | {g(d,'last_verified','—')} | [doc]({link(rdir, d)}) |")
    if not rbs:
        out.append("| _(none yet)_ | | | |")
    write(os.path.join(rdir, "INDEX.md"), "\n".join(out) + "\n")

# ---- tickets index (epic-aware; flat repos stay byte-identical) ----
tdir = os.path.join(root, "tickets")


def read_ticket(dpath):
    # workspace ticket card is summary.md (fall back to README.md for kit-style tickets)
    name = os.path.basename(dpath)
    for cand in ("summary.md", "README.md"):
        rp = os.path.join(dpath, cand)
        if not os.path.exists(rp):
            continue
        fm = parse_front_matter(rp)
        if fm and fm.get("doc_type") in ("ticket", "epic"):
            fm["_abspath"] = rp
            return fm
        # Workspace ticket without front-matter (non-invasive): derive a minimal record from the
        # folder name + the first `# ` heading of the card. No file is modified.
        summ = ""
        try:
            with open(rp, encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s.startswith("# "):
                        summ = s[2:].strip()
                        break
        except Exception:
            pass
        return {"doc_type": "ticket", "title": name, "summary": summ[:100],
                "status": "", "date": "", "_abspath": rp}
    return None


entries = []
if os.path.isdir(tdir):
    for name in sorted(os.listdir(tdir)):
        d = os.path.join(tdir, name)
        if not os.path.isdir(d) or name.startswith("_") or name.startswith("."):
            continue
        fm = read_ticket(d)
        if not fm:
            continue
        children = []
        if fm.get("doc_type") == "epic":
            for cn in sorted(os.listdir(d)):
                cd = os.path.join(d, cn)
                if not os.path.isdir(cd) or cn.startswith("_") or cn.startswith("."):
                    continue
                cfm = read_ticket(cd)
                if cfm and cfm.get("doc_type") == "ticket":
                    children.append(cfm)
        fm["_children"] = children
        entries.append(fm)

    out = [GEN, "# Tickets Index", "",
           "Prior work, newest first. `result` = the blessed one-line answer (skip re-reading the folder).", "",
           "| date | ticket | summary | status | result | doc |",
           "|------|--------|---------|--------|--------|-----|"]
    for d in sorted(entries, key=lambda x: g(x, "date"), reverse=True):
        out.append(f"| {g(d,'date','—')} | {g(d,'title')} | {g(d,'summary')} | "
                   f"{g(d,'status','—')} | {g(d,'result','—')} | [doc]({link(tdir, d)}) |")
        for c in sorted(d.get("_children", []), key=lambda x: g(x, "date"), reverse=True):
            out.append(f"| {g(c,'date','—')} | ↳ {g(c,'title')} | {g(c,'summary')} | "
                       f"{g(c,'status','—')} | {g(c,'result','—')} | [doc]({link(tdir, c)}) |")
    if not entries:
        out.append("| _(none yet)_ | | | | |")
    write(os.path.join(tdir, "INDEX.md"), "\n".join(out) + "\n")

n_children = sum(len(e.get("_children", [])) for e in entries)
print(f"indexed {len(docs)} knowledge docs ({len(tables)} tables, {len(mems)} memory), "
      f"{len(entries)} top-level tickets/epics (+{n_children} children)")
PY
