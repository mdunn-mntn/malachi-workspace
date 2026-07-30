---
name: reference_confluence_api_access
description: Confluence REST API works READ and WRITE with the same Atlassian/Jira token — fetch pages, CQL search, create pages (Malachi's docs live in TAR space under TI Projects); WebFetch fails (auth-gated) but curl works
metadata: 
  node_type: memory
  type: reference
  originSessionId: eee87269-027a-4bd9-9df4-5666d4c3fde9
doc_type: memory
keywords: [confluence api, jira_api_token, rest api, cql search, tar space, ti projects, page id, storage format, markdown to xhtml, webfetch auth]
domain: [jira-process, routing-people]
lifecycle: active
last_verified: 2026-07-30
---
Confluence Cloud pages are auth-gated, so `WebFetch` on a `mntn.atlassian.net/wiki/...` URL just hits the Atlassian login redirect. But the **same `JIRA_API_TOKEN` (Atlassian account token) authenticates the Confluence REST API** — pull any page's full storage-format body with:

```bash
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
  "https://mntn.atlassian.net/wiki/rest/api/content/<PAGE_ID>?expand=body.storage,version,title"
```

`<PAGE_ID>` is the numeric id in the page URL (`.../pages/2927263763/Aerospike+Datastore` → `2927263763`). The `body.storage.value` field is XHTML (code blocks in `<ac:plain-text-body><![CDATA[...]]>`). Confirmed working 2026-06-09 (pulled the BP "Aerospike Datastore" page). So when given a Confluence link, hit the API directly instead of asking the user to export a PDF. Related: [[reference_jira_conventions]].

**WRITE also works (confirmed 2026-07-08 — created the MM taxonomy page 3691708511):**

```bash
# create a page (body = storage-format XHTML; Confluence validates it on POST — a 400 means bad XHTML)
jq -n --rawfile body page.html '{type:"page", title:"...", space:{key:"TAR"},
  ancestors:[{id:"3261857885"}], body:{storage:{value:$body, representation:"storage"}}}' > payload.json
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" -X POST -H "Content-Type: application/json" \
  "https://mntn.atlassian.net/wiki/rest/api/content" -d @payload.json
```

- **Malachi's docs home = TAR (Targeting) space**, tree: Targeting Department Overview → Audience Intelligence Squad Resources → **TI Projects (id `3261857885`)** — put reference docs there as siblings of "IP Scoring Pipeline Overview".
- **Find own pages via CQL:** `GET /wiki/rest/api/content/search?cql=creator=currentUser()%20order%20by%20created%20desc&expand=space,ancestors`.
- Storage-format notes: HTML entities (`&mdash;` `&cup;` etc.) are fine; `<ac:structured-macro ac:name="info">` for callouts; plain `<table><tbody><tr><th>/<td>` renders native tables.
- **Markdown → storage-format conversion (fastest path for an existing `.md`):** the python `markdown` lib emits XHTML Confluence accepts as-is. `pip install --user markdown` (works in this env), then `markdown.markdown(md, extensions=['tables','fenced_code','sane_lists'])` → put in `body.storage.value`. Drop the leading `# H1` (the page title carries it). Confirmed 2026-07-29 (published the AUDI-1176 RFD, page **3722346650**, to TAR root). Publishing to the space ROOT works (omit `ancestors`); prefer the `TI Projects` ancestor (`3261857885`) for reference docs. Get the URL from `GET .../content/<id>?expand=version` → `_links.base + _links.webui` (or the `tinyui` short link).
- **UPDATE an existing page (PUT, not POST):** re-publishing an edited `.md` to the SAME page uses `PUT /wiki/rest/api/content/<id>` with `{id, type:"page", title, space:{key:"TAR"}, version:{number:<current+1>, message:"..."}, body:{storage:{value, representation:"storage"}}}`. Two gotchas: `title` is **required** on update (reuse the current title verbatim or you rename the page), and `version.number` MUST be exactly current+1 or Confluence **409s** — so `GET .../content/<id>?expand=version` first to read the current number. **Always verify the update landed** by GET-ing `body.storage` back and grepping for your new content (a 200 on the PUT only means well-formed XHTML, not that the render is what you intended). Confirmed 2026-07-30 (bumped the AUDI-1176 RFD v1→v2 to fold in the incrementality consumer). Reusable converter+marker-check-then-PUT pattern: `scratchpad/rfd_convert.py`.
