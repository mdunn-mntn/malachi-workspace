---
name: reference-mntn-google-drive-access
description: "Read MNTN Google Sheets/Docs via the gcloud token (gcloud auth login --enable-gdrive-access, then the Sheets/Drive v3 REST API) — the Drive MCP connector is authed to a personal gmail and CANNOT see mountain.com files, and the local Drive mount only syncs My Drive, not Shared-with-me"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [google drive access, google sheets api, gcloud enable-gdrive-access, drive mcp wrong account, shared with me, 401 spreadsheet, docs.google.com export csv, drive scope, sheets v4, mntn drive]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-05
---

**To read any MNTN Google Sheet or Doc, use the `gcloud` token — not the Drive MCP connector.**

The Drive MCP connector is authenticated to Malachi's **personal gmail**, so every
`mountain.com`-owned or shared-with-mountain.com file returns "Requested entity was not found" and
`search_files` returns empty. The local Drive mount
(`~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/`) only carries **My Drive** and **Shared
drives** — files merely *shared with* the account (the common case for a colleague's sheet) never
appear there. An unauthenticated `docs.google.com/.../export?format=csv` returns **401**.

**Setup (one time, interactive — the user must run it):**
```bash
gcloud auth login --enable-gdrive-access
```
The default gcloud credential carries only `cloud-platform`/`compute`/`email` scopes, no Drive.
Verify: `curl -s "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=$(gcloud auth print-access-token)"`
and check `scope` contains `drive`.

**Then read anything:**
```bash
TOKEN=$(gcloud auth print-access-token)
# metadata (confirms owner + real filename — worth doing first)
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://www.googleapis.com/drive/v3/files/<ID>?fields=id,name,mimeType,owners(emailAddress),modifiedTime&supportsAllDrives=true"
# native Google Sheet: list tabs, then pull values (URL-encode tab names with spaces or '/')
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://sheets.googleapis.com/v4/spreadsheets/<ID>?fields=properties.title,sheets(properties(title,index))"
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://sheets.googleapis.com/v4/spreadsheets/<ID>/values/'<tab>'!A1:Z500?majorDimension=ROWS"
# uploaded .xlsx/.pdf (not a native Sheet): add alt=media to the Drive endpoint
curl -s -H "Authorization: Bearer $TOKEN" "https://www.googleapis.com/drive/v3/files/<ID>?alt=media" -o out.xlsx
```

**Always pull metadata first** — it reveals the owner and true filename. On BAE-4923 that exposed
that the "vendor's queries" sheet Mike linked was actually *our own* `audi_1089_verify_claims.xlsx`
(owner malachi@mountain.com), not a BAE artifact — a materially different read of the ticket.

Native Sheets need the **Sheets** API (a Drive `alt=media` GET fails on them); uploaded Office files
need **Drive** `alt=media` (the Sheets API fails on them). Check `mimeType` to pick.

Related: [[project_bae_4923_ddp_claim_validation]]
