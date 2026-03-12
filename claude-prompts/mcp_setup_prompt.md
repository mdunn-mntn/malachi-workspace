# MCP Server Setup for MNTN Workspace

## Goal
Set up MCP (Model Context Protocol) servers so Claude Code has native tool access to BigQuery, GitHub, Jira, and Google Drive. Currently zero MCP servers are configured.

## Environment Context
- **Workspace**: `/Users/malachi/Developer/work/mntn/workspace`
- **GCP**: Authenticated as `malachi@mountain.com`, primary project `dw-main-silver`
- **Jira**: API token in `$JIRA_API_TOKEN`, base URL in `$JIRA_BASE_URL` (both set in `~/.zshrc`)
- **GitHub**: Repo `git@github.com:mdunn-mntn/malachi-workspace.git`
- **Google Drive**: Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`
- **Existing config**: `~/.claude/settings.json` has BQ/gcloud bash permissions, no MCP servers configured
- **Platform**: macOS (Darwin)

## Instructions

### Step 1: Research Available MCP Servers

Before installing anything, search for the current best MCP server for each service. MCP is evolving fast — package names and availability change frequently.

```bash
# Search npm for BigQuery MCP servers
npm search mcp bigquery 2>/dev/null | head -20

# Search for Jira MCP servers
npm search mcp jira 2>/dev/null | head -20

# Search for Google Drive MCP servers
npm search mcp google-drive 2>/dev/null | head -20

# Check what claude mcp search finds (if available)
claude mcp search bigquery 2>/dev/null
claude mcp search jira 2>/dev/null
claude mcp search google-drive 2>/dev/null
```

Also check these known packages (verify they exist before using):
- BigQuery: `@google-cloud/bigquery-mcp`, `@anthropic-ai/mcp-bigquery`, `mcp-server-bigquery`
- Jira: `@anthropic-ai/mcp-jira`, `mcp-server-jira`, `@modelcontextprotocol/server-jira`
- Google Drive: `@anthropic-ai/mcp-google-drive`, `@modelcontextprotocol/server-gdrive`

### Step 2: Set Up GitHub MCP (Most Mature — Do This First)

GitHub has an official MCP endpoint. This is the most reliable one to start with.

```bash
cd /Users/malachi/Developer/work/mntn/workspace

claude mcp add --transport http github --scope project \
  https://api.githubcopilot.com/mcp/
```

Verify:
```bash
claude mcp list
claude mcp get github
```

After adding, launch Claude Code and run `/mcp` to authenticate with GitHub.

### Step 3: Set Up BigQuery MCP

Based on what you found in Step 1, add the BigQuery server. Example commands (adjust package name based on research):

```bash
# Option A: If @google-cloud/bigquery-mcp exists
claude mcp add --transport stdio bigquery --scope project \
  --env GOOGLE_CLOUD_PROJECT=dw-main-silver \
  -- npx -y @google-cloud/bigquery-mcp

# Option B: If a different package name
claude mcp add --transport stdio bigquery --scope project \
  --env GOOGLE_CLOUD_PROJECT=dw-main-silver \
  -- npx -y <correct-package-name>
```

**Important BQ context**: We query across multiple projects:
- `dw-main-silver` — logdata, summarydata, aggregates, core (primary)
- `dw-main-bronze` — raw, integrationprod, external, tpa

The MCP server should use `dw-main-silver` as default but allow cross-project queries. Verify the server supports this before committing to it.

**Safety**: We need READ-ONLY access. No DDL/DML. If the MCP server doesn't enforce read-only mode, check if there's a config flag for it, or note this as a risk.

### Step 4: Set Up Jira MCP

```bash
# Adjust package name based on Step 1 research
claude mcp add --transport stdio jira --scope project \
  --env JIRA_API_TOKEN="$JIRA_API_TOKEN" \
  --env JIRA_BASE_URL="$JIRA_BASE_URL" \
  --env JIRA_EMAIL="malachi@mountain.com" \
  -- npx -y <jira-mcp-package>
```

If no good stdio server exists, check if Atlassian offers an HTTP MCP endpoint (similar to GitHub's).

### Step 5: Set Up Google Drive MCP

```bash
# Adjust package name based on Step 1 research
claude mcp add --transport stdio google-drive --scope project \
  -- npx -y <google-drive-mcp-package>
```

This will likely need OAuth authentication via `/mcp` after adding.

### Step 6: Verify Everything

```bash
# List all configured servers
claude mcp list

# Check the generated config
cat /Users/malachi/Developer/work/mntn/workspace/.mcp.json
```

Launch Claude Code and run `/mcp` to:
1. Check all servers show as connected
2. Authenticate any that need OAuth (GitHub, Google Drive)
3. Test a simple operation on each (e.g., list BQ datasets, fetch a Jira ticket)

### Step 7: Commit

```bash
cd /Users/malachi/Developer/work/mntn/workspace
git add .mcp.json
git commit -m "add MCP server configuration for github, bigquery, jira, google drive"
git push origin main
```

## Fallback: If a Package Doesn't Exist

If npm search turns up nothing for a service:
1. Check https://github.com/modelcontextprotocol/servers for the official list
2. Check https://mcp.so or https://glama.ai/mcp/servers for community servers
3. If nothing exists yet, skip that server and note it — MCP ecosystem is growing fast
4. For Jira specifically: the existing `curl` + `$JIRA_API_TOKEN` workflow in CLAUDE.md works fine as a fallback

## Success Criteria
- `.mcp.json` exists at workspace root with at least GitHub and BigQuery configured
- `claude mcp list` shows all servers
- Each server is authenticated and functional
- Config is committed and pushed

## What NOT to Do
- Don't modify `~/.claude/settings.json` — MCP config goes in `.mcp.json`
- Don't install MCP servers globally — use `npx -y` for stdio servers
- Don't add API tokens directly in `.mcp.json` — use `--env` to reference environment variables
- Don't add `Co-Authored-By` lines in commit messages
