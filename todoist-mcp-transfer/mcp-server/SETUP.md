# Todoist MCP Server — Setup Instructions

## Prerequisites
- Node.js 18+
- A Todoist API token (get it from **Settings > Integrations > Developer** at todoist.com)

## Install

```bash
git clone <this-repo-url> todoist-mcp-server
cd todoist-mcp-server
npm install
npm run build
```

## Set Your API Token

Add to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
export TODOIST_API_TOKEN="your-token-here"
```

Then reload: `source ~/.zshrc`

## Register with Claude Code (recommended — bakes token into config)

```bash
claude mcp add todoist -e TODOIST_API_TOKEN=your-token-here -- node /path/to/todoist-mcp-server/build/index.js
```

Restart Claude Code after registering. Do both the shell export AND the `-e` flag — the shell export covers other tools, and the `-e` flag ensures Claude Code always has the token regardless of how it's launched.

## Register with Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "todoist": {
      "command": "node",
      "args": ["/path/to/todoist-mcp-server/build/index.js"],
      "env": {
        "TODOIST_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

## Available Tools (19)

### Tasks
- `todoist_list_tasks` — List/filter tasks by project, section, label, or filter query
- `todoist_get_task` — Get a single task by ID
- `todoist_create_task` — Create a task with description, priority, due date, labels
- `todoist_update_task` — Update task content, priority, due date, labels
- `todoist_close_task` — Complete a task
- `todoist_reopen_task` — Reopen a completed task
- `todoist_delete_task` — Permanently delete a task
- `todoist_move_task` — Move a task to a different project/section/parent

### Projects
- `todoist_list_projects` — List all projects
- `todoist_get_project` — Get project details
- `todoist_create_project` — Create a new project
- `todoist_delete_project` — Delete a project

### Sections
- `todoist_list_sections` — List sections (optionally by project)
- `todoist_create_section` — Create a section in a project
- `todoist_delete_section` — Delete a section

### Labels
- `todoist_list_labels` — List all labels
- `todoist_create_label` — Create a label
- `todoist_delete_label` — Delete a label

### Comments
- `todoist_list_comments` — List comments on a task or project
- `todoist_add_comment` — Add a comment to a task or project

## Priority Mapping (Todoist is inverted!)

| ABCDE Grade | Meaning | Todoist API Priority | Todoist UI Display |
|-------------|---------|---------------------|-------------------|
| A (Must Do) | Serious consequences | `4` | p1 (red) |
| B (Should Do) | Mild consequences | `3` | p2 (orange) |
| C (Nice to Do) | No consequences | `2` | p3 (blue) |
| D (Delegate) | Someone else can do it | `1` | p4 (grey) |
| E (Eliminate) | Don't create it | — | — |

## Troubleshooting

### MCP server shows "Failed to connect"

1. **Check the build exists:**
   ```bash
   ls /path/to/todoist-mcp-server/build/index.js
   ```
   If missing, run `npm run build` in the project directory.

2. **Check your API token is set:**
   ```bash
   echo $TODOIST_API_TOKEN
   ```
   If empty, the token isn't in your shell profile or wasn't passed via `-e` flag.

3. **Most common fix** — re-register with the token baked in:
   ```bash
   claude mcp remove todoist
   claude mcp add todoist -e TODOIST_API_TOKEN=your-token-here -- node /path/to/todoist-mcp-server/build/index.js
   ```
   Then restart Claude Code.

4. **Test the server manually:**
   ```bash
   TODOIST_API_TOKEN=your-token node /path/to/todoist-mcp-server/build/index.js
   ```
   If it crashes, check Node version (`node -v`, need 18+) and run `npm install` again.

### Token works in shell but not in Claude Code
Claude Code may launch from a GUI (VS Code, desktop app) that doesn't source `~/.zshrc`. The `-e` flag on `claude mcp add` solves this — it injects the env var directly into the server process.
