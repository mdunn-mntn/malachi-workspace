# Todoist MCP + Eat That Frog — Quick Start Transfer

## Step 1: Install the MCP Server

```bash
cd mcp-server
npm install
npm run build
```

## Step 2: Set Your API Token

Add to `~/.zshrc` (or `~/.bashrc`):

```bash
export TODOIST_API_TOKEN="f6feeb3aa8f7323a2abdd5e6873c764a7978ca7c"
```

Then reload: `source ~/.zshrc`

## Step 3: Register with Claude Code

Replace `/full/path/to` with wherever you put the `mcp-server` folder:

```bash
claude mcp add todoist -e TODOIST_API_TOKEN=f6feeb3aa8f7323a2abdd5e6873c764a7978ca7c -- node /full/path/to/mcp-server/build/index.js
```

## Step 4: Install the Memory Files

1. Open Claude Code from your project directory
2. Find your project memory path:
   ```bash
   ls ~/.claude/projects/
   ```
   Look for the folder matching your working directory (dashes replace slashes, e.g. `-Users-malachi-Developer-personal`)

3. Create the memory folder if it doesn't exist:
   ```bash
   mkdir -p ~/.claude/projects/<your-project-folder>/memory/
   ```

4. Copy the memory files:
   ```bash
   cp claude-memory/* ~/.claude/projects/<your-project-folder>/memory/
   ```

## Step 5: Restart Claude Code

Restart Claude Code. Verify the MCP server connects:

```bash
claude mcp list
```

You should see `todoist` with a green checkmark.

## Step 6: Test It

Open Claude Code and say: **"plan my day"**

## What's in This Package

```
todoist-mcp-transfer/
├── QUICK-START.md          ← You are here
├── mcp-server/             ← The Todoist MCP server (source only, no node_modules)
│   ├── SETUP.md            ← Detailed setup + troubleshooting
│   ├── package.json
│   ├── package-lock.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts
│       └── tools/
│           ├── comments.ts
│           ├── labels.ts
│           ├── projects.ts
│           ├── sections.ts
│           └── tasks.ts
└── claude-memory/          ← Memory files for Claude Code
    ├── MEMORY.md           ← Memory index
    ├── feedback_todoist_eat_that_frog.md  ← Eat That Frog ABCDE rules
    └── user_profile.md     ← User profile context
```
