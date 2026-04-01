#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { TodoistApi } from "@doist/todoist-api-typescript";
import { registerTaskTools } from "./tools/tasks.js";
import { registerProjectTools } from "./tools/projects.js";
import { registerSectionTools } from "./tools/sections.js";
import { registerLabelTools } from "./tools/labels.js";
import { registerCommentTools } from "./tools/comments.js";

const token = process.env.TODOIST_API_TOKEN;
if (!token) {
  console.error("TODOIST_API_TOKEN environment variable is required");
  process.exit(1);
}

const api = new TodoistApi(token);
const server = new McpServer({ name: "todoist", version: "1.0.0" });

registerTaskTools(server, api);
registerProjectTools(server, api);
registerSectionTools(server, api);
registerLabelTools(server, api);
registerCommentTools(server, api);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Todoist MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
