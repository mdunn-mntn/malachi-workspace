import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { TodoistApi } from "@doist/todoist-api-typescript";
import { z } from "zod";

const TODOIST_COLORS = [
  "berry_red", "red", "orange", "yellow", "olive_green", "lime_green",
  "green", "mint_green", "teal", "sky_blue", "light_blue", "blue",
  "grape", "violet", "lavender", "magenta", "salmon", "charcoal",
  "grey", "taupe",
] as const;

export function registerProjectTools(server: McpServer, api: TodoistApi) {
  server.tool(
    "todoist_list_projects",
    "List all projects in Todoist.",
    {},
    async () => {
      try {
        const projects = await api.getProjects();
        return { content: [{ type: "text", text: JSON.stringify(projects, null, 2) }] };
      } catch (error) {
        return { content: [{ type: "text", text: `Error listing projects: ${error}` }], isError: true };
      }
    }
  );

  server.tool(
    "todoist_get_project",
    "Get details of a specific project.",
    {
      project_id: z.string().describe("The project ID"),
    },
    async (params) => {
      try {
        const project = await api.getProject(params.project_id);
        return { content: [{ type: "text", text: JSON.stringify(project, null, 2) }] };
      } catch (error) {
        return { content: [{ type: "text", text: `Error getting project: ${error}` }], isError: true };
      }
    }
  );

  server.tool(
    "todoist_create_project",
    "Create a new project.",
    {
      name: z.string().describe("Project name"),
      color: z.enum(TODOIST_COLORS).optional().describe("Color name (e.g. 'berry_red', 'blue', 'green')"),
      is_favorite: z.boolean().optional().describe("Add to favorites"),
    },
    async (params) => {
      try {
        const project = await api.addProject({
          name: params.name,
          color: params.color,
          isFavorite: params.is_favorite,
        });
        return { content: [{ type: "text", text: JSON.stringify(project, null, 2) }] };
      } catch (error) {
        return { content: [{ type: "text", text: `Error creating project: ${error}` }], isError: true };
      }
    }
  );

  server.tool(
    "todoist_delete_project",
    "Permanently delete a project and all its tasks. This cannot be undone.",
    {
      project_id: z.string().describe("The project ID to delete"),
    },
    async (params) => {
      try {
        await api.deleteProject(params.project_id);
        return { content: [{ type: "text", text: `Project ${params.project_id} deleted successfully.` }] };
      } catch (error) {
        return { content: [{ type: "text", text: `Error deleting project: ${error}` }], isError: true };
      }
    }
  );
}
