import { z } from "zod";
export function registerSectionTools(server, api) {
    server.tool("todoist_list_sections", "List sections, optionally filtered by project.", {
        project_id: z.string().optional().describe("Filter by project ID"),
    }, async (params) => {
        try {
            const sections = await api.getSections(params.project_id ? { projectId: params.project_id } : undefined);
            return { content: [{ type: "text", text: JSON.stringify(sections, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error listing sections: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_create_section", "Create a new section within a project.", {
        name: z.string().describe("Section name"),
        project_id: z.string().describe("Project ID to create the section in"),
    }, async (params) => {
        try {
            const section = await api.addSection({
                name: params.name,
                projectId: params.project_id,
            });
            return { content: [{ type: "text", text: JSON.stringify(section, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error creating section: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_delete_section", "Delete a section and move its tasks to the parent project. This cannot be undone.", {
        section_id: z.string().describe("The section ID to delete"),
    }, async (params) => {
        try {
            await api.deleteSection(params.section_id);
            return { content: [{ type: "text", text: `Section ${params.section_id} deleted successfully.` }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error deleting section: ${error}` }], isError: true };
        }
    });
}
