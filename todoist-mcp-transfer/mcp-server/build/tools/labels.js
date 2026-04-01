import { z } from "zod";
const TODOIST_COLORS = [
    "berry_red", "red", "orange", "yellow", "olive_green", "lime_green",
    "green", "mint_green", "teal", "sky_blue", "light_blue", "blue",
    "grape", "violet", "lavender", "magenta", "salmon", "charcoal",
    "grey", "taupe",
];
export function registerLabelTools(server, api) {
    server.tool("todoist_list_labels", "List all personal labels.", {}, async () => {
        try {
            const labels = await api.getLabels();
            return { content: [{ type: "text", text: JSON.stringify(labels, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error listing labels: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_create_label", "Create a new personal label.", {
        name: z.string().describe("Label name"),
        color: z.enum(TODOIST_COLORS).optional().describe("Color name (e.g. 'berry_red', 'blue', 'green')"),
    }, async (params) => {
        try {
            const label = await api.addLabel({
                name: params.name,
                color: params.color,
            });
            return { content: [{ type: "text", text: JSON.stringify(label, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error creating label: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_delete_label", "Delete a label. Tasks with this label will have it removed.", {
        label_id: z.string().describe("The label ID to delete"),
    }, async (params) => {
        try {
            await api.deleteLabel(params.label_id);
            return { content: [{ type: "text", text: `Label ${params.label_id} deleted successfully.` }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error deleting label: ${error}` }], isError: true };
        }
    });
}
