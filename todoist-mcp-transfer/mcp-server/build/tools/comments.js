import { z } from "zod";
export function registerCommentTools(server, api) {
    server.tool("todoist_list_comments", "List comments on a task or project. Provide either task_id or project_id (not both).", {
        task_id: z.string().optional().describe("Task ID to get comments for"),
        project_id: z.string().optional().describe("Project ID to get comments for"),
    }, async (params) => {
        try {
            if (!params.task_id && !params.project_id) {
                return { content: [{ type: "text", text: "Error: provide either task_id or project_id" }], isError: true };
            }
            let comments;
            if (params.task_id) {
                comments = await api.getComments({ taskId: params.task_id });
            }
            else {
                comments = await api.getComments({ projectId: params.project_id });
            }
            return { content: [{ type: "text", text: JSON.stringify(comments, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error listing comments: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_add_comment", "Add a comment to a task or project. Provide either task_id or project_id (not both).", {
        content: z.string().describe("Comment text (supports markdown)"),
        task_id: z.string().optional().describe("Task ID to comment on"),
        project_id: z.string().optional().describe("Project ID to comment on"),
    }, async (params) => {
        try {
            if (!params.task_id && !params.project_id) {
                return { content: [{ type: "text", text: "Error: provide either task_id or project_id" }], isError: true };
            }
            let comment;
            if (params.task_id) {
                comment = await api.addComment({ content: params.content, taskId: params.task_id });
            }
            else {
                comment = await api.addComment({ content: params.content, projectId: params.project_id });
            }
            return { content: [{ type: "text", text: JSON.stringify(comment, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error adding comment: ${error}` }], isError: true };
        }
    });
}
