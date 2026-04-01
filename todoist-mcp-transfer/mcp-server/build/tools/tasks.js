import { z } from "zod";
export function registerTaskTools(server, api) {
    server.tool("todoist_list_tasks", "List tasks from Todoist. Can filter by project, section, label, or Todoist filter query (e.g. 'today', 'overdue', 'p1').", {
        project_id: z.string().optional().describe("Filter by project ID"),
        section_id: z.string().optional().describe("Filter by section ID"),
        label: z.string().optional().describe("Filter by label name"),
        filter: z.string().optional().describe("Todoist filter query (e.g. 'today', 'overdue', 'p1 & #Work'). When set, project_id/section_id/label are ignored."),
    }, async (params) => {
        try {
            let tasks;
            if (params.filter) {
                tasks = await api.getTasksByFilter({ query: params.filter });
            }
            else {
                tasks = await api.getTasks({
                    projectId: params.project_id,
                    sectionId: params.section_id,
                    label: params.label,
                });
            }
            return { content: [{ type: "text", text: JSON.stringify(tasks, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error listing tasks: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_get_task", "Get a single task by its ID.", {
        task_id: z.string().describe("The task ID"),
    }, async (params) => {
        try {
            const task = await api.getTask(params.task_id);
            return { content: [{ type: "text", text: JSON.stringify(task, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error getting task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_create_task", "Create a new task in Todoist. Use due_string for natural language dates OR due_date for YYYY-MM-DD, not both.", {
        content: z.string().describe("Task title/content"),
        description: z.string().optional().describe("Task description (supports markdown)"),
        project_id: z.string().optional().describe("Project ID to add task to"),
        section_id: z.string().optional().describe("Section ID within project"),
        parent_id: z.string().optional().describe("Parent task ID (makes this a subtask)"),
        labels: z.array(z.string()).optional().describe("Label names to apply"),
        priority: z.number().min(1).max(4).optional().describe("Priority: 1=normal, 2=medium, 3=high, 4=urgent (note: 4 shows as p1 in Todoist UI)"),
        due_string: z.string().optional().describe("Natural language due date (e.g. 'tomorrow at 3pm', 'every monday')"),
        due_date: z.string().optional().describe("Due date in YYYY-MM-DD format (mutually exclusive with due_string)"),
    }, async (params) => {
        try {
            const args = {
                content: params.content,
                ...(params.description && { description: params.description }),
                ...(params.project_id && { projectId: params.project_id }),
                ...(params.section_id && { sectionId: params.section_id }),
                ...(params.parent_id && { parentId: params.parent_id }),
                ...(params.labels && { labels: params.labels }),
                ...(params.priority && { priority: params.priority }),
            };
            if (params.due_string) {
                args.dueString = params.due_string;
            }
            else if (params.due_date) {
                args.dueDate = params.due_date;
            }
            const task = await api.addTask(args);
            return { content: [{ type: "text", text: JSON.stringify(task, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error creating task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_update_task", "Update an existing task. Use due_string for natural language dates OR due_date for YYYY-MM-DD, not both.", {
        task_id: z.string().describe("The task ID to update"),
        content: z.string().optional().describe("New task title/content"),
        description: z.string().optional().describe("New description"),
        labels: z.array(z.string()).optional().describe("New label names (replaces existing)"),
        priority: z.number().min(1).max(4).optional().describe("Priority: 1=normal, 2=medium, 3=high, 4=urgent"),
        due_string: z.string().optional().describe("Natural language due date, or 'no date' to clear"),
        due_date: z.string().optional().describe("Due date in YYYY-MM-DD format (mutually exclusive with due_string)"),
    }, async (params) => {
        try {
            const args = {
                ...(params.content && { content: params.content }),
                ...(params.description && { description: params.description }),
                ...(params.labels && { labels: params.labels }),
                ...(params.priority && { priority: params.priority }),
            };
            if (params.due_string) {
                args.dueString = params.due_string;
            }
            else if (params.due_date) {
                args.dueDate = params.due_date;
            }
            const task = await api.updateTask(params.task_id, args);
            return { content: [{ type: "text", text: JSON.stringify(task, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error updating task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_close_task", "Complete/close a task.", {
        task_id: z.string().describe("The task ID to close"),
    }, async (params) => {
        try {
            await api.closeTask(params.task_id);
            return { content: [{ type: "text", text: `Task ${params.task_id} closed successfully.` }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error closing task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_reopen_task", "Reopen a previously completed task.", {
        task_id: z.string().describe("The task ID to reopen"),
    }, async (params) => {
        try {
            await api.reopenTask(params.task_id);
            return { content: [{ type: "text", text: `Task ${params.task_id} reopened successfully.` }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error reopening task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_delete_task", "Permanently delete a task. This cannot be undone.", {
        task_id: z.string().describe("The task ID to delete"),
    }, async (params) => {
        try {
            await api.deleteTask(params.task_id);
            return { content: [{ type: "text", text: `Task ${params.task_id} deleted successfully.` }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error deleting task: ${error}` }], isError: true };
        }
    });
    server.tool("todoist_move_task", "Move a task to a different project, section, or parent task. Provide exactly one destination.", {
        task_id: z.string().describe("The task ID to move"),
        project_id: z.string().optional().describe("Destination project ID"),
        section_id: z.string().optional().describe("Destination section ID"),
        parent_id: z.string().optional().describe("New parent task ID (makes this a subtask)"),
    }, async (params) => {
        try {
            let moveArgs;
            if (params.project_id) {
                moveArgs = { projectId: params.project_id };
            }
            else if (params.section_id) {
                moveArgs = { sectionId: params.section_id };
            }
            else if (params.parent_id) {
                moveArgs = { parentId: params.parent_id };
            }
            else {
                return { content: [{ type: "text", text: "Error: provide exactly one of project_id, section_id, or parent_id" }], isError: true };
            }
            const task = await api.moveTask(params.task_id, moveArgs);
            return { content: [{ type: "text", text: JSON.stringify(task, null, 2) }] };
        }
        catch (error) {
            return { content: [{ type: "text", text: `Error moving task: ${error}` }], isError: true };
        }
    });
}
