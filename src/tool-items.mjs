import { existsSync } from "node:fs";
import { basename, resolve } from "node:path";

function dynamicToolItem(id, tool, args, status = "inProgress") {
	return { type: "dynamicToolCall", id, namespace: null, tool, arguments: args || {}, status, contentItems: null, success: null, durationMs: null };
}

function fileChangeItem(id, tool, args, cwd, status = "inProgress") {
	const path = args?.path ? resolve(cwd, args.path) : cwd;
	const kind = tool === "write" && !existsSync(path) ? { type: "add" } : { type: "update", movePath: null };
	return { type: "fileChange", id, changes: [{ path, kind, diff: "" }], status };
}

function commandExecutionItem(id, command, cwd, status = "inProgress", commandActions = []) {
	return {
		type: "commandExecution", id, pluginId: null, scriptPath: null,
		command, cwd, processId: null, source: "agent", status,
		commandActions, aggregatedOutput: null, exitCode: null, durationMs: null,
	};
}

function resolvedToolPath(args, cwd) {
	const rawPath = args?.file_path ?? args?.path;
	return rawPath ? resolve(cwd, rawPath) : cwd;
}

function readItem(id, args, cwd, status) {
	const path = resolvedToolPath(args, cwd);
	const command = `read ${path}`;
	return commandExecutionItem(id, command, cwd, status, [{
		type: "read",
		command,
		name: basename(path) || path,
		path,
	}]);
}

function grepItem(id, args, cwd, status) {
	const path = resolvedToolPath(args, cwd);
	const query = args?.pattern || "";
	const command = `grep ${query} ${path}`;
	return commandExecutionItem(id, command, cwd, status, [{ type: "search", command, path, query }]);
}

function listFilesItem(id, tool, args, cwd, status) {
	const path = resolvedToolPath(args, cwd);
	const pattern = tool === "find" ? args?.pattern || "*" : "";
	const command = `${tool}${pattern ? ` ${pattern}` : ""} ${path}`;
	return commandExecutionItem(id, command, cwd, status, [{ type: "listFiles", command, path }]);
}

/** Convert Pi built-in tool calls to the app-server items Codex clients render. */
export function toolItemFromCall(id, tool, args, cwd, status = "inProgress") {
	if (tool === "bash" || tool === "powershell") return commandExecutionItem(id, args?.command || tool, cwd, status);
	if (tool === "read") return readItem(id, args, cwd, status);
	if (tool === "grep") return grepItem(id, args, cwd, status);
	if (tool === "find" || tool === "ls") return listFilesItem(id, tool, args, cwd, status);
	if (tool === "edit" || tool === "write") return fileChangeItem(id, tool, args, cwd, status);
	return dynamicToolItem(id, tool, args, status);
}
