#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { createConnection, createServer as createNetServer } from "node:net";
import { chmodSync, mkdirSync, existsSync, unlinkSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import {
	createAgentSession,
	ModelRuntime,
	SessionManager,
	resolveModelScopeWithDiagnostics,
} from "@earendil-works/pi-coding-agent";

const VERSION = "0.1.0";
const agentDir = process.env.PI_CODING_AGENT_DIR || join(homedir(), ".pi", "agent");
const defaultSocket = join(agentDir, "app-server", "control.sock");
const execFileSyncSafe = (file, args) => {
	try {
		return execFileSync(file, args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 2000 });
	} catch {
		return "";
	}
};
const CODEX_APP_SERVER_VERSION = execFileSyncSafe("codex", ["--version"]).match(/\d+\.\d+\.\d+/)?.[0] || VERSION;
const REMOTE_SEGMENT_TARGET_BYTES = 100 * 1024;
const REMOTE_SEGMENT_MAX_BYTES = 150 * 1024;
const REMOTE_MESSAGE_MAX_BYTES = 100 * 1024 * 1024;
const REMOTE_SEGMENT_COUNT_MAX = 1024;
const REMOTE_OUTBOUND_MAX_BYTES = 16 * 1024 * 1024;
const REMOTE_CHUNK_BUFFER_MAX_BYTES = 128 * 1024 * 1024;

function parseArgs(argv) {
	const options = { command: "start", socket: defaultSocket, host: "127.0.0.1" };
	let index = 0;
	if (argv[0] && !argv[0].startsWith("-")) options.command = argv[index++];
	while (index < argv.length) {
		const arg = argv[index++];
		switch (arg) {
			case "--socket": options.socket = resolve(argv[index++]); break;
			case "--host": options.host = argv[index++]; break;
			case "--port": options.port = Number(argv[index++]); break;
			case "--session": options.session = resolve(argv[index++]); break;
			case "--pid": options.pid = Number(argv[index++]); break;
			case "--force": options.force = true; break;
			case "--remote": options.remote = true; break;
			case "--models": options.models = String(argv[index++] || "").split(",").map((pattern) => pattern.trim()).filter(Boolean); break;
			case "--remote-url": options.remoteUrl = argv[index++]; break;
			case "--wait": options.wait = true; break;
			case "--re-enroll": options.reEnroll = true; break;
			case "--help": options.help = true; break;
			default: throw new Error(`Unknown option: ${arg}`);
		}
	}
	return options;
}

function shellQuote(value) {
	return `'${String(value).replaceAll("'", "'\\''")}'`;
}

function resumeCommand(sessionFile) {
	return `pi --session ${shellQuote(sessionFile)}`;
}

function processExists(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

async function unixSocketIsLive(socketPath) {
	return new Promise((resolvePromise) => {
		const socket = createConnection(socketPath);
		const finish = (live) => {
			socket.destroy();
			resolvePromise(live);
		};
		socket.setTimeout(500, () => finish(false));
		socket.once("connect", () => finish(true));
		socket.once("error", () => finish(false));
	});
}

function isLoopbackHost(host) {
	return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

function sessionIdFromFile(sessionFile) {
	try {
		const firstLine = readFileSync(sessionFile, "utf8").split("\n", 1)[0];
		return JSON.parse(firstLine).id;
	} catch {
		return undefined;
	}
}

function sessionControlSocket(sessionFile) {
	const sessionId = sessionIdFromFile(sessionFile);
	return sessionId ? join(agentDir, "app-server", "session-control", `${sessionId}.sock`) : undefined;
}

function requestSessionShutdown(sessionFile) {
	const socketPath = sessionControlSocket(sessionFile);
	if (!socketPath) return Promise.resolve(undefined);
	return new Promise((resolvePromise) => {
		let settled = false;
		let buffer = "";
		const finish = (response) => {
			if (settled) return;
			settled = true;
			socket.destroy();
			resolvePromise(response);
		};
		const socket = createConnection(socketPath);
		socket.setTimeout(1000, () => finish(undefined));
		socket.on("connect", () => socket.write('{"command":"shutdown"}\n'));
		socket.on("data", (chunk) => {
			buffer += chunk.toString();
			const line = buffer.split("\n", 1)[0];
			if (!line) return;
			try {
				finish(JSON.parse(line));
			} catch {
				finish(undefined);
			}
		});
		socket.on("error", () => finish(undefined));
	});
}

function findSessionProcess(sessionFile) {
	const pids = execFileSyncSafe("lsof", ["-t", "--", sessionFile])
		.split(/\s+/)
		.map(Number)
		.filter((pid) => pid && pid !== process.pid);
	for (const pid of pids) {
		const command = execFileSyncSafe("ps", ["-p", String(pid), "-o", "command="]).trim();
		if (/\bpi(?:$|\s)|pi-coding-agent/.test(command) && !command.includes("pi-remote-daemon")) return pid;
	}
	return undefined;
}

async function waitForExit(pid, timeoutMs) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (!processExists(pid)) return true;
		await new Promise((resolvePromise) => setTimeout(resolvePromise, 100));
	}
	return !processExists(pid);
}

async function takeOver(sessionFile, requestedPid, force, quiet = false) {
	if (!existsSync(sessionFile)) throw new Error(`Session file does not exist: ${sessionFile}`);
	const bridgeResponse = await requestSessionShutdown(sessionFile);
	const pid = requestedPid || bridgeResponse?.pid || findSessionProcess(sessionFile);
	if (!pid) {
		if (!quiet) {
			console.log(`No running Pi TUI found for ${sessionFile}`);
			console.log(`Resume command: ${resumeCommand(sessionFile)}`);
		}
		return;
	}
	if (pid === process.pid) throw new Error("Refusing to terminate the daemon itself");
	if (bridgeResponse?.accepted) {
		if (!(await waitForExit(pid, 5000))) {
			if (!force) throw new Error(`Pi TUI ${pid} did not exit after graceful shutdown; rerun with --force`);
			process.kill(pid, "SIGKILL");
			if (!(await waitForExit(pid, 2000))) throw new Error(`Could not terminate Pi TUI ${pid}`);
		}
	} else {
		process.kill(pid, "SIGTERM");
		if (!(await waitForExit(pid, 5000))) {
			if (!force) throw new Error(`Pi TUI ${pid} did not exit; rerun with --force`);
			process.kill(pid, "SIGKILL");
			if (!(await waitForExit(pid, 2000))) throw new Error(`Could not terminate Pi TUI ${pid}`);
		}
	}
	if (!quiet) {
		console.log(`Stopped Pi TUI ${pid}`);
		console.log(`Resume command: ${resumeCommand(sessionFile)}`);
	}
}

function textFromContent(content) {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content.filter((part) => part?.type === "text").map((part) => part.text || "").join("");
}

function textFromInput(input) {
	return (input || []).filter((part) => part?.type === "text").map((part) => part.text || "").join("\n");
}

function timestampSeconds(value, fallback = Date.now()) {
	const timestamp = typeof value === "number" ? value : Date.parse(value || "");
	return Math.floor((Number.isFinite(timestamp) ? timestamp : fallback) / 1000);
}

function strictTimestampSeconds(value, label) {
	const timestamp = typeof value === "number" ? value : Date.parse(value || "");
	if (!Number.isFinite(timestamp)) throw new Error(`${label} is not a valid timestamp`);
	return Math.floor(typeof value === "number" && value < 10_000_000_000 ? value : timestamp / 1000);
}

function userInputsFromContent(content) {
	if (typeof content === "string") return content ? [{ type: "text", text: content, text_elements: [] }] : [];
	if (!Array.isArray(content)) return [];
	return content.flatMap((part) => {
		if (part?.type === "text" && part.text) return [{ type: "text", text: part.text, text_elements: [] }];
		if (part?.type !== "image") return [];
		if (part.source?.type === "url") return [{ type: "image", url: part.source.url }];
		const data = part.data || part.source?.data;
		const mimeType = part.mimeType || part.source?.mediaType;
		return data && mimeType ? [{ type: "image", url: `data:${mimeType};base64,${data}` }] : [];
	});
}

function dynamicToolItem(id, tool, args, status = "inProgress") {
	return { type: "dynamicToolCall", id, namespace: null, tool, arguments: args || {}, status, contentItems: null, success: null, durationMs: null };
}

function fileChangeItem(id, tool, args, cwd, status = "inProgress") {
	const path = args?.path ? resolve(cwd, args.path) : cwd;
	const kind = tool === "write" && !existsSync(path) ? { type: "add" } : { type: "update", movePath: null };
	return { type: "fileChange", id, changes: [{ path, kind, diff: "" }], status };
}

function commandExecutionItem(id, tool, args, cwd, status = "inProgress") {
	return {
		type: "commandExecution", id, pluginId: null, scriptPath: null,
		command: args?.command || tool, cwd, processId: null, source: "agent", status,
		commandActions: [], aggregatedOutput: null, exitCode: null, durationMs: null,
	};
}

function toolItemFromCall(id, tool, args, cwd, status = "inProgress") {
	if (tool === "bash" || tool === "powershell") return commandExecutionItem(id, tool, args, cwd, status);
	if (tool === "edit" || tool === "write") return fileChangeItem(id, tool, args, cwd, status);
	return dynamicToolItem(id, tool, args, status);
}

function applyToolResult(item, message) {
	if (!item) return;
	const text = textFromContent(message?.content);
	const failed = Boolean(message?.isError);
	item.status = failed ? "failed" : "completed";
	item.durationMs ??= null;
	if (item.type === "commandExecution") {
		item.aggregatedOutput = text;
		item.exitCode = failed ? 1 : 0;
	} else if (item.type === "fileChange") {
		const diff = message?.details?.diff || message?.details?.patch || text;
		for (const change of item.changes) change.diff = diff || "";
	} else if (item.type === "dynamicToolCall") {
		item.contentItems = text ? [{ type: "inputText", text }] : [];
		item.success = !failed;
	}
}

function turnsFromManager(manager) {
	const entries = manager.getBranch();
	const turnMetadata = new Map();
	for (const entry of entries) {
		if (entry.type === "custom" && entry.customType === REMOTE_METADATA_TYPE && entry.data?.kind === "turn") {
			turnMetadata.set(entry.data.userEntryId, entry.data);
		}
	}
	const turns = [];
	let current;
	let toolItems = new Map();
	for (const entry of entries) {
		if (entry.type === "compaction" && current) {
			current.items.push({ type: "contextCompaction", id: `compaction-${entry.id}` });
			continue;
		}
		if (entry.type !== "message") continue;
		const message = entry.message;
		const occurredAt = timestampSeconds(message?.timestamp || entry.timestamp);
		if (message?.role === "user") {
			const metadata = turnMetadata.get(entry.id);
			current = {
				id: metadata?.turnId || `turn-${entry.id}`,
				items: [], itemsView: "full", status: metadata?.status || "completed",
				error: metadata?.error ? { message: metadata.error, codexErrorInfo: null, additionalDetails: null, misalignment: null } : null,
				startedAt: occurredAt, completedAt: metadata?.completedAt || occurredAt, durationMs: 0,
			};
			Object.defineProperty(current, "persistedCompletedAt", { value: metadata?.completedAt || null });
			const content = userInputsFromContent(message.content);
			if (content.length) current.items.push({ type: "userMessage", id: metadata?.userItemId || `user-${entry.id}`, clientId: metadata?.clientUserMessageId || null, content });
			turns.push(current);
			toolItems = new Map();
			continue;
		}
		if (!current) continue;
		current.completedAt = current.persistedCompletedAt || occurredAt;
		current.durationMs = Math.max(0, current.completedAt * 1000 - current.startedAt * 1000);
		if (message?.role === "assistant") {
			for (let index = 0; index < (message.content || []).length; index++) {
				const part = message.content[index];
				const itemId = `${entry.id}-${index}`;
				if (part?.type === "text" && part.text) current.items.push({ type: "agentMessage", id: itemId, text: part.text, phase: null, memoryCitation: null, delivery: null, questions: null });
				if (part?.type === "thinking" && part.thinking) current.items.push({ type: "reasoning", id: itemId, summary: [], content: [part.thinking] });
				if (part?.type === "toolCall") {
					const item = toolItemFromCall(part.id || itemId, part.name || "tool", part.arguments, manager.getCwd() || process.cwd());
					current.items.push(item);
					toolItems.set(part.id, item);
				}
			}
			if (message.stopReason === "error") {
				current.status = "failed";
				current.error = { message: message.errorMessage || "Model request failed", codexErrorInfo: null, additionalDetails: null, misalignment: null };
			} else if (message.stopReason === "aborted") current.status = "interrupted";
		}
		if (message?.role === "toolResult") applyToolResult(toolItems.get(message.toolCallId), message);
	}
	return turns;
}

const REMOTE_METADATA_TYPE = "pi-remote-daemon";

function page(items, params = {}, defaultDirection = "asc") {
	const limit = Math.max(1, Math.min(Number(params.limit) || 50, 100));
	const direction = params.sortDirection || defaultDirection;
	const ordered = direction === "desc" ? [...items].reverse() : items;
	const offset = Math.max(0, Number.parseInt(params.cursor || "0", 10) || 0);
	const data = ordered.slice(offset, offset + limit);
	return { data, nextCursor: offset + data.length < ordered.length ? String(offset + data.length) : null, backwardsCursor: data.length ? String(offset) : null };
}

function remoteState(manager) {
	const state = { goal: null, queue: [], turns: new Map() };
	for (const entry of manager.getEntries()) {
		if (entry.type !== "custom" || entry.customType !== REMOTE_METADATA_TYPE || !entry.data || typeof entry.data !== "object") continue;
		if (entry.data.kind === "goal") state.goal = entry.data.goal || null;
		if (entry.data.kind === "queue") state.queue = Array.isArray(entry.data.queue) ? entry.data.queue : [];
		if (entry.data.kind === "turn" && entry.data.turnId && entry.data.userEntryId) state.turns.set(entry.data.turnId, entry.data);
	}
	return state;
}

function saveRemoteState(manager, kind, value) {
	manager.appendCustomEntry(REMOTE_METADATA_TYPE, kind === "turn" ? { kind, ...value } : { kind, [kind]: value });
}

function sessionMessages(manager) {
	return manager.getBranch().filter((entry) => entry.type === "message").map((entry) => entry.message);
}

function effortForCodex(effort) {
	return effort === "off" ? "none" : effort === "max" ? "ultra" : effort;
}

function modelForCodex(model, scopedModel) {
	const levels = model.reasoning ? Object.keys(model.thinkingLevelMap || { minimal: true, low: true, medium: true, high: true, xhigh: true }).map(effortForCodex) : [];
	const supportedReasoningEfforts = [...new Set(levels)].map((reasoningEffort) => ({ reasoningEffort, description: `${reasoningEffort} reasoning` }));
	const defaultReasoningEffort = effortForCodex(scopedModel?.thinkingLevel || (model.reasoning ? (levels.includes("medium") ? "medium" : levels[0]) : "none"));
	return {
		id: `${model.provider}/${model.id}`,
		model: model.id,
		upgrade: null,
		upgradeInfo: null,
		availabilityNux: null,
		displayName: model.name || model.id,
		description: "Pi model",
		modelSpecialty: null,
		hidden: false,
		supportedReasoningEfforts,
		defaultReasoningEffort,
		inputModalities: model.input || ["text"],
		supportsPersonality: false,
		multiAgentVersion: null,
		additionalSpeedTiers: [],
		serviceTiers: [],
		defaultServiceTier: null,
		isDefault: false,
	};
}

function normalizeRemoteControlBaseUrl(value) {
	const url = new URL(value || "https://chatgpt.com/backend-api");
	const host = url.hostname.toLowerCase();
	const isChatGpt = host === "chatgpt.com" || host === "chatgpt-staging.com" || host.endsWith(".chatgpt.com") || host.endsWith(".chatgpt-staging.com");
	const isLocal = host === "localhost" || host === "127.0.0.1" || host === "[::1]" || host === "::1";
	if ((url.protocol !== "https:" || !isChatGpt) && !((url.protocol === "http:" || url.protocol === "https:") && isLocal)) {
		throw new Error("remote URL must use HTTPS on chatgpt.com/chatgpt-staging.com, or HTTP/HTTPS on localhost");
	}
	if (!url.pathname.endsWith("/")) url.pathname += "/";
	return url.toString();
}

function pairingResponseForCodex(pairing) {
	const expiresAt = strictTimestampSeconds(pairing.expires_at, "Remote-control pairing expires_at");
	if (!pairing.pairing_code || !pairing.environment_id) throw new Error("Remote-control pairing response is incomplete");
	return {
		pairingCode: pairing.pairing_code,
		manualPairingCode: pairing.manual_pairing_code || null,
		environmentId: pairing.environment_id,
		expiresAt,
	};
}

function remoteClientForCodex(client) {
	return {
		clientId: client.client_id,
		displayName: client.display_name ?? null,
		deviceType: client.device_type ?? null,
		platform: client.platform ?? null,
		osVersion: client.os_version ?? null,
		deviceModel: client.device_model ?? null,
		appVersion: client.app_version ?? null,
		lastSeenAt: client.last_seen_at ? strictTimestampSeconds(client.last_seen_at, "Remote-control client last_seen_at") : null,
	};
}

class RemoteControlClient {
	constructor(daemon, options) {
		this.daemon = daemon;
		this.options = options;
		this.baseUrl = normalizeRemoteControlBaseUrl(options.remoteUrl);
		this.subscribeCursor = undefined;
		this.lastInboundSeq = new Map();
		this.legacyStreams = new Map();
		this.heartbeatTimer = undefined;
		this.lastPongAt = 0;
		this.statePath = join(agentDir, "app-server", "remote-control.json");
		this.installationId = undefined;
		this.enrollment = undefined;
		this.socket = undefined;
		this.reconnectTimer = undefined;
		this.refreshTimer = undefined;
		this.reconnectAttempt = 0;
		this.outbound = new Map();
		this.outboundBytes = 0;
		this.chunks = new Map();
		this.chunkBytes = 0;
		this.inboundChain = Promise.resolve();
		this.connectionStatus = "disabled";
		this.stopped = false;
	}

	loadState() {
		try {
			const state = JSON.parse(readFileSync(this.statePath, "utf8"));
			this.installationId = state.installationId;
			this.enrollment = state.enrollment;
		} catch {}
		if (!this.installationId) this.installationId = randomUUID();
	}

	saveState() {
		const directory = join(this.statePath, "..");
		mkdirSync(directory, { recursive: true, mode: 0o700 });
		chmodSync(directory, 0o700);
		writeFileSync(this.statePath, JSON.stringify({ installationId: this.installationId, enrollment: this.enrollment }, null, 2) + "\n", { mode: 0o600 });
		chmodSync(this.statePath, 0o600);
	}

	remoteUrls() {
		return {
			enroll: new URL("wham/remote/control/server/enroll", this.baseUrl).toString(),
			refresh: new URL("wham/remote/control/server/refresh", this.baseUrl).toString(),
			pair: new URL("wham/remote/control/server/pair", this.baseUrl).toString(),
			pairStatus: new URL("wham/remote/control/server/pair/status", this.baseUrl).toString(),
			websocket: new URL("wham/remote/control/server", this.baseUrl).toString().replace(/^http/, "ws"),
		};
	}

	async authHeaders() {
		const codexHome = process.env.CODEX_HOME || join(homedir(), ".codex");
		let auth;
		try {
			auth = JSON.parse(readFileSync(join(codexHome, "auth.json"), "utf8"));
		} catch {
			throw new Error(`ChatGPT auth not found at ${join(codexHome, "auth.json")}`);
		}
		const accessToken = auth.tokens?.access_token;
		const accountId = auth.tokens?.account_id;
		if (!accessToken || !accountId) throw new Error("Codex ChatGPT auth is missing access_token or account_id");
		return {
			Authorization: `Bearer ${accessToken}`,
			"chatgpt-account-id": accountId,
			"x-codex-installation-id": this.installationId,
			"content-type": "application/json",
		};
	}

	async remoteFetch(url, options) {
		return fetch(url, { ...options, redirect: "error", signal: AbortSignal.timeout(30_000) });
	}

	async updateEnrollment(response, action) {
		const enrolled = await response.json();
		if (!enrolled.server_id || !enrolled.environment_id || !enrolled.remote_control_token || !Number.isFinite(Date.parse(enrolled.expires_at || ""))) throw new Error(`Remote-control ${action} response is incomplete`);
		if (this.enrollment && !this.options.reEnroll && (enrolled.server_id !== this.enrollment.serverId || enrolled.environment_id !== this.enrollment.environmentId)) {
			throw new Error(`Remote-control ${action} returned a different server or environment`);
		}
		this.enrollment = {
			serverId: enrolled.server_id,
			environmentId: enrolled.environment_id,
			remoteControlToken: enrolled.remote_control_token,
			expiresAt: enrolled.expires_at,
			serverName: this.enrollment?.serverName || this.options.serverName || `${process.env.HOSTNAME || "pi-host"}-pi`,
		};
		this.saveState();
		this.scheduleRefresh();
		console.error(`[remote] ${action} server=${this.enrollment.serverId} environment=${this.enrollment.environmentId} expires=${this.enrollment.expiresAt}`);
		return this.enrollment;
	}

	async refreshEnrollment() {
		if (!this.enrollment) throw new Error("Remote-control enrollment is missing");
		const response = await this.remoteFetch(this.remoteUrls().refresh, {
			method: "POST",
			headers: await this.authHeaders(),
			body: JSON.stringify({ server_id: this.enrollment.serverId, installation_id: this.installationId }),
		});
		if (!response.ok) throw new Error(`Remote-control token refresh failed: HTTP ${response.status}`);
		return this.updateEnrollment(response, "refreshed");
	}

	async ensureEnrollment() {
		this.loadState();
		const expiresAt = Date.parse(this.enrollment?.expiresAt || "");
		if (!this.options.reEnroll && this.enrollment?.remoteControlToken && Number.isFinite(expiresAt)) {
			if (expiresAt > Date.now() + 5 * 60 * 1000) {
				this.scheduleRefresh();
				return this.enrollment;
			}
			return this.refreshEnrollment();
		}
		const response = await this.remoteFetch(this.remoteUrls().enroll, {
			method: "POST",
			headers: await this.authHeaders(),
			body: JSON.stringify({
				name: this.options.serverName || `${process.env.HOSTNAME || "pi-host"}-pi`,
				os: process.platform === "darwin" ? "macos" : process.platform,
				arch: process.arch === "arm64" ? "aarch64" : process.arch === "x64" ? "x86_64" : process.arch,
				app_server_version: CODEX_APP_SERVER_VERSION,
				installation_id: this.installationId,
			}),
		});
		if (!response.ok) throw new Error(`Remote-control enrollment failed: HTTP ${response.status}`);
		return this.updateEnrollment(response, "enrolled");
	}

	scheduleRefresh() {
		if (this.refreshTimer) clearTimeout(this.refreshTimer);
		const expiresAt = Date.parse(this.enrollment?.expiresAt || "");
		if (!Number.isFinite(expiresAt)) return;
		const delay = Math.max(1_000, Math.min(expiresAt - Date.now() - 5 * 60 * 1000, 2_147_000_000));
		this.refreshTimer = setTimeout(() => void this.refreshAndReconnect(), delay);
	}

	async refreshAndReconnect() {
		if (this.stopped) return;
		try {
			await this.refreshEnrollment();
			this.socket?.close(1000, "remote-control token refreshed");
		} catch (error) {
			console.error(`[remote] token refresh failed: ${error.message}`);
			this.refreshTimer = setTimeout(() => void this.refreshAndReconnect(), 30_000);
		}
	}

	async pairingStatus(params) {
		const enrollment = await this.ensureEnrollment();
		const code = params?.manualPairingCode || params?.pairingCode;
		if (!code || Boolean(params?.manualPairingCode) === Boolean(params?.pairingCode)) throw new Error("Provide exactly one pairingCode or manualPairingCode");
		const response = await this.remoteFetch(this.remoteUrls().pairStatus, {
			method: "POST",
			headers: { Authorization: `Bearer ${enrollment.remoteControlToken}`, "content-type": "application/json" },
			body: JSON.stringify(params.manualPairingCode ? { manual_pairing_code: code } : { pairing_code: code }),
		});
		if (!response.ok) throw new Error(`Remote-control pairing status failed: HTTP ${response.status} ${await response.text()}`);
		const status = await response.json();
		console.error(`[remote] pairing status claimed=${Boolean(status.claimed)}`);
		return { claimed: Boolean(status.claimed) };
	}

	async pair(params = {}) {
		const enrollment = await this.ensureEnrollment();
		console.error(`[remote] requesting pairing code for server=${enrollment.serverId}`);
		const response = await this.remoteFetch(this.remoteUrls().pair, {
			method: "POST",
			headers: { Authorization: `Bearer ${enrollment.remoteControlToken}`, "content-type": "application/json" },
			body: JSON.stringify({ manual_code: Boolean(params.manualCode) }),
		});
		if (!response.ok) throw new Error(`Remote-control pairing failed: HTTP ${response.status} ${await response.text()}`);
		const pairing = await response.json();
		if (pairing.server_id !== enrollment.serverId || pairing.environment_id !== enrollment.environmentId) throw new Error("Remote-control pairing returned a different server or environment");
		console.log(`Pairing code: ${pairing.manual_pairing_code || pairing.pairing_code}`);
		console.log(`Host: ${pairing.environment_id}`);
		console.log(`Expires: ${pairing.expires_at}`);
		console.error(`[remote] pairing code issued server=${pairing.server_id} environment=${pairing.environment_id} expires=${pairing.expires_at}`);
		if (this.options.wait) await this.waitForPairing(pairing);
		return pairingResponseForCodex(pairing);
	}

	async waitForPairing(pairing) {
		const code = pairing.manual_pairing_code || pairing.pairing_code;
		const expiresAt = Date.parse(pairing.expires_at);
		console.error("[remote] waiting for Codex Mobile to claim the pairing code");
		while (Date.now() < expiresAt) {
			try {
				const response = await this.remoteFetch(this.remoteUrls().pairStatus, {
					method: "POST",
					headers: { Authorization: `Bearer ${this.enrollment.remoteControlToken}`, "content-type": "application/json" },
					body: JSON.stringify(pairing.manual_pairing_code ? { manual_pairing_code: code } : { pairing_code: code }),
				});
				if (!response.ok) throw new Error(`HTTP ${response.status} ${await response.text()}`);
				const status = await response.json();
				console.error(`[remote] pairing status claimed=${Boolean(status.claimed)}`);
				if (status.claimed) {
					console.error("[remote] pairing completed");
					return status;
				}
			} catch (error) {
				console.error(`[remote] pairing status check failed: ${error.message}`);
			}
			await new Promise((resolve) => setTimeout(resolve, 2000));
		}
		throw new Error("Pairing code expired before Codex Mobile claimed it");
	}

	status() {
		return {
			status: this.connectionStatus,
			installationId: this.installationId || null,
			serverName: this.enrollment?.serverName || this.options.serverName || "pi-host",
			environmentId: this.enrollment?.environmentId || null,
		};
	}

	publishStatus() {
		if (!this.daemon) return;
		const params = this.status();
		for (const client of this.daemon.clients) {
			if (!client.isRemote && client.readyState === WebSocket.OPEN) this.daemon.send(client, { method: "remoteControl/status/changed", params });
		}
	}

	async listClients(params = {}) {
		if (!params.environmentId) throw new Error("environmentId is required");
		if (params.limit != null && (!Number.isInteger(params.limit) || params.limit < 1 || params.limit > 100)) throw new Error("limit must be between 1 and 100");
		if (params.order && !["asc", "desc"].includes(params.order)) throw new Error("order must be asc or desc");
		this.loadState();
		const url = new URL(`wham/remote/control/environments/${encodeURIComponent(params.environmentId)}/clients`, this.baseUrl);
		if (params.cursor) url.searchParams.set("cursor", params.cursor);
		if (params.limit != null) url.searchParams.set("limit", String(params.limit));
		if (params.order) url.searchParams.set("order", params.order === "desc" ? "desc" : "asc");
		const auth = await this.authHeaders();
		const response = await this.remoteFetch(url, { headers: { Authorization: auth.Authorization, "chatgpt-account-id": auth["chatgpt-account-id"] } });
		if (!response.ok) throw new Error(`Remote-control client list failed: HTTP ${response.status} ${await response.text()}`);
		const result = await response.json();
		return { data: (result.items || []).map(remoteClientForCodex), nextCursor: result.cursor || null };
	}

	async revokeClient(params = {}) {
		if (!params.environmentId) throw new Error("environmentId is required");
		if (!params.clientId) throw new Error("clientId is required");
		this.loadState();
		const url = new URL(`wham/remote/control/environments/${encodeURIComponent(params.environmentId)}/clients/${encodeURIComponent(params.clientId)}`, this.baseUrl);
		const auth = await this.authHeaders();
		const response = await this.remoteFetch(url, { method: "DELETE", headers: { Authorization: auth.Authorization, "chatgpt-account-id": auth["chatgpt-account-id"] } });
		if (!response.ok) throw new Error(`Remote-control client revoke failed: HTTP ${response.status} ${await response.text()}`);
		return {};
	}

	remoteKey(clientId, streamId) {
		return `${clientId}:${streamId || ""}`;
	}

	sendEnvelope(envelope) {
		if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
		this.socket.send(JSON.stringify(envelope));
	}

	buildServerEnvelopes(event, clientId, streamId, seqId) {
		const envelope = { ...event, client_id: clientId, stream_id: streamId, seq_id: seqId };
		if (Buffer.byteLength(JSON.stringify(envelope)) <= REMOTE_SEGMENT_MAX_BYTES) return [envelope];
		if (!event.message) return [];
		const raw = Buffer.from(JSON.stringify(event.message));
		if (raw.length > REMOTE_MESSAGE_MAX_BYTES) return [];
		const segmentCount = Math.ceil(raw.length / REMOTE_SEGMENT_TARGET_BYTES);
		if (segmentCount > REMOTE_SEGMENT_COUNT_MAX) return [];
		return Array.from({ length: segmentCount }, (_, segmentId) => {
			const start = segmentId * REMOTE_SEGMENT_TARGET_BYTES;
			return {
				type: "server_message_chunk",
				segment_id: segmentId,
				segment_count: segmentCount,
				message_size_bytes: raw.length,
				message_chunk_base64: raw.subarray(start, start + REMOTE_SEGMENT_TARGET_BYTES).toString("base64"),
				client_id: clientId,
				stream_id: streamId,
				seq_id: seqId,
			};
		});
	}

	enqueueServerEvent(event, clientId, streamId, seqId) {
		const envelopes = this.buildServerEnvelopes(event, clientId, streamId, seqId);
		if (!envelopes.length) {
			console.error(`[remote] dropped oversized event client=${clientId} stream=${streamId} seq=${seqId}`);
			return;
		}
		const key = this.remoteKey(clientId, streamId);
		const entries = envelopes.map((envelope) => ({ envelope, bytes: Buffer.byteLength(JSON.stringify(envelope)) }));
		const bytes = entries.reduce((total, entry) => total + entry.bytes, 0);
		if (this.outboundBytes + bytes > REMOTE_OUTBOUND_MAX_BYTES) {
			console.error(`[remote] dropped event because unacknowledged relay buffer is full client=${clientId} stream=${streamId}`);
			return;
		}
		this.outbound.set(key, [...(this.outbound.get(key) || []), ...entries]);
		this.outboundBytes += bytes;
		for (const entry of entries) this.sendEnvelope(entry.envelope);
	}

	acknowledge(clientId, streamId, seqId, segmentId) {
		if (typeof seqId !== "number") return;
		const key = this.remoteKey(clientId, streamId);
		const entries = this.outbound.get(key);
		if (!entries) return;
		const kept = entries.filter((entry) => {
			const envelope = entry.envelope;
			const cursor = [envelope.seq_id, envelope.segment_id ?? Number.MAX_SAFE_INTEGER];
			return cursor[0] > seqId || (cursor[0] === seqId && cursor[1] > (segmentId ?? Number.MAX_SAFE_INTEGER));
		});
		this.outboundBytes -= entries.filter((entry) => !kept.includes(entry)).reduce((total, entry) => total + entry.bytes, 0);
		if (kept.length) this.outbound.set(key, kept);
		else this.outbound.delete(key);
	}

	replayOutbound() {
		for (const entries of this.outbound.values()) for (const entry of entries) this.sendEnvelope(entry.envelope);
	}

	dropChunk(key) {
		const assembly = this.chunks.get(key);
		if (!assembly) return;
		this.chunkBytes -= assembly.segments.reduce((total, segment) => total + (segment?.length || 0), 0);
		this.chunks.delete(key);
	}

	reassembleClientChunk(envelope) {
		const { client_id: clientId, stream_id: streamId, seq_id: seqId, segment_id: segmentId, segment_count: segmentCount, message_size_bytes: messageSizeBytes, message_chunk_base64: encoded } = envelope;
		if (!clientId || !streamId || !Number.isInteger(seqId) || !Number.isInteger(segmentId) || !Number.isInteger(segmentCount) || !Number.isInteger(messageSizeBytes) || segmentCount < 1 || segmentCount > REMOTE_SEGMENT_COUNT_MAX || segmentId < 0 || segmentId >= segmentCount || messageSizeBytes < 1 || messageSizeBytes > REMOTE_MESSAGE_MAX_BYTES || typeof encoded !== "string" || !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) return undefined;
		if ((this.lastInboundSeq.get(this.remoteKey(clientId, streamId)) ?? -1) >= seqId) return undefined;
		const chunk = Buffer.from(encoded, "base64");
		if (!chunk.length || chunk.length > REMOTE_SEGMENT_TARGET_BYTES) return undefined;
		const key = `${clientId}:${streamId}:${seqId}`;
		let assembly = this.chunks.get(key);
		if (!assembly) {
			if (this.chunks.size >= 128) this.dropChunk(this.chunks.keys().next().value);
			assembly = { segmentCount, messageSizeBytes, segments: new Array(segmentCount) };
			this.chunks.set(key, assembly);
		}
		if (assembly.segmentCount !== segmentCount || assembly.messageSizeBytes !== messageSizeBytes) {
			this.dropChunk(key);
			return undefined;
		}
		if (!assembly.segments[segmentId]) {
			if (this.chunkBytes + chunk.length > REMOTE_CHUNK_BUFFER_MAX_BYTES) {
				this.dropChunk(key);
				return undefined;
			}
			assembly.segments[segmentId] = chunk;
			this.chunkBytes += chunk.length;
		}
		if (assembly.segments.filter(Boolean).length !== segmentCount) return undefined;
		const raw = Buffer.concat(assembly.segments);
		this.dropChunk(key);
		if (raw.length !== messageSizeBytes) return undefined;
		try {
			return { ...envelope, type: "client_message", message: JSON.parse(raw.toString("utf8")) };
		} catch {
			return undefined;
		}
	}

	removeRemoteClient(clientId, streamId) {
		const resolvedStreamId = streamId || this.legacyStreams.get(clientId);
		if (!resolvedStreamId) return;
		const key = this.remoteKey(clientId, resolvedStreamId);
		const client = this.daemon.remoteClients.get(key);
		if (client) {
			this.daemon.remoteClients.delete(key);
			this.daemon.clients.delete(client);
			for (const managed of this.daemon.sessions.values()) managed.clients.delete(client);
		}
		const entries = this.outbound.get(key) || [];
		this.outboundBytes -= entries.reduce((total, entry) => total + entry.bytes, 0);
		this.outbound.delete(key);
		this.lastInboundSeq.delete(key);
		if (this.legacyStreams.get(clientId) === resolvedStreamId) this.legacyStreams.delete(clientId);
		for (const chunkKey of this.chunks.keys()) if (chunkKey.startsWith(`${clientId}:${resolvedStreamId}:`)) this.dropChunk(chunkKey);
	}

	scheduleReconnect(error) {
		if (this.stopped || this.reconnectTimer) return;
		this.connectionStatus = "connecting";
		this.publishStatus();
		const delay = Math.min(30_000, 1_000 * 2 ** Math.min(this.reconnectAttempt++, 5));
		console.error(`[remote] ${error}; retrying in ${delay}ms`);
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = undefined;
			void this.start().catch((startError) => this.scheduleReconnect(startError.message));
		}, delay);
	}

	startHeartbeat() {
		if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
		this.lastPongAt = Date.now();
		this.heartbeatTimer = setInterval(() => {
			if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
			if (Date.now() - this.lastPongAt > 60_000) {
				this.socket.terminate();
				return;
			}
			this.socket.ping();
		}, 10_000);
	}

	async start() {
		this.stopped = false;
		if (this.socket && [WebSocket.OPEN, WebSocket.CONNECTING].includes(this.socket.readyState)) return this.status();
		this.connectionStatus = "connecting";
		this.publishStatus();
		const enrollment = await this.ensureEnrollment();
		const headers = {
			"x-codex-server-id": enrollment.serverId,
			"x-codex-name": Buffer.from(enrollment.serverName).toString("base64"),
			"x-codex-protocol-version": "3",
			Authorization: `Bearer ${enrollment.remoteControlToken}`,
			"x-codex-installation-id": this.installationId,
		};
		if (this.subscribeCursor) headers["x-codex-subscribe-cursor"] = this.subscribeCursor;
		this.socket = new WebSocket(this.remoteUrls().websocket, { headers, maxPayload: REMOTE_SEGMENT_MAX_BYTES });
		this.socket.on("open", () => {
			this.connectionStatus = "connected";
			this.reconnectAttempt = 0;
			console.error("pi-remote-daemon connected to Codex remote-control relay");
			this.startHeartbeat();
			this.replayOutbound();
			this.publishStatus();
		});
		this.socket.on("pong", () => { this.lastPongAt = Date.now(); });
		this.socket.on("message", (data) => {
			this.inboundChain = this.inboundChain
				.then(() => this.handleEnvelope(data.toString()))
				.catch((error) => console.error(`[remote] message handling failed: ${error.message}`));
		});
		this.socket.on("close", () => {
			this.socket = undefined;
			if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
			this.heartbeatTimer = undefined;
			if (this.stopped) {
				this.connectionStatus = "disabled";
				this.publishStatus();
				return;
			}
			this.scheduleReconnect("Codex remote-control relay disconnected");
		});
		this.socket.on("error", (error) => {
			this.connectionStatus = "errored";
			this.publishStatus();
			console.error(`[remote] relay error: ${error.message}`);
		});
		return this.status();
	}

	async handleEnvelope(raw) {
		if (Buffer.byteLength(raw) > REMOTE_SEGMENT_MAX_BYTES) return;
		let envelope;
		try { envelope = JSON.parse(raw); } catch {
			console.error("[remote] received invalid JSON envelope");
			return;
		}
		if (envelope.type === "ack") {
			this.acknowledge(envelope.client_id, envelope.stream_id, envelope.seq_id, envelope.segment_id);
			if (envelope.cursor) this.subscribeCursor = envelope.cursor;
			return;
		}
		if (envelope.type === "client_closed") {
			this.removeRemoteClient(envelope.client_id, envelope.stream_id);
			if (envelope.cursor) this.subscribeCursor = envelope.cursor;
			return;
		}
		if (envelope.type === "client_message_chunk") {
			envelope = this.reassembleClientChunk(envelope);
			if (!envelope) return;
		}
		const rpcMethod = envelope.message?.method || (envelope.message ? "response" : "-");
		console.error(`[remote] received type=${envelope.type || "unknown"} rpc=${rpcMethod} client=${envelope.client_id || "-"} stream=${envelope.stream_id || "-"} seq=${envelope.seq_id ?? "-"}`);
		const clientId = envelope.client_id;
		if (!clientId) return;
		const isInitialize = envelope.type === "client_message" && envelope.message?.method === "initialize";
		let streamId = envelope.stream_id;
		if (!streamId && isInitialize) {
			streamId = this.legacyStreams.get(clientId) || randomUUID();
			this.legacyStreams.set(clientId, streamId);
		} else if (!streamId) streamId = this.legacyStreams.get(clientId);
		if (!streamId) return;
		const key = this.remoteKey(clientId, streamId);
		if (Number.isInteger(envelope.seq_id) && (this.lastInboundSeq.get(key) ?? -1) >= envelope.seq_id) return;
		let client = this.daemon.remoteClients.get(key);
		if (!client && isInitialize) {
			client = this.makeVirtualClient(clientId, streamId);
			this.daemon.remoteClients.set(key, client);
			this.daemon.clients.add(client);
		}
		if (envelope.type === "ping") {
			if (!client) client = this.makeVirtualClient(clientId, streamId);
			client.sendEvent({ type: "pong", status: this.daemon.remoteClients.has(key) ? "active" : "unknown" });
			if (envelope.cursor) this.subscribeCursor = envelope.cursor;
			return;
		}
		if (!client || envelope.type !== "client_message" || !envelope.message) return;
		if (Number.isInteger(envelope.seq_id)) this.lastInboundSeq.set(key, envelope.seq_id);
		await this.daemon.handleMessage(client, client.state, envelope.message);
		if (envelope.cursor) this.subscribeCursor = envelope.cursor;
	}

	makeVirtualClient(clientId, streamId) {
		let sequence = 1;
		const client = {
			readyState: WebSocket.OPEN,
			isRemote: true,
			clientId,
			streamId,
			state: { initialized: false, subscriptions: new Set() },
			sendEvent: (event) => {
				const seqId = sequence++;
				const sentRpc = event.message?.method || (event.message?.error ? `error:${event.message.error.code}` : (event.message ? "response" : "-"));
				console.error(`[remote] sent type=${event.type || "unknown"} rpc=${sentRpc} client=${clientId} stream=${streamId} seq=${seqId}`);
				this.enqueueServerEvent(event, clientId, streamId, seqId);
			},
			send: (encoded) => client.sendEvent({ type: "server_message", message: JSON.parse(encoded) }),
		};
		return client;
	}

	stop() {
		this.stopped = true;
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		if (this.refreshTimer) clearTimeout(this.refreshTimer);
		if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
		this.reconnectTimer = undefined;
		this.refreshTimer = undefined;
		this.heartbeatTimer = undefined;
		this.socket?.close();
		this.socket = undefined;
		for (const client of this.daemon ? [...this.daemon.remoteClients.values()] : []) this.removeRemoteClient(client.clientId, client.streamId);
		for (const key of [...this.chunks.keys()]) this.dropChunk(key);
		this.connectionStatus = "disabled";
		this.publishStatus();
	}
}

class PermissionBridge {
	constructor(daemon) {
		this.daemon = daemon;
		this.path = join(agentDir, "app-server", "permission.sock");
		this.server = undefined;
	}

	async start() {
		const directory = join(this.path, "..");
		mkdirSync(directory, { recursive: true, mode: 0o700 });
		chmodSync(directory, 0o700);
		if (existsSync(this.path)) {
			if (await unixSocketIsLive(this.path)) throw new Error(`A permission bridge already owns ${this.path}`);
			unlinkSync(this.path);
		}
		this.server = createNetServer((connection) => {
			let buffer = "";
			connection.setEncoding("utf8");
			connection.on("data", (chunk) => {
				buffer += chunk;
				const newline = buffer.indexOf("\n");
				if (newline < 0) return;
				const line = buffer.slice(0, newline);
				buffer = "";
				void (async () => {
					try {
						const request = JSON.parse(line);
						const allowed = await this.daemon.requestPermission(request);
						connection.end(`${JSON.stringify({ ok: true, allowed })}\n`);
					} catch {
						connection.end('{"ok":false,"allowed":false}\n');
					}
				})();
			});
		});
		await new Promise((resolvePromise, reject) => {
			this.server.once("error", reject);
			this.server.listen(this.path, resolvePromise);
		});
		chmodSync(this.path, 0o600);
	}

	async stop() {
		if (this.server) await new Promise((resolvePromise) => this.server.close(resolvePromise));
		this.server = undefined;
		if (existsSync(this.path)) unlinkSync(this.path);
	}
}

class PiDaemon {
	constructor(options) {
		this.options = options;
		this.sessions = new Map();
		this.clients = new Set();
		this.remoteClients = new Map();
		this.pendingRequests = new Map();
		this.permissionBridge = new PermissionBridge(this);
		this.scopedModels = [];
		this.modelRuntime = undefined;
		this.server = undefined;
		this.remote = new RemoteControlClient(this, options);
		this.websocketServer = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });
	}

	async requestFromClient(client, method, params, timeoutMs = 5 * 60_000) {
		const id = `pi-${randomUUID()}`;
		return new Promise((resolvePromise) => {
			const timer = setTimeout(() => {
				this.pendingRequests.delete(id);
				resolvePromise(undefined);
			}, timeoutMs);
			this.pendingRequests.set(id, { client, resolve: (result) => {
				clearTimeout(timer);
				resolvePromise(result);
			} });
			this.send(client, { id, method, params });
		});
	}

	resolveClientResponse(client, message) {
		const pending = this.pendingRequests.get(message.id);
		if (!pending || pending.client !== client) return false;
		this.pendingRequests.delete(message.id);
		pending.resolve(message.result);
		return true;
	}

	async requestPermission(request) {
		if (request?.type !== "command" || !request.sessionId || !request.toolCallId || !request.command) return false;
		const managed = this.sessions.get(request.sessionId);
		if (!managed?.activeTurn) return false;
		const client = [...managed.clients].find((candidate) => candidate.readyState === WebSocket.OPEN);
		if (!client) return false;
		const result = await this.requestFromClient(client, "item/commandExecution/requestApproval", {
			kind: "command",
			threadId: managed.id,
			turnId: managed.activeTurn.id,
			itemId: request.toolCallId,
			startedAtMs: Date.now(),
			approvalId: null,
			environmentId: null,
			command: request.command,
			cwd: request.cwd || managed.cwd,
		});
		return result?.decision === "accept" || result?.decision === "acceptForSession";
	}

	async init() {
		this.modelRuntime = await ModelRuntime.create({
			authPath: join(agentDir, "auth.json"),
			modelsPath: join(agentDir, "models.json"),
			refreshOnCreate: false,
		});
		let patterns = this.options.models || [];
		if (!patterns.length) try {
			const settings = JSON.parse(readFileSync(join(agentDir, "settings.json"), "utf8"));
			patterns = Array.isArray(settings.enabledModels) ? settings.enabledModels : [];
		} catch {}
		if (patterns.length) {
			const resolved = await resolveModelScopeWithDiagnostics(patterns, this.modelRuntime);
			this.scopedModels = resolved.scopedModels;
			for (const diagnostic of resolved.diagnostics) console.error(`[models] ${diagnostic.message}`);
		}
	}

	async start() {
		await this.init();
		if (this.options.port && !isLoopbackHost(this.options.host)) throw new Error("Refusing unauthenticated TCP listener outside localhost");
		const appServerDir = resolve(this.options.socket, "..");
		mkdirSync(appServerDir, { recursive: true, mode: 0o700 });
		chmodSync(appServerDir, 0o700);
		if (!this.options.port && this.options.socket && existsSync(this.options.socket)) {
			if (await unixSocketIsLive(this.options.socket)) throw new Error(`A daemon already owns ${this.options.socket}`);
			unlinkSync(this.options.socket);
		}
		this.server = createServer((request, response) => {
			if (request.url === "/readyz" || request.url === "/healthz") {
				response.writeHead(200, { "content-type": "text/plain" });
				response.end("ok\n");
				return;
			}
			response.writeHead(404);
			response.end();
		});
		this.server.on("upgrade", (request, socket, head) => {
			if (this.options.port && request.headers.origin) {
				socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
				return;
			}
			this.websocketServer.handleUpgrade(request, socket, head, (client) => {
				this.websocketServer.emit("connection", client, request);
			});
		});
		this.websocketServer.on("connection", (client) => this.attachClient(client));
		await this.permissionBridge.start();
		await new Promise((resolvePromise, reject) => {
			this.server.once("error", reject);
			if (this.options.port) this.server.listen(this.options.port, this.options.host, resolvePromise);
			else this.server.listen(this.options.socket, resolvePromise);
		});
		const endpoint = this.options.port ? `ws://${this.options.host}:${this.options.port}` : this.options.socket;
		console.error(`pi-remote-daemon ${VERSION} listening on ${endpoint}`);
		if (this.options.session) await this.loadByPath(this.options.session);
		if (this.options.remote) await this.remote.start().catch((error) => this.remote.scheduleReconnect(error.message));
	}

	attachClient(client) {
		const state = { initialized: false, subscriptions: new Set() };
		this.clients.add(client);
		client.on("message", async (data) => {
			try {
				const message = JSON.parse(data.toString());
				await this.handleMessage(client, state, message);
			} catch (error) {
				this.send(client, { id: null, error: { code: -32700, message: error instanceof Error ? error.message : String(error) } });
			}
		});
		client.on("close", () => {
			this.clients.delete(client);
			for (const managed of this.sessions.values()) managed.clients.delete(client);
		});
	}

	send(client, message) {
		if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(message));
	}

	sendResult(client, id, result) {
		if (id !== undefined) this.send(client, { id, result });
	}

	sendError(client, id, code, message) {
		this.send(client, { ...(id === undefined ? {} : { id }), error: { code, message } });
	}

	broadcast(threadId, message) {
		const managed = this.sessions.get(threadId);
		if (!managed) return;
		for (const client of managed.clients) this.send(client, message);
	}

	async allSessionInfo() {
		return SessionManager.listAll();
	}

	async infoForId(threadId) {
		const infos = await this.allSessionInfo();
		return infos.find((info) => info.id === threadId);
	}

	async forkThread(params) {
		if (params.lastTurnId && params.beforeTurnId) throw new Error("lastTurnId and beforeTurnId cannot be combined");
		const unsupported = ["serviceTier", "runtimeWorkspaceRoots", "baseInstructions", "developerInstructions", "personality"];
		const requested = unsupported.find((field) => params[field] !== undefined && params[field] !== null && (!Array.isArray(params[field]) || params[field].length));
		if (requested) throw new Error(`${requested} is not supported by the Pi daemon`);
		if (params.config && Object.keys(params.config).length) throw new Error("config overrides are not supported by the Pi daemon");
		const sourceManaged = params.path
			? [...this.sessions.values()].find((managed) => managed.session.sessionFile && resolve(managed.session.sessionFile) === resolve(params.path))
			: this.sessions.get(params.threadId);
		if (sourceManaged?.activeTurn) throw new Error("Cannot fork while the source thread is running");
		const info = params.path ? { path: resolve(params.path), cwd: params.cwd || process.cwd() } : await this.infoForId(params.threadId);
		if (!info?.path) throw new Error(`Unknown thread: ${params.threadId}`);
		const cwd = params.cwd || info.cwd || process.cwd();
		let manager;
		if (params.lastTurnId || params.beforeTurnId) {
			const source = SessionManager.open(info.path);
			const entries = source.getBranch();
			const turns = turnsFromManager(source);
			const requestedId = params.lastTurnId || params.beforeTurnId;
			const turnIndex = turns.findIndex((turn) => turn.id === requestedId);
			if (turnIndex < 0) throw new Error(`Unknown turn: ${requestedId}`);
			const nextTurnId = params.beforeTurnId ? requestedId : turns[turnIndex + 1]?.id;
			const metadata = remoteState(source).turns;
			const boundaryUserId = nextTurnId ? metadata.get(nextTurnId)?.userEntryId || nextTurnId.replace(/^turn-/, "") : null;
			const boundaryIndex = boundaryUserId ? entries.findIndex((entry) => entry.id === boundaryUserId) : entries.length;
			if (boundaryUserId && boundaryIndex < 0) throw new Error(`Could not locate turn boundary: ${nextTurnId}`);
			const leafIndex = boundaryIndex - 1;
			if (leafIndex < 0) {
				manager = SessionManager.create(cwd);
			} else {
				const branchPath = source.createBranchedSession(entries[leafIndex].id);
				if (!branchPath) throw new Error("Cannot fork an in-memory session");
				if (resolve(cwd) === resolve(info.cwd || cwd)) manager = SessionManager.open(branchPath);
				else {
					manager = SessionManager.forkFrom(branchPath, cwd);
					try { unlinkSync(branchPath); } catch {}
				}
			}
		} else {
			manager = SessionManager.forkFrom(info.path, cwd);
		}
		const result = await createAgentSession({
			cwd,
			agentDir,
			modelRuntime: this.modelRuntime,
			scopedModels: this.scopedModels,
			sessionManager: manager,
		});
		const managed = this.registerSession(result.session, cwd);
		if (params.model) await managed.session.setModel(this.resolveModel(params.model, params.modelProvider, managed.session.model?.provider));
		else if (params.modelProvider) throw new Error("modelProvider requires model");
		return managed;
	}

	async revertBeforeTurn(managed, beforeTurnId) {
		if (managed.activeTurn) throw new Error("Cannot change history while a turn is running");
		const manager = managed.session.sessionManager;
		const turns = turnsFromManager(manager);
		if (!turns.some((turn) => turn.id === beforeTurnId)) throw new Error(`Unknown turn: ${beforeTurnId}`);
		const entries = manager.getBranch();
		const userEntryId = remoteState(manager).turns.get(beforeTurnId)?.userEntryId || beforeTurnId.replace(/^turn-/, "");
		const boundaryIndex = entries.findIndex((entry) => entry.id === userEntryId);
		if (boundaryIndex < 0) throw new Error(`Could not locate turn boundary: ${beforeTurnId}`);
		if (boundaryIndex === 0) {
			manager.resetLeaf();
			if (managed.session.model) manager.appendModelChange(managed.session.model.provider, managed.session.model.id);
			if (managed.session.sessionName) manager.appendSessionInfo(managed.session.sessionName);
			managed.session.agent.state.messages = [];
		} else {
			const result = await managed.session.navigateTree(entries[boundaryIndex - 1].id, { summarize: false });
			if (result.cancelled) throw new Error("History change was cancelled");
		}
	}

	async searchThreads(params) {
		const needle = String(params.searchTerm || "").trim().toLocaleLowerCase();
		if (!needle) throw new Error("searchTerm is required");
		const results = [];
		for (const info of await this.allSessionInfo()) {
			const manager = SessionManager.open(info.path);
			const text = sessionMessages(manager).map((message) => textFromContent(message.content)).join("\n");
			const index = text.toLocaleLowerCase().indexOf(needle);
			if (index < 0) continue;
			const snippet = text.slice(Math.max(0, index - 80), index + needle.length + 160);
			results.push({ thread: this.threadFromInfo(info, this.sessions.get(info.id)), snippet });
		}
		results.sort((left, right) => left.thread.updatedAt - right.thread.updatedAt);
		return page(results, params, "desc");
	}

	async searchOccurrences(params) {
		const managed = await this.loadById(params.threadId);
		const needle = String(params.searchTerm || "").toLocaleLowerCase();
		if (!needle) throw new Error("searchTerm is required");
		const turns = turnsFromManager(managed.session.sessionManager);
		const matches = [];
		for (const turn of turns) for (const item of turn.items) {
			const text = item.text || textFromContent(item.content);
			const index = text.toLocaleLowerCase().indexOf(needle);
			if (index >= 0) matches.push({ turnId: turn.id, itemId: item.id, snippet: text, snippetMatchRange: { start: index, end: index + needle.length }, turnCursor: turn.id });
		}
		return page(matches, params);
	}

	threadFromInfo(info, managed) {
		const createdAt = Math.floor(info.created.getTime() / 1000);
		const updatedAt = Math.floor(info.modified.getTime() / 1000);
		let persistedContext;
		if (!managed) {
			try { persistedContext = SessionManager.open(info.path).buildSessionContext(); } catch {}
		}
		return {
			id: info.id,
			environments: null,
			extra: null,
			sessionId: info.id,
			forkedFromId: info.parentSessionPath ? sessionIdFromFile(info.parentSessionPath) || null : null,
			parentThreadId: null,
			preview: info.firstMessage || "",
			ephemeral: false,
			section: null,
			sectionEnteredAt: null,
			projectId: null,
			historyMode: "legacy",
			modelProvider: managed?.session.model?.provider || persistedContext?.model?.provider || "unknown",
			model: managed?.session.model?.id || persistedContext?.model?.modelId || null,
			reasoningEffort: effortForCodex(managed?.session.thinkingLevel || persistedContext?.thinkingLevel) || null,
			createdAt,
			updatedAt,
			recencyAt: updatedAt,
			status: managed?.session.isStreaming ? { type: "active", activeFlags: [] } : { type: "idle" },
			path: info.path,
			cwd: info.cwd || managed?.cwd || process.cwd(),
			cliVersion: `pi-${VERSION}`,
			originator: null,
			source: "appServer",
			canAcceptDirectInput: managed ? true : null,
			threadSource: null,
			agentNickname: null,
			agentRole: null,
			gitInfo: null,
			name: info.name || managed?.session.sessionManager?.getSessionName?.() || null,
			daybreakEnabled: null,
			turns: [],
		};
	}

	threadFromManaged(managed, includeTurns = false) {
		const manager = managed.session.sessionManager;
		const entries = manager.getBranch();
		const messages = managed.session.messages || [];
		const first = messages.find((message) => message.role === "user");
		const preview = textFromContent(first?.content).slice(0, 200);
		const header = manager.getHeader();
		const createdAt = timestampSeconds(header?.timestamp, managed.createdAt * 1000);
		const updatedAt = entries.length ? timestampSeconds(entries.at(-1).timestamp) : createdAt;
		return {
			id: managed.id,
			environments: null,
			extra: null,
			sessionId: managed.id,
			forkedFromId: header?.parentSession ? sessionIdFromFile(header.parentSession) || null : null,
			parentThreadId: null,
			preview,
			ephemeral: !managed.session.sessionFile,
			section: null,
			sectionEnteredAt: null,
			projectId: null,
			historyMode: "legacy",
			modelProvider: managed.session.model?.provider || "unknown",
			model: managed.session.model?.id || null,
			reasoningEffort: effortForCodex(managed.session.thinkingLevel) || null,
			createdAt,
			updatedAt,
			recencyAt: updatedAt,
			status: managed.session.isStreaming ? { type: "active", activeFlags: [] } : { type: "idle" },
			path: managed.session.sessionFile || null,
			cwd: managed.cwd,
			cliVersion: `pi-${VERSION}`,
			originator: null,
			source: "appServer",
			canAcceptDirectInput: true,
			threadSource: null,
			agentNickname: null,
			agentRole: null,
			gitInfo: null,
			name: managed.session.sessionName || null,
			daybreakEnabled: null,
			turns: includeTurns ? turnsFromManager(manager) : [],
		};
	}

	isAllowedModel(model) {
		return this.scopedModels.length === 0 || this.scopedModels.some((entry) => entry.model.provider === model.provider && entry.model.id === model.id);
	}

	resolveModel(modelValue, providerValue, currentProvider) {
		if (!modelValue) return undefined;
		let provider = providerValue || currentProvider;
		let modelId = modelValue;
		const separator = modelValue.indexOf("/");
		const qualifiedProvider = separator > 0 ? modelValue.slice(0, separator) : null;
		if (qualifiedProvider && (!providerValue || qualifiedProvider === providerValue)) {
			provider = qualifiedProvider;
			modelId = modelValue.slice(separator + 1);
		}
		let model = provider ? this.modelRuntime.getModel(provider, modelId) : undefined;
		if (!model) {
			const matches = this.modelRuntime.getModels().filter((candidate) => candidate.id === modelId);
			if (matches.length === 1) model = matches[0];
		}
		if (!model) throw new Error(`Unknown model: ${modelValue}`);
		if (!this.isAllowedModel(model)) throw new Error(`Model is outside the configured Pi scope: ${model.provider}/${model.id}`);
		return model;
	}

	threadSettings(managed) {
		return {
			cwd: managed.cwd,
			approvalPolicy: "on-request",
			approvalsReviewer: "user",
			sandboxPolicy: { type: "dangerFullAccess" },
			activePermissionProfile: { id: "pi-remote", extends: null },
			model: managed.session.model?.id || "",
			modelProvider: managed.session.model?.provider || "",
			serviceTier: null,
			effort: effortForCodex(managed.session.thinkingLevel) || null,
			summary: null,
			collaborationMode: { mode: "default", settings: { model: managed.session.model?.id || "", reasoning_effort: effortForCodex(managed.session.thinkingLevel) || null, developer_instructions: null } },
			personality: null,
		};
	}

	async updateThreadSettings(managed, params) {
		if (params.approvalPolicy && params.approvalPolicy !== "on-request") throw new Error("Only the on-request approval policy is supported by the Pi daemon");
		if (params.approvalsReviewer && params.approvalsReviewer !== "user") throw new Error("Only user approval review is supported by the Pi daemon");
		if (params.sandboxPolicy && params.sandboxPolicy.type !== "dangerFullAccess") throw new Error("Pi sandbox policies are not supported");
		if (params.permissions && params.permissions !== "pi-remote") throw new Error("Unknown Pi permission profile");
		const unsupported = ["serviceTier", "summary", "collaborationMode", "personality"];
		const requested = unsupported.find((field) => params[field] !== undefined && params[field] !== null);
		if (requested) throw new Error(`${requested} is not supported by the Pi daemon`);
		if (params.cwd && resolve(params.cwd) !== resolve(managed.cwd)) throw new Error("Changing cwd on a loaded Pi session is not supported");
		let changed = false;
		if (params.model) {
			await managed.session.setModel(this.resolveModel(params.model, params.modelProvider, managed.session.model?.provider));
			changed = true;
		}
		if (params.effort) {
			const effort = params.effort === "none" ? "off" : params.effort === "ultra" ? "max" : params.effort;
			if (!["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effort)) throw new Error(`Unsupported reasoning effort: ${params.effort}`);
			managed.session.setThinkingLevel(effort);
			changed = true;
		}
		const threadSettings = this.threadSettings(managed);
		if (changed) this.broadcast(managed.id, { method: "thread/settings/updated", params: { threadId: managed.id, threadSettings } });
		return threadSettings;
	}

	async createSession(params = {}) {
		const { cwd, ephemeral = false, model, modelProvider } = params;
		if (params.approvalPolicy && params.approvalPolicy !== "on-request") throw new Error("Only the on-request approval policy is supported by the Pi daemon");
		if (params.approvalsReviewer && params.approvalsReviewer !== "user") throw new Error("Only user approval review is supported by the Pi daemon");
		if (params.sandbox && params.sandbox !== "danger-full-access" && params.sandbox.type !== "dangerFullAccess") throw new Error("Pi sandbox modes are not supported");
		if (params.permissions && params.permissions !== "pi-remote") throw new Error("Unknown Pi permission profile");
		const unsupported = ["runtimeWorkspaceRoots", "environments", "dynamicTools", "selectedCapabilityRoots", "baseInstructions", "developerInstructions", "serviceTier", "serviceName", "personality"];
		const requested = unsupported.find((field) => params[field] !== undefined && params[field] !== null && (!Array.isArray(params[field]) || params[field].length));
		if (requested) throw new Error(`${requested} is not supported by the Pi daemon`);
		if (params.config && Object.keys(params.config).length) throw new Error("config overrides are not supported by the Pi daemon");
		if (params.historyMode && params.historyMode !== "legacy") throw new Error("Only legacy history mode is supported by the Pi daemon");
		const workingDirectory = cwd || process.cwd();
		const selectedModel = this.resolveModel(model, modelProvider);
		const sessionManager = ephemeral
			? SessionManager.inMemory(workingDirectory)
			: SessionManager.create(workingDirectory);
		const result = await createAgentSession({
			cwd: workingDirectory,
			agentDir,
			model: selectedModel,
			modelRuntime: this.modelRuntime,
			scopedModels: this.scopedModels,
			sessionManager,
		});
		return this.registerSession(result.session, workingDirectory);
	}

	async loadByPath(sessionPath) {
		await takeOver(sessionPath, undefined, this.options.force, true);
		const manager = SessionManager.open(sessionPath);
		const result = await createAgentSession({
			cwd: manager.getCwd() || process.cwd(),
			agentDir,
			modelRuntime: this.modelRuntime || (await ModelRuntime.create({ refreshOnCreate: false })),
			scopedModels: this.scopedModels,
			sessionManager: manager,
		});
		return this.registerSession(result.session, manager.getCwd() || process.cwd());
	}

	async loadById(threadId) {
		const existing = this.sessions.get(threadId);
		if (existing) return existing;
		const info = await this.infoForId(threadId);
		if (!info) throw new Error(`Unknown thread: ${threadId}`);
		return this.loadByPath(info.path);
	}

	registerSession(session, cwd) {
		const managed = {
			id: session.sessionId,
			session,
			cwd,
			createdAt: Math.floor(Date.now() / 1000),
			clients: new Set(),
			activeTurn: undefined,
		};
		this.sessions.set(managed.id, managed);
		session.subscribe((event) => this.handlePiEvent(managed, event));
		return managed;
	}

	beginTurn(managed, input, clientUserMessageId) {
		if (managed.activeTurn) throw new Error("Thread is already running");
		const turnId = randomUUID();
		const turn = { id: turnId, items: [], itemsView: "full", status: "inProgress", error: null, startedAt: Math.floor(Date.now() / 1000), completedAt: null, durationMs: null };
		const userItem = {
			type: "userMessage", id: randomUUID(), clientId: clientUserMessageId || null,
			content: (input || []).map((part) => {
				if (part?.type !== "text") return part;
				const { textElements, ...rest } = part;
				return { ...rest, text_elements: part.text_elements || textElements || [] };
			}),
		};
		managed.activeTurn = {
			id: turnId, items: [userItem], userItem, startedAt: turn.startedAt, startedAtMs: Date.now(), finished: false, error: null,
			agentItemId: null, agentText: "", reasoningItemId: null, reasoningText: "", toolOutputs: new Map(), toolStartedAt: new Map(),
			entryIdsBefore: new Set(managed.session.sessionManager.getEntries().map((entry) => entry.id)), clientUserMessageId: clientUserMessageId || null,
		};
		return turn;
	}

	announceTurn(managed, turn) {
		this.broadcast(managed.id, { method: "turn/started", params: { threadId: managed.id, turn } });
		this.broadcast(managed.id, { method: "thread/status/changed", params: { threadId: managed.id, status: { type: "active", activeFlags: [] } } });
		const item = managed.activeTurn?.userItem;
		if (item) {
			const timestamp = Date.now();
			this.broadcast(managed.id, { method: "item/started", params: { threadId: managed.id, turnId: turn.id, item, startedAtMs: timestamp } });
			this.broadcast(managed.id, { method: "item/completed", params: { threadId: managed.id, turnId: turn.id, item, completedAtMs: timestamp } });
		}
	}

	async runTurn(managed, turnId, input, options) {
		const active = managed.activeTurn;
		if (!active || active.id !== turnId) return;
		try {
			const unsupported = (input || []).find((part) => part?.type !== "text" && part?.type !== "image");
			if (unsupported) throw new Error(`Input type is not supported by the Pi daemon: ${unsupported.type || "unknown"}`);
			if ((input || []).some((part) => part?.type === "image" && !part.url)) throw new Error("Image input requires a URL");
			const text = textFromInput(input);
			const images = (input || []).filter((part) => part?.type === "image" && part.url).map((part) => ({
				type: "image",
				source: { type: "url", url: part.url },
			}));
			await managed.session.prompt(text, { images: images.length ? images : undefined, source: "rpc" });
			const status = active.interrupted ? "interrupted" : "completed";
			this.recordTurnMetadata(managed, active, status);
			this.finishTurn(managed, status, active.id);
		} catch (error) {
			active.error = error instanceof Error ? error.message : String(error);
			const status = active.interrupted ? "interrupted" : "failed";
			this.recordTurnMetadata(managed, active, status);
			this.finishTurn(managed, status, active.id);
		}
	}

	recordTurnMetadata(managed, active, status) {
		const userEntry = managed.session.sessionManager.getEntries().find((entry) => !active.entryIdsBefore.has(entry.id) && entry.type === "message" && entry.message?.role === "user");
		if (userEntry) saveRemoteState(managed.session.sessionManager, "turn", {
			turnId: active.id, userEntryId: userEntry.id, userItemId: active.userItem.id, clientUserMessageId: active.clientUserMessageId,
			status, error: active.error || null, completedAt: Math.floor(Date.now() / 1000),
		});
	}

	finishTurn(managed, status, expectedTurnId) {
		const active = managed.activeTurn;
		if (!active || active.finished || (expectedTurnId && active.id !== expectedTurnId)) return;
		active.finished = true;
		const turn = {
			id: active.id,
			items: active.items,
			itemsView: "full",
			status,
			error: active.error ? {
				message: active.error,
				codexErrorInfo: null,
				additionalDetails: null,
				misalignment: null,
			} : null,
			startedAt: active.startedAt,
			completedAt: Math.floor(Date.now() / 1000),
			durationMs: Date.now() - active.startedAtMs,
		};
		this.broadcast(managed.id, { method: "turn/completed", params: { threadId: managed.id, turn } });
		this.broadcast(managed.id, {
			method: "thread/status/changed",
			params: { threadId: managed.id, status: { type: "idle" } },
		});
		managed.activeTurn = undefined;
	}

	handlePiEvent(managed, event) {
		const active = managed.activeTurn;
		if (!active) return;
		if (event.type === "message_update") {
			const deltaEvent = event.assistantMessageEvent;
			if (deltaEvent?.type === "text_start") {
				active.agentItemId = randomUUID();
				active.agentText = "";
				const item = { type: "agentMessage", id: active.agentItemId, text: "", phase: null, memoryCitation: null, delivery: null, questions: null };
				active.items.push(item);
				this.broadcast(managed.id, { method: "item/started", params: { threadId: managed.id, turnId: active.id, item, startedAtMs: Date.now() } });
			}
			if (deltaEvent?.type === "text_delta" && active.agentItemId) {
				active.agentText += deltaEvent.delta || "";
				this.broadcast(managed.id, { method: "item/agentMessage/delta", params: { threadId: managed.id, turnId: active.id, itemId: active.agentItemId, delta: deltaEvent.delta || "" } });
			}
			if (deltaEvent?.type === "text_end" && active.agentItemId) {
				const item = active.items.find((candidate) => candidate.id === active.agentItemId);
				if (item) item.text = deltaEvent.content || active.agentText;
				if (item) this.broadcast(managed.id, { method: "item/completed", params: { threadId: managed.id, turnId: active.id, item, completedAtMs: Date.now() } });
				active.agentItemId = null;
				active.agentText = "";
			}
			if (deltaEvent?.type === "thinking_start") {
				active.reasoningItemId = randomUUID();
				active.reasoningText = "";
				const item = { type: "reasoning", id: active.reasoningItemId, summary: [], content: [] };
				active.items.push(item);
				this.broadcast(managed.id, { method: "item/started", params: { threadId: managed.id, turnId: active.id, item, startedAtMs: Date.now() } });
			}
			if (deltaEvent?.type === "thinking_delta" && active.reasoningItemId) {
				active.reasoningText += deltaEvent.delta || "";
				this.broadcast(managed.id, { method: "item/reasoning/textDelta", params: { threadId: managed.id, turnId: active.id, itemId: active.reasoningItemId, delta: deltaEvent.delta || "", contentIndex: 0 } });
			}
			if (deltaEvent?.type === "thinking_end" && active.reasoningItemId) {
				const item = active.items.find((candidate) => candidate.id === active.reasoningItemId);
				if (item) item.content = [deltaEvent.content || active.reasoningText];
				if (item) this.broadcast(managed.id, { method: "item/completed", params: { threadId: managed.id, turnId: active.id, item, completedAtMs: Date.now() } });
				active.reasoningItemId = null;
				active.reasoningText = "";
			}
		}
		if (event.type === "tool_execution_start") {
			const item = toolItemFromCall(event.toolCallId, event.toolName, event.args, managed.cwd);
			active.items.push(item);
			active.toolStartedAt.set(event.toolCallId, Date.now());
			this.broadcast(managed.id, { method: "item/started", params: { threadId: managed.id, turnId: active.id, item, startedAtMs: Date.now() } });
		}
		if (event.type === "tool_execution_update") {
			const item = active.items.find((candidate) => candidate.id === event.toolCallId);
			const output = textFromContent(event.partialResult?.content);
			const previous = active.toolOutputs.get(event.toolCallId) || "";
			const delta = output.startsWith(previous) ? output.slice(previous.length) : output;
			active.toolOutputs.set(event.toolCallId, output);
			if (delta && item?.type === "commandExecution") this.broadcast(managed.id, { method: "item/commandExecution/outputDelta", params: { threadId: managed.id, turnId: active.id, itemId: event.toolCallId, delta } });
		}
		if (event.type === "tool_execution_end") {
			const item = active.items.find((candidate) => candidate.id === event.toolCallId);
			applyToolResult(item, { ...event.result, isError: event.isError });
			if (item) item.durationMs = Math.max(0, Date.now() - (active.toolStartedAt.get(event.toolCallId) || Date.now()));
			if (item) this.broadcast(managed.id, { method: "item/completed", params: { threadId: managed.id, turnId: active.id, item, completedAtMs: Date.now() } });
		}
	}

	async handleMessage(client, state, message) {
		const { id, method, params = {} } = message;
		if (!method && id !== undefined && this.resolveClientResponse(client, message)) return;
		if (method === "initialized") return;
		if (method === "initialize") {
			if (state.initialized) return this.sendError(client, id, -32600, "Already initialized");
			state.initialized = true;
			this.sendResult(client, id, {
				userAgent: `codex_cli_rs/${CODEX_APP_SERVER_VERSION}`,
				codexHome: process.env.CODEX_HOME || join(homedir(), ".codex"),
				platformFamily: process.platform === "win32" ? "windows" : "unix",
				platformOs: process.platform === "darwin" ? "macos" : process.platform,
			});
			return;
		}
		if (!state.initialized) return this.sendError(client, id, -32000, "Not initialized");
		try {
			switch (method) {
				case "thread/list": {
					const infos = await this.allSessionInfo();
					let threads = infos.map((info) => this.threadFromInfo(info, this.sessions.get(info.id)));
					for (const managed of this.sessions.values()) if (!threads.some((thread) => thread.id === managed.id)) threads.push(this.threadFromManaged(managed));
					if (params.cwd) {
						const cwds = Array.isArray(params.cwd) ? params.cwd : [params.cwd];
						threads = threads.filter((thread) => cwds.includes(thread.cwd));
					}
					if (params.modelProviders?.length) threads = threads.filter((thread) => params.modelProviders.includes(thread.modelProvider));
					if (params.searchTerm) {
						const needle = String(params.searchTerm).toLocaleLowerCase();
						threads = threads.filter((thread) => `${thread.name || ""}\n${thread.preview}`.toLocaleLowerCase().includes(needle));
					}
					const sortKey = params.sortKey === "created_at" ? "createdAt" : params.sortKey === "updated_at" ? "updatedAt" : "recencyAt";
					threads.sort((left, right) => left[sortKey] - right[sortKey]);
					this.sendResult(client, id, page(threads, params, "desc"));
					return;
				}
				case "thread/start": {
					const managed = await this.createSession(params);
					managed.clients.add(client);
					const thread = this.threadFromManaged(managed);
					this.sendResult(client, id, this.threadStartResponse(managed, thread));
					this.send(client, { method: "thread/started", params: { thread } });
					return;
				}
				case "thread/resume": {
					const managed = await this.loadById(params.threadId);
					managed.clients.add(client);
					const thread = this.threadFromManaged(managed, true);
					this.sendResult(client, id, {
						...this.threadStartResponse(managed, thread),
						turnsBackwardsCursor: null,
						itemsBackwardsCursor: null,
					});
					return;
				}
				case "thread/fork": {
					const managed = await this.forkThread(params);
					managed.clients.add(client);
					this.sendResult(client, id, this.threadStartResponse(managed, this.threadFromManaged(managed, !params.excludeTurns)));
					return;
				}
				case "thread/name/set":
				case "thread/setName": {
					const managed = await this.loadById(params.threadId);
					managed.session.setSessionName(String(params.name || ""));
					this.sendResult(client, id, {});
					this.broadcast(managed.id, { method: "thread/name/updated", params: { threadId: managed.id, threadName: managed.session.sessionName || null } });
					return;
				}
				case "thread/search":
					this.sendResult(client, id, await this.searchThreads(params));
					return;
				case "thread/searchOccurrences":
					this.sendResult(client, id, await this.searchOccurrences(params));
					return;
				case "thread/read": {
					const managed = this.sessions.get(params.threadId);
					const info = managed ? undefined : await this.infoForId(params.threadId);
					if (!managed && !info) throw new Error(`Unknown thread: ${params.threadId}`);
					this.sendResult(client, id, { thread: managed ? this.threadFromManaged(managed, Boolean(params.includeTurns)) : this.threadFromInfo(info) });
					return;
				}
				case "thread/settings/update": {
					const managed = await this.loadById(params.threadId);
					await this.updateThreadSettings(managed, params);
					this.sendResult(client, id, {});
					return;
				}
				case "thread/turns/list": {
					const managed = await this.loadById(params.threadId);
					this.sendResult(client, id, page(turnsFromManager(managed.session.sessionManager), params, "desc"));
					return;
				}
				case "thread/items/list": {
					const managed = await this.loadById(params.threadId);
					const entries = turnsFromManager(managed.session.sessionManager).flatMap((turn) => turn.items.map((item) => ({ turnId: turn.id, item })));
					this.sendResult(client, id, page(params.turnId ? entries.filter((entry) => entry.turnId === params.turnId) : entries, params));
					return;
				}
				case "thread/loaded/list":
					this.sendResult(client, id, page([...this.sessions.keys()], params));
					return;
				case "thread/unsubscribe": {
					const managed = this.sessions.get(params.threadId);
					let status = "notLoaded";
					if (managed) status = managed.clients.delete(client) ? "unsubscribed" : "notSubscribed";
					this.sendResult(client, id, { status });
					return;
				}
				case "thread/delete": {
					const managed = this.sessions.get(params.threadId);
					if (managed?.activeTurn) throw new Error("Cannot delete while a turn is running");
					const info = await this.infoForId(params.threadId);
					const path = managed?.session.sessionFile || info?.path;
					if (!path && !managed) throw new Error(`Unknown thread: ${params.threadId}`);
					if (path) unlinkSync(path);
					this.sendResult(client, id, {});
					this.broadcast(params.threadId, { method: "thread/deleted", params: { threadId: params.threadId } });
					if (managed) {
						managed.session.dispose();
						this.sessions.delete(params.threadId);
					}
					return;
				}
				case "thread/compact/start": {
					const managed = await this.loadById(params.threadId);
					if (managed.activeTurn) throw new Error("Cannot compact while a turn is running");
					await managed.session.compact();
					this.sendResult(client, id, {});
					return;
				}
				case "thread/revert": {
					const managed = await this.loadById(params.threadId);
					await this.revertBeforeTurn(managed, params.beforeTurnId);
					const hasHistory = turnsFromManager(managed.session.sessionManager).length > 0;
					this.sendResult(client, id, { thread: this.threadFromManaged(managed), turnsBackwardsCursor: hasHistory ? "0" : null, itemsBackwardsCursor: hasHistory ? "0" : null });
					return;
				}
				case "thread/rollback": {
					const managed = await this.loadById(params.threadId);
					const turns = turnsFromManager(managed.session.sessionManager);
					if (!Number.isInteger(params.numTurns) || params.numTurns < 1 || params.numTurns > turns.length) throw new Error("numTurns must be between 1 and the number of turns");
					await this.revertBeforeTurn(managed, turns[turns.length - params.numTurns].id);
					this.sendResult(client, id, { thread: this.threadFromManaged(managed, true) });
					return;
				}
				case "turn/start": {
					const managed = await this.loadById(params.threadId);
					await this.updateThreadSettings(managed, params);
					managed.clients.add(client);
					const turn = this.beginTurn(managed, params.input, params.clientUserMessageId);
					this.sendResult(client, id, { turn });
					this.announceTurn(managed, turn);
					void this.runTurn(managed, turn.id, params.input);
					return;
				}
				case "turn/steer": {
					const managed = this.sessions.get(params.threadId);
					if (!managed?.activeTurn) throw new Error("Thread is not running");
					if (params.expectedTurnId && params.expectedTurnId !== managed.activeTurn.id) throw new Error("expectedTurnId does not match the active turn");
					await managed.session.steer(textFromInput(params.input));
					this.sendResult(client, id, { turnId: managed.activeTurn.id });
					return;
				}
				case "turn/interrupt": {
					const managed = this.sessions.get(params.threadId);
					if (managed?.activeTurn && params.turnId && params.turnId !== managed.activeTurn.id) throw new Error("turnId does not match the active turn");
					if (managed?.activeTurn) {
						const turnId = managed.activeTurn.id;
						managed.activeTurn.interrupted = true;
						await managed.session.abort();
						if (managed.activeTurn) this.finishTurn(managed, "interrupted", turnId);
					}
					this.sendResult(client, id, {});
					return;
				}
				case "remoteControl/status/read":
					this.sendResult(client, id, this.remote?.status() || { status: "disabled", installationId: null, serverName: "pi-host", environmentId: null });
					return;
				case "remoteControl/enable":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					await this.remote.start();
					this.sendResult(client, id, this.remote.status());
					return;
				case "remoteControl/disable":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					this.remote.stop();
					this.sendResult(client, id, this.remote.status());
					return;
				case "remoteControl/pairing/start":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					this.sendResult(client, id, await this.remote.pair(params));
					return;
				case "remoteControl/pairing/status":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					this.sendResult(client, id, await this.remote.pairingStatus(params));
					return;
				case "remoteControl/client/list":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					this.sendResult(client, id, await this.remote.listClients(params));
					return;
				case "remoteControl/client/revoke":
					if (!this.remote) throw new Error("Remote control was not enabled; start with --remote");
					this.sendResult(client, id, await this.remote.revokeClient(params));
					return;
				case "threadSection/list":
					this.sendResult(client, id, { data: [], nextCursor: null });
					return;
				case "thread/goal/get": {
					const managed = await this.loadById(params.threadId);
					this.sendResult(client, id, { goal: remoteState(managed.session.sessionManager).goal });
					return;
				}
				case "thread/goal/set": {
					const managed = await this.loadById(params.threadId);
					const current = remoteState(managed.session.sessionManager).goal;
					const now = Math.floor(Date.now() / 1000);
					const goal = {
						threadId: managed.id,
						objective: params.objective ?? current?.objective ?? "",
						status: params.status ?? current?.status ?? "active",
						tokenBudget: Object.prototype.hasOwnProperty.call(params, "tokenBudget") ? params.tokenBudget : current?.tokenBudget ?? null,
						tokensUsed: current?.tokensUsed ?? 0,
						timeUsedSeconds: current?.timeUsedSeconds ?? 0,
						createdAt: current?.createdAt ?? now,
						updatedAt: now,
					};
					saveRemoteState(managed.session.sessionManager, "goal", goal);
					this.sendResult(client, id, { goal });
					this.broadcast(managed.id, { method: "thread/goal/updated", params: { threadId: managed.id, turnId: managed.activeTurn?.id || null, goal } });
					return;
				}
				case "thread/goal/clear": {
					const managed = await this.loadById(params.threadId);
					const cleared = Boolean(remoteState(managed.session.sessionManager).goal);
					saveRemoteState(managed.session.sessionManager, "goal", null);
					this.sendResult(client, id, { cleared });
					if (cleared) this.broadcast(managed.id, { method: "thread/goal/cleared", params: { threadId: managed.id } });
					return;
				}
				case "thread/queue/list": {
					const managed = await this.loadById(params.threadId);
					this.sendResult(client, id, page(remoteState(managed.session.sessionManager).queue, params));
					return;
				}
				case "thread/queue/add": {
					const managed = await this.loadById(params.threadId);
					if (!params.clientUserMessageId) throw new Error("clientUserMessageId is required");
					const state = remoteState(managed.session.sessionManager);
					const queuedSubmission = { id: randomUUID(), input: params.input || [], clientUserMessageId: params.clientUserMessageId };
					state.queue.push(queuedSubmission);
					saveRemoteState(managed.session.sessionManager, "queue", state.queue);
					this.sendResult(client, id, { queuedSubmission });
					this.broadcast(managed.id, { method: "thread/queue/changed", params: { threadId: managed.id } });
					return;
				}
				case "thread/queue/update": {
					const managed = await this.loadById(params.threadId);
					const state = remoteState(managed.session.sessionManager);
					const queuedSubmission = state.queue.find((entry) => entry.id === params.queuedSubmissionId);
					if (!queuedSubmission) throw new Error(`Unknown queued submission: ${params.queuedSubmissionId}`);
					queuedSubmission.input = params.input || [];
					saveRemoteState(managed.session.sessionManager, "queue", state.queue);
					this.sendResult(client, id, { queuedSubmission });
					this.broadcast(managed.id, { method: "thread/queue/changed", params: { threadId: managed.id } });
					return;
				}
				case "thread/queue/delete": {
					const managed = await this.loadById(params.threadId);
					const state = remoteState(managed.session.sessionManager);
					const queue = state.queue.filter((entry) => entry.id !== params.queuedSubmissionId);
					saveRemoteState(managed.session.sessionManager, "queue", queue);
					const deleted = queue.length !== state.queue.length;
					this.sendResult(client, id, { deleted });
					if (deleted) this.broadcast(managed.id, { method: "thread/queue/changed", params: { threadId: managed.id } });
					return;
				}
				case "thread/queue/reorder": {
					const managed = await this.loadById(params.threadId);
					const state = remoteState(managed.session.sessionManager);
					const byId = new Map(state.queue.map((entry) => [entry.id, entry]));
					if (params.queuedSubmissionIds.length !== state.queue.length || new Set(params.queuedSubmissionIds).size !== state.queue.length || params.queuedSubmissionIds.some((entry) => !byId.has(entry))) throw new Error("Queue reorder must contain every queued submission exactly once");
					const queue = params.queuedSubmissionIds.map((entry) => byId.get(entry));
					saveRemoteState(managed.session.sessionManager, "queue", queue);
					this.sendResult(client, id, {});
					this.broadcast(managed.id, { method: "thread/queue/changed", params: { threadId: managed.id } });
					return;
				}
				case "thread/queue/start": {
					const managed = await this.loadById(params.threadId);
					const state = remoteState(managed.session.sessionManager);
					const index = params.queuedSubmissionId ? state.queue.findIndex((entry) => entry.id === params.queuedSubmissionId) : 0;
					if (index < 0 || !state.queue[index]) throw new Error("No queued submission is available");
					const [queuedSubmission] = state.queue.splice(index, 1);
					saveRemoteState(managed.session.sessionManager, "queue", state.queue);
					managed.clients.add(client);
					const turn = this.beginTurn(managed, queuedSubmission.input, queuedSubmission.clientUserMessageId);
					this.sendResult(client, id, { turn });
					this.broadcast(managed.id, { method: "thread/queue/changed", params: { threadId: managed.id } });
					this.announceTurn(managed, turn);
					void this.runTurn(managed, turn.id, queuedSubmission.input);
					return;
				}
				case "config/read":
					this.sendResult(client, id, { config: {}, origins: {}, layers: null });
					return;
				case "configRequirements/read":
					this.sendResult(client, id, { requirements: null });
					return;
				case "collaborationMode/list":
					this.sendResult(client, id, { data: [] });
					return;
				case "plugin/installed":
					this.sendResult(client, id, { marketplaces: [], marketplaceLoadErrors: [] });
					return;
				case "model/list": {
					const entries = this.scopedModels.length
						? this.scopedModels.map((entry) => modelForCodex(entry.model, entry))
						: this.modelRuntime.getModels().map((model) => modelForCodex(model));
					const data = [...new Map(entries.map((model) => [model.id, model])).values()];
					this.sendResult(client, id, { data, nextCursor: null });
					return;
				}
				case "account/read":
					this.sendResult(client, id, { account: null });
					return;
				default:
					this.sendError(client, id, -32601, `Method not implemented: ${method}`);
			}
		} catch (error) {
			this.sendError(client, id, -32000, error instanceof Error ? error.message : String(error));
		}
	}

	threadStartResponse(managed, thread) {
		const model = managed.session.model;
		return {
			thread,
			model: model?.id || "",
			modelProvider: model?.provider || "",
			serviceTier: null,
			cwd: managed.cwd,
			runtimeWorkspaceRoots: [],
			instructionSources: [],
			approvalPolicy: "on-request",
			approvalsReviewer: "user",
			sandbox: { type: "dangerFullAccess" },
			activePermissionProfile: { id: "pi-remote", extends: null },
			reasoningEffort: effortForCodex(managed.session.thinkingLevel) || null,
			multiAgentMode: "explicitRequestOnly",
		};
	}

	async stop() {
		for (const managed of this.sessions.values()) {
			try {
				if (managed.activeTurn) await managed.session.abort();
				managed.session.dispose();
			} catch (error) {
				console.error(`Failed to dispose ${managed.id}:`, error);
			}
		}
		this.sessions.clear();
		for (const pending of this.pendingRequests.values()) pending.resolve(undefined);
		this.pendingRequests.clear();
		await this.permissionBridge.stop();
		this.remote?.stop();
		this.websocketServer.close();
		if (this.server) await new Promise((resolvePromise) => this.server.close(() => resolvePromise()));
		if (!this.options.port && existsSync(this.options.socket)) unlinkSync(this.options.socket);
	}
}

function printHelp() {
	console.log(`Usage:
  pi-remote-daemon start [--remote] [--socket PATH] [--port PORT]
  pi-remote-daemon pair [--remote-url URL] [--wait] [--re-enroll]
  pi-remote-daemon takeover --session PATH [--pid PID] [--force]

Options:
  --socket PATH  Unix socket path (default: ${defaultSocket})
  --port PORT    Listen on a WebSocket TCP port instead of Unix socket
  --session PATH Session file to load or take over
  --pid PID      Pi TUI PID to terminate
  --force        Use SIGKILL after graceful termination times out
  --remote       Connect this daemon to Codex Mobile's relay
  --models PATTERNS  Comma-separated model scope (overrides enabledModels)
  --remote-url   Codex backend URL (default: https://chatgpt.com/backend-api)
  --wait         Log pairing status until mobile claims the code
  --re-enroll    Refresh enrollment metadata while keeping the installation ID`);
}

const options = parseArgs(process.argv.slice(2));
if (options.help) {
	printHelp();
	process.exit(0);
}

try {
	if (options.command === "pair") {
		await new RemoteControlClient(undefined, options).pair();
		process.exit(0);
	}
	if (options.command === "takeover" || (options.command === "start" && options.session)) {
		if (!options.session) throw new Error("takeover requires --session PATH");
		await takeOver(options.session, options.pid, options.force);
	}
	const daemon = new PiDaemon(options);
	let stopping = false;
	const shutdown = async (signal) => {
		if (stopping) return;
		stopping = true;
		console.error(`Received ${signal}; stopping daemon`);
		await daemon.stop();
		if (options.session) console.log(`Resume command: ${resumeCommand(options.session)}`);
		process.exit(0);
	};
	process.once("SIGINT", () => void shutdown("SIGINT"));
	process.once("SIGTERM", () => void shutdown("SIGTERM"));
	await daemon.start();
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
