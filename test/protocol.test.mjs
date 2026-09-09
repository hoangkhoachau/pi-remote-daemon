import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { WebSocket } from "ws";
import { selectModelCandidate } from "../src/model-resolution.mjs";
import { toolItemFromCall } from "../src/tool-items.mjs";

const root = new URL("..", import.meta.url);

async function startDaemon() {
	const agentDir = mkdtempSync(join(tmpdir(), "pi-remote-test-"));
	const socketPath = join(agentDir, "app-server", "control.sock");
	const child = spawn(process.execPath, ["src/main.mjs", "start"], {
		cwd: root,
		env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
		stdio: ["ignore", "ignore", "pipe"],
	});
	let stderr = "";
	child.stderr.setEncoding("utf8");
	await new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error(`daemon startup timed out: ${stderr}`)), 15_000);
		child.stderr.on("data", (chunk) => {
			stderr += chunk;
			if (stderr.includes("listening on")) {
				clearTimeout(timer);
				resolve();
			}
		});
		child.once("exit", (code) => {
			clearTimeout(timer);
			reject(new Error(`daemon exited with ${code}: ${stderr}`));
		});
	});
	return {
		socketPath,
		agentDir,
		async stop() {
			child.kill("SIGTERM");
			if (child.exitCode == null) await new Promise((resolve) => child.once("exit", resolve));
			rmSync(agentDir, { recursive: true, force: true });
		},
	};
}

async function requestUnixSocket(socketPath, request) {
	return new Promise((resolve, reject) => {
		const socket = createConnection(socketPath);
		let buffer = "";
		const timer = setTimeout(() => {
			socket.destroy();
			reject(new Error(`socket request timed out: ${socketPath}`));
		}, 5_000);
		socket.once("connect", () => socket.write(`${JSON.stringify(request)}\n`));
		socket.on("data", (chunk) => {
			buffer += chunk;
			const newline = buffer.indexOf("\n");
			if (newline < 0) return;
			clearTimeout(timer);
			socket.destroy();
			resolve(JSON.parse(buffer.slice(0, newline)));
		});
		socket.once("error", (error) => {
			clearTimeout(timer);
			reject(error);
		});
	});
}

async function connect(socketPath, options) {
	const socket = new WebSocket(`ws+unix://${socketPath}:/`, options);
	await new Promise((resolve, reject) => {
		socket.once("open", resolve);
		socket.once("error", reject);
	});
	return socket;
}

function rpcClient(socket) {
	let nextId = 1;
	const pending = new Map();
	socket.on("message", (raw) => {
		const message = JSON.parse(raw);
		if (message.id != null) pending.get(message.id)?.(message);
	});
	return (method, params = {}) => new Promise((resolve) => {
		const id = nextId++;
		pending.set(id, resolve);
		socket.send(JSON.stringify({ id, method, params }));
	});
}

test("serves basic app-server RPC and rejects fake success", async () => {
	const daemon = await startDaemon();
	try {
		const socket = await connect(daemon.socketPath);
		const rpc = rpcClient(socket);
		const initialized = await rpc("initialize", { clientInfo: { name: "test", version: "1" }, capabilities: {} });
		assert.match(initialized.result.userAgent, /^codex_cli_rs\//);
		socket.send(JSON.stringify({ method: "initialized" }));
		assert.ok(Array.isArray((await rpc("model/list")).result.data));
		assert.equal((await rpc("process/spawn")).error.code, -32601);
		socket.close();
	} finally {
		await daemon.stop();
	}
});

test("projects Pi read calls as Codex read command actions", () => {
	const item = toolItemFromCall("read-call", "read", { path: "docs/guide.md" }, "/workspace");
	assert.deepEqual(item, {
		type: "commandExecution",
		id: "read-call",
		pluginId: null,
		scriptPath: null,
		command: "read /workspace/docs/guide.md",
		cwd: "/workspace",
		processId: null,
		source: "agent",
		status: "inProgress",
		commandActions: [{
			type: "read",
			command: "read /workspace/docs/guide.md",
			name: "guide.md",
			path: "/workspace/docs/guide.md",
		}],
		aggregatedOutput: null,
		exitCode: null,
		durationMs: null,
	});
});

test("projects Pi search and directory tools as Codex command actions", () => {
	assert.deepEqual(toolItemFromCall("grep-call", "grep", { pattern: "TODO", path: "src" }, "/workspace").commandActions, [{
		type: "search",
		command: "grep TODO /workspace/src",
		path: "/workspace/src",
		query: "TODO",
	}]);
	assert.deepEqual(toolItemFromCall("find-call", "find", { pattern: "*.mjs", path: "src" }, "/workspace").commandActions, [{
		type: "listFiles",
		command: "find *.mjs /workspace/src",
		path: "/workspace/src",
	}]);
	assert.deepEqual(toolItemFromCall("ls-call", "ls", {}, "/workspace").commandActions, [{
		type: "listFiles",
		command: "ls /workspace",
		path: "/workspace",
	}]);
});

test("resolves a bare Mobile model ID through the configured model scope", () => {
	const models = [
		{ provider: "openai-codex", id: "gpt-5.6-luna" },
		{ provider: "openai", id: "gpt-5.6-luna" },
		{ provider: "google", id: "gemini-flash-lite-latest" },
		{ provider: "google-vertex", id: "gemini-flash-lite-latest" },
	];
	const scopedModels = [{ model: models[0] }, { model: models[2] }];
	assert.equal(selectModelCandidate(models, scopedModels, "gpt-5.6-luna")?.provider, "openai-codex");
	assert.equal(selectModelCandidate(models, scopedModels, "gemini-flash-lite-latest")?.provider, "google");
});

test("starts and resumes a paginated thread without inline history", async () => {
	const daemon = await startDaemon();
	try {
		const socket = await connect(daemon.socketPath);
		const rpc = rpcClient(socket);
		await rpc("initialize", { clientInfo: { name: "test", version: "1" }, capabilities: {} });
		const started = await rpc("thread/start", {
			cwd: daemon.agentDir,
			ephemeral: true,
			historyMode: "paginated",
			config: { ui: { source: "mobile" } },
		});
		assert.ok(started.result?.thread?.id, JSON.stringify(started));
		assert.equal(started.result.thread.historyMode, "paginated");
		assert.deepEqual(started.result.thread.turns, []);
		assert.equal(started.result.turnsBackwardsCursor, null);
		assert.equal(started.result.itemsBackwardsCursor, null);

		const resumed = await rpc("thread/resume", { threadId: started.result.thread.id });
		assert.equal(resumed.result.thread.historyMode, "paginated");
		assert.deepEqual(resumed.result.thread.turns, []);
		assert.equal((await rpc("thread/turns/list", { threadId: started.result.thread.id, sortDirection: "desc" })).result.nextCursor, null);
		socket.close();
	} finally {
		await daemon.stop();
	}
});

test("lets a Pi TUI claim and release session ownership", async () => {
	const daemon = await startDaemon();
	try {
		const controlSocket = join(daemon.agentDir, "app-server", "daemon-control.sock");
		assert.deepEqual(await requestUnixSocket(controlSocket, { command: "claim", sessionId: "tui-owned", pid: 123 }), { ok: true, released: false });
		assert.deepEqual(await requestUnixSocket(controlSocket, { command: "release", sessionId: "tui-owned", pid: 123 }), { ok: true });
	} finally {
		await daemon.stop();
	}
});

test("accepts only the pair and start commands", async () => {
	const child = spawn(process.execPath, ["src/main.mjs", "start", "--no-remote"], {
		cwd: root,
		stdio: ["ignore", "ignore", "pipe"],
	});
	let stderr = "";
	child.stderr.setEncoding("utf8");
	child.stderr.on("data", (chunk) => { stderr += chunk; });
	const code = await new Promise((resolve) => child.once("exit", resolve));
	assert.notEqual(code, 0);
	assert.match(stderr, /Usage: pi-remote-daemon <pair\|start>/);
});
