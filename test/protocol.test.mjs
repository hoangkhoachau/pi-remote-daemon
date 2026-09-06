import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { WebSocket } from "ws";

const root = new URL("..", import.meta.url);

async function unusedPort() {
	const server = createServer();
	await new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(0, "127.0.0.1", resolve);
	});
	const port = server.address().port;
	await new Promise((resolve) => server.close(resolve));
	return port;
}

async function startDaemon() {
	const port = await unusedPort();
	const agentDir = mkdtempSync(join(tmpdir(), "pi-remote-test-"));
	const child = spawn(process.execPath, ["src/main.mjs", "start", "--port", String(port)], {
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
		port,
		async stop() {
			child.kill("SIGTERM");
			if (child.exitCode == null) await new Promise((resolve) => child.once("exit", resolve));
			rmSync(agentDir, { recursive: true, force: true });
		},
	};
}

async function connect(port, options) {
	const socket = new WebSocket(`ws://127.0.0.1:${port}`, options);
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
		const socket = await connect(daemon.port);
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

test("rejects browser-origin TCP WebSocket upgrades", async () => {
	const daemon = await startDaemon();
	try {
		const status = await new Promise((resolve, reject) => {
			const socket = new WebSocket(`ws://127.0.0.1:${daemon.port}`, { headers: { Origin: "https://evil.example" } });
			socket.once("unexpected-response", (_request, response) => resolve(response.statusCode));
			socket.once("open", () => reject(new Error("browser-origin socket unexpectedly opened")));
			socket.once("error", () => {});
		});
		assert.equal(status, 403);
	} finally {
		await daemon.stop();
	}
});

test("rejects credential-bearing remote control on untrusted hosts", async () => {
	const child = spawn(process.execPath, ["src/main.mjs", "pair", "--remote-url", "https://attacker.example"], {
		cwd: root,
		stdio: ["ignore", "ignore", "pipe"],
	});
	let stderr = "";
	child.stderr.setEncoding("utf8");
	child.stderr.on("data", (chunk) => { stderr += chunk; });
	const code = await new Promise((resolve) => child.once("exit", resolve));
	assert.notEqual(code, 0);
	assert.match(stderr, /remote URL must use HTTPS/);
});
