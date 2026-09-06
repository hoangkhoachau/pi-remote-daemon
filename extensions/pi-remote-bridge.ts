import { createServer, type Server } from "node:net";
import { chmod, mkdir, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

const agentDir = process.env.PI_CODING_AGENT_DIR || join(homedir(), ".pi", "agent");
const controlDir = join(agentDir, "app-server", "session-control");

export default function piRemoteBridge(pi: ExtensionAPI) {
	let server: Server | undefined;
	let socketPath: string | undefined;

	async function closeControl(): Promise<void> {
		const currentServer = server;
		server = undefined;
		if (currentServer) {
			await new Promise<void>((resolve) => currentServer.close(() => resolve()));
		}
		if (socketPath) await rm(socketPath, { force: true }).catch(() => {});
		socketPath = undefined;
	}

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.mode !== "tui") return;
		await closeControl();

		const sessionId = ctx.sessionManager.getSessionId();
		if (!ctx.sessionManager.getSessionFile() || !sessionId) return;
		socketPath = join(controlDir, `${sessionId}.sock`);
		await mkdir(controlDir, { recursive: true, mode: 0o700 });
		await rm(socketPath, { force: true });

		const sessionContext = ctx;
		server = createServer((connection) => {
			let buffer = "";
			connection.setEncoding("utf8");
			connection.on("data", (chunk) => {
				buffer += chunk;
				while (true) {
					const newline = buffer.indexOf("\n");
					if (newline < 0) return;
					const line = buffer.slice(0, newline).trim();
					buffer = buffer.slice(newline + 1);
					try {
						const request = JSON.parse(line);
						if (request.command === "ping") {
							connection.write(`${JSON.stringify({ ok: true, pid: process.pid, sessionId })}\n`);
						} else if (request.command === "shutdown") {
							connection.write(`${JSON.stringify({ ok: true, accepted: true, pid: process.pid, sessionId })}\n`);
							sessionContext.shutdown();
						} else {
							connection.write(`${JSON.stringify({ ok: false, error: "unknown command" })}\n`);
						}
					} catch {
						connection.write(`${JSON.stringify({ ok: false, error: "invalid request" })}\n`);
					}
				}
			});
		});

		await new Promise<void>((resolve, reject) => {
			server?.once("error", reject);
			server?.listen(socketPath, resolve);
		});
		await chmod(socketPath, 0o600);
	});

	pi.on("session_shutdown", async () => {
		await closeControl();
	});
}
