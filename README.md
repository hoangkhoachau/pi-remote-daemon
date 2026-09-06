# pi-remote-daemon

Standalone daemon for controlling Pi sessions through a Codex app-server-compatible JSON-RPC endpoint.

## Install

```sh
git clone https://github.com/hoangkhoachau/pi-remote-daemon.git
cd pi-remote-daemon
npm install
```

The repository launcher is `bin/pi-remote-daemon`. Add it to your `PATH` or link it from a directory already in your `PATH`.

For graceful takeover of an active Pi TUI session, install the optional bridge extension:

```sh
mkdir -p ~/.pi/agent/extensions
cp extensions/pi-remote-bridge.ts ~/.pi/agent/extensions/
```

Restart Pi or run `/reload` after installing the extension.

## Run

Start on a Unix socket:

```sh
pi-remote-daemon start
```

The model selector mirrors Pi's `enabledModels` scope from `~/.pi/agent/settings.json`, and model-change requests outside that scope are rejected. Override it for a daemon with `--models`:

```sh
pi-remote-daemon start --models 'openai-codex/gpt-5.6-*,anthropic/claude-sonnet-5'
```

Enable Codex Mobile remote control:

```sh
pi-remote-daemon pair --wait
pi-remote-daemon start --remote
```

`pair` reads the ChatGPT login from `~/.codex/auth.json`, enrolls this daemon, and prints a pairing code. Enter that code in the Codex mobile app. With `--wait`, it logs `claimed=false` until the app accepts the code, then logs `pairing completed`.

Start on a WebSocket TCP port:

```sh
pi-remote-daemon start --port 4318
```

Take over an existing Pi session:

```sh
pi-remote-daemon takeover --session ~/.pi/agent/sessions/.../session.jsonl
```

The optional `pi-remote-bridge.ts` extension exposes a per-session Unix socket. When present, the daemon asks Pi to shut down through `ctx.shutdown()`, so Pi waits for idle, cleans up the TUI, and prints its own resume command before the daemon opens the JSONL session. Without the bridge, pass `--pid` for a direct SIGTERM takeover.

```text
To resume this session: pi --session '/path/to/session.jsonl'
```

Use `--pid` when process discovery cannot identify the TUI (an idle Pi may not have its JSONL file open). Use `--force` only when a graceful SIGTERM does not stop it.

## Supported protocol methods

- `initialize`
- `thread/list`
- `thread/start`
- `thread/resume`
- `thread/read`
- `thread/fork` (including `lastTurnId` and `beforeTurnId`)
- `thread/name/set` (`thread/setName` remains as a legacy alias)
- `thread/settings/update`
- `thread/compact/start`, `thread/revert`, `thread/rollback`, `thread/delete`
- `thread/search`
- `thread/searchOccurrences`
- `thread/turns/list` and `thread/items/list` with cursors
- `thread/loaded/list`
- `thread/unsubscribe`
- `thread/goal/get`, `thread/goal/set`, `thread/goal/clear`
- `thread/queue/add`, `thread/queue/list`, `thread/queue/update`, `thread/queue/delete`, `thread/queue/reorder`, `thread/queue/start`
- `turn/start`
- `turn/steer`
- `turn/interrupt`
- `model/list`
- `account/read`

The remote connector follows the open-source Codex remote-control transport. It refreshes the remote-control token before expiry, segments relay messages, reassembles inbound segments, and replays unacknowledged outbound messages after reconnect. It uses the ChatGPT credentials already stored by Codex and persists enrollment data in `~/.pi/agent/app-server/remote-control.json`.

Goals and queue state are stored as Pi custom session entries (`pi-remote-daemon`) in the session JSONL. They survive daemon restarts and do not enter the LLM context.

Dangerous `bash` commands use the existing `permission-gate.ts` extension. In an in-process daemon session, it sends an `item/commandExecution/requestApproval` request to the connected client and fails closed if no client approves it. Restart or `/reload` Pi after updating that extension.

The TCP listener is intentionally restricted to loopback because this daemon has no local authentication, and browser-origin WebSocket upgrades are rejected. Remote-control URLs are limited to HTTPS ChatGPT hosts or localhost so Codex credentials cannot be sent to arbitrary servers. The Unix socket startup refuses to replace a live daemon socket.

This remains a partial app-server bridge. Unsupported methods and settings return errors instead of placeholder success responses.

Run the protocol and transport security checks with `npm test`.
