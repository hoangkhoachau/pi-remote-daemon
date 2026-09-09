# pi-remote-daemon

Standalone daemon for controlling Pi sessions through a Codex app-server-compatible JSON-RPC endpoint.

## Install

```sh
git clone https://github.com/hoangkhoachau/pi-remote-daemon.git
cd pi-remote-daemon
npm install
```

The repository launcher is `bin/pi-remote-daemon`. Add it to your `PATH` or link it from a directory already in your `PATH`.

For graceful daemon/TUI session handoff, install the optional bridge extension:

```sh
mkdir -p ~/.pi/agent/extensions
cp extensions/pi-remote-bridge.ts ~/.pi/agent/extensions/
```

Restart Pi or run `/reload` after installing the extension.

## Run

There are only two commands:

```sh
# Prints a code and waits for Codex Mobile to claim it.
pi-remote-daemon pair

# Starts the daemon and connects it to Codex Mobile.
pi-remote-daemon start
```

`pair` reads the ChatGPT login from `~/.codex/auth.json`, enrolls this daemon, and prints a pairing code. Enter it in Codex Mobile; the command confirms when pairing completes.

The daemon listens on its standard Unix socket at `~/.pi/agent/app-server/control.sock`. Its model list follows `enabledModels` in `~/.pi/agent/settings.json`.

The optional `pi-remote-bridge.ts` extension makes handoff safe when a Pi TUI and the daemon resume the same session. It is not required for basic pairing and remote control.

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
- Paginated history mode: persisted as Pi session metadata; start/resume responses omit inline turns and provide cursors
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
