import { readFileSync, writeFileSync, renameSync, mkdtempSync, openSync, fstatSync, readSync, closeSync, existsSync, mkdirSync, copyFileSync, constants } from "node:fs"
import { randomUUID } from "node:crypto"
import { fileURLToPath } from "node:url"
import { resolve, dirname } from "node:path"

export default async ({ client }) => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..")
  const launch = JSON.parse(readFileSync(resolve(root, "guard-launch.json"), "utf8"))
  const sessions = new Map()
  const captures = new Map()
  const startup = resolve(root, "shell-init.sh")
  const captureKey = ({ sessionID, callID }) => JSON.stringify([sessionID, callID])
  const hooks = {
    config: async (config) => {
      config.shell = "/bin/bash"
      writeFileSync(startup, 'exec >>"$TORII_SHELL_OUTPUT" 2>&1\nprintf ready >"$TORII_SHELL_OUTPUT.ready"\nunset BASH_ENV TORII_SHELL_OUTPUT\n', { mode: 0o600 })
      const proof = resolve(root, "guard-ready.json")
      writeFileSync(proof + ".tmp", JSON.stringify(launch) + "\n", { mode: 0o600 })
      renameSync(proof + ".tmp", proof)
    },
    "shell.env": async (input, output) => {
      if (!input.sessionID || !input.callID) return
      const directory = mkdtempSync(resolve(root, "shell-"))
      const path = resolve(directory, "output")
      writeFileSync(path, "", { mode: 0o600, flag: "wx" })
      captures.set(captureKey(input), path)
      output.env.BASH_ENV = startup
      output.env.TORII_SHELL_OUTPUT = path
    },
    "tool.execute.after": async (input, output) => {
      if (input.tool !== "bash") return
      const key = captureKey(input)
      const path = captures.get(key)
      captures.delete(key)
      if (!path || !existsSync(path + ".ready")) throw new Error("Shell output capture did not start")
      const fd = openSync(path, "r")
      let bytes
      let size
      try {
        size = fstatSync(fd).size
        bytes = Buffer.alloc(Math.min(size, 50 * 1024))
        let offset = 0
        while (offset < bytes.length) {
          const count = readSync(fd, bytes, offset, bytes.length - offset, Math.max(0, size - bytes.length) + offset)
          if (!count) throw new Error("Shell output capture ended during read")
          offset += count
        }
      } finally {
        closeSync(fd)
      }
      let start = 0
      if (size > bytes.length) {
        while (start < bytes.length && (bytes[start] & 0xc0) === 0x80) start++
      }
      const lines = bytes.subarray(start).toString("utf8").split("\n")
      const truncated = size > bytes.length || lines.length > 2000
      let text = lines.slice(-2000).join("\n") || "(no output)"
      let outputPath
      if (truncated) {
        const directory = resolve(process.env.XDG_DATA_HOME, "opencode/tool-output")
        mkdirSync(directory, { recursive: true, mode: 0o700 })
        outputPath = resolve(directory, "tool_torii_" + randomUUID())
        copyFileSync(path, outputPath, constants.COPYFILE_EXCL)
        text = `...output truncated...\n\nFull output saved to: ${outputPath}\n\n` + text
      }
      const marker = output.output.indexOf("\n\n<shell_metadata>")
      if (marker !== -1) text += output.output.slice(marker)
      output.output = text
      output.metadata = { ...output.metadata, output: text, truncated, ...(truncated ? { outputPath } : {}) }
    },
    "chat.params": async ({ sessionID }) => {
      if (sessions.get(sessionID)?.stopping) throw new Error("provider_stream_incomplete")
    },
    event: ({ event }) => {
      if (event.type === "session.error" && event.properties?.sessionID) {
        const path = resolve(root, "provider-error.json")
        if (!existsSync(path)) return
        let marker
        try { marker = JSON.parse(readFileSync(path, "utf8")) } catch { return }
        if (marker?.launch !== launch.launch || marker.session_id !== event.properties.sessionID || marker.error) return
        const error = event.properties.error
        const data = error?.data ?? {}
        let body
        try { body = JSON.parse(data.responseBody ?? "{}") } catch {}
        const clean = (value) => String(value ?? "")
          .replace(/\bBearer\s+\S+|\b(?:sk-|key-)[A-Za-z0-9_-]+/gi, "[credential]")
          .replace(/\b\d{5,}:[A-Za-z0-9_-]{30,}|[A-Za-z0-9_-]{40,}/g, "[credential]")
          .replace(/https?:\/\/\S+/gi, "[url]")
          .replace(/\s+/g, " ").slice(0, 300)
        const code = body?.error?.code ?? body?.code
        const status = data.statusCode ?? body?.error?.status
        let reset = Number.isFinite(body?.resetAt) && body.resetAt > 0 ? body.resetAt / 1000 : null
        if (reset === null) {
          const header = Object.entries(data.responseHeaders ?? {}).find(([name]) => name.toLowerCase() === "retry-after")?.[1]
          const seconds = typeof header === "string" && header.trim() ? Number(header) : NaN
          const retry = Number.isFinite(seconds) && seconds >= 0 ? Date.now() + seconds * 1000 : Date.parse(header)
          if (Number.isFinite(retry)) reset = retry / 1000
        }
        marker.error = { name: clean(error?.name).slice(0, 64),
          status: Number.isInteger(status) ? status : null,
          code: code === "weekly_included_input_token_limit_exceeded" ? code : clean(code).slice(0, 64),
          reset_at: reset,
          message: clean(body?.error?.message ?? data.message ?? error?.message) }
        writeFileSync(path + ".tmp", JSON.stringify(marker) + "\n", { mode: 0o600 })
        renameSync(path + ".tmp", path)
        return
      }
      const part = event.properties?.part
      if (event.type !== "message.part.updated" || !part?.sessionID) return
      const sid = part.sessionID
      let state = sessions.get(sid)
      if (!state) {
        state = { consecutive: 0, progress: new Set(), finished: new Set(), stopping: false }
        sessions.set(sid, state)
      }
      if (part.type === "tool" && ["completed", "error"].includes(part.state?.status)) {
        state.progress.add(part.messageID)
      }
      if (part.type !== "step-finish" || state.finished.has(part.id) || state.stopping) return
      state.finished.add(part.id)
      const progress = state.progress.delete(part.messageID)
      const incomplete = !part.reason || part.reason === "unknown" || part.reason === "tool-calls"
      state.consecutive = incomplete && !progress ? state.consecutive + 1 : 0
      if (state.consecutive < 3) return
      state.stopping = true
      const marker = { version: 1, code: "provider_stream_incomplete", session_id: sid, consecutive: 3 }
      const path = resolve(root, "stream-incomplete.json")
      writeFileSync(path + ".tmp", JSON.stringify(marker) + "\n", { mode: 0o600 })
      renameSync(path + ".tmp", path)
      void client.session.abort({ path: { id: sid } }).catch(() => {})
    }
  }
  return hooks
}
