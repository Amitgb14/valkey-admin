import fs from "node:fs"
import express from "express"
import { GlideClient } from "@valkey/valkey-glide"
import net from "net"
import { defer, timer, firstValueFrom } from "rxjs"
import { retry, map } from "rxjs/operators"
import tls from "tls"
import { getConfig, updateConfig } from "./config.js"
import * as Streamer from "./effects/ndjson-streamer.js"
import { setupCollectors, stopCollectors } from "./init-collectors.js"
import { getCommandLogs } from "./handlers/commandlog-handler.js"
import { monitorHandler, useMonitor } from "./handlers/monitor-handler.js"
import { calculateHotKeysFromHotSlots } from "./analyzers/calculate-hot-keys.js"
import { enrichHotKeys } from "./analyzers/enrich-hot-keys.js"
import cpuFold from "./analyzers/calculate-cpu-usage.js"
import memoryFold from "./analyzers/memory-metrics.js"
import { cpuQuerySchema, memoryQuerySchema, parseQuery } from "./api-schema.js"
import { sanitizeUrl } from "./utils/helpers.js"

export const checkValkeyPing = ({ host = "localhost", port = 6379, useTLS = false }) =>
  new Promise((resolve) => {
    let socket

    try {
      if (useTLS) {
        socket = tls.connect({ host, port, rejectUnauthorized: false }, () => {
          try {
            socket.write("*1\r\n$4\r\nPING\r\n")
          } catch {
            resolve(false)
            socket.destroy()
          }
        })
      } else {
        socket = new net.Socket()
        socket.connect(port, host, () => {
          try {
            socket.write("*1\r\n$4\r\nPING\r\n")
          } catch {
            resolve(false)
            socket.destroy()
          }
        })
      }

      socket.on("data", (data) => {
        socket.destroy()
        const str = data.toString()
        resolve(str.includes("PONG") || str.includes("NOAUTH"))
      })

      socket.on("error", () => resolve(false))
      socket.on("timeout", () => {
        socket.destroy()
        resolve(false)
      })

    } catch {
      resolve(false)
    }
  })

export const waitForValkey = async (
  { host, port, useTLS },
  { retries = 30, delayMs = 1000 } = {},
) => {
  const attempt$ = defer(() =>
    checkValkeyPing({ host, port, useTLS }),
  ).pipe(
    map((isUp) => {
      if (!isUp) {
        throw new Error("Valkey not up")
      }
      return true
    }),
    retry({
      count: retries - 1,
      delay: () => timer(delayMs),
    }),
  )

  try {
    await firstValueFrom(attempt$)
    return true
  } catch {
    return false
  }
}

async function main() {
  const cfg = getConfig()
  const ensureDir = (dir) => { if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true }) }
  ensureDir(cfg.server.data_dir)

  const addresses = [
    {
      host: process.env.VALKEY_HOST,
      port: Number(process.env.VALKEY_PORT),
    },
  ]
  const credentials =
    process.env.VALKEY_PASSWORD ? {
      username: process.env.VALKEY_USERNAME,
      password: process.env.VALKEY_PASSWORD,
    } : undefined

  const useTLS = process.env.VALKEY_TLS === "true"
  const isReady = await waitForValkey({ host: addresses[0].host, port: addresses[0].port, useTLS }) 
  if (!isReady) {
    console.error("Valkey is not reachable")
    process.exit(1)
  }
  const client = await GlideClient.createClient({
    addresses,
    credentials,
    useTLS,
    ...(useTLS && process.env.VALKEY_VERIFY_CERT === "false" && {
      advancedConfiguration: {
        tlsAdvancedConfiguration: {
          insecure: true,
        },
      },
    }),

    requestTimeout: 5000,
    clientName: "test_client",
  })
  const ownConnectionId = sanitizeUrl(`${process.env.VALKEY_HOST}-${process.env.VALKEY_PORT}`)

  await setupCollectors(client, cfg)

  const app = express()
  app.use(express.json())

  // public API goes here:
  app.get("/health", (req, res) => res.json({ ok: true }))

  app.get("/memory", async (req, res) => {
    try {
      const { maxPoints, since, until } = parseQuery(memoryQuerySchema)(req.query)
      const series = await Streamer.memory_stats(memoryFold({ maxPoints, since, until }))
      res.json(series)
    } catch (e) {
      console.log(e)
      res.status(500).json({ error: e.message })
    }
  })

  app.get("/cpu", async (req, res) => {
    try {
      const { maxPoints, tolerance, since, until } = parseQuery(cpuQuerySchema)(req.query)
      const series = await Streamer.info_cpu(cpuFold({ maxPoints, tolerance, since, until }))
      res.json(series)
    } catch (e) {
      res.status(500).json({ error: e.message })
    }
  })

  app.get("/commandlog", getCommandLogs)

  app.get("/slowlog_len", async (req, res) => {
    try {
      const rows = await Streamer.slowlog_len()
      res.json({ rows })
    } catch (e) {
      res.status(500).json({ error: e.message })
    }
  })

  app.get("/monitor", async (req, res) => {
    const result = await monitorHandler(req.query.action)
    return res.json(result)
  })

  app.get("/hot-keys", async (req, res) => {
    if (req.query.useHotSlots === "true") {
      const hotKeys = await calculateHotKeysFromHotSlots(client, req.query.count).then(enrichHotKeys(client))
      return res.json({ hotKeys })
    }
    else useMonitor(req, res, cfg, client)
  })

  app.post("/update-config", async (req, res) => {
    try {
      const result = updateConfig(req.body)
      return res.status(result.statusCode).json(result)
    }
    catch (error) {
      return res.status(500).json({
        success: false,
        message: error instanceof Error ? error.message : String(error),
        data: error,
      })
    }
  })

  app.post("/connection/close", async (req, res) => {
    try {
      const { connectionId } = req.body
      client.close()
      if (connectionId !== ownConnectionId) {
        return res.status(400).json({
          ok: false,
          error: "Invalid connectionId",
        })
      }
      res.status(200).json({
        ok: true,
        connectionId,
      })
      setImmediate(shutdown)
    } catch (err) {
      console.log("Error is ", err)
      return res.status(500).json({
        ok: false,
        err,
      })
    }
  })

  // Setting port to 0 means Express will dynamically find a port
  const port = Number(cfg.server.port || 0)
  const backendServerHost = process.env.SERVER_HOST ?? "localhost"
  const backendServerPort = process.env.SERVER_PORT ?? "8080"
  const server = app.listen(port, async () => {
    const assignedPort = server.address().port
    console.log(`listening on http://0.0.0.0:${assignedPort}`)
    try {
      const registerURI = `http://${backendServerHost}:${backendServerPort}/orchestrator/register`
      console.debug("Sending Register request to ", registerURI)
      const response = await fetch(registerURI,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            metricsServerUri: `http://0.0.0.0:${assignedPort}`,
            pid: process.pid,
            nodeId: ownConnectionId,
          }),
        },
      )

      const text = await response.text()

      if (!response.ok) {
        console.error("Register failed:", response.status, text)
      } else {
        console.log("Register success:", text)
      }
    } catch (err) {
      console.error("Register request failed:", err)
    }
    process.send?.({ type: "metrics-started", payload: { metricsHost: "http://0.0.0.0", metricsPort: assignedPort } })
    const pingIntervalMs = cfg.backend.ping_interval
    setInterval(async () => {
      try {
        const response = await fetch(`http://${backendServerHost}:${backendServerPort}/orchestrator/ping`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ nodeId: ownConnectionId }),
        })

        if (!response.ok) {
          const text = await response.text()
          console.debug("Ping failed:", response.status, text)
        } else {
          console.debug(`Ping successful for node: ${ownConnectionId}`)
        }
      } catch (err) {
        console.debug("Ping request error:", err)
      }
    }, pingIntervalMs)
  })

  const shutdown = async () => {
    console.log("shutting down")
    try {
      await stopCollectors()
      if (client) {
        client.close()
      }
      server.close(() => process.exit(0))
    } catch (e) {
      console.error("shutdown error", e)
      process.exit(1)
    }
  }

  process.on("SIGINT", shutdown)
  process.on("SIGTERM", shutdown)
}
main()
