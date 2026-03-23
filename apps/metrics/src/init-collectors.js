import { makeFetcher } from "./effects/fetchers.js"
import { makeMonitorStream } from "./effects/monitor-stream.js"
import { makeNdjsonWriter } from "./effects/ndjson-writer.js"
import { startCollector } from "./epics/collector-rx.js"
import { MONITOR } from "./utils/constants.js"

/*
  State per collector with shape:
  {
    isRunning: boolean,
    lastUpdatedAt: timestamp,
    nextCycleAt: timestamp, // Calculated only for collectors, not the monitor as the monitor is controlled manually
  }
*/
const collectorsState = {}

const updateCollectorMeta = (name, patch) => {
  const prev = collectorsState[name] || {}
  const next = { ...prev, ...patch }
  collectorsState[name] = next
  return next
}

// Metric files capacity weights per metric
const CAPACITY_WEIGHTS = {
  memory: 0.30,
  monitor: 0.25,
  commandlog_slow: 0.10,
  commandlog_large_reply: 0.10,
  commandlog_large_request: 0.10,
  cpu: 0.10,
  slowlog_len: 0.05,
}

const MIN_FILE_SIZE = 256 * 1024         // 256 KB
const MAX_FILE_SIZE = 10 * 1024 * 1024  // 10 MB
const MIN_FILES = 4

const computeCapacity = (weight, totalMb) => {
  const capacityBytes = weight * totalMb * 1024 * 1024
  const maxFileSize = Math.min(MAX_FILE_SIZE, Math.max(MIN_FILE_SIZE, Math.floor(capacityBytes / MIN_FILES)))
  const maxFiles = Math.max(MIN_FILES, Math.floor(capacityBytes / maxFileSize))
  return { maxFiles, maxFileSize }
}

const warnIfStorageTooSmall = (retentionSizeMb, weights) => {
  const smallestWeight = Math.min(...Object.values(weights))
  const minTotalBytes = (MIN_FILE_SIZE * MIN_FILES) / smallestWeight
  const minTotalMb = Math.ceil(minTotalBytes / (1024 * 1024))
  if (retentionSizeMb < minTotalMb) {
    console.warn(
      `retention_size_mb ${retentionSizeMb} is below minimum ${minTotalMb} for active epics — ` +
      `per-metric capacity will be clamped to at least ${MIN_FILES} files × ${MIN_FILE_SIZE / 1024} KB`,
    )
  }
}

const normalizeWeights = (epics) => {
  const activeEpicPrefixes = epics.map((e) => e.file_prefix || e.name)
  const rawTotal = activeEpicPrefixes.reduce((sum, epicPrefix) => sum + (CAPACITY_WEIGHTS[epicPrefix] ?? 0), 0)
  if (rawTotal === 0) {
    const equal = 1 / activeEpicPrefixes.length
    return Object.fromEntries(activeEpicPrefixes.map((p) => [p, equal]))
  }
  return Object.fromEntries(activeEpicPrefixes.map((p) => [p, (CAPACITY_WEIGHTS[p] ?? 0) / rawTotal]))
}

// Use it in endpoints to return metadata to server then to UI
//  to show when the data was collected and will be refreshed
export const getCollectorMeta = (name) => collectorsState[name]
const collectorStoppers = {}
let monitorStopper

updateCollectorMeta(MONITOR, {
  isRunning: false,
})
const startMonitor = (cfg) => {
  const weights = normalizeWeights(cfg.epics)
  warnIfStorageTooSmall(cfg.storage.retention_size_mb, weights)
  const { maxFiles, maxFileSize } = computeCapacity(weights[MONITOR], cfg.storage.retention_size_mb)
  const nd = makeNdjsonWriter({
    dataDir: cfg.server.data_dir,
    filePrefix: MONITOR,
    maxFiles,
    maxFileSize,
  })

  const monitorEpic = cfg.epics.find((e) => e.name === MONITOR)

  const sink = {
    appendRows: async (rows) => {
      await nd.appendRows(rows)
      console.debug(`[${monitorEpic.name}] wrote ${rows.length} logs to ${cfg.server.data_dir}/`)
    },
    close: nd.close,
  }

  updateCollectorMeta(monitorEpic.name, {
    isRunning: true,
    startedAt: Date.now(),
    willCompleteAt: Date.now() + monitorEpic.monitoringDuration,
  })

  const stream$ = makeMonitorStream(async (logs) => {
    await sink.appendRows(logs)
  }, monitorEpic)

  const subscription = stream$.subscribe({
    next: (logs) => {
      updateCollectorMeta(monitorEpic.name, {
        lastUpdatedAt: Date.now(),
      })
      console.debug(`[${monitorEpic.name}] monitor cycle complete (${logs.length} logs)`)
    },
    error: (err) => {
      updateCollectorMeta(monitorEpic.name, {
        isRunning: false,
        lastErrorAt: Date.now(),
        lastError: String(err),
        willCompleteAt: null,
      })
      console.error(`[${monitorEpic.name}] monitor error:`, err)
    },
    complete: () => {
      updateCollectorMeta(monitorEpic.name, {
        completedAt: Date.now(),
        isRunning: false,
      })
      console.debug(`[${monitorEpic.name}] monitor completed`)
    },
  })

  monitorStopper = async () => {
    console.debug(`[${monitorEpic.name}] stopping monitor...`)
    updateCollectorMeta(monitorEpic.name, {
      stoppedAt: Date.now(),
      isRunning: false,
      willCompleteAt: null,
    })
    subscription.unsubscribe()
    await sink.close()
  }
}

const stopMonitor = async () => await monitorStopper()

const setupCollectors = async (client, cfg) => {
  const weights = normalizeWeights(cfg.epics)
  warnIfStorageTooSmall(cfg.storage.retention_size_mb, weights)
  const fetcher = makeFetcher(client)
  await Promise.all(cfg.epics
    .filter((f) => f.name !== MONITOR && fetcher[f.type])
    .map(async (f) => {
      const fn = fetcher[f.type]
      const prefix = f.file_prefix || f.name
      const { maxFiles, maxFileSize } = computeCapacity(weights[prefix], cfg.storage.retention_size_mb)
      const nd = makeNdjsonWriter({
        dataDir: cfg.server.data_dir,
        filePrefix: prefix,
        maxFiles,
        maxFileSize,
      })

      updateCollectorMeta(f.name, {
        isRunning: true,
        lastUpdatedAt: null,
        nextCycleAt: Date.now() + f.poll_ms,
        startedAt: Date.now(),
      })

      const sink = {
        appendRows: async (rows) => {
          await nd.appendRows(rows)
          updateCollectorMeta(f.name, {
            nextCycleAt: Date.now() + f.poll_ms,
            lastUpdatedAt: Date.now(),
          })
        },
        close: () => {
          updateCollectorMeta(f.name, {
            isRunning: false,
            nextCycleAt: null,
            stoppedAt: Date.now(),
          })
          nd.close()
        },
      }

      const rows = await fn()

      await sink.appendRows(rows)
      collectorStoppers[f.name] = startCollector({
        name: f.name,
        pollMs: f.poll_ms,
        fetch: fn,
        writer: sink,
        batchMs: cfg.collector.batch_ms,
        batchMax: cfg.collector.batch_max,
      })
    }),
  )
}

const stopCollectors = async () => {
  const stopperFuncs = Object.values(collectorStoppers).filter(Boolean)
  await Promise.all(stopperFuncs.map((stop) => stop()))
}

export { setupCollectors, startMonitor, stopMonitor, stopCollectors }
