import fs from "node:fs"
import readline from "node:readline"
import path from "node:path"
import { COMMANDLOG_LARGE_REPLY, COMMANDLOG_LARGE_REQUEST, COMMANDLOG_SLOW, MONITOR } from "../utils/constants.js"

const DATA_DIR = process.env.DATA_DIR || path.resolve(process.cwd(), "data")

const dayStr = (date) => date.toISOString().slice(0, 10).replace(/-/g, "")

const parseFile = (f) => {
  const m = f.match(/_(\d{8})(?:_(\d+))?\.ndjson$/)
  return m ? { date: Number(m[1]), seq: Number(m[2] ?? 0) } : null
}

const filesFor = async (prefix, dates) => {
  return (await fs.promises.readdir(DATA_DIR))
    // filter by date and prefix
    .filter((file) => dates.some((date) => file.startsWith(`${prefix}_${dayStr(date)}`)))
    // sort by date then sequence number
    .sort((a, b) => {
      const pa = parseFile(a)
      const pb = parseFile(b)
      if (!pa || !pb) return 0
      return pa.date - pb.date || pa.seq - pb.seq
    })
    .map((file) => path.join(DATA_DIR, file))
}

// streamNdjson is a transducer-inspired streaming fold, which means you can apply filter, map, reduce to the stream
// without creating intermediate arrays, so it's faster and more memory-efficient than chaining these functions.
// I.e. if you need to apply transformations to the stream you're reading — supply corresponding functions as arguments
// instead of chaining calls like (await streamNdjson).filter.map.reduce
// If you don't supply filter, map, reduce — the default behavior is to return an array of objects (see default args).
//
// If you pass { reducer, seed }, it will fold matching objects into an accumulator.
// The `finalize` function runs after reduction to flush the last timestamp bucket.
// Without it, the final delta cannot be computed as it requires comparing the last bucket against the one before last.
export async function streamNdjson(
  prefix,
  {
    filterFn = () => true,
    finalize = (acc) => acc,
    limit = Infinity,
    mapFn,
    reducer = (acc, curr) => { acc.push(curr); return acc },
    seed = [],
  } = {},
) {
  const today = new Date()
  const yesterday = new Date(today)
  yesterday.setDate(today.getDate() - 1)

  const files = await filesFor(prefix, [yesterday, today])

  let acc = seed
  let count = 0

  for (const file of files) {
    let fileStream
    let rl

    try {
      fileStream = fs.createReadStream(file, { encoding: "utf8" })
      rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity })

      for await (const line of rl) {
        if (count >= limit) {
          break
        }

        if (!line.trim()) continue

        try {
          const obj = JSON.parse(line)
          if (!filterFn(obj)) continue

          acc = reducer(acc, mapFn ? mapFn(obj) : obj)
          count++
        } catch {
          // ignore bad lines
        }
      }
    } finally {
      if (rl) rl.close()
      if (fileStream) fileStream.destroy()
    }
  }

  return finalize(acc)
}

export const [memory_stats, info_cpu, slowlog_len, commandlog_slow, commandlog_large_reply, commandlog_large_request, monitor] =
  ["memory", "cpu", "slowlog_len", COMMANDLOG_SLOW, COMMANDLOG_LARGE_REPLY, COMMANDLOG_LARGE_REQUEST, MONITOR]
    .map((filePrefix) => (options = {}) => streamNdjson(filePrefix, options))
