import fs from "node:fs"
import path from "node:path"
import * as R from "ramda"

const dayStr = (ts) => new Date(ts).toISOString().slice(0, 10).replace(/-/g, "")

const discoverMaxSeq = async (dataDir, filePrefix, day) => {
  const fileNames = await fs.promises.readdir(dataDir).catch(() => [])
  let maxSeq = 0
  const prefix = `${filePrefix}_${day}_`
  for (const name of fileNames) {
    if (!name.startsWith(prefix) || !name.endsWith(".ndjson")) continue
    const match = name.match(/_(\d+)\.ndjson$/)
    if (match) maxSeq = Math.max(maxSeq, Number(match[1]))
  }
  return maxSeq
}

const listPrefixFiles = async (dataDir, filePrefix) => {
  const fileNames = await fs.promises.readdir(dataDir).catch(() => [])
  return fileNames
    .filter((f) => f.startsWith(`${filePrefix}_`) && f.endsWith(".ndjson"))
    .sort()
}

export const makeNdjsonWriter = ({ dataDir, filePrefix, maxFiles, maxFileSize }) => {
  let prevDay
  let seq

  const fileFor = async (ts) => {
    const day = dayStr(ts)
    if (day !== prevDay) {
      prevDay = day
      seq = await discoverMaxSeq(dataDir, filePrefix, day)
    }

    const currentFile = path.join(dataDir, `${filePrefix}_${day}_${seq}.ndjson`)
    const currentSize = await fs.promises.stat(currentFile).then((s) => s.size).catch(() => 0)
    if (currentSize >= maxFileSize) {
      await advanceSeq()
      return { file: path.join(dataDir, `${filePrefix}_${day}_${seq}.ndjson`), size: 0 }
    }

    return { file: currentFile, size: currentSize }
  }

  const rotate = async () => {
    const files = await listPrefixFiles(dataDir, filePrefix)
    let toDelete = files.length - maxFiles + 1 // +1 to make room for the new file
    for (const f of files) {
      if (toDelete <= 0) break
      await fs.promises.unlink(path.join(dataDir, f)).catch(() => {})
      toDelete--
    }
  }

  const advanceSeq = async () => {
    const prefixFiles = await listPrefixFiles(dataDir, filePrefix)
    if (prefixFiles.length >= maxFiles) {
      await rotate()
    }
    seq++
  }

  const appendRows = async (rows = []) => {
    if (R.isEmpty(rows)) return

    const ts = Number.isFinite(rows[0]?.ts) ? rows[0].ts : Date.now()
    await fs.promises.mkdir(dataDir, { recursive: true })

    const { file, size } = await fileFor(ts)
    const budget = maxFileSize - size

    const chunk = []
    let chunkSize = 0
    for (const row of rows) {
      const line = JSON.stringify(row) + "\n"
      if (chunkSize + line.length > budget && chunk.length > 0) break
      chunk.push(line)
      chunkSize += line.length
    }

    await fs.promises.appendFile(file, chunk.join(""), "utf8")

    const overflow = rows.slice(chunk.length)
    if (overflow.length > 0) {
      await advanceSeq()
      return appendRows(overflow)
    }
  }

  const close = async () => {}

  return { appendRows, close }
}
