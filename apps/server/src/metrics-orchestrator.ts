import { GlideClient, ConnectionError } from "@valkey/valkey-glide"
import { ChildProcess, spawn } from "child_process"
import { fileURLToPath } from "url"
import { Router, type Request, type Response } from "express"
import path from "path"
import { discoverCluster } from "./connection"
import { ConnectionDetails } from "./actions/connection"
import { clients } from "./index"

// Assumes nodeId is unique among all clusters
export type MetricsServerMap = Map<string,
  {
    metricsURI: string;
    pid: number | undefined;
    lastSeen: string;
  }
>

type ClusterNodeInfo = {
  host: string;
  port: number | string;
  username?: string;
  password?: string;          
  tls: boolean;
  verifyTlsCertificate: boolean;
  replicas?: { id: string; host: string; port: number }[];
}

type ClusterNodeMap = Record<string, ClusterNodeInfo>;

interface ClusterRegistry {
  [clusterId: string]: ClusterNodeMap
}

export const clusterNodesRegistry: ClusterRegistry = {}

export const metricsServerMap: MetricsServerMap = new Map()

export const initialConnectionDetails: ConnectionDetails = {
  host: process.env.VALKEY_HOST ?? "",
  port: process.env.VALKEY_PORT ?? "",
  username: process.env.VALKEY_USERNAME,
  password: process.env.VALKEY_PASSWORD,
  tls: process.env.VALKEY_TLS === "true",
  verifyTlsCertificate: process.env.VALKEY_VERIFY_CERT === "true",
}

const ttl = Number(process.env.TTL) || 20000

export function createMetricsOrchestratorRouter() {
  const router = Router()

  router.post("/register", (req: Request, res: Response) => {
    const { metricsServerUri, nodeId, pid } = req.body

    const nodeBelongsToCluster = Object.values(clusterNodesRegistry).some((clusterNodes) => nodeId in clusterNodes)
    const nodeConnected = clients.has(nodeId)

    if (nodeBelongsToCluster || nodeConnected)  {
      const now = Date.now().toString()  
      const entry = metricsServerMap.get(nodeId)
      // If we spawned the metrics process using the orchestrator
      if (entry) {
        entry.metricsURI = metricsServerUri 
        entry.lastSeen = now
        res.send(`Registered node: ${nodeId}`)
      }
      // If the metrics process was spawned using Electron
      else {
        metricsServerMap.set(nodeId, {
          metricsURI: metricsServerUri,
          pid: Number(pid),
          lastSeen: now,
        })
        res.send(`Registered node: ${nodeId}`)
      }
    }   
    else {
      res.status(404).send("Invalid nodeId")
    }
  })

  router.post("/ping", async (req: Request, res: Response) => {
    const { nodeId } = req.body
    const entry = metricsServerMap.get(nodeId)
    if (entry) {
      entry.lastSeen = Date.now().toString()
      res.send(`Health ping received for node: ${nodeId}`)
    }
    else {
      res.status(404).send("Node not found")
    }
  })
  return router
}

async function connectToInitialValkeyNode(connectionDetails: ConnectionDetails) {
  const { host, port, username, password, tls, verifyTlsCertificate } = connectionDetails
  const addresses = [
    { host, port: Number(port) },
  ]
  const credentials =
    password ? {
      username,
      password,
    } : undefined

  const client = await GlideClient.createClient({
    addresses,
    credentials,
    useTLS: tls,
    ...(tls && !verifyTlsCertificate && {
      advancedConfiguration: {
        tlsAdvancedConfiguration: {
          insecure: true,
        },
      },
    }),
    requestTimeout: 5000,
    clientName: "test_client",
  })

  return client
}

async function getClusterTopology(client: GlideClient, node: ConnectionDetails) {
  if (!client) client = await connectToInitialValkeyNode(node)
  const { clusterNodes, clusterId }  = await discoverCluster(client, { connectionDetails: node })
  return { clusterNodes, clusterId }
}

async function updateClusterNodeRegistry(clusterId: string) {
  for (const node of Object.values(clusterNodesRegistry[clusterId])) {
    const nodeConnectionDetails = { ...node, port: String(node.port) }
    try {
      const client = await connectToInitialValkeyNode(nodeConnectionDetails)
      const { clusterNodes, clusterId } = await getClusterTopology(client, nodeConnectionDetails)
      if (clusterId) clusterNodesRegistry[clusterId] = clusterNodes 
    }
    catch (err) {
      if (err instanceof ConnectionError) {
        console.warn("There was an error discovering cluster nodes. Attempting to connect to another node")
        continue
      }
      console.error(err)
    }
  }
  return clusterNodesRegistry
}

async function findDiff(metricsServerMap: MetricsServerMap, clusterNodeMap: ClusterNodeMap) {
  // These are nodes that are in the clusterMap but not metricsMap
  const nodesToAdd: ClusterNodeMap = Object.fromEntries(
    Object.entries(clusterNodeMap)
      .filter(([key]) => !metricsServerMap.has(key)),
  )
  const now = Date.now()
  // These are nodes that are in the metricsMap but not in clusterMap or stale nodes
  const nodesToRemove: string[] = Array.from(metricsServerMap.entries())
    .filter(([key, value]) => {
      return !clusterNodeMap[key] || now - Number(value.lastSeen) > ttl
    })
    .map(([key]) => key)

  return { nodesToAdd, nodesToRemove }
}

async function updateMetricsServers(nodesToAdd: ClusterNodeMap, nodesToRemove: string[]) {
  await startMetricsServers(nodesToAdd)
  await stopMetricsServers(nodesToRemove)
}

async function startMetricsServers(nodesToStart: ClusterNodeMap) {
  await Promise.all(
    Object.entries(nodesToStart).map(async ([key, value]) => {
      if (!metricsServerMap.has(key)) {
        await startMetricsServer(value, key)
      }
    }),
  )
}

async function stopMetricsServers(nodesToStop: string[]) {
  await Promise.all(
    nodesToStop.map(async (node) => {
      if (metricsServerMap.has(node)) {
        await stopMetricsServer(node)
      }  
    }),
  )
}

export async function stopAllMetricsServers(metricsMap: MetricsServerMap) {
  metricsMap.forEach((metricsServer, nodeId) => {
    try {
      if (metricsServer.pid)
        process.kill(metricsServer.pid)
    } catch (e) {
      console.warn(`Failed to kill metrics server ${nodeId}:`, e)
    }
  })
  metricsMap.clear()
}

export async function startMetricsServer(nodeToStart: ClusterNodeInfo, nodeId: string) {
  const isElectron = process.env.ELECTRON_APP === "true"
  const processResourcesPath = process.env.PROCESS_RESOURCES_PATH  ?? ""
  const metricsServerPath = isElectron
    ? path.join(processResourcesPath, "server-metrics.js")
    : fileURLToPath(new URL("../../metrics/dist/index.cjs", import.meta.url))

  const configPath = isElectron
    ? path.join(processResourcesPath, "config.yml")
    : fileURLToPath(new URL("../../metrics/config.yml", import.meta.url))

  console.log("Starting metrics server for: ", nodeId)
  const proc: ChildProcess = spawn(process.execPath, [metricsServerPath], {
    env: {
      ...process.env,
      PORT: "0",
      VALKEY_HOST: nodeToStart.host,
      VALKEY_PORT: String(nodeToStart.port),
      VALKEY_USERNAME: nodeToStart.username,
      VALKEY_PASSWORD: nodeToStart.password,
      VALKEY_TLS: String(nodeToStart.tls),
      VALKEY_VERIFY_CERT: String(nodeToStart.verifyTlsCertificate),
      DATA_DIR: `${process.env.DATA_DIR}/${nodeId}`,
      CONFIG_PATH: configPath,
    },
    stdio: "inherit",//["ignore", "ignore", "ignore"],
  })
  proc.on("exit", (code, signal) => {
    if (code !== 0) {
      console.warn(`Metrics server for ${nodeToStart.host}:${nodeToStart.port} exited with code ${code} and signal ${signal}`)
    }
  })

  proc.on("error", (err) => {
    console.error(`Failed to start metrics server for ${nodeToStart.host}:${nodeToStart.port}:`, err)
  })

  // Don't need to set metricsURI here since we need to wait for server to register itself
  metricsServerMap.set(nodeId,
    {
      metricsURI: "",
      pid: proc.pid,
      lastSeen: Date.now().toString(),
    },
  )
}

async function stopMetricsServer(nodeToStop: string) {
  try {
    console.log("Killing metrics server for ", nodeToStop)
    const entry = metricsServerMap.get(nodeToStop)
    if (entry?.pid) {
      process?.kill(entry.pid,"SIGTERM")
      metricsServerMap.delete(nodeToStop)
    }
  }
  catch (e) {
    console.warn(`Failed to kill metrics server for ${nodeToStop}:`, e)
  }
}

export async function reconcileClusterMetricsServers(
  clusterNodesRegistry: ClusterRegistry, 
  metricsServerMap: MetricsServerMap, 
  connectionDetails: ConnectionDetails, 
) {
  let clusterIds = Object.keys(clusterNodesRegistry) 
  if (clusterIds.length === 0) {
    try {
      const client = await connectToInitialValkeyNode(connectionDetails)
      const { clusterNodes, clusterId } = await getClusterTopology(client, connectionDetails)
      if (clusterId) clusterNodesRegistry[clusterId] = clusterNodes 
      clusterIds = Object.keys(clusterNodesRegistry)
    } catch (err) {
      console.error(err)
    }
  }
  await Promise.all(
    clusterIds.map(async (clusterId) => {
      try {
        const updatedClusterNodeRegistry = await updateClusterNodeRegistry(clusterId)
        const { nodesToAdd, nodesToRemove } = await findDiff(metricsServerMap, updatedClusterNodeRegistry[clusterId])
        // Early return if nothing has changed
        if (Object.keys(nodesToAdd).length === 0 && nodesToRemove.length === 0) {
          console.debug("Cluster nodes and metrics servers are in sync")
          return
        }
        await updateMetricsServers(nodesToAdd, nodesToRemove)
      } catch (err) {
        console.error(`Failed to reconcile metrics servers for cluster ${clusterId}:`, err)
      }
    }),
  )
}
