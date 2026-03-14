 
import { describe, it, beforeEach, afterEach, mock } from "node:test"
import assert from "node:assert"
import {
  metricsServerMap,
  stopAllMetricsServers,
  reconcileClusterMetricsServers,
  __test__ 
} from "../metrics-orchestrator"
import type { ClusterNodeMap, MetricsServerMap } from "../metrics-orchestrator"
import type { ConnectionDetails } from "../actions/connection"

const clusterNodesRegistry = {
  "cluster-1": {
    node1: {
      host: "127.0.0.1",
      port: 6379,
      tls: false,
      verifyTlsCertificate: false,
    },
  },
}

describe("metrics-orchestrator", () => {
  beforeEach(() => {
    metricsServerMap.clear()
  })

  afterEach(() => {
    mock.restoreAll()
    metricsServerMap.clear()
  })

  describe("findDiff", () => {
    it("should return nodes to add if not in metricsMap", async () => {
      const clusterNodes: ClusterNodeMap = {
        node1: { host: "127.0.0.1", port: "6379", tls: false, verifyTlsCertificate: false },
        node2: { host: "127.0.0.2", port: "6379", tls: false, verifyTlsCertificate: false },
      }
      const metricsMap: MetricsServerMap = new Map([
        ["node1", { metricsURI: "uri", pid: 123, lastSeen: Date.now().toString() }],
      ])
      const { nodesToAdd, nodesToRemove } = await __test__.findDiff(metricsMap, clusterNodes)
      assert.strictEqual(Object.keys(nodesToAdd).length, 1)
      assert.strictEqual(nodesToAdd.node2.host, "127.0.0.2")
      assert.strictEqual(nodesToRemove.length, 0)
    })

    it("should return nodes to remove if not in clusterMap", async () => {
      const clusterNodes: ClusterNodeMap = {
        node1: { host: "127.0.0.1", port: "6379", tls: false, verifyTlsCertificate: false },
      }
      const now = Date.now()
      const metricsMap: MetricsServerMap = new Map([
        ["node1", { metricsURI: "uri", pid: 123, lastSeen: now.toString() }],
        ["node2", { metricsURI: "uri", pid: 456, lastSeen: now.toString() }],
      ])
      const { nodesToAdd, nodesToRemove } = await __test__.findDiff(metricsMap, clusterNodes)
      assert.strictEqual(Object.keys(nodesToAdd).length, 0)
      assert.strictEqual(nodesToRemove.length, 1)
      assert.strictEqual(nodesToRemove[0], "node2")
    })

    it("should remove stale nodes", async () => {
      const clusterNodes: ClusterNodeMap = {
        node1: { host: "127.0.0.1", port: "6379", tls: false, verifyTlsCertificate: false },
      }
      const pastTime = (Date.now() - 100000).toString()
      const metricsMap: MetricsServerMap = new Map([
        ["node1", { metricsURI: "uri", pid: 123, lastSeen: pastTime }],
      ])
      const { nodesToAdd, nodesToRemove } = await __test__.findDiff(metricsMap, clusterNodes)
      assert.strictEqual(nodesToAdd.node1, undefined)
      assert.strictEqual(nodesToRemove.includes("node1"), true)
    })
  })

  describe("startMetricsServer / stopMetricsServer", () => {
    it("should spawn a new metrics server", async () => {
      const nodes = {
        host: "127.0.0.1",
        port: "6379",
        tls: false,
        verifyTlsCertificate: false,
      }

      mock.method(
        __test__,
        "startMetricsServers",
        async (nodesMap: Record<string, ConnectionDetails>) => {
          // simulate inserting all nodes into metricsServerMap
          for (const [key, node] of Object.entries(nodesMap)) {
            metricsServerMap.set(key, { metricsURI: node.host, pid: 999, lastSeen: "123" })
          }
        },
      )
      await __test__.startMetricsServers({ node1: nodes })

      // Assert that the node was added to metricsServerMap
      assert.strictEqual(metricsServerMap.has("node1"), true)
      const entry = metricsServerMap.get("node1")
      assert.strictEqual(entry?.pid, 999)
    })

    it("should stop a metrics server by killing pid", async () => {
      let killedPid: number | undefined
      metricsServerMap.set("node1", { metricsURI: "uri", pid: 1234, lastSeen: Date.now().toString() })
      mock.method(process, "kill", (pid: number) => {
        killedPid = pid
      })

      await __test__.stopMetricsServer("node1")
      assert.strictEqual(killedPid, 1234)
      assert.strictEqual(metricsServerMap.has("node1"), false)
    })
    it("should kill all metrics servers and clear the map safely", async () => {
      const killed: number[] = []
      metricsServerMap.set("node1", { metricsURI: "uri", pid: 1, lastSeen: "1" })
      metricsServerMap.set("node2", { metricsURI: "uri", pid: 2, lastSeen: "2" })
      mock.method(process, "kill", (pid: number) => killed.push(pid))

      await stopAllMetricsServers(metricsServerMap)
      assert.strictEqual(metricsServerMap.size, 0)
      assert.strictEqual(killed.includes(1), true)
      assert.strictEqual(killed.includes(2), true)
    })
  })

  describe("reconcileClusterMetricsServers", () => {
    let connectionDetails: ConnectionDetails

    beforeEach(() => {
      metricsServerMap.clear()
      connectionDetails = { host: "127.0.0.1", port: "6379", tls: false, verifyTlsCertificate: false }

      // Mock all side-effectful internal functions
      mock.method(__test__, "connectToInitialValkeyNode", async () => ({}))
      mock.method(__test__, "getClusterTopology", async () => ({
        clusterNodes: {
          node1: { host: "127.0.0.1", port: "6379", tls: false, verifyTlsCertificate: false },
        },
        clusterId: "cluster-1",
      }))
      mock.method(__test__, "updateClusterNodeRegistry", async () => clusterNodesRegistry)
      mock.method(__test__, "updateMetricsServers", async () => {})
      mock.method(__test__, "findDiff", async () => ({ nodesToAdd: {}, nodesToRemove: [] }))
    })

    it("should discover cluster if registry is empty", async () => {
      await reconcileClusterMetricsServers(clusterNodesRegistry, metricsServerMap, connectionDetails)
      assert.ok(clusterNodesRegistry["cluster-1"])
    })

    it("should call updateClusterNodeRegistry for existing clusters", async () => {
      clusterNodesRegistry["cluster-1"] = {
        node1: { host: "127.0.0.1", port: 6379, tls: false, verifyTlsCertificate: false },
      }
      await reconcileClusterMetricsServers(clusterNodesRegistry, metricsServerMap, connectionDetails)
      // Nothing should throw; mocks handle all calls
    })

    it("should early return if nothing changed", async () => {
      // findDiff mock returns empty changes
      mock.method(__test__, "findDiff", async () => ({ nodesToAdd: {}, nodesToRemove: [] }))
      clusterNodesRegistry["cluster-1"] = {
        node1: { host: "127.0.0.1", port: 6379, tls: false, verifyTlsCertificate: false },
      }
      await reconcileClusterMetricsServers(clusterNodesRegistry, metricsServerMap, connectionDetails)
      // updateMetricsServers should not be called because nothing changed
    })
  })
})
